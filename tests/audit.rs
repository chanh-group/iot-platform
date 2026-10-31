mod common;

use std::io::{self, Write};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use common::{TestApp, TestWebSocket};
use futures_util::{SinkExt, StreamExt};
use reqwest::header::SET_COOKIE;
use rumqttc::{QoS, SubscribeReasonCode};
use serde_json::{Value, json};
use tokio_tungstenite::tungstenite::Message;

const PASSWORD: &str = "AUDIT_PASSWORD_SENTINEL";
const METRICS_TOKEN: &str = "AUDIT_METRICS_TOKEN_SENTINEL_0123456789";
const METADATA_CREATE: &str = "AUDIT_METADATA_CREATE_SENTINEL";
const METADATA_UPDATE: &str = "AUDIT_METADATA_UPDATE_SENTINEL";
const METADATA_ROLLBACK: &str = "AUDIT_METADATA_ROLLBACK_SENTINEL";
const COMMAND_PARAMS: &str = "AUDIT_COMMAND_PARAMS_SENTINEL";
const TELEMETRY_PAYLOAD: &str = "AUDIT_TELEMETRY_PAYLOAD_SENTINEL";

#[derive(Clone, Default)]
struct LogBuffer(Arc<Mutex<Vec<u8>>>);

impl LogBuffer {
    fn contents(&self) -> String {
        String::from_utf8(self.0.lock().unwrap().clone()).unwrap()
    }
}

impl Write for LogBuffer {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for LogBuffer {
    type Writer = Self;

    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}

#[derive(Debug, sqlx::FromRow)]
struct AuditRow {
    occurred_at: i64,
    actor_type: String,
    actor_id: Option<String>,
    action: String,
    target_type: Option<String>,
    target_id: Option<String>,
    request_id: Option<String>,
    remote_ip: Option<String>,
    details_json: Option<String>,
}

async fn audit_row(app: &TestApp, request_id: &str) -> AuditRow {
    sqlx::query_as(
        "SELECT occurred_at, actor_type, actor_id, action, target_type, target_id,
                request_id, remote_ip, details_json
         FROM audit_events WHERE request_id = ?",
    )
    .bind(request_id)
    .fetch_one(&app.pool)
    .await
    .unwrap()
}

fn assert_audit(
    row: &AuditRow,
    actor_id: &str,
    action: &str,
    target_type: &str,
    target_id: &str,
    request_id: &str,
    details: Value,
) {
    assert!(row.occurred_at > 0);
    assert_eq!(row.actor_type, "admin");
    assert_eq!(row.actor_id.as_deref(), Some(actor_id));
    assert_eq!(row.action, action);
    assert_eq!(row.target_type.as_deref(), Some(target_type));
    assert_eq!(row.target_id.as_deref(), Some(target_id));
    assert_eq!(row.request_id.as_deref(), Some(request_id));
    assert_eq!(row.remote_ip.as_deref(), Some("127.0.0.1"));
    assert_eq!(
        serde_json::from_str::<Value>(row.details_json.as_deref().unwrap()).unwrap(),
        details
    );
}

async fn next_ws_json(socket: &mut TestWebSocket) -> Value {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let Some(Ok(Message::Text(text))) = socket.next().await {
                return serde_json::from_str(text.as_str()).unwrap();
            }
        }
    })
    .await
    .expect("receive WebSocket event")
}

async fn next_ws_type(
    socket: &mut TestWebSocket,
    event_type: &str,
    messages: &mut Vec<Value>,
) -> Value {
    loop {
        let message = next_ws_json(socket).await;
        let matches = message["type"] == event_type;
        messages.push(message.clone());
        if matches {
            return message;
        }
    }
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

#[tokio::test]
async fn audit_context_details_transactions_and_redaction_follow_contract() {
    let logs = LogBuffer::default();
    tracing_subscriber::fmt()
        .json()
        .with_writer(logs.clone())
        .with_max_level(tracing::Level::INFO)
        .try_init()
        .expect("install test log capture");

    let app = TestApp::builder()
        .metrics_token(METRICS_TOKEN)
        .start()
        .await;
    app.create_admin("audit-admin", PASSWORD).await;
    let client = app.client();
    let login = client
        .post(format!("{}/api/v1/auth/login", app.base))
        .header("X-IoT-CSRF", "1")
        .header("X-Request-ID", "audit-login-001")
        .json(&json!({"username":"audit-admin", "password":PASSWORD}))
        .send()
        .await
        .unwrap();
    assert_eq!(login.status(), reqwest::StatusCode::OK);
    let cookie = login
        .headers()
        .get(SET_COOKIE)
        .unwrap()
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_owned();
    let actor_id: String =
        sqlx::query_scalar("SELECT id FROM users WHERE username = 'audit-admin'")
            .fetch_one(&app.pool)
            .await
            .unwrap();
    let password_hash: String = sqlx::query_scalar("SELECT password_hash FROM users WHERE id = ?")
        .bind(&actor_id)
        .fetch_one(&app.pool)
        .await
        .unwrap();

    let devices_url = format!("{}/api/v1/devices", app.base);
    let created = client
        .post(&devices_url)
        .header("X-IoT-CSRF", "1")
        .header("X-Request-ID", "audit-device-create")
        .json(&json!({
            "name":"audit-device",
            "metadata":{"private":METADATA_CREATE},
            "enabled":true
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(created.status(), reqwest::StatusCode::CREATED);
    let created: Value = created.json().await.unwrap();
    let device_id = created["device"]["id"].as_str().unwrap().to_owned();
    let initial_token = created["credential"]["token"].as_str().unwrap().to_owned();
    let initial_credential_id = created["credential"]["id"].as_str().unwrap().to_owned();
    assert!(initial_token.starts_with("iot_"));
    let initial_token_hash: Vec<u8> =
        sqlx::query_scalar("SELECT token_hash FROM device_credentials WHERE id = ?")
            .bind(&initial_credential_id)
            .fetch_one(&app.pool)
            .await
            .unwrap();

    assert_audit(
        &audit_row(&app, "audit-device-create").await,
        &actor_id,
        "device.create",
        "device",
        &device_id,
        "audit-device-create",
        json!({"name":"audit-device", "enabled":true}),
    );

    let mut socket = common::ws_connect(&app, &cookie).await;
    assert_eq!(next_ws_json(&mut socket).await["type"], "hello");
    socket
        .send(Message::Text(
            json!({"action":"subscribe", "channels":["overview", format!("device:{device_id}")]})
                .to_string()
                .into(),
        ))
        .await
        .unwrap();
    assert_eq!(next_ws_json(&mut socket).await["type"], "subscribed");
    let mut ws_messages = Vec::new();

    let update = client
        .patch(format!("{devices_url}/{device_id}"))
        .header("X-IoT-CSRF", "1")
        .header("X-Request-ID", "audit-device-update")
        .json(&json!({
            "name":"audit-device-renamed",
            "metadata":{"private":METADATA_UPDATE}
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(update.status(), reqwest::StatusCode::OK);
    assert_audit(
        &audit_row(&app, "audit-device-update").await,
        &actor_id,
        "device.update",
        "device",
        &device_id,
        "audit-device-update",
        json!({"name":true,"metadata":true}),
    );

    let no_op = client
        .patch(format!("{devices_url}/{device_id}"))
        .header("X-IoT-CSRF", "1")
        .header("X-Request-ID", "audit-device-noop")
        .json(&json!({
            "name":"audit-device-renamed",
            "metadata":{"private":METADATA_UPDATE}
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(no_op.status(), reqwest::StatusCode::OK);
    assert_audit(
        &audit_row(&app, "audit-device-noop").await,
        &actor_id,
        "device.update",
        "device",
        &device_id,
        "audit-device-noop",
        json!({"name":false,"metadata":false}),
    );

    sqlx::query(
        "CREATE TRIGGER fail_device_update_audit BEFORE INSERT ON audit_events
         WHEN NEW.action = 'device.update'
         BEGIN SELECT RAISE(FAIL, 'injected audit failure'); END",
    )
    .execute(&app.pool)
    .await
    .unwrap();
    let failed_update = client
        .patch(format!("{devices_url}/{device_id}"))
        .header("X-IoT-CSRF", "1")
        .header("X-Request-ID", "audit-device-update-fail")
        .json(&json!({"metadata":{"private":METADATA_ROLLBACK}}))
        .send()
        .await
        .unwrap();
    assert_eq!(
        failed_update.status(),
        reqwest::StatusCode::SERVICE_UNAVAILABLE
    );
    let unchanged: (String, String) =
        sqlx::query_as("SELECT name, metadata_json FROM devices WHERE id = ?")
            .bind(&device_id)
            .fetch_one(&app.pool)
            .await
            .unwrap();
    assert_eq!(unchanged.0, "audit-device-renamed");
    assert_eq!(
        serde_json::from_str::<Value>(&unchanged.1).unwrap(),
        json!({"private":METADATA_UPDATE})
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM audit_events WHERE request_id = ?")
            .bind("audit-device-update-fail")
            .fetch_one(&app.pool)
            .await
            .unwrap(),
        0
    );
    sqlx::query("DROP TRIGGER fail_device_update_audit")
        .execute(&app.pool)
        .await
        .unwrap();

    let disabled = client
        .patch(format!("{devices_url}/{device_id}"))
        .header("X-IoT-CSRF", "1")
        .header("X-Request-ID", "audit-device-disable")
        .json(&json!({"enabled":false}))
        .send()
        .await
        .unwrap();
    assert_eq!(disabled.status(), reqwest::StatusCode::OK);
    assert_audit(
        &audit_row(&app, "audit-device-disable").await,
        &actor_id,
        "device.disable",
        "device",
        &device_id,
        "audit-device-disable",
        json!({"name":false,"metadata":false,"enabled_from":true,"enabled_to":false}),
    );

    let rotated = client
        .post(format!("{devices_url}/{device_id}/credentials/rotate"))
        .header("X-IoT-CSRF", "1")
        .header("X-Request-ID", "audit-credential-rotate")
        .json(&json!({}))
        .send()
        .await
        .unwrap();
    assert_eq!(rotated.status(), reqwest::StatusCode::OK);
    let rotated: Value = rotated.json().await.unwrap();
    let rotated_token = rotated["credential"]["token"].as_str().unwrap().to_owned();
    let rotated_credential_id = rotated["credential"]["id"].as_str().unwrap().to_owned();
    let rotated_token_hash: Vec<u8> =
        sqlx::query_scalar("SELECT token_hash FROM device_credentials WHERE id = ?")
            .bind(&rotated_credential_id)
            .fetch_one(&app.pool)
            .await
            .unwrap();
    assert_audit(
        &audit_row(&app, "audit-credential-rotate").await,
        &actor_id,
        "credential.rotate",
        "device",
        &device_id,
        "audit-credential-rotate",
        json!({
            "old_credential_id":initial_credential_id,
            "new_credential_id":rotated_credential_id
        }),
    );

    let enabled = client
        .patch(format!("{devices_url}/{device_id}"))
        .header("X-IoT-CSRF", "1")
        .header("X-Request-ID", "audit-device-enable")
        .json(&json!({"enabled":true}))
        .send()
        .await
        .unwrap();
    assert_eq!(enabled.status(), reqwest::StatusCode::OK);
    assert_audit(
        &audit_row(&app, "audit-device-enable").await,
        &actor_id,
        "device.enable",
        "device",
        &device_id,
        "audit-device-enable",
        json!({"name":false,"metadata":false,"enabled_from":false,"enabled_to":true}),
    );

    let get_device: Value = client
        .get(format!("{devices_url}/{device_id}"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let list_devices: Value = client
        .get(&devices_url)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(!get_device.to_string().contains(&initial_token));
    assert!(!get_device.to_string().contains(&rotated_token));
    assert!(!get_device.to_string().contains("token_hash"));
    assert!(!list_devices.to_string().contains(&initial_token));
    assert!(!list_devices.to_string().contains(&rotated_token));
    assert!(!list_devices.to_string().contains("token_hash"));

    let (mqtt, mut eventloop, _) =
        common::mqtt_connect(app.mqtt_addr, &device_id, &device_id, &rotated_token, true).await;
    let ack_topic = format!("devices/{device_id}/telemetry/ack");
    assert_eq!(
        common::mqtt_subscribe(&mqtt, &mut eventloop, &ack_topic, QoS::AtLeastOnce).await,
        SubscribeReasonCode::Success(QoS::AtLeastOnce)
    );
    let telemetry_topic = format!("devices/{device_id}/telemetry");
    let telemetry = json!({
        "event_id":"audit-secret-sentinel-event",
        "values":{"private":TELEMETRY_PAYLOAD}
    })
    .to_string();
    common::mqtt_publish(
        &mqtt,
        &telemetry_topic,
        QoS::AtLeastOnce,
        false,
        telemetry.as_bytes(),
    )
    .await;
    common::mqtt_wait_puback(&mut eventloop, Duration::from_secs(5)).await;
    assert_eq!(
        common::mqtt_wait_telemetry_ack(&mut eventloop, &ack_topic, Duration::from_secs(5)).await["status"],
        "stored"
    );
    let telemetry_event = next_ws_type(&mut socket, "telemetry", &mut ws_messages).await;
    assert_eq!(
        telemetry_event["data"]["values"]["private"],
        TELEMETRY_PAYLOAD
    );

    let command_response = client
        .post(format!("{devices_url}/{device_id}/commands"))
        .header("X-IoT-CSRF", "1")
        .header("X-Request-ID", "audit-command-create")
        .json(&json!({
            "name":"audit.command",
            "params":{"private":COMMAND_PARAMS}
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(command_response.status(), reqwest::StatusCode::ACCEPTED);
    let command_response: Value = command_response.json().await.unwrap();
    let command_id = command_response["command_id"].as_str().unwrap().to_owned();
    assert_audit(
        &audit_row(&app, "audit-command-create").await,
        &actor_id,
        "command.create",
        "command",
        &command_id,
        "audit-command-create",
        json!({"name":"audit.command", "expires_at":command_response["expires_at"]}),
    );
    let command_event = next_ws_type(&mut socket, "command", &mut ws_messages).await;
    assert_eq!(command_event["data"]["command_id"], command_id);
    assert!(!command_event.to_string().contains(COMMAND_PARAMS));

    let config_update = client
        .patch(format!("{}/api/v1/config/runtime", app.base))
        .header("X-IoT-CSRF", "1")
        .header("X-Request-ID", "audit-config-update")
        .json(&json!({
            "expected_revision":0,
            "settings":{"logging":{"level":"debug"}}
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(config_update.status(), reqwest::StatusCode::OK);
    let config_update: Value = config_update.json().await.unwrap();
    assert_eq!(config_update["revision"], 1);
    assert_audit(
        &audit_row(&app, "audit-config-update").await,
        &actor_id,
        "config.update",
        "config",
        "runtime",
        "audit-config-update",
        json!({"old_revision":0,"new_revision":1,"paths":["logging.level"]}),
    );
    next_ws_type(&mut socket, "config", &mut ws_messages).await;

    let config: Value = client
        .get(format!("{}/api/v1/config", app.base))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let config_text = config.to_string();
    assert!(!config_text.contains(&initial_token));
    assert!(!config_text.contains(&rotated_token));
    assert!(!config_text.contains(PASSWORD));
    assert!(!config_text.contains(&password_hash));
    assert!(!config_text.contains(METRICS_TOKEN));

    socket.close(None).await.unwrap();
    let ws_text = serde_json::to_string(&ws_messages).unwrap();
    assert!(!ws_text.contains(&initial_token));
    assert!(!ws_text.contains(&rotated_token));
    assert!(!ws_text.contains(PASSWORD));
    assert!(!ws_text.contains(&password_hash));
    assert!(!ws_text.contains(COMMAND_PARAMS));

    let audit_rows: Vec<AuditRow> = sqlx::query_as(
        "SELECT occurred_at, actor_type, actor_id, action, target_type, target_id,
                request_id, remote_ip, details_json
         FROM audit_events ORDER BY id",
    )
    .fetch_all(&app.pool)
    .await
    .unwrap();
    let audit_text = format!("{audit_rows:?}");
    let logs_text = logs.contents();
    let initial_hash_hex = hex(&initial_token_hash);
    let rotated_hash_hex = hex(&rotated_token_hash);
    let redacted_values = [
        ("password", PASSWORD),
        ("metrics token", METRICS_TOKEN),
        ("password hash", password_hash.as_str()),
        ("initial token", initial_token.as_str()),
        ("rotated token", rotated_token.as_str()),
        ("initial token hash", initial_hash_hex.as_str()),
        ("rotated token hash", rotated_hash_hex.as_str()),
        ("created metadata", METADATA_CREATE),
        ("updated metadata", METADATA_UPDATE),
        ("rollback metadata", METADATA_ROLLBACK),
        ("command params", COMMAND_PARAMS),
        ("telemetry payload", TELEMETRY_PAYLOAD),
    ];
    for (name, secret) in redacted_values {
        assert!(!audit_text.contains(secret), "audit leaked {name}");
        assert!(!logs_text.contains(secret), "logs leaked {name}");
    }
}
