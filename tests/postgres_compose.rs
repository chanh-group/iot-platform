mod common;

use std::{fs, os::unix::fs::PermissionsExt, path::PathBuf, time::Duration};

use common::{
    mqtt_connect, mqtt_publish, mqtt_subscribe, mqtt_wait_puback, mqtt_wait_telemetry_ack,
};
use rumqttc::QoS;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sqlx::PgPool;
use tokio::time::timeout;
use uuid::Uuid;

const DATABASE: &str = "iot";

#[derive(Serialize, Deserialize)]
struct State {
    device_id: String,
    device_token: String,
    telemetry_id: String,
    telemetry_payload: String,
    command_id: String,
}

#[tokio::test]
#[ignore = "requires local PostgreSQL Compose fixture; run tests/postgres_compose.sh"]
async fn postgres_compose_seed_or_verify_persistent_fixture() {
    let stage = std::env::var("IOT_COMPOSE_TEST_STAGE").expect("seed or verify stage");
    let state_path = PathBuf::from(
        std::env::var_os("IOT_COMPOSE_STATE_FILE").expect("test-owned state file path"),
    );
    let base = format!(
        "http://127.0.0.1:{}",
        std::env::var("IOT_HTTP_PORT").expect("Compose HTTP port")
    );
    let mqtt_addr = format!(
        "127.0.0.1:{}",
        std::env::var("IOT_MQTT_PORT").expect("Compose MQTT port")
    )
    .parse()
    .unwrap();
    let url = std::env::var("IOT_TEST_POSTGRES_URL").expect("host URL to Compose fixture");
    let expected_database =
        std::env::var("IOT_EXPECTED_DATABASE").unwrap_or_else(|_| DATABASE.into());
    assert_fixture_database(&url, &expected_database).await;
    wait_ready(&base).await;

    let client = reqwest::Client::builder()
        .cookie_store(true)
        .build()
        .unwrap();
    let password = std::env::var("IOT_TEST_ADMIN_PASSWORD").expect("test admin password");
    let login = client
        .post(format!("{base}/api/v1/auth/login"))
        .header("X-IoT-CSRF", "1")
        .json(&json!({"username":"compose-admin","password":password}))
        .send()
        .await
        .unwrap();
    assert_eq!(login.status(), reqwest::StatusCode::OK);

    match stage.as_str() {
        "seed" => seed(&client, &base, mqtt_addr, &state_path).await,
        "verify" => verify(&client, &base, mqtt_addr, &state_path).await,
        _ => panic!("unknown compose test stage"),
    }
}

async fn seed(
    client: &reqwest::Client,
    base: &str,
    mqtt_addr: std::net::SocketAddr,
    state_path: &std::path::Path,
) {
    let response = client
        .post(format!("{base}/api/v1/devices"))
        .header("X-IoT-CSRF", "1")
        .json(&json!({"name":"compose-pg-device","metadata":{"fixture":true}}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::CREATED);
    let body: Value = response.json().await.unwrap();
    let device_id = body["device"]["id"].as_str().unwrap().to_owned();
    let device_token = body["credential"]["token"].as_str().unwrap().to_owned();
    let telemetry_id = format!("compose-pg-{}", Uuid::now_v7());
    let telemetry_payload = serde_json::to_string(&json!({
        "event_id": telemetry_id,
        "ts": iot_server::core::auth::now_ms(),
        "values": {"temperature": 23.5}
    }))
    .unwrap();

    let (mqtt, mut eventloop, _) =
        mqtt_connect(mqtt_addr, &device_id, &device_id, &device_token, true).await;
    let ack_topic = format!("devices/{device_id}/telemetry/ack");
    assert!(matches!(
        mqtt_subscribe(&mqtt, &mut eventloop, &ack_topic, QoS::AtLeastOnce).await,
        rumqttc::SubscribeReasonCode::Success(_)
    ));
    mqtt_publish(
        &mqtt,
        &format!("devices/{device_id}/telemetry"),
        QoS::AtLeastOnce,
        false,
        telemetry_payload.as_bytes(),
    )
    .await;
    mqtt_wait_puback(&mut eventloop, Duration::from_secs(5)).await;
    let ack = mqtt_wait_telemetry_ack(&mut eventloop, &ack_topic, Duration::from_secs(5)).await;
    assert_eq!(ack["status"], "stored");
    drop(mqtt);

    let command = client
        .post(format!("{base}/api/v1/devices/{device_id}/commands"))
        .header("X-IoT-CSRF", "1")
        .json(&json!({
            "name":"compose-restart-check",
            "params":{"value":1},
            "idempotency_key":"compose-restart-check"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(command.status(), reqwest::StatusCode::ACCEPTED);
    let command_id = command.json::<Value>().await.unwrap()["command_id"]
        .as_str()
        .unwrap()
        .to_owned();

    let runtime = client
        .patch(format!("{base}/api/v1/config/runtime"))
        .header("X-IoT-CSRF", "1")
        .json(&json!({
            "expected_revision": 0,
            "settings": {"logging":{"level":"debug"}}
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(runtime.status(), reqwest::StatusCode::OK);
    assert_eq!(runtime.json::<Value>().await.unwrap()["revision"], 1);

    let state = State {
        device_id,
        device_token,
        telemetry_id,
        telemetry_payload,
        command_id,
    };
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(state_path)
        .unwrap();
    use std::io::Write;
    file.write_all(&serde_json::to_vec(&state).unwrap())
        .unwrap();
    fs::set_permissions(state_path, fs::Permissions::from_mode(0o600)).unwrap();
}

async fn verify(
    client: &reqwest::Client,
    base: &str,
    mqtt_addr: std::net::SocketAddr,
    state_path: &std::path::Path,
) {
    let state: State = serde_json::from_slice(&fs::read(state_path).unwrap()).unwrap();
    let me: Value = client
        .get(format!("{base}/api/v1/auth/me"))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(me["username"], "compose-admin");

    let device: Value = client
        .get(format!("{base}/api/v1/devices/{}", state.device_id))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(device["id"], state.device_id);
    assert_eq!(device["metadata"]["fixture"], true);
    let config: Value = client
        .get(format!("{base}/api/v1/config"))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(config["revision"], 1);
    assert_eq!(config["config"]["logging"]["level"], "debug");
    let history: Value = client
        .get(format!(
            "{base}/api/v1/devices/{}/telemetry",
            state.device_id
        ))
        .query(&[("after_id", "0"), ("limit", "10")])
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(history["items"].as_array().unwrap().len(), 1);
    assert_eq!(history["items"][0]["event_id"], state.telemetry_id);

    let command: Value = client
        .get(format!("{base}/api/v1/commands/{}", state.command_id))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(command["id"], state.command_id);
    assert!(matches!(
        command["status"].as_str(),
        Some("pending" | "dispatched")
    ));
    let pool = PgPool::connect(&std::env::var("IOT_TEST_POSTGRES_URL").unwrap())
        .await
        .unwrap();
    let command_audits: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM audit_events WHERE action = 'command.create' AND target_id = $1",
    )
    .bind(&state.command_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(command_audits, 1);
    pool.close().await;

    let (mqtt, mut eventloop, _) = mqtt_connect(
        mqtt_addr,
        &state.device_id,
        &state.device_id,
        &state.device_token,
        true,
    )
    .await;
    let ack_topic = format!("devices/{}/telemetry/ack", state.device_id);
    assert!(matches!(
        mqtt_subscribe(&mqtt, &mut eventloop, &ack_topic, QoS::AtLeastOnce).await,
        rumqttc::SubscribeReasonCode::Success(_)
    ));
    mqtt_publish(
        &mqtt,
        &format!("devices/{}/telemetry", state.device_id),
        QoS::AtLeastOnce,
        false,
        state.telemetry_payload.as_bytes(),
    )
    .await;
    mqtt_wait_puback(&mut eventloop, Duration::from_secs(5)).await;
    let duplicate =
        mqtt_wait_telemetry_ack(&mut eventloop, &ack_topic, Duration::from_secs(5)).await;
    assert_eq!(duplicate["status"], "duplicate");
}

async fn assert_fixture_database(url: &str, expected: &str) {
    let pool = PgPool::connect(url).await.unwrap();
    let (database, schema): (String, String) =
        sqlx::query_as("SELECT current_database(), current_schema()")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(database, expected, "unexpected Compose database");
    assert_eq!(schema, "public", "unexpected Compose schema");
    pool.close().await;
}

async fn wait_ready(base: &str) {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(1))
        .build()
        .unwrap();
    timeout(Duration::from_secs(30), async {
        loop {
            if client
                .get(format!("{base}/readyz"))
                .send()
                .await
                .is_ok_and(|response| response.status().is_success())
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("Compose app reaches readiness");
}
