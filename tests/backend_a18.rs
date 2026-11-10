mod common;

use std::{
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    time::Duration,
};

use common::{
    free_port, mqtt_connect, mqtt_connect_refused, mqtt_publish, mqtt_subscribe, mqtt_wait_closed,
    mqtt_wait_puback, mqtt_wait_publish, mqtt_wait_telemetry_ack,
};
use futures_util::{SinkExt, StreamExt};
use reqwest::StatusCode;
use rumqttc::{ConnectReturnCode, Event, Packet, QoS, SubscribeReasonCode};
use serde_json::{Value, json};
use sqlx::{PgPool, SqlitePool};
use tempfile::TempDir;
use tokio::{net::TcpStream, time::timeout};
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream, connect_async,
    tungstenite::{Message, client::IntoClientRequest},
};
use uuid::Uuid;

const ADMIN_PASSWORD: &str = "backend-a18-test-password";
const A18_DATABASE: &str = "iot_a18_test";

#[tokio::test]
async fn shared_a18_contracts_pass_on_sqlite() {
    run_a18_contracts(None).await;
}

#[tokio::test]
#[ignore = "requires an isolated PostgreSQL 18.6 database; run tests/postgres_recovery.sh"]
async fn shared_a18_contracts_pass_on_postgres() {
    let url = std::env::var("IOT_TEST_POSTGRES_URL").expect("isolated PostgreSQL test URL");
    run_a18_contracts(Some(url)).await;
}

enum TestDb {
    Sqlite(SqlitePool),
    Postgres(PgPool),
}

struct Fixture {
    _dir: TempDir,
    config: PathBuf,
    base: String,
    mqtt_addr: std::net::SocketAddr,
    db: TestDb,
}

struct AppProcess(Option<Child>);

impl AppProcess {
    fn spawn(config: &Path) -> Self {
        Self(Some(
            Command::new(env!("CARGO_BIN_EXE_iot-server"))
                .args(["run", "--config"])
                .arg(config)
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()
                .expect("spawn server"),
        ))
    }

    fn try_wait(&mut self) -> Option<std::process::ExitStatus> {
        self.0.as_mut().unwrap().try_wait().unwrap()
    }

    async fn terminate(&mut self) -> std::process::ExitStatus {
        let mut child = self.0.take().expect("server is running");
        let pid = child.id().to_string();
        let signal = tokio::task::spawn_blocking(move || {
            Command::new("kill").args(["-TERM", &pid]).status().unwrap()
        })
        .await
        .unwrap();
        assert!(signal.success(), "send SIGTERM");
        tokio::task::spawn_blocking(move || child.wait())
            .await
            .unwrap()
            .unwrap()
    }
}

impl Drop for AppProcess {
    fn drop(&mut self) {
        if let Some(mut child) = self.0.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

impl Fixture {
    async fn new(postgres_url: Option<&str>) -> Self {
        let dir = tempfile::tempdir().expect("create isolated app data directory");
        let http_port = free_port();
        let mqtt_port = loop {
            let port = free_port();
            if port != http_port {
                break port;
            }
        };
        let config = dir.path().join("conf.toml");
        let database_config = if postgres_url.is_some() {
            "[database]\nurl_env = 'IOT_TEST_POSTGRES_URL'\n"
        } else {
            "[database]\nurl = 'sqlite://./data/iot.db'\n"
        };
        std::fs::write(
            &config,
            format!(
                "config_version = 1\n\
                 [server]\ndata_dir = './data'\nshutdown_timeout_ms = 5000\n\
                 [http]\nlisten = '127.0.0.1:{http_port}'\n\
                 [mqtt]\nlisten = '127.0.0.1:{mqtt_port}'\n\
                 {database_config}\
                 [commands]\nmax_pending_per_device = 1\n\
                 [telemetry]\nqueue_capacity = 16\nbatch_size = 1\nflush_interval_ms = 10\n\
                 "
            ),
        )
        .unwrap();

        let db = if let Some(url) = postgres_url {
            let pool = PgPool::connect(url)
                .await
                .expect("connect fixture database");
            let (database, schema): (String, String) =
                sqlx::query_as("SELECT current_database(), current_schema()")
                    .fetch_one(&pool)
                    .await
                    .expect("verify database fixture before migrations");
            assert_eq!(database, A18_DATABASE, "refusing a non-fixture database");
            assert_eq!(schema, "public", "refusing a non-fixture schema");
            TestDb::Postgres(pool)
        } else {
            TestDb::Sqlite(
                SqlitePool::connect_lazy(&format!(
                    "sqlite://{}",
                    dir.path().join("data/iot.db").display()
                ))
                .expect("connect fixture SQLite database"),
            )
        };

        let fixture = Self {
            _dir: dir,
            config,
            base: format!("http://127.0.0.1:{http_port}"),
            mqtt_addr: format!("127.0.0.1:{mqtt_port}").parse().unwrap(),
            db,
        };
        fixture.migrate().await;
        assert_eq!(
            fixture.user_count().await,
            0,
            "migrate must not create an admin"
        );
        fixture.create_admin().await;
        fixture
    }

    async fn migrate(&self) {
        for _ in 0..2 {
            let output = Command::new(env!("CARGO_BIN_EXE_iot-server"))
                .args(["migrate", "--config"])
                .arg(&self.config)
                .stdout(Stdio::null())
                .output()
                .expect("run migration CLI");
            assert!(
                output.status.success(),
                "migration failed: {}",
                String::from_utf8_lossy(&output.stderr)
            );
        }
    }

    async fn create_admin(&self) {
        let password_file = self._dir.path().join("admin-password");
        std::fs::write(&password_file, format!("{ADMIN_PASSWORD}\n")).unwrap();
        let output = Command::new(env!("CARGO_BIN_EXE_iot-server"))
            .args(["admin", "create", "--username", "admin", "--config"])
            .arg(&self.config)
            .arg("--password-file")
            .arg(password_file)
            .stdout(Stdio::null())
            .output()
            .expect("run admin bootstrap CLI");
        assert!(
            output.status.success(),
            "admin create failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    async fn start(&self) -> AppProcess {
        let mut server = AppProcess::spawn(&self.config);
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(1))
            .build()
            .unwrap();
        timeout(Duration::from_secs(15), async {
            loop {
                assert!(
                    server.try_wait().is_none(),
                    "server exited before readiness"
                );
                if client
                    .get(format!("{}/readyz", self.base))
                    .send()
                    .await
                    .is_ok_and(|response| response.status().is_success())
                {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .expect("server reaches readiness");
        server
    }

    async fn user_count(&self) -> i64 {
        match &self.db {
            TestDb::Sqlite(pool) => {
                sqlx::query_scalar("SELECT COUNT(*) FROM users")
                    .fetch_one(pool)
                    .await
            }
            TestDb::Postgres(pool) => {
                sqlx::query_scalar("SELECT COUNT(*) FROM users")
                    .fetch_one(pool)
                    .await
            }
        }
        .unwrap()
    }

    async fn install_command_audit_failure(&self) {
        match &self.db {
            TestDb::Sqlite(pool) => {
                sqlx::query(
                    "CREATE TRIGGER fail_command_audit BEFORE INSERT ON audit_events
                     WHEN NEW.action = 'command.create'
                     BEGIN SELECT RAISE(FAIL, 'injected audit failure'); END",
                )
                .execute(pool)
                .await
                .unwrap();
            }
            TestDb::Postgres(pool) => {
                sqlx::query(
                    "CREATE FUNCTION iot_test_fail_command_audit() RETURNS trigger
                     LANGUAGE plpgsql AS $$ BEGIN
                         IF NEW.action = 'command.create' THEN
                             RAISE EXCEPTION 'injected audit failure';
                         END IF;
                         RETURN NEW;
                     END $$",
                )
                .execute(pool)
                .await
                .unwrap();
                sqlx::query(
                    "CREATE TRIGGER fail_command_audit BEFORE INSERT ON audit_events
                     FOR EACH ROW EXECUTE FUNCTION iot_test_fail_command_audit()",
                )
                .execute(pool)
                .await
                .unwrap();
            }
        }
    }

    async fn remove_command_audit_failure(&self) {
        match &self.db {
            TestDb::Sqlite(pool) => {
                sqlx::query("DROP TRIGGER fail_command_audit")
                    .execute(pool)
                    .await
                    .unwrap();
            }
            TestDb::Postgres(pool) => {
                sqlx::query("DROP TRIGGER fail_command_audit ON audit_events")
                    .execute(pool)
                    .await
                    .unwrap();
                sqlx::query("DROP FUNCTION iot_test_fail_command_audit()")
                    .execute(pool)
                    .await
                    .unwrap();
            }
        }
    }

    async fn command_count_for_key(&self, device_id: &str, key: &str) -> i64 {
        match &self.db {
            TestDb::Sqlite(pool) => {
                sqlx::query_scalar(
                    "SELECT COUNT(*) FROM commands WHERE device_id = ? AND idempotency_key = ?",
                )
                .bind(device_id)
                .bind(key)
                .fetch_one(pool)
                .await
            }
            TestDb::Postgres(pool) => {
                sqlx::query_scalar(
                    "SELECT COUNT(*) FROM commands WHERE device_id = $1 AND idempotency_key = $2",
                )
                .bind(device_id)
                .bind(key)
                .fetch_one(pool)
                .await
            }
        }
        .unwrap()
    }

    async fn command_audit_details(&self, command_id: &str) -> String {
        match &self.db {
            TestDb::Sqlite(pool) => {
                sqlx::query_scalar(
                    "SELECT details_json FROM audit_events
                     WHERE action = 'command.create' AND target_id = ?",
                )
                .bind(command_id)
                .fetch_one(pool)
                .await
            }
            TestDb::Postgres(pool) => {
                sqlx::query_scalar(
                    "SELECT details_json::text FROM audit_events
                     WHERE action = 'command.create' AND target_id = $1",
                )
                .bind(command_id)
                .fetch_one(pool)
                .await
            }
        }
        .unwrap()
    }

    async fn rotate_audit_count(&self, device_id: &str) -> i64 {
        match &self.db {
            TestDb::Sqlite(pool) => {
                sqlx::query_scalar(
                    "SELECT COUNT(*) FROM audit_events
                     WHERE action = 'credential.rotate' AND target_id = ?",
                )
                .bind(device_id)
                .fetch_one(pool)
                .await
            }
            TestDb::Postgres(pool) => {
                sqlx::query_scalar(
                    "SELECT COUNT(*) FROM audit_events
                     WHERE action = 'credential.rotate' AND target_id = $1",
                )
                .bind(device_id)
                .fetch_one(pool)
                .await
            }
        }
        .unwrap()
    }

    async fn age_telemetry(&self, device_id: &str, event_id: &str) {
        match &self.db {
            TestDb::Sqlite(pool) => {
                sqlx::query(
                    "UPDATE telemetry_events SET received_at = 1
                     WHERE device_id = ? AND event_id = ?",
                )
                .bind(device_id)
                .bind(event_id)
                .execute(pool)
                .await
                .unwrap();
            }
            TestDb::Postgres(pool) => {
                sqlx::query(
                    "UPDATE telemetry_events SET received_at = 1
                     WHERE device_id = $1 AND event_id = $2",
                )
                .bind(device_id)
                .bind(event_id)
                .execute(pool)
                .await
                .unwrap();
            }
        }
    }
}

struct Device {
    id: String,
    token: String,
}

async fn run_a18_contracts(postgres_url: Option<String>) {
    let fixture = Fixture::new(postgres_url.as_deref()).await;
    let mut server = fixture.start().await;
    let http = reqwest::Client::builder()
        .cookie_store(true)
        .build()
        .unwrap();
    let login = http
        .post(format!("{}/api/v1/auth/login", fixture.base))
        .header("X-IoT-CSRF", "1")
        .json(&json!({"username":"ADMIN","password":ADMIN_PASSWORD}))
        .send()
        .await
        .unwrap();
    assert_eq!(login.status(), StatusCode::OK);
    let cookie = login.headers()[reqwest::header::SET_COOKIE]
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_owned();
    assert_eq!(
        http.get(format!("{}/api/v1/auth/me", fixture.base))
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );

    let device_a = create_device(&http, &fixture.base, "a18-device-a").await;
    let device_b = create_device(&http, &fixture.base, "a18-device-b").await;
    let device_c = create_device(&http, &fixture.base, "a18-device-c").await;
    let baseline: Value = http
        .get(telemetry_url(&fixture.base, &device_a.id))
        .query(&[("after_id", "0"), ("limit", "1")])
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    let baseline_id = baseline["through_id"].as_i64().unwrap();

    let mut socket = ws_connect(&fixture.base, &cookie).await;
    assert_eq!(ws_next_json(&mut socket).await["type"], "hello");
    subscribe_ws_device(&mut socket, &device_a.id).await;

    assert_eq!(
        mqtt_connect_refused(
            fixture.mqtt_addr,
            &device_a.id,
            &device_a.id,
            "iot_invalid_token",
        )
        .await,
        ConnectReturnCode::BadUserNamePassword
    );
    let (b_client, mut b_eventloop, _) = mqtt_connect(
        fixture.mqtt_addr,
        &device_b.id,
        &device_b.id,
        &device_b.token,
        true,
    )
    .await;
    assert!(matches!(
        mqtt_subscribe(
            &b_client,
            &mut b_eventloop,
            &format!("devices/{}/data/#", device_b.id),
            QoS::AtLeastOnce,
        )
        .await,
        SubscribeReasonCode::Success(_)
    ));
    assert!(matches!(
        mqtt_subscribe(
            &b_client,
            &mut b_eventloop,
            &format!("devices/{}/commands", device_b.id),
            QoS::AtLeastOnce,
        )
        .await,
        SubscribeReasonCode::Success(_)
    ));

    let (acl_client, mut acl_eventloop, _) = mqtt_connect(
        fixture.mqtt_addr,
        &device_a.id,
        &device_a.id,
        &device_a.token,
        true,
    )
    .await;
    assert_eq!(
        mqtt_subscribe(
            &acl_client,
            &mut acl_eventloop,
            &format!("devices/{}/commands", device_b.id),
            QoS::AtLeastOnce,
        )
        .await,
        SubscribeReasonCode::Failure
    );
    mqtt_publish(
        &acl_client,
        &format!("devices/{}/data/attack", device_b.id),
        QoS::AtLeastOnce,
        false,
        b"cross-device",
    )
    .await;
    mqtt_wait_closed(&mut acl_eventloop, Duration::from_secs(3)).await;
    assert!(
        timeout(Duration::from_millis(150), async {
            loop {
                if let Event::Incoming(Packet::Publish(publish)) = b_eventloop.poll().await.unwrap()
                    && publish.topic == format!("devices/{}/data/attack", device_b.id)
                {
                    return publish;
                }
            }
        })
        .await
        .is_err()
    );
    drop(acl_client);

    let (a_client, mut a_eventloop, _) = mqtt_connect(
        fixture.mqtt_addr,
        &device_a.id,
        &device_a.id,
        &device_a.token,
        true,
    )
    .await;
    let ack_topic = format!("devices/{}/telemetry/ack", device_a.id);
    let telemetry_topic = format!("devices/{}/telemetry", device_a.id);
    assert!(matches!(
        mqtt_subscribe(&a_client, &mut a_eventloop, &ack_topic, QoS::AtLeastOnce).await,
        SubscribeReasonCode::Success(_)
    ));

    let first_id = format!("a18-{}", Uuid::now_v7());
    let first_payload = telemetry_payload(&first_id, 11.0);
    mqtt_publish(
        &a_client,
        &telemetry_topic,
        QoS::AtLeastOnce,
        false,
        &first_payload,
    )
    .await;
    mqtt_wait_puback(&mut a_eventloop, Duration::from_secs(5)).await;
    let first_ack =
        mqtt_wait_telemetry_ack(&mut a_eventloop, &ack_topic, Duration::from_secs(5)).await;
    assert_eq!(first_ack["status"], "stored");
    assert_eq!(
        ws_next_type(&mut socket, "telemetry").await["data"]["event_id"],
        first_id
    );

    mqtt_publish(
        &a_client,
        &telemetry_topic,
        QoS::AtLeastOnce,
        false,
        &first_payload,
    )
    .await;
    mqtt_wait_puback(&mut a_eventloop, Duration::from_secs(5)).await;
    let duplicate =
        mqtt_wait_telemetry_ack(&mut a_eventloop, &ack_topic, Duration::from_secs(5)).await;
    assert_eq!(duplicate["status"], "duplicate");
    let conflict_payload = telemetry_payload(&first_id, 12.0);
    mqtt_publish(
        &a_client,
        &telemetry_topic,
        QoS::AtLeastOnce,
        false,
        &conflict_payload,
    )
    .await;
    mqtt_wait_puback(&mut a_eventloop, Duration::from_secs(5)).await;
    let conflict =
        mqtt_wait_telemetry_ack(&mut a_eventloop, &ack_topic, Duration::from_secs(5)).await;
    assert_eq!(conflict["status"], "rejected");
    assert_eq!(conflict["code"], "event_id_conflict");

    socket.close(None).await.unwrap();
    let second_id = format!("a18-{}", Uuid::now_v7());
    let second_payload = telemetry_payload(&second_id, 22.0);
    mqtt_publish(
        &a_client,
        &telemetry_topic,
        QoS::AtLeastOnce,
        false,
        &second_payload,
    )
    .await;
    mqtt_wait_puback(&mut a_eventloop, Duration::from_secs(5)).await;
    assert_eq!(
        mqtt_wait_telemetry_ack(&mut a_eventloop, &ack_topic, Duration::from_secs(5)).await["status"],
        "stored"
    );

    let mut reconnected_ws = ws_connect(&fixture.base, &cookie).await;
    assert_eq!(ws_next_json(&mut reconnected_ws).await["type"], "hello");
    subscribe_ws_device(&mut reconnected_ws, &device_a.id).await;
    assert!(
        timeout(Duration::from_millis(100), reconnected_ws.next())
            .await
            .is_err()
    );
    let first_page: Value = http
        .get(telemetry_url(&fixture.base, &device_a.id))
        .query(&[
            ("after_id", baseline_id.to_string().as_str()),
            ("limit", "1"),
        ])
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(first_page["items"][0]["event_id"], first_id);
    let next_cursor = first_page["next_cursor"].as_str().unwrap();
    let second_page: Value = http
        .get(telemetry_url(&fixture.base, &device_a.id))
        .query(&[
            ("after_id", baseline_id.to_string()),
            ("cursor", next_cursor.to_owned()),
            ("limit", "1".to_owned()),
        ])
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(second_page["items"][0]["event_id"], second_id);
    assert!(second_page["next_cursor"].is_null());

    fixture.install_command_audit_failure().await;
    let failed_key = format!("rollback-{}", Uuid::now_v7());
    let failed_body = json!({"name":"rollback-probe","idempotency_key":failed_key}).to_string();
    let failed = post_command(&http, &fixture.base, &device_c.id, &failed_body).await;
    assert_eq!(failed.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(
        fixture
            .command_count_for_key(&device_c.id, &failed_key)
            .await,
        0
    );
    fixture.remove_command_audit_failure().await;

    let pending_a =
        json!({"name":"race-a","idempotency_key":format!("race-a-{}",Uuid::now_v7())}).to_string();
    let pending_b =
        json!({"name":"race-b","idempotency_key":format!("race-b-{}",Uuid::now_v7())}).to_string();
    let (race_a, race_b) = tokio::join!(
        post_command(&http, &fixture.base, &device_c.id, &pending_a),
        post_command(&http, &fixture.base, &device_c.id, &pending_b),
    );
    let statuses = [race_a.status(), race_b.status()];
    assert_eq!(
        statuses
            .iter()
            .filter(|status| **status == StatusCode::ACCEPTED)
            .count(),
        1
    );
    assert_eq!(
        statuses
            .iter()
            .filter(|status| **status == StatusCode::CONFLICT)
            .count(),
        1
    );

    let command_body = json!({
        "name":"restart",
        "params":{"private_value":"redact-me"},
        "ttl_ms":60000,
        "idempotency_key":format!("command-{}",Uuid::now_v7())
    })
    .to_string();
    let created = post_command(&http, &fixture.base, &device_b.id, &command_body).await;
    assert_eq!(created.status(), StatusCode::ACCEPTED);
    let location = created.headers()[reqwest::header::LOCATION]
        .to_str()
        .unwrap()
        .to_owned();
    let command: Value = created.json().await.unwrap();
    let command_id = command["command_id"].as_str().unwrap().to_owned();
    assert_eq!(command["status"], "pending");
    let replay = post_command(&http, &fixture.base, &device_b.id, &command_body).await;
    assert_eq!(replay.status(), StatusCode::ACCEPTED);
    assert_eq!(replay.headers()["idempotent-replayed"], "true");
    let delivered = mqtt_wait_publish(&mut b_eventloop, Duration::from_secs(10)).await;
    assert_eq!(delivered.topic, format!("devices/{}/commands", device_b.id));
    let delivered_json: Value = serde_json::from_slice(&delivered.payload).unwrap();
    assert_eq!(delivered_json["command_id"], command_id);
    assert_eq!(
        serde_json::from_str::<Value>(&fixture.command_audit_details(&command_id).await).unwrap()["name"],
        "restart"
    );
    assert!(
        !fixture
            .command_audit_details(&command_id)
            .await
            .contains("redact-me")
    );

    let command_ack = json!({
        "command_id":command_id,
        "status":"succeeded",
        "result":{"ok":true},
        "error":null
    })
    .to_string();
    mqtt_publish(
        &b_client,
        &format!("devices/{}/commands/ack", device_b.id),
        QoS::AtLeastOnce,
        false,
        command_ack.as_bytes(),
    )
    .await;
    mqtt_wait_puback(&mut b_eventloop, Duration::from_secs(5)).await;
    wait_for_command(&http, &fixture.base, &location).await;

    let rotated = http
        .post(format!(
            "{}/api/v1/devices/{}/credentials/rotate",
            fixture.base, device_a.id
        ))
        .header("X-IoT-CSRF", "1")
        .json(&json!({}))
        .send()
        .await
        .unwrap();
    assert_eq!(rotated.status(), StatusCode::OK);
    let rotated: Value = rotated.json().await.unwrap();
    let new_token = rotated["credential"]["token"].as_str().unwrap();
    mqtt_wait_closed(&mut a_eventloop, Duration::from_secs(5)).await;
    assert_eq!(
        mqtt_connect_refused(
            fixture.mqtt_addr,
            &device_a.id,
            &device_a.id,
            &device_a.token
        )
        .await,
        ConnectReturnCode::BadUserNamePassword
    );
    let (_new_client, _new_eventloop, _) = mqtt_connect(
        fixture.mqtt_addr,
        &device_a.id,
        &device_a.id,
        new_token,
        true,
    )
    .await;
    assert_eq!(fixture.rotate_audit_count(&device_a.id).await, 1);

    fixture.age_telemetry(&device_a.id, &first_id).await;
    let loaded = iot_server::config::load(&fixture.config).unwrap();
    let storage = iot_server::storage::runtime::StorageRuntime::connect(&loaded)
        .await
        .unwrap();
    assert_eq!(
        storage
            .repositories
            .telemetry_retention
            .delete_before(2, 5_000)
            .await
            .unwrap(),
        1
    );
    storage.close().await;

    drop(a_client);
    drop(b_client);
    reconnected_ws.close(None).await.unwrap();
    assert!(server.terminate().await.success());
    let mut restarted = fixture.start().await;
    assert_eq!(
        http.get(format!("{}/api/v1/auth/me", fixture.base))
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );
    let persisted: Value = http
        .get(telemetry_url(&fixture.base, &device_a.id))
        .query(&[
            ("after_id", baseline_id.to_string().as_str()),
            ("limit", "10"),
        ])
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(persisted["items"].as_array().unwrap().len(), 1);
    assert_eq!(persisted["items"][0]["event_id"], second_id);
    let command_after_restart: Value = http
        .get(format!("{}{}", fixture.base, location))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(command_after_restart["status"], "succeeded");
    assert!(restarted.terminate().await.success());
}

async fn create_device(client: &reqwest::Client, base: &str, name: &str) -> Device {
    let response = client
        .post(format!("{base}/api/v1/devices"))
        .header("X-IoT-CSRF", "1")
        .json(&json!({"name":name,"metadata":{}}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);
    let body: Value = response.json().await.unwrap();
    Device {
        id: body["device"]["id"].as_str().unwrap().to_owned(),
        token: body["credential"]["token"].as_str().unwrap().to_owned(),
    }
}

fn telemetry_url(base: &str, device_id: &str) -> String {
    format!("{base}/api/v1/devices/{device_id}/telemetry")
}

async fn post_command(
    client: &reqwest::Client,
    base: &str,
    device_id: &str,
    body: &str,
) -> reqwest::Response {
    client
        .post(format!("{base}/api/v1/devices/{device_id}/commands"))
        .header("X-IoT-CSRF", "1")
        .header(reqwest::header::CONTENT_TYPE, "application/json")
        .body(body.to_owned())
        .send()
        .await
        .unwrap()
}

fn telemetry_payload(event_id: &str, temperature: f64) -> Vec<u8> {
    serde_json::to_vec(&json!({
        "event_id":event_id,
        "ts":iot_server::core::auth::now_ms(),
        "values":{"temperature":temperature}
    }))
    .unwrap()
}

type TestWs = WebSocketStream<MaybeTlsStream<TcpStream>>;

async fn ws_connect(base: &str, cookie: &str) -> TestWs {
    let authority = base.trim_start_matches("http://");
    let mut request = format!("ws://{authority}/api/v1/events")
        .into_client_request()
        .unwrap();
    request.headers_mut().insert(
        reqwest::header::COOKIE,
        reqwest::header::HeaderValue::from_str(cookie).unwrap(),
    );
    request.headers_mut().insert(
        reqwest::header::ORIGIN,
        reqwest::header::HeaderValue::from_str(base).unwrap(),
    );
    connect_async(request).await.unwrap().0
}

async fn ws_next_json(socket: &mut TestWs) -> Value {
    timeout(Duration::from_secs(5), async {
        loop {
            if let Some(Ok(Message::Text(text))) = socket.next().await {
                return serde_json::from_str(text.as_str()).unwrap();
            }
        }
    })
    .await
    .expect("receive websocket message")
}

async fn ws_next_type(socket: &mut TestWs, expected: &str) -> Value {
    timeout(Duration::from_secs(5), async {
        loop {
            let value = ws_next_json(socket).await;
            if value["type"] == expected {
                return value;
            }
            assert_eq!(value["type"], "presence", "unexpected websocket event");
        }
    })
    .await
    .expect("receive expected websocket event")
}

async fn subscribe_ws_device(socket: &mut TestWs, device_id: &str) {
    socket
        .send(Message::Text(
            json!({"action":"subscribe","channels":[format!("device:{device_id}")]})
                .to_string()
                .into(),
        ))
        .await
        .unwrap();
    assert_eq!(
        ws_next_type(socket, "subscribed").await["type"],
        "subscribed"
    );
}

async fn wait_for_command(client: &reqwest::Client, base: &str, location: &str) {
    timeout(Duration::from_secs(5), async {
        loop {
            let response = client
                .get(format!("{base}{location}"))
                .send()
                .await
                .unwrap();
            let command: Value = response.json().await.unwrap();
            if command["status"] == "succeeded" {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("command ACK becomes durable");
}
