#![cfg(debug_assertions)]

mod common;

use std::{
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    time::Duration,
};

use common::{free_port, mqtt_connect, mqtt_wait_publish};
use rumqttc::{Event, EventLoop, Packet, QoS};
use serde_json::{Value, json};
use sqlx::SqlitePool;
use tempfile::TempDir;
use tokio::time::timeout;
use uuid::Uuid;

const EVENT_PREFIX: &str = "crash-recovery";
const DEVICE_TOKEN_PREFIX: &str = "iot_";

struct Server(Option<Child>);

impl Server {
    fn spawn(config: &Path, pause: Option<(&str, &Path)>) -> Self {
        let mut command = Command::new(env!("CARGO_BIN_EXE_iot-server"));
        command
            .args(["run", "--config"])
            .arg(config)
            .env_remove("IOT_TEST_PAUSE_BEFORE_COMMIT")
            .env_remove("IOT_TEST_PAUSE_AFTER_COMMIT")
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        if let Some((env_var, gate)) = pause {
            command.env(env_var, gate);
        }
        Self(Some(command.spawn().expect("spawn server binary")))
    }

    fn kill(&mut self) {
        if let Some(mut child) = self.0.take() {
            child.kill().expect("SIGKILL server process");
            child.wait().expect("reap killed server process");
        }
    }

    fn try_wait(&mut self) -> Option<std::process::ExitStatus> {
        self.0
            .as_mut()
            .expect("server process is running")
            .try_wait()
            .expect("check server process")
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        self.kill();
    }
}

async fn start_server(config: &Path, http_port: u16, pause: Option<(&str, &Path)>) -> Server {
    let mut server = Server::spawn(config, pause);
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(1))
        .build()
        .expect("build readiness client");
    timeout(Duration::from_secs(10), async {
        loop {
            if let Some(status) = server.try_wait() {
                panic!("server exited before readiness: {status}");
            }
            if client
                .get(format!("http://127.0.0.1:{http_port}/readyz"))
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
    .expect("server becomes ready");
    server
}

struct Fixture {
    _dir: TempDir,
    config: PathBuf,
    http_port: u16,
    mqtt_port: u16,
    device_id: String,
    token: String,
    pool: SqlitePool,
}

impl Fixture {
    async fn new() -> Self {
        let dir = tempfile::tempdir().expect("create isolated data directory");
        let http_port = free_port();
        let mqtt_port = loop {
            let port = free_port();
            if port != http_port {
                break port;
            }
        };
        let config = dir.path().join("conf.toml");
        std::fs::write(
            &config,
            format!(
                "config_version = 1\n\
                 [server]\n\
                 data_dir = \"./data\"\n\
                 shutdown_timeout_ms = 1000\n\
                 [http]\n\
                 listen = \"127.0.0.1:{http_port}\"\n\
                 [mqtt]\n\
                 listen = \"127.0.0.1:{mqtt_port}\"\n\
                 [database]\n\
                 url = \"sqlite://./data/iot.db\"\n\
                 [telemetry]\n\
                 batch_size = 1\n\
                 flush_interval_ms = 10\n"
            ),
        )
        .expect("write test config");

        let migrated = Command::new(env!("CARGO_BIN_EXE_iot-server"))
            .args(["migrate", "--config"])
            .arg(&config)
            .output()
            .expect("run migrations");
        assert!(
            migrated.status.success(),
            "migration failed: {}",
            String::from_utf8_lossy(&migrated.stderr)
        );
        let pool = SqlitePool::connect(&format!(
            "sqlite://{}",
            dir.path().join("data/iot.db").display()
        ))
        .await
        .expect("connect to server database");
        let device_id = Uuid::now_v7().to_string();
        let token = format!(
            "{DEVICE_TOKEN_PREFIX}{}",
            iot_server::core::auth::generate_session_token().expect("generate token")
        );
        seed_device(&pool, &device_id, &token).await;
        Self {
            _dir: dir,
            config,
            http_port,
            mqtt_port,
            device_id,
            token,
            pool,
        }
    }

    async fn start(&self, pause: Option<(&str, &Path)>) -> Server {
        start_server(&self.config, self.http_port, pause).await
    }
}

async fn seed_device(pool: &SqlitePool, device_id: &str, token: &str) {
    let now = iot_server::core::auth::now_ms();
    sqlx::query(
        "INSERT INTO devices (id, name, enabled, created_at, updated_at)
         VALUES (?, ?, 1, ?, ?)",
    )
    .bind(device_id)
    .bind(format!("crash-{}", &device_id[..8]))
    .bind(now)
    .bind(now)
    .execute(pool)
    .await
    .expect("insert test device");
    sqlx::query(
        "INSERT INTO device_credentials
         (id, device_id, token_hash, status, created_at)
         VALUES (?, ?, ?, 'active', ?)",
    )
    .bind(Uuid::now_v7().to_string())
    .bind(device_id)
    .bind(iot_server::core::auth::hash_token(token).as_slice())
    .bind(now)
    .execute(pool)
    .await
    .expect("insert test device credential");
}

impl Fixture {
    async fn connect(&self) -> (rumqttc::AsyncClient, EventLoop) {
        let (client, eventloop, _) = mqtt_connect(
            format!("127.0.0.1:{}", self.mqtt_port)
                .parse()
                .expect("parse MQTT address"),
            &self.device_id,
            &self.device_id,
            &self.token,
            true,
        )
        .await;
        (client, eventloop)
    }

    async fn event_count(&self, event_id: &str) -> i64 {
        sqlx::query_scalar(
            "SELECT COUNT(*) FROM telemetry_events WHERE device_id = ? AND event_id = ?",
        )
        .bind(&self.device_id)
        .bind(event_id)
        .fetch_one(&self.pool)
        .await
        .expect("count persisted telemetry")
    }
}

async fn wait_for_pause(server: &mut Server, gate: &Path) {
    let paused = paused_path(gate);
    timeout(Duration::from_secs(10), async {
        loop {
            if paused.exists() {
                return;
            }
            if let Some(status) = server.try_wait() {
                panic!("server exited before reaching crash hook: {status}");
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("server reaches crash hook");
}

fn paused_path(gate: &Path) -> PathBuf {
    let mut path = gate.as_os_str().to_owned();
    path.push(".paused");
    PathBuf::from(path)
}

fn drive_eventloop(eventloop: EventLoop) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut eventloop = eventloop;
        while eventloop.poll().await.is_ok() {}
    })
}

async fn subscribe(client: &rumqttc::AsyncClient, eventloop: &mut EventLoop, topic: &str) {
    client.subscribe(topic, QoS::AtLeastOnce).await.unwrap();
    timeout(Duration::from_secs(5), async {
        loop {
            if let Event::Incoming(Packet::SubAck(ack)) = eventloop.poll().await.unwrap() {
                assert!(
                    ack.return_codes
                        .iter()
                        .all(|code| matches!(code, rumqttc::SubscribeReasonCode::Success(_)))
                );
                return;
            }
        }
    })
    .await
    .expect("receive MQTT SUBACK");
}

fn event_payload(event_id: &str) -> Vec<u8> {
    json!({
        "event_id": event_id,
        "ts": iot_server::core::auth::now_ms(),
        "values": {"temperature": 21.5}
    })
    .to_string()
    .into_bytes()
}

async fn receive_ack(eventloop: &mut EventLoop, device_id: &str) -> Value {
    let publish = mqtt_wait_publish(eventloop, Duration::from_secs(10)).await;
    assert_eq!(publish.topic, format!("devices/{device_id}/telemetry/ack"));
    serde_json::from_slice(&publish.payload).expect("parse telemetry ACK")
}

async fn send_and_receive_ack(
    client: &rumqttc::AsyncClient,
    eventloop: &mut EventLoop,
    device_id: &str,
    event_id: &str,
    payload: &[u8],
) -> Value {
    client
        .publish(
            format!("devices/{device_id}/telemetry"),
            QoS::AtLeastOnce,
            false,
            payload,
        )
        .await
        .unwrap();
    let ack = receive_ack(eventloop, device_id).await;
    assert_eq!(ack["event_id"], event_id);
    ack
}

#[tokio::test]
async fn kill_before_commit_recovers_without_losing_or_duplicating_event() {
    let fixture = Fixture::new().await;
    let event_id = format!("{EVENT_PREFIX}-before-{}", Uuid::now_v7());
    let payload = event_payload(&event_id);
    let gate = fixture._dir.path().join("before-commit");
    std::fs::write(&gate, b"hold").unwrap();
    let mut server = fixture
        .start(Some(("IOT_TEST_PAUSE_BEFORE_COMMIT", &gate)))
        .await;
    let (client, mut eventloop) = fixture.connect().await;
    let ack_topic = format!("devices/{}/telemetry/ack", fixture.device_id);
    subscribe(&client, &mut eventloop, &ack_topic).await;
    client
        .publish(
            format!("devices/{}/telemetry", fixture.device_id),
            QoS::AtLeastOnce,
            false,
            payload.clone(),
        )
        .await
        .unwrap();
    let poll_task = drive_eventloop(eventloop);
    wait_for_pause(&mut server, &gate).await;
    server.kill();
    drop(client);
    poll_task.abort();
    let _ = poll_task.await;
    assert_eq!(fixture.event_count(&event_id).await, 0);

    let mut restarted = fixture.start(None).await;
    let (client, mut eventloop) = fixture.connect().await;
    subscribe(&client, &mut eventloop, &ack_topic).await;
    let ack = send_and_receive_ack(
        &client,
        &mut eventloop,
        &fixture.device_id,
        &event_id,
        &payload,
    )
    .await;
    assert_eq!(ack["status"], "stored");
    assert_eq!(fixture.event_count(&event_id).await, 1);
    restarted.kill();
}

#[tokio::test]
async fn kill_after_commit_before_ack_retries_as_duplicate() {
    let fixture = Fixture::new().await;
    let event_id = format!("{EVENT_PREFIX}-after-commit-{}", Uuid::now_v7());
    let payload = event_payload(&event_id);
    let gate = fixture._dir.path().join("after-commit");
    std::fs::write(&gate, b"hold").unwrap();
    let mut server = fixture
        .start(Some(("IOT_TEST_PAUSE_AFTER_COMMIT", &gate)))
        .await;
    let (client, mut eventloop) = fixture.connect().await;
    let ack_topic = format!("devices/{}/telemetry/ack", fixture.device_id);
    subscribe(&client, &mut eventloop, &ack_topic).await;
    client
        .publish(
            format!("devices/{}/telemetry", fixture.device_id),
            QoS::AtLeastOnce,
            false,
            payload.clone(),
        )
        .await
        .unwrap();
    let poll_task = drive_eventloop(eventloop);
    wait_for_pause(&mut server, &gate).await;
    assert_eq!(fixture.event_count(&event_id).await, 1);
    server.kill();
    drop(client);
    poll_task.abort();
    let _ = poll_task.await;

    let mut restarted = fixture.start(None).await;
    let (client, mut eventloop) = fixture.connect().await;
    subscribe(&client, &mut eventloop, &ack_topic).await;
    let ack = send_and_receive_ack(
        &client,
        &mut eventloop,
        &fixture.device_id,
        &event_id,
        &payload,
    )
    .await;
    assert_eq!(ack["status"], "duplicate");
    assert_eq!(fixture.event_count(&event_id).await, 1);
    restarted.kill();
}

#[tokio::test]
async fn kill_after_ack_preserves_telemetry_and_recovers_pending_command() {
    let fixture = Fixture::new().await;
    let event_id = format!("{EVENT_PREFIX}-after-ack-{}", Uuid::now_v7());
    let payload = event_payload(&event_id);
    let gate = fixture._dir.path().join("resume-after-commit");
    std::fs::write(&gate, b"hold").unwrap();
    let mut server = fixture
        .start(Some(("IOT_TEST_PAUSE_AFTER_COMMIT", &gate)))
        .await;
    let (client, mut eventloop) = fixture.connect().await;
    let ack_topic = format!("devices/{}/telemetry/ack", fixture.device_id);
    subscribe(&client, &mut eventloop, &ack_topic).await;
    let ack_task = tokio::spawn({
        let device_id = fixture.device_id.clone();
        let event_id = event_id.clone();
        async move {
            send_and_receive_ack(&client, &mut eventloop, &device_id, &event_id, &payload).await
        }
    });
    wait_for_pause(&mut server, &gate).await;
    std::fs::remove_file(&gate).expect("release post-commit hook");
    let ack = timeout(Duration::from_secs(10), ack_task)
        .await
        .expect("ACK after releasing post-commit hook")
        .expect("ACK task succeeds");
    assert_eq!(ack["status"], "stored");
    assert_eq!(fixture.event_count(&event_id).await, 1);

    let command_id = Uuid::now_v7().to_string();
    let now = iot_server::core::auth::now_ms();
    sqlx::query(
        "INSERT INTO commands
         (id, device_id, name, params_json, status, created_at, expires_at, next_attempt_at)
         VALUES (?, ?, 'restart-recovery', '{}', 'pending', ?, ?, ?)",
    )
    .bind(&command_id)
    .bind(&fixture.device_id)
    .bind(now)
    .bind(now + 300_000)
    .bind(now)
    .execute(&fixture.pool)
    .await
    .expect("seed pending command");

    server.kill();
    let mut restarted = fixture.start(None).await;
    let (client, mut eventloop) = fixture.connect().await;
    let commands_topic = format!("devices/{}/commands", fixture.device_id);
    client
        .subscribe(&commands_topic, QoS::AtLeastOnce)
        .await
        .unwrap();
    let command = timeout(Duration::from_secs(10), async {
        loop {
            match eventloop.poll().await.unwrap() {
                Event::Incoming(Packet::SubAck(ack)) => assert!(
                    ack.return_codes
                        .iter()
                        .all(|code| matches!(code, rumqttc::SubscribeReasonCode::Success(_)))
                ),
                Event::Incoming(Packet::Publish(publish)) if publish.topic == commands_topic => {
                    break serde_json::from_slice::<Value>(&publish.payload)
                        .expect("parse recovered command");
                }
                _ => {}
            }
        }
    })
    .await
    .expect("recover pending command after restart");
    assert_eq!(command["command_id"], command_id);
    assert_eq!(fixture.event_count(&event_id).await, 1);
    let status: String = sqlx::query_scalar("SELECT status FROM commands WHERE id = ?")
        .bind(&command_id)
        .fetch_one(&fixture.pool)
        .await
        .expect("read recovered command state");
    assert_eq!(status, "dispatched");
    restarted.kill();
}
