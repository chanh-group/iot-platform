use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::time::Duration;

#[cfg(debug_assertions)]
use rumqttc::{Event as MqttEvent, EventLoop, Packet, Publish, QoS};
#[cfg(debug_assertions)]
use sqlx::SqlitePool;
#[cfg(debug_assertions)]
use uuid::Uuid;

mod common;

use common::{PASSWORD, free_port, wait_ready};
#[cfg(debug_assertions)]
use common::{mqtt_connect, mqtt_connect_refused, mqtt_subscribe};
use iot_server::config;
use iot_server::runtime::data_dir_lock::DataDirLock;
use reqwest::StatusCode;
#[cfg(debug_assertions)]
use serde_json::Value;
use serde_json::json;
use tempfile::TempDir;
use tokio::io::AsyncWriteExt;

struct Server {
    child: Option<Child>,
}

impl Server {
    fn spawn(config_path: &Path) -> Self {
        Self::spawn_with_env(config_path, None)
    }

    fn spawn_with_env(config_path: &Path, pause: Option<(&str, &Path)>) -> Self {
        let mut command = Command::new(env!("CARGO_BIN_EXE_iot-server"));
        command
            .args(["run", "--config"])
            .arg(config_path)
            .env_remove("IOT_TEST_PAUSE_BEFORE_COMMIT")
            .env_remove("IOT_TEST_PAUSE_AFTER_COMMIT")
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        if let Some((env_var, path)) = pause {
            command.env(env_var, path);
        }
        Self {
            child: Some(command.spawn().unwrap()),
        }
    }

    fn id(&self) -> u32 {
        self.child.as_ref().unwrap().id()
    }

    fn wait_with_output(mut self) -> io::Result<Output> {
        self.child.take().unwrap().wait_with_output()
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

fn write_config(dir: &TempDir, port: u16) -> PathBuf {
    let path = dir.path().join("conf.toml");
    fs::write(
        &path,
        format!(
            "config_version = 1\n\
             [server]\n\
             data_dir = \"./data\"\n\
             shutdown_timeout_ms = 5000\n\
             [http]\n\
             listen = \"127.0.0.1:{port}\"\n\
             [database]\n\
             url = \"sqlite://./data/iot.db\"\n"
        ),
    )
    .unwrap();
    path
}

#[cfg(debug_assertions)]
fn write_mqtt_config(
    dir: &TempDir,
    http_port: u16,
    mqtt_port: u16,
    shutdown_timeout_ms: u64,
) -> PathBuf {
    let path = dir.path().join("conf.toml");
    fs::write(
        &path,
        format!(
            "config_version = 1\n\
             [server]\n\
             data_dir = \"./data\"\n\
             shutdown_timeout_ms = {shutdown_timeout_ms}\n\
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
    .unwrap();
    path
}

#[cfg(debug_assertions)]
async fn seed_device(dir: &TempDir, config_path: &Path) -> (SqlitePool, String, String) {
    let migrate = run_cli(&["migrate"], config_path);
    assert!(
        migrate.status.success(),
        "migrate failed: {}",
        String::from_utf8_lossy(&migrate.stderr)
    );
    let pool = SqlitePool::connect(&format!(
        "sqlite://{}",
        dir.path().join("data/iot.db").display()
    ))
    .await
    .unwrap();
    let device_id = Uuid::now_v7().to_string();
    let token = format!(
        "iot_{}",
        iot_server::core::auth::generate_session_token().unwrap()
    );
    let now = iot_server::core::auth::now_ms();
    sqlx::query(
        "INSERT INTO devices (id, name, enabled, created_at, updated_at)
         VALUES (?, ?, 1, ?, ?)",
    )
    .bind(&device_id)
    .bind(&device_id)
    .bind(now)
    .bind(now)
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO device_credentials
         (id, device_id, token_hash, status, created_at)
         VALUES (?, ?, ?, 'active', ?)",
    )
    .bind(Uuid::now_v7().to_string())
    .bind(&device_id)
    .bind(iot_server::core::auth::hash_token(&token).as_slice())
    .bind(now)
    .execute(&pool)
    .await
    .unwrap();
    (pool, device_id, token)
}

#[cfg(debug_assertions)]
fn paused_path(gate: &Path) -> PathBuf {
    let mut name = gate.as_os_str().to_owned();
    name.push(".paused");
    PathBuf::from(name)
}

#[cfg(debug_assertions)]
async fn wait_for_pause(gate: &Path) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while !paused_path(gate).exists() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("telemetry worker reaches the pre-commit hook");
}

#[cfg(debug_assertions)]
fn drive_mqtt(
    eventloop: EventLoop,
) -> (
    tokio::task::JoinHandle<()>,
    tokio::sync::mpsc::Receiver<Option<Publish>>,
) {
    let (sender, receiver) = tokio::sync::mpsc::channel(8);
    let task = tokio::spawn(async move {
        let mut eventloop = eventloop;
        loop {
            match eventloop.poll().await {
                Ok(MqttEvent::Incoming(Packet::Publish(publish))) => {
                    if sender.send(Some(publish)).await.is_err() {
                        break;
                    }
                }
                Ok(_) => {}
                Err(_) => {
                    let _ = sender.send(None).await;
                    break;
                }
            }
        }
    });
    (task, receiver)
}

#[cfg(debug_assertions)]
fn send_signal(server: &Server, signal: &str) {
    let result = Command::new("kill")
        .args([signal, &server.id().to_string()])
        .status()
        .unwrap();
    assert!(result.success(), "failed to send {signal}");
}

fn run_cli(args: &[&str], config_path: &Path) -> Output {
    Command::new(env!("CARGO_BIN_EXE_iot-server"))
        .args(args)
        .arg("--config")
        .arg(config_path)
        .output()
        .unwrap()
}

#[tokio::test]
async fn sigterm_drains_in_flight_request_and_releases_lock() {
    let dir = TempDir::new().unwrap();
    let port = free_port();
    let config_path = write_config(&dir, port);

    let migrate = run_cli(&["migrate"], &config_path);
    assert!(
        migrate.status.success(),
        "migrate failed: {}",
        String::from_utf8_lossy(&migrate.stderr)
    );

    let password_file = dir.path().join("password.txt");
    fs::write(&password_file, PASSWORD).unwrap();
    let create = Command::new(env!("CARGO_BIN_EXE_iot-server"))
        .args(["admin", "create", "--config"])
        .arg(&config_path)
        .args(["--password-file"])
        .arg(&password_file)
        .output()
        .unwrap();
    assert!(
        create.status.success(),
        "admin create failed: {}",
        String::from_utf8_lossy(&create.stderr)
    );

    let server = Server::spawn(&config_path);
    let base = format!("http://127.0.0.1:{port}");
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .unwrap();
    wait_ready(&client, &base).await;

    let login = tokio::spawn({
        let client = client.clone();
        let url = format!("{base}/api/v1/auth/login");
        async move {
            client
                .post(url)
                .header("content-type", "application/json")
                .header("x-iot-csrf", "1")
                .json(&json!({ "username": "admin", "password": PASSWORD }))
                .send()
                .await
        }
    });
    tokio::time::sleep(Duration::from_millis(50)).await;

    let kill = Command::new("kill")
        .args(["-TERM", &server.id().to_string()])
        .status()
        .unwrap();
    assert!(kill.success());

    let response = tokio::time::timeout(Duration::from_secs(5), login)
        .await
        .expect("login did not finish")
        .expect("login task panicked")
        .expect("login request failed");
    assert_eq!(response.status(), StatusCode::OK);

    let output = tokio::time::timeout(
        Duration::from_secs(10),
        tokio::task::spawn_blocking(move || server.wait_with_output()),
    )
    .await
    .expect("server did not exit")
    .expect("wait task panicked")
    .expect("wait failed");

    assert!(
        output.status.success(),
        "exit {:?}: stdout={} stderr={}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(!stderr.contains("panicked"), "{stderr}");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("server starting"), "{stdout}");
    assert!(stdout.contains("server stopped"), "{stdout}");

    let loaded = config::load_with_env(&config_path, |_| None).unwrap();
    let _lock = DataDirLock::try_new(&loaded.data_dir).expect("lock was not released");
}

#[tokio::test]
async fn second_signal_exits_immediately_without_graceful_cleanup() {
    let dir = TempDir::new().unwrap();
    let port = free_port();
    let config_path = write_config(&dir, port);

    let server = Server::spawn(&config_path);
    let base = format!("http://127.0.0.1:{port}");
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .unwrap();
    wait_ready(&client, &base).await;

    // An unfinished login body keeps one request in flight, so graceful
    // shutdown stays busy until the second signal forces the exit.
    let mut pending = tokio::net::TcpStream::connect(("127.0.0.1", port))
        .await
        .unwrap();
    pending
        .write_all(
            b"POST /api/v1/auth/login HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nX-IoT-CSRF: 1\r\nContent-Length: 64\r\n\r\n{\"username\":\"admin\"",
        )
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    let pid = server.id().to_string();
    let first = Command::new("kill").args(["-TERM", &pid]).status().unwrap();
    assert!(first.success());
    tokio::time::sleep(Duration::from_millis(50)).await;
    let second = Command::new("kill").args(["-INT", &pid]).status().unwrap();
    assert!(second.success());

    let output = tokio::time::timeout(
        Duration::from_secs(5),
        tokio::task::spawn_blocking(move || server.wait_with_output()),
    )
    .await
    .expect("server did not exit after the second signal")
    .expect("wait task panicked")
    .expect("wait failed");

    assert_eq!(output.status.code(), Some(1), "exit {:?}", output.status);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(!stderr.contains("panicked"), "{stderr}");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("second shutdown signal"), "{stdout}");
    assert!(!stdout.contains("server stopped"), "{stdout}");

    let loaded = config::load_with_env(&config_path, |_| None).unwrap();
    let _lock = DataDirLock::try_new(&loaded.data_dir).expect("lock was not released");
}

#[cfg(debug_assertions)]
#[tokio::test]
async fn sigterm_commits_queued_telemetry_then_kicks_mqtt_and_releases_lock() {
    let dir = TempDir::new().unwrap();
    let http_port = free_port();
    let mqtt_port = loop {
        let port = free_port();
        if port != http_port {
            break port;
        }
    };
    let config_path = write_mqtt_config(&dir, http_port, mqtt_port, 5_000);
    let (pool, device_id, token) = seed_device(&dir, &config_path).await;
    let (other_pool, other_device, other_token) = seed_device(&dir, &config_path).await;
    let gate = dir.path().join("before-commit");
    fs::write(&gate, b"hold").unwrap();

    let server =
        Server::spawn_with_env(&config_path, Some(("IOT_TEST_PAUSE_BEFORE_COMMIT", &gate)));
    let base = format!("http://127.0.0.1:{http_port}");
    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(1))
        .build()
        .unwrap();
    wait_ready(&http, &base).await;

    let (client, mut eventloop, _) = mqtt_connect(
        format!("127.0.0.1:{mqtt_port}").parse().unwrap(),
        &device_id,
        &device_id,
        &token,
        true,
    )
    .await;
    let ack_topic = format!("devices/{device_id}/telemetry/ack");
    assert!(matches!(
        mqtt_subscribe(&client, &mut eventloop, &ack_topic, QoS::AtLeastOnce).await,
        rumqttc::SubscribeReasonCode::Success(_)
    ));
    let (reader, mut incoming) = drive_mqtt(eventloop);
    let event_id = format!("shutdown-{}", Uuid::now_v7());
    let payload = serde_json::to_vec(&json!({
        "event_id": event_id,
        "ts": iot_server::core::auth::now_ms(),
        "values": {"temperature": 22.0}
    }))
    .unwrap();
    client
        .publish(
            format!("devices/{device_id}/telemetry"),
            QoS::AtLeastOnce,
            false,
            payload,
        )
        .await
        .unwrap();
    wait_for_pause(&gate).await;
    assert!(
        tokio::time::timeout(Duration::from_millis(100), incoming.recv())
            .await
            .is_err(),
        "uncommitted telemetry must not produce an application ACK"
    );

    send_signal(&server, "-TERM");
    tokio::time::sleep(Duration::from_millis(20)).await;
    let ready = http.get(format!("{base}/readyz")).send().await;
    assert!(!ready.is_ok_and(|response| response.status().is_success()));
    let refused = mqtt_connect_refused(
        format!("127.0.0.1:{mqtt_port}").parse().unwrap(),
        &other_device,
        &other_device,
        &other_token,
    )
    .await;
    assert_ne!(refused, rumqttc::ConnectReturnCode::Success);
    fs::remove_file(&gate).unwrap();

    let ack = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match incoming.recv().await {
                Some(Some(publish)) if publish.topic == ack_topic => {
                    break serde_json::from_slice::<Value>(&publish.payload).unwrap();
                }
                Some(None) => panic!("MQTT session closed before committed telemetry ACK"),
                Some(Some(_)) => {}
                None => panic!("MQTT reader ended before telemetry ACK"),
            }
        }
    })
    .await
    .expect("committed telemetry ACK arrives during shutdown");
    assert_eq!(ack["event_id"], event_id);
    assert_eq!(ack["status"], "stored");

    let output = tokio::time::timeout(
        Duration::from_secs(10),
        tokio::task::spawn_blocking(move || server.wait_with_output()),
    )
    .await
    .expect("server did not finish graceful shutdown")
    .expect("wait task panicked")
    .expect("wait failed");
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM telemetry_events WHERE device_id = ? AND event_id = ?",
        )
        .bind(&device_id)
        .bind(&event_id)
        .fetch_one(&pool)
        .await
        .unwrap(),
        1
    );
    tokio::time::timeout(Duration::from_secs(2), async {
        while incoming.recv().await.is_some() {}
    })
    .await
    .expect("MQTT session was not kicked");
    reader.await.unwrap();
    other_pool.close().await;
    let loaded = config::load_with_env(&config_path, |_| None).unwrap();
    let _lock = DataDirLock::try_new(&loaded.data_dir).expect("lock was not released");
}

#[cfg(debug_assertions)]
#[tokio::test]
async fn shutdown_deadline_logs_uncommitted_telemetry_and_exits_nonzero() {
    let dir = TempDir::new().unwrap();
    let http_port = free_port();
    let mqtt_port = loop {
        let port = free_port();
        if port != http_port {
            break port;
        }
    };
    let config_path = write_mqtt_config(&dir, http_port, mqtt_port, 1_000);
    let (pool, device_id, token) = seed_device(&dir, &config_path).await;
    let gate = dir.path().join("before-commit-deadline");
    fs::write(&gate, b"hold").unwrap();

    let server =
        Server::spawn_with_env(&config_path, Some(("IOT_TEST_PAUSE_BEFORE_COMMIT", &gate)));
    let base = format!("http://127.0.0.1:{http_port}");
    let http = reqwest::Client::new();
    wait_ready(&http, &base).await;
    let (client, mut eventloop, _) = mqtt_connect(
        format!("127.0.0.1:{mqtt_port}").parse().unwrap(),
        &device_id,
        &device_id,
        &token,
        true,
    )
    .await;
    let ack_topic = format!("devices/{device_id}/telemetry/ack");
    assert!(matches!(
        mqtt_subscribe(&client, &mut eventloop, &ack_topic, QoS::AtLeastOnce).await,
        rumqttc::SubscribeReasonCode::Success(_)
    ));
    let (reader, mut incoming) = drive_mqtt(eventloop);
    let event_id = format!("shutdown-deadline-{}", Uuid::now_v7());
    client
        .publish(
            format!("devices/{device_id}/telemetry"),
            QoS::AtLeastOnce,
            false,
            serde_json::to_vec(&json!({
                "event_id": event_id,
                "ts": iot_server::core::auth::now_ms(),
                "values": {"temperature": 23.0}
            }))
            .unwrap(),
        )
        .await
        .unwrap();
    wait_for_pause(&gate).await;
    assert!(
        tokio::time::timeout(Duration::from_millis(100), incoming.recv())
            .await
            .is_err(),
        "uncommitted telemetry must not produce an application ACK"
    );
    send_signal(&server, "-TERM");

    let output = tokio::time::timeout(
        Duration::from_secs(10),
        tokio::task::spawn_blocking(move || server.wait_with_output()),
    )
    .await
    .expect("server did not stop after the shutdown deadline")
    .expect("wait task panicked")
    .expect("wait failed");
    assert!(
        !output.status.success(),
        "shutdown deadline must fail the process"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.lines().any(|line| {
            line.contains("telemetry worker did not drain before shutdown deadline")
                && (line.contains("\"items_remaining\":1") || line.contains("items_remaining=1"))
        }),
        "missing remaining-item log: {stdout} {stderr}"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM telemetry_events WHERE device_id = ? AND event_id = ?",
        )
        .bind(&device_id)
        .bind(&event_id)
        .fetch_one(&pool)
        .await
        .unwrap(),
        0
    );
    reader.abort();
    let _ = reader.await;
    let loaded = config::load_with_env(&config_path, |_| None).unwrap();
    let _lock = DataDirLock::try_new(&loaded.data_dir).expect("lock was not released");
}
