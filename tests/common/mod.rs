//! Shared integration-test harness: isolated data dir, config, app, client.

#![allow(dead_code)]

use std::fs;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::{Duration, Instant};

use iot_server::config::{self, LoadedConfig};
use iot_server::core::auth;
use iot_server::core::events::EventBus;
use iot_server::core::presence::Presence;
use iot_server::core::telemetry::{self as telemetry_core, TelemetryPayload};
use iot_server::http::{AppState, router};
use iot_server::mqtt::server::MqttServer;
use iot_server::runtime::data_dir_lock::DataDirLock;
use iot_server::storage::sqlite::users;
use iot_server::storage::sqlite::{migrate, pool};
use reqwest::StatusCode;
use rumqttc::{
    AsyncClient, ConnectReturnCode, ConnectionError, Event as MqttEvent, EventLoop, MqttOptions,
    Packet,
};
use sqlx::SqlitePool;
use tempfile::TempDir;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::task::JoinHandle;
use uuid::Uuid;

pub const PASSWORD: &str = "correct horse battery staple";

/// Reserves a free loopback port, retrying up to three times.
pub fn free_port() -> u16 {
    for _ in 0..3 {
        if let Ok(listener) = std::net::TcpListener::bind("127.0.0.1:0") {
            return listener.local_addr().unwrap().port();
        }
    }
    panic!("could not reserve a free port after 3 attempts");
}

pub fn cookie_client() -> reqwest::Client {
    reqwest::Client::builder()
        .cookie_store(true)
        .build()
        .unwrap()
}

pub async fn wait_ready(client: &reqwest::Client, base: &str) {
    for _ in 0..100 {
        if let Ok(response) = client.get(format!("{base}/readyz")).send().await
            && response.status() == StatusCode::OK
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("server at {base} did not become ready");
}

pub struct TestApp {
    pub base: String,
    pub pool: SqlitePool,
    pub config: Arc<LoadedConfig>,
    pub mqtt_addr: SocketAddr,
    pub presence: Arc<Presence>,
    mqtt: MqttServer,
    telemetry_task: JoinHandle<()>,
    handle: JoinHandle<()>,
    _lock: DataDirLock,
    _dir: TempDir,
}

/// Device fixture: canonical id plus the plaintext token only tests may see.
pub struct TestDevice {
    pub id: String,
    pub token: String,
}

impl TestApp {
    pub fn builder() -> TestAppBuilder {
        TestAppBuilder::default()
    }

    pub async fn start() -> Self {
        Self::builder().start().await
    }

    pub fn client(&self) -> reqwest::Client {
        cookie_client()
    }

    pub async fn create_admin(&self, username: &str, password: &str) {
        let hash = auth::hash_password(password).unwrap();
        let mut tx = self.pool.begin().await.unwrap();
        users::create_admin(&mut tx, username, &hash, auth::now_ms())
            .await
            .unwrap();
        tx.commit().await.unwrap();
    }

    /// Inserts a device plus its active credential, bypassing the Phase 3 API.
    pub async fn create_device(&self, name: &str, enabled: bool) -> TestDevice {
        let id = Uuid::now_v7().to_string();
        let token = format!("iot_{}", auth::generate_session_token().unwrap());
        let now = auth::now_ms();
        sqlx::query(
            "INSERT INTO devices (id, name, metadata_json, enabled, created_at, updated_at)
             VALUES (?, ?, '{}', ?, ?, ?)",
        )
        .bind(&id)
        .bind(name)
        .bind(enabled)
        .bind(now)
        .bind(now)
        .execute(&self.pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO device_credentials (id, device_id, token_hash, status, created_at)
             VALUES (?, ?, ?, 'active', ?)",
        )
        .bind(Uuid::now_v7().to_string())
        .bind(&id)
        .bind(auth::hash_token(&token).to_vec())
        .bind(now)
        .execute(&self.pool)
        .await
        .unwrap();
        TestDevice { id, token }
    }

    /// Revokes the device's active credential without issuing a replacement.
    pub async fn revoke_active_credential(&self, device_id: &str) {
        sqlx::query(
            "UPDATE device_credentials SET status = 'revoked', revoked_at = ?
             WHERE device_id = ? AND status = 'active'",
        )
        .bind(auth::now_ms())
        .bind(device_id)
        .execute(&self.pool)
        .await
        .unwrap();
    }

    /// Revokes the active credential and inserts a fresh one; returns the new token.
    pub async fn rotate_credential(&self, device_id: &str) -> String {
        let token = format!("iot_{}", auth::generate_session_token().unwrap());
        let now = auth::now_ms();
        let mut tx = self.pool.begin().await.unwrap();
        sqlx::query(
            "UPDATE device_credentials SET status = 'revoked', revoked_at = ?
             WHERE device_id = ? AND status = 'active'",
        )
        .bind(now)
        .bind(device_id)
        .execute(&mut *tx)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO device_credentials (id, device_id, token_hash, status, created_at)
             VALUES (?, ?, ?, 'active', ?)",
        )
        .bind(Uuid::now_v7().to_string())
        .bind(device_id)
        .bind(auth::hash_token(&token).to_vec())
        .bind(now)
        .execute(&mut *tx)
        .await
        .unwrap();
        tx.commit().await.unwrap();
        token
    }

    pub async fn last_seen(&self, device_id: &str) -> Option<i64> {
        sqlx::query_scalar("SELECT last_seen_at FROM devices WHERE id = ?")
            .bind(device_id)
            .fetch_one(&self.pool)
            .await
            .unwrap()
    }

    pub fn abort_mqtt(&self) {
        self.mqtt.abort();
    }

    pub async fn retained_count(&self) -> isize {
        self.mqtt.retained_count().await
    }

    /// Restarts the test broker with a fresh in-memory retained store.
    pub async fn restart_mqtt(&mut self) {
        self.mqtt.abort();
        let mut config = self.config.config.mqtt.clone();
        config.listen = "127.0.0.1:0".parse().expect("valid test MQTT address");
        let readiness = Arc::new(AtomicBool::new(false));
        let mqtt = MqttServer::start(
            &config,
            readiness,
            self.pool.clone(),
            Arc::clone(&self.presence),
        )
        .await
        .expect("restart test MQTT server");
        self.mqtt_addr = mqtt.local_addr();
        self.mqtt = mqtt;
    }
}

impl Drop for TestApp {
    fn drop(&mut self) {
        self.mqtt.abort();
        self.telemetry_task.abort();
        self.handle.abort();
    }
}

pub struct TestAppBuilder {
    dashboard: bool,
    cookie_secure: bool,
    metrics_token: Option<String>,
    max_body_bytes: u64,
    login_max_attempts: u64,
    max_retained_messages: u64,
    max_retained_bytes: u64,
}

impl Default for TestAppBuilder {
    fn default() -> Self {
        Self {
            dashboard: true,
            cookie_secure: false,
            metrics_token: None,
            max_body_bytes: 262_144,
            login_max_attempts: 5,
            max_retained_messages: 10_000,
            max_retained_bytes: 16_777_216,
        }
    }
}

impl TestAppBuilder {
    pub fn dashboard(mut self, enabled: bool) -> Self {
        self.dashboard = enabled;
        self
    }

    pub fn cookie_secure(mut self, secure: bool) -> Self {
        self.cookie_secure = secure;
        self
    }

    pub fn metrics_token(mut self, token: &str) -> Self {
        self.metrics_token = Some(token.to_owned());
        self
    }

    pub fn max_body_bytes(mut self, bytes: u64) -> Self {
        self.max_body_bytes = bytes;
        self
    }

    pub fn login_max_attempts(mut self, attempts: u64) -> Self {
        self.login_max_attempts = attempts;
        self
    }

    pub fn retained_limits(mut self, messages: u64, bytes: u64) -> Self {
        self.max_retained_messages = messages;
        self.max_retained_bytes = bytes;
        self
    }

    pub async fn start(self) -> TestApp {
        let dir = TempDir::new().unwrap();
        let config_path = dir.path().join("conf.toml");
        let mqtt_port = free_port();
        let metrics_token_env = self.metrics_token.as_ref().map_or(String::new(), |_| {
            "metrics_bearer_token_env = \"TEST_METRICS_BEARER_TOKEN\"\n".to_owned()
        });
        fs::write(
            &config_path,
            format!(
                "config_version = 1\n\
                 [server]\n\
                 data_dir = \"./data\"\n\
                 [http]\n\
                 listen = \"127.0.0.1:0\"\n\
                 dashboard = {}\n\
                 cookie_secure = {}\n\
                 metrics = {}\n\
                 {}\
                 max_body_bytes = {}\n\
                 [mqtt]\n\
                 listen = \"127.0.0.1:{}\"\n\
                 max_retained_messages = {}\n\
                 max_retained_bytes = {}\n\
                 [database]\n\
                 url = \"sqlite://./data/iot.db\"\n\
                 [auth]\n\
                 login_max_attempts = {}\n",
                self.dashboard,
                self.cookie_secure,
                self.metrics_token.is_some(),
                metrics_token_env,
                self.max_body_bytes,
                mqtt_port,
                self.max_retained_messages,
                self.max_retained_bytes,
                self.login_max_attempts,
            ),
        )
        .unwrap();

        let metrics_token = self.metrics_token.clone();
        let loaded = Arc::new(
            config::load_with_env(&config_path, move |name| {
                (name == "TEST_METRICS_BEARER_TOKEN")
                    .then(|| metrics_token.clone())
                    .flatten()
            })
            .unwrap(),
        );
        let lock = DataDirLock::try_new(&loaded.data_dir).unwrap();
        let pool = pool::connect(&loaded).await.unwrap();
        migrate::run(&pool).await.unwrap();

        let mqtt_ready = Arc::new(AtomicBool::new(false));
        let presence = Arc::new(Presence::new());
        let telemetry = &loaded.config.telemetry;
        let events = EventBus::new();
        let limits = TelemetryPayload::try_from(telemetry).unwrap();
        let telemetry_ready = Arc::new(AtomicBool::new(true));
        let (ingest, telemetry_task) = telemetry_core::spawn_worker(
            pool.clone(),
            events.clone(),
            telemetry.queue_capacity as usize,
            telemetry.batch_size as usize,
            telemetry.max_batch_bytes as usize,
            Duration::from_millis(telemetry.flush_interval_ms),
            Arc::clone(&telemetry_ready),
        );
        let mqtt = MqttServer::start_with_ingest(
            &loaded.config.mqtt,
            Arc::clone(&mqtt_ready),
            pool.clone(),
            Arc::clone(&presence),
            ingest,
            limits,
            telemetry.queue_capacity as usize,
        )
        .await
        .unwrap();
        let mqtt_addr = mqtt.local_addr();
        let state = AppState::new(
            pool.clone(),
            Arc::clone(&loaded),
            mqtt_ready,
            telemetry_ready,
            events,
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let app = router::build(state);
        let handle = tokio::spawn(async move {
            axum::serve(
                listener,
                app.into_make_service_with_connect_info::<SocketAddr>(),
            )
            .await
            .unwrap();
        });

        let base = format!("http://{addr}");
        wait_ready(&cookie_client(), &base).await;

        TestApp {
            base,
            pool,
            config: loaded,
            mqtt_addr,
            presence,
            mqtt,
            telemetry_task,
            handle,
            _lock: lock,
            _dir: dir,
        }
    }
}

/// MQTT connection options with `client_id`/`username`/token from the fixture.
pub fn mqtt_options(
    addr: SocketAddr,
    client_id: &str,
    username: &str,
    token: &str,
    clean_session: bool,
) -> MqttOptions {
    let mut options = MqttOptions::new(client_id, addr.ip().to_string(), addr.port());
    options
        .set_keep_alive(Duration::from_secs(30))
        .set_clean_session(clean_session)
        .set_credentials(username, token);
    options
}

/// Connects and returns the CONNACK `session_present` flag; panics if refused.
pub async fn mqtt_connect(
    addr: SocketAddr,
    client_id: &str,
    username: &str,
    token: &str,
    clean_session: bool,
) -> (AsyncClient, EventLoop, bool) {
    let options = mqtt_options(addr, client_id, username, token, clean_session);
    let (client, mut eventloop) = AsyncClient::new(options, 32);
    let session_present = match mqtt_next(&mut eventloop, Duration::from_secs(5)).await {
        Ok(MqttEvent::Incoming(Packet::ConnAck(ack))) => {
            assert_eq!(ack.code, ConnectReturnCode::Success);
            ack.session_present
        }
        Ok(other) => panic!("expected CONNACK, got {other:?}"),
        Err(error) => panic!("connect failed: {error:?}"),
    };
    (client, eventloop, session_present)
}

/// Connects expecting a refusal and returns the CONNACK code.
pub async fn mqtt_connect_refused(
    addr: SocketAddr,
    client_id: &str,
    username: &str,
    token: &str,
) -> ConnectReturnCode {
    let options = mqtt_options(addr, client_id, username, token, true);
    let (client, mut eventloop) = AsyncClient::new(options, 8);
    let result = mqtt_next(&mut eventloop, Duration::from_secs(5)).await;
    drop(client);
    match result {
        Err(error) => match *error {
            ConnectionError::ConnectionRefused(code) => code,
            other => panic!("expected refused connect, got {other:?}"),
        },
        Ok(other) => panic!("expected refused connect, got {other:?}"),
    }
}

pub async fn mqtt_next(
    eventloop: &mut EventLoop,
    dur: Duration,
) -> Result<MqttEvent, Box<ConnectionError>> {
    match tokio::time::timeout(dur, eventloop.poll()).await {
        Ok(result) => result.map_err(Box::new),
        Err(_) => panic!("timeout waiting for MQTT event"),
    }
}

/// Waits until the client observes the connection closing.
pub async fn mqtt_wait_closed(eventloop: &mut EventLoop, dur: Duration) {
    let deadline = Instant::now() + dur;
    loop {
        let left = deadline.saturating_duration_since(Instant::now());
        if left.is_zero() {
            panic!("connection stayed open");
        }
        match tokio::time::timeout(left, eventloop.poll()).await {
            Ok(Ok(_)) => {}
            Ok(Err(_)) => return,
            Err(_) => panic!("connection stayed open"),
        }
    }
}

/// Waits until `check` holds, polling every 20 ms.
pub async fn wait_until<F: FnMut() -> bool>(mut check: F, dur: Duration, what: &str) {
    let deadline = Instant::now() + dur;
    loop {
        if check() {
            return;
        }
        if Instant::now() >= deadline {
            panic!("timed out waiting for {what}");
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// Sends a raw CONNECT and returns the broker's answer bytes.
/// MQTT 3.1 (level 3) is not supported by rumqttc, so rejection is checked on the wire.
pub async fn raw_connack(
    addr: SocketAddr,
    proto_ver: u8,
    client_id: &str,
    username: &str,
    password: &str,
) -> Vec<u8> {
    let mut stream =
        tokio::time::timeout(Duration::from_secs(5), tokio::net::TcpStream::connect(addr))
            .await
            .unwrap()
            .unwrap();
    stream
        .write_all(&connect_packet(proto_ver, client_id, username, password))
        .await
        .unwrap();
    let mut buf = [0u8; 64];
    let read = tokio::time::timeout(Duration::from_secs(5), stream.read(&mut buf))
        .await
        .unwrap()
        .unwrap();
    buf[..read].to_vec()
}

fn push_string(buf: &mut Vec<u8>, value: &[u8]) {
    buf.extend_from_slice(&(value.len() as u16).to_be_bytes());
    buf.extend_from_slice(value);
}

/// Connects raw, sends a QoS 1 PUBLISH, and reports whether the broker closed
/// the socket without a PUBACK.
///
/// rumqttc refuses to send wildcard topic names, so publish-filter denials are
/// checked on the wire.
pub async fn raw_publish_denied(
    addr: SocketAddr,
    client_id: &str,
    username: &str,
    password: &str,
    topic: &str,
    retain: bool,
) -> bool {
    let mut stream =
        tokio::time::timeout(Duration::from_secs(5), tokio::net::TcpStream::connect(addr))
            .await
            .unwrap()
            .unwrap();
    stream
        .write_all(&connect_packet(4, client_id, username, password))
        .await
        .unwrap();
    let mut connack = [0u8; 4];
    tokio::time::timeout(Duration::from_secs(5), stream.read_exact(&mut connack))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(connack, [0x20, 0x02, 0x00, 0x00], "raw connect was refused");

    let mut packet = vec![if retain { 0x33 } else { 0x32 }];
    let mut body = Vec::new();
    push_string(&mut body, topic.as_bytes());
    body.extend_from_slice(&1u16.to_be_bytes());
    packet.push(body.len() as u8);
    packet.extend_from_slice(&body);
    stream.write_all(&packet).await.unwrap();

    let mut buf = [0u8; 8];
    match tokio::time::timeout(Duration::from_secs(5), stream.read(&mut buf)).await {
        Ok(Ok(0)) | Err(_) => true,
        Ok(Ok(read)) => buf[..read] != [0x40, 0x02, 0x00, 0x01],
        Ok(Err(_)) => true,
    }
}

fn connect_packet(proto_ver: u8, client_id: &str, username: &str, password: &str) -> Vec<u8> {
    let protocol_name: &[u8] = if proto_ver == 3 { b"MQIsdp" } else { b"MQTT" };
    let mut body = Vec::new();
    body.extend_from_slice(&(protocol_name.len() as u16).to_be_bytes());
    body.extend_from_slice(protocol_name);
    body.push(proto_ver);
    body.push(0x02 | 0x80 | 0x40); // clean session + username + password
    body.extend_from_slice(&30u16.to_be_bytes());
    if proto_ver == 5 {
        body.push(0); // empty CONNECT properties
    }
    push_string(&mut body, client_id.as_bytes());
    push_string(&mut body, username.as_bytes());
    push_string(&mut body, password.as_bytes());

    let mut packet = vec![0x10];
    let mut remaining = body.len();
    loop {
        let mut byte = (remaining % 128) as u8;
        remaining /= 128;
        if remaining > 0 {
            byte |= 0x80;
        }
        packet.push(byte);
        if remaining == 0 {
            break;
        }
    }
    packet.extend_from_slice(&body);
    packet
}

/// Decodes the CONNACK reason/return code from raw bytes.
pub fn connack_code(bytes: &[u8], proto_ver: u8) -> u8 {
    assert!(bytes.len() >= 4, "short CONNACK: {bytes:02x?}");
    assert_eq!(bytes[0], 0x20, "not a CONNACK: {bytes:02x?}");
    if proto_ver == 5 {
        assert!(bytes.len() >= 5, "short MQTT 5 CONNACK: {bytes:02x?}");
        assert_eq!(
            bytes[1], 3,
            "unexpected MQTT 5 CONNACK length: {bytes:02x?}"
        );
        assert_eq!(bytes[2], 0, "session present must be false");
        assert_eq!(bytes[4], 0, "expected empty properties: {bytes:02x?}");
        bytes[3]
    } else {
        assert_eq!(
            bytes[1], 2,
            "unexpected MQTT 3 CONNACK length: {bytes:02x?}"
        );
        assert_eq!(bytes[2], 0, "session present must be false");
        bytes[3]
    }
}
