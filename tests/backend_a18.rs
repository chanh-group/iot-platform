mod common;

use std::{
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::Arc,
    time::Duration,
};

use common::{
    free_port, mqtt_connect, mqtt_connect_refused, mqtt_publish, mqtt_subscribe, mqtt_wait_closed,
    mqtt_wait_puback, mqtt_wait_publish, mqtt_wait_telemetry_ack, raw_publish_denied,
};
use futures_util::{SinkExt, StreamExt};
use reqwest::StatusCode;
use rumqttc::{ConnectReturnCode, Event, Packet, QoS, SubscribeReasonCode};
use serde_json::{Value, json};
use sqlx::{PgPool, SqlitePool};
use tempfile::TempDir;
use tokio::{
    net::TcpStream,
    sync::{Notify, mpsc, oneshot},
    time::timeout,
};
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
        let stdout = std::fs::File::create(config.with_extension("log"))
            .expect("create isolated server log");
        let stderr = stdout.try_clone().expect("clone server log handle");
        Self(Some(
            Command::new(env!("CARGO_BIN_EXE_iot-server"))
                .args(["run", "--config"])
                .arg(config)
                .stdout(Stdio::from(stdout))
                .stderr(Stdio::from(stderr))
                .spawn()
                .expect("spawn server"),
        ))
    }

    #[cfg(debug_assertions)]
    fn spawn_paused(config: &Path, gate: &Path) -> Self {
        Self(Some(
            Command::new(env!("CARGO_BIN_EXE_iot-server"))
                .args(["run", "--config"])
                .arg(config)
                .env("IOT_TEST_PAUSE_BEFORE_COMMIT", gate)
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .spawn()
                .expect("spawn paused server"),
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

    #[cfg(debug_assertions)]
    async fn terminate_with_output(&mut self) -> std::process::Output {
        let child = self.0.take().expect("server is running");
        let pid = child.id().to_string();
        let signal = tokio::task::spawn_blocking(move || {
            Command::new("kill").args(["-TERM", &pid]).status().unwrap()
        })
        .await
        .unwrap();
        assert!(signal.success(), "send SIGTERM");
        tokio::task::spawn_blocking(move || child.wait_with_output())
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
                 [server]\ndata_dir = './data'\nshutdown_timeout_ms = 1000\n\
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
        self.wait_ready(&mut server).await;
        server
    }

    #[cfg(debug_assertions)]
    async fn start_paused(&self, gate: &Path) -> AppProcess {
        let mut server = AppProcess::spawn_paused(&self.config, gate);
        self.wait_ready(&mut server).await;
        server
    }

    async fn wait_ready(&self, server: &mut AppProcess) {
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

    async fn seed_cleanup_sessions(&self) -> (String, String, String) {
        let user_id: String = match &self.db {
            TestDb::Sqlite(pool) => {
                sqlx::query_scalar("SELECT id FROM users LIMIT 1")
                    .fetch_one(pool)
                    .await
            }
            TestDb::Postgres(pool) => {
                sqlx::query_scalar("SELECT id FROM users LIMIT 1")
                    .fetch_one(pool)
                    .await
            }
        }
        .unwrap();
        let now = iot_server::core::auth::now_ms();
        let expired = Uuid::now_v7().to_string();
        let revoked = Uuid::now_v7().to_string();
        let live = Uuid::now_v7().to_string();
        let expired_hash = iot_server::core::auth::hash_token(&expired);
        let revoked_hash = iot_server::core::auth::hash_token(&revoked);
        let live_hash = iot_server::core::auth::hash_token(&live);
        match &self.db {
            TestDb::Sqlite(pool) => {
                sqlx::query(
                    "INSERT INTO sessions
                     (id, user_id, token_hash, created_at, last_seen_at, expires_at, revoked_at)
                     VALUES (?, ?, ?, ?, ?, ?, NULL), (?, ?, ?, ?, ?, ?, ?), (?, ?, ?, ?, ?, ?, NULL)",
                )
                .bind(&expired)
                .bind(&user_id)
                .bind(expired_hash.as_slice())
                .bind(now - 1_000)
                .bind(now - 1_000)
                .bind(now - 1)
                .bind(&revoked)
                .bind(&user_id)
                .bind(revoked_hash.as_slice())
                .bind(now)
                .bind(now)
                .bind(now + 86_400_000)
                .bind(now - 8 * 86_400_000)
                .bind(&live)
                .bind(&user_id)
                .bind(live_hash.as_slice())
                .bind(now)
                .bind(now)
                .bind(now + 86_400_000)
                .execute(pool)
                .await
                .unwrap();
            }
            TestDb::Postgres(pool) => {
                sqlx::query(
                    "INSERT INTO sessions
                     (id, user_id, token_hash, created_at, last_seen_at, expires_at, revoked_at)
                     VALUES ($1, $2, $3, $4, $5, $6, NULL),
                            ($7, $8, $9, $10, $11, $12, $13),
                            ($14, $15, $16, $17, $18, $19, NULL)",
                )
                .bind(&expired)
                .bind(&user_id)
                .bind(expired_hash.as_slice())
                .bind(now - 1_000)
                .bind(now - 1_000)
                .bind(now - 1)
                .bind(&revoked)
                .bind(&user_id)
                .bind(revoked_hash.as_slice())
                .bind(now)
                .bind(now)
                .bind(now + 86_400_000)
                .bind(now - 8 * 86_400_000)
                .bind(&live)
                .bind(&user_id)
                .bind(live_hash.as_slice())
                .bind(now)
                .bind(now)
                .bind(now + 86_400_000)
                .execute(pool)
                .await
                .unwrap();
            }
        }
        (expired, revoked, live)
    }

    async fn session_exists(&self, id: &str) -> bool {
        match &self.db {
            TestDb::Sqlite(pool) => {
                sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM sessions WHERE id = ?)")
                    .bind(id)
                    .fetch_one(pool)
                    .await
            }
            TestDb::Postgres(pool) => {
                sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM sessions WHERE id = $1)")
                    .bind(id)
                    .fetch_one(pool)
                    .await
            }
        }
        .unwrap()
    }

    async fn expire_session_for_token(&self, token_hash: &[u8]) {
        let now = iot_server::core::auth::now_ms();
        match &self.db {
            TestDb::Sqlite(pool) => {
                sqlx::query(
                    "UPDATE sessions SET created_at = ? - 1, expires_at = ? WHERE token_hash = ?",
                )
                .bind(now - 1)
                .bind(now - 1)
                .bind(token_hash)
                .execute(pool)
                .await
                .unwrap();
            }
            TestDb::Postgres(pool) => {
                sqlx::query("UPDATE sessions SET created_at = $1::bigint - 1, expires_at = $1 WHERE token_hash = $2")
                    .bind(now - 1)
                    .bind(token_hash)
                    .execute(pool)
                    .await
                    .unwrap();
            }
        }
    }

    async fn wait_for_session_cleanup(&self, expired: &str, revoked: &str, live: &str) {
        timeout(Duration::from_secs(5), async {
            loop {
                if !self.session_exists(expired).await
                    && !self.session_exists(revoked).await
                    && self.session_exists(live).await
                {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("scheduled session cleanup removes only expired and old-revoked sessions");
    }

    async fn set_command_expired(&self, command_id: &str, now: i64) {
        match &self.db {
            TestDb::Sqlite(pool) => {
                sqlx::query("UPDATE commands SET expires_at = ?, next_attempt_at = ? WHERE id = ?")
                    .bind(now - 1)
                    .bind(now - 1)
                    .bind(command_id)
                    .execute(pool)
                    .await
                    .unwrap();
            }
            TestDb::Postgres(pool) => {
                sqlx::query(
                    "UPDATE commands SET expires_at = $1, next_attempt_at = $2 WHERE id = $3",
                )
                .bind(now - 1)
                .bind(now - 1)
                .bind(command_id)
                .execute(pool)
                .await
                .unwrap();
            }
        }
    }

    async fn command_state(&self, command_id: &str) -> (String, i64, i64) {
        match &self.db {
            TestDb::Sqlite(pool) => {
                sqlx::query_as(
                    "SELECT status, attempts, next_attempt_at FROM commands WHERE id = ?",
                )
                .bind(command_id)
                .fetch_one(pool)
                .await
            }
            TestDb::Postgres(pool) => {
                sqlx::query_as(
                    "SELECT status, attempts, next_attempt_at FROM commands WHERE id = $1",
                )
                .bind(command_id)
                .fetch_one(pool)
                .await
            }
        }
        .unwrap()
    }

    async fn command_late_ack_recorded(&self, command_id: &str) -> bool {
        match &self.db {
            TestDb::Sqlite(pool) => {
                sqlx::query_scalar("SELECT late_ack_at IS NOT NULL FROM commands WHERE id = ?")
                    .bind(command_id)
                    .fetch_one(pool)
                    .await
            }
            TestDb::Postgres(pool) => {
                sqlx::query_scalar("SELECT late_ack_at IS NOT NULL FROM commands WHERE id = $1")
                    .bind(command_id)
                    .fetch_one(pool)
                    .await
            }
        }
        .unwrap()
    }

    async fn command_ack_projection(
        &self,
        command_id: &str,
    ) -> (String, Option<i64>, bool, bool, bool, bool) {
        match &self.db {
            TestDb::Sqlite(pool) => {
                sqlx::query_as(
                    "SELECT status, completed_at, result_json IS NULL, error IS NULL,
                            late_ack_at IS NULL, late_ack_json IS NULL
                     FROM commands WHERE id = ?",
                )
                .bind(command_id)
                .fetch_one(pool)
                .await
            }
            TestDb::Postgres(pool) => {
                sqlx::query_as(
                    "SELECT status, completed_at, result_json IS NULL, error IS NULL,
                            late_ack_at IS NULL, late_ack_json IS NULL
                     FROM commands WHERE id = $1",
                )
                .bind(command_id)
                .fetch_one(pool)
                .await
            }
        }
        .unwrap()
    }

    async fn make_command_due(&self, command_id: &str, now: i64) {
        match &self.db {
            TestDb::Sqlite(pool) => {
                sqlx::query("UPDATE commands SET next_attempt_at = ? WHERE id = ?")
                    .bind(now - 1)
                    .bind(command_id)
                    .execute(pool)
                    .await
                    .unwrap();
            }
            TestDb::Postgres(pool) => {
                sqlx::query("UPDATE commands SET next_attempt_at = $1 WHERE id = $2")
                    .bind(now - 1)
                    .bind(command_id)
                    .execute(pool)
                    .await
                    .unwrap();
            }
        }
    }

    async fn telemetry_count(&self, device_id: &str, event_id: &str) -> i64 {
        match &self.db {
            TestDb::Sqlite(pool) => {
                sqlx::query_scalar(
                    "SELECT COUNT(*) FROM telemetry_events WHERE device_id = ? AND event_id = ?",
                )
                .bind(device_id)
                .bind(event_id)
                .fetch_one(pool)
                .await
            }
            TestDb::Postgres(pool) => {
                sqlx::query_scalar(
                    "SELECT COUNT(*) FROM telemetry_events WHERE device_id = $1 AND event_id = $2",
                )
                .bind(device_id)
                .bind(event_id)
                .fetch_one(pool)
                .await
            }
        }
        .unwrap()
    }

    async fn attribute_count(&self, device_id: &str) -> i64 {
        match &self.db {
            TestDb::Sqlite(pool) => {
                sqlx::query_scalar("SELECT COUNT(*) FROM device_attributes WHERE device_id = ?")
                    .bind(device_id)
                    .fetch_one(pool)
                    .await
            }
            TestDb::Postgres(pool) => {
                sqlx::query_scalar("SELECT COUNT(*) FROM device_attributes WHERE device_id = $1")
                    .bind(device_id)
                    .fetch_one(pool)
                    .await
            }
        }
        .unwrap()
    }

    async fn wait_for_log(&self, fragment: &str) {
        let path = self.config.with_extension("log");
        timeout(Duration::from_secs(5), async {
            loop {
                if std::fs::read_to_string(&path).is_ok_and(|contents| contents.contains(fragment))
                {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("server log confirms the ACK worker processed the message");
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

struct FailedPublishChannel;

#[async_trait::async_trait]
impl iot_server::core::device_channel::DeviceChannel for FailedPublishChannel {
    async fn publish(
        &self,
        _device_id: &str,
        _topic: &str,
        _payload: &[u8],
    ) -> Result<(), iot_server::core::device_channel::DeviceChannelError> {
        Err(iot_server::core::device_channel::DeviceChannelError::Unavailable)
    }

    async fn kick(
        &self,
        _device_id: &str,
    ) -> Result<(), iot_server::core::device_channel::DeviceChannelError> {
        Ok(())
    }

    async fn is_online(
        &self,
        _device_id: &str,
    ) -> Result<bool, iot_server::core::device_channel::DeviceChannelError> {
        Ok(true)
    }
}

async fn run_a18_contracts(postgres_url: Option<String>) {
    let fixture = Fixture::new(postgres_url.as_deref()).await;
    let cleanup_sessions = fixture.seed_cleanup_sessions().await;
    let mut server = fixture.start().await;
    fixture
        .wait_for_session_cleanup(
            &cleanup_sessions.0,
            &cleanup_sessions.1,
            &cleanup_sessions.2,
        )
        .await;
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
    let device_e = create_device(&http, &fixture.base, "a18-device-e").await;
    let disabled_device = create_device(&http, &fixture.base, "a18-disabled-device").await;
    let disabled = http
        .patch(format!(
            "{}/api/v1/devices/{}",
            fixture.base, disabled_device.id
        ))
        .header("X-IoT-CSRF", "1")
        .json(&json!({"enabled":false}))
        .send()
        .await
        .unwrap();
    assert_eq!(disabled.status(), StatusCode::OK);
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
    assert_eq!(
        mqtt_connect_refused(
            fixture.mqtt_addr,
            &disabled_device.id,
            &disabled_device.id,
            &disabled_device.token,
        )
        .await,
        ConnectReturnCode::BadUserNamePassword,
        "disabled-device credentials have the same refusal result"
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
    for filter in [
        format!("devices/{}/telemetry/ack", device_a.id),
        format!("devices/{}/commands", device_a.id),
        format!("devices/{}/status", device_a.id),
        format!("devices/{}/data/#", device_a.id),
        format!("devices/{}/data/sensors/+", device_a.id),
        format!("devices/{}/commands", device_b.id),
        format!("devices/{}/data/#", device_b.id),
        "devices/+/telemetry".to_owned(),
        "devices/#".to_owned(),
        device_a.id.clone(),
        format!("devices/{}/#", device_a.id),
        format!("devices/{}/telemetry", device_a.id),
        format!("devices/{}/attributes", device_a.id),
        "$SYS/#".to_owned(),
        format!("$share/group/devices/{}/data/#", device_a.id),
    ] {
        let expected = if filter == format!("devices/{}/data/#", device_a.id)
            || filter == format!("devices/{}/data/sensors/+", device_a.id)
            || filter == format!("devices/{}/commands", device_a.id)
            || filter == format!("devices/{}/status", device_a.id)
            || filter == format!("devices/{}/telemetry/ack", device_a.id)
        {
            SubscribeReasonCode::Success(QoS::AtLeastOnce)
        } else {
            SubscribeReasonCode::Failure
        };
        assert_eq!(
            mqtt_subscribe(&acl_client, &mut acl_eventloop, &filter, QoS::AtLeastOnce).await,
            expected,
            "ACL filter {filter}"
        );
    }
    for topic in [
        format!("devices/{}/attributes", device_a.id),
        format!("devices/{}/status", device_a.id),
        format!("devices/{}/data/sensors/room", device_a.id),
    ] {
        mqtt_publish(&acl_client, &topic, QoS::AtLeastOnce, false, b"{}").await;
        mqtt_wait_puback(&mut acl_eventloop, Duration::from_secs(5)).await;
    }
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
    for topic in [
        format!("devices/{}/telemetry/ack", device_a.id),
        format!("devices/{}/commands", device_a.id),
        "$SYS/brokers".to_owned(),
        format!("$share/group/devices/{}/data/x", device_a.id),
    ] {
        assert_publish_denied(fixture.mqtt_addr, &device_a, &topic, false).await;
    }
    for topic in [
        format!("devices/{}/data/+", device_a.id),
        format!("devices/{}/telemetry/#", device_a.id),
    ] {
        assert!(
            raw_publish_denied(
                fixture.mqtt_addr,
                &device_a.id,
                &device_a.id,
                &device_a.token,
                &topic,
                false,
            )
            .await,
            "wildcard publish must be rejected: {topic}"
        );
    }
    assert_publish_denied(
        fixture.mqtt_addr,
        &device_a,
        &format!("devices/{}/commands", device_a.id),
        true,
    )
    .await;
    let (retained_client, mut retained_eventloop, _) = mqtt_connect(
        fixture.mqtt_addr,
        &device_e.id,
        &device_e.id,
        &device_e.token,
        true,
    )
    .await;
    let retained_ack_topic = format!("devices/{}/telemetry/ack", device_e.id);
    assert!(matches!(
        mqtt_subscribe(
            &retained_client,
            &mut retained_eventloop,
            &retained_ack_topic,
            QoS::AtLeastOnce,
        )
        .await,
        SubscribeReasonCode::Success(_)
    ));
    let retained_event_id = format!("a18-retained-{}", Uuid::now_v7());
    mqtt_publish(
        &retained_client,
        &format!("devices/{}/telemetry", device_e.id),
        QoS::AtLeastOnce,
        true,
        &telemetry_payload(&retained_event_id, 33.0),
    )
    .await;
    let retained_ack = mqtt_wait_telemetry_ack(
        &mut retained_eventloop,
        &retained_ack_topic,
        Duration::from_secs(5),
    )
    .await;
    assert_eq!(retained_ack["status"], "rejected");
    assert_eq!(retained_ack["code"], "retained_not_allowed");
    assert_eq!(
        fixture
            .telemetry_count(&device_e.id, &retained_event_id)
            .await,
        0
    );
    drop(retained_client);
    let (retained_attributes_client, mut retained_attributes_eventloop, _) = mqtt_connect(
        fixture.mqtt_addr,
        &device_e.id,
        &device_e.id,
        &device_e.token,
        true,
    )
    .await;
    mqtt_publish(
        &retained_attributes_client,
        &format!("devices/{}/attributes", device_e.id),
        QoS::AtLeastOnce,
        true,
        br#"{"values":{"firmware":"must-not-store"}}"#,
    )
    .await;
    mqtt_wait_closed(&mut retained_attributes_eventloop, Duration::from_secs(3)).await;
    drop(retained_attributes_client);
    fixture
        .wait_for_log("retained attributes publish rejected")
        .await;
    assert_eq!(fixture.attribute_count(&device_e.id).await, 0);
    assert_retained_rejections_do_not_use_broker_storage(&fixture, &device_e).await;

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
    let race_response = if statuses[0] == StatusCode::ACCEPTED {
        race_a
    } else {
        race_b
    };
    let race_command: Value = race_response.json().await.unwrap();
    let race_command_id = race_command["command_id"].as_str().unwrap().to_owned();
    let wrong_device_state_before = fixture.command_ack_projection(&race_command_id).await;
    mqtt_publish(
        &b_client,
        &format!("devices/{}/commands/ack", device_b.id),
        QoS::AtLeastOnce,
        false,
        json!({
            "command_id": race_command_id.clone(),
            "status": "succeeded",
            "result": {"wrong_device": true}
        })
        .to_string()
        .as_bytes(),
    )
    .await;
    mqtt_wait_puback(&mut b_eventloop, Duration::from_secs(5)).await;
    fixture
        .wait_for_log("cross-device command ACK ignored")
        .await;
    assert_eq!(
        fixture.command_ack_projection(&race_command_id).await,
        wrong_device_state_before,
        "processed wrong-device ACK leaves command and ACK fields unchanged"
    );

    fixture
        .set_command_expired(&race_command_id, iot_server::core::auth::now_ms())
        .await;
    timeout(Duration::from_secs(5), async {
        loop {
            if fixture.command_state(&race_command_id).await.0 == "expired" {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("command dispatcher expires a due command");
    let (c_client, mut c_eventloop, _) = mqtt_connect(
        fixture.mqtt_addr,
        &device_c.id,
        &device_c.id,
        &device_c.token,
        true,
    )
    .await;
    mqtt_publish(
        &c_client,
        &format!("devices/{}/commands/ack", device_c.id),
        QoS::AtLeastOnce,
        false,
        json!({
            "command_id": race_command_id,
            "status": "succeeded",
            "result": {"late": true}
        })
        .to_string()
        .as_bytes(),
    )
    .await;
    mqtt_wait_puback(&mut c_eventloop, Duration::from_secs(5)).await;
    timeout(Duration::from_secs(5), async {
        while !fixture.command_late_ack_recorded(&race_command_id).await {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("late ACK is recorded without reopening an expired command");
    assert_eq!(fixture.command_state(&race_command_id).await.0, "expired");
    drop(c_client);

    let failed_dispatch = post_command(
        &http,
        &fixture.base,
        &device_e.id,
        &json!({
            "name":"dispatcher-retry",
            "idempotency_key":format!("dispatcher-retry-{}", Uuid::now_v7())
        })
        .to_string(),
    )
    .await;
    assert_eq!(failed_dispatch.status(), StatusCode::ACCEPTED);
    let failed_dispatch: Value = failed_dispatch.json().await.unwrap();
    let failed_dispatch_id = failed_dispatch["command_id"].as_str().unwrap().to_owned();
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

    let expired_client = reqwest::Client::builder()
        .cookie_store(true)
        .build()
        .unwrap();
    let expired_login = expired_client
        .post(format!("{}/api/v1/auth/login", fixture.base))
        .header("X-IoT-CSRF", "1")
        .json(&json!({"username":"admin","password":ADMIN_PASSWORD}))
        .send()
        .await
        .unwrap();
    assert_eq!(expired_login.status(), StatusCode::OK);
    let cookie = expired_login.headers()[reqwest::header::SET_COOKIE]
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap();
    let token = cookie.split_once('=').unwrap().1;
    let token_hash = iot_server::core::auth::hash_token(token);
    fixture.expire_session_for_token(&token_hash).await;
    assert_eq!(
        expired_client
            .get(format!("{}/api/v1/auth/me", fixture.base))
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        http.post(format!("{}/api/v1/auth/logout", fixture.base))
            .header("X-IoT-CSRF", "1")
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::NO_CONTENT
    );
    assert_eq!(
        http.get(format!("{}/api/v1/auth/me", fixture.base))
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::UNAUTHORIZED
    );
    assert!(restarted.terminate().await.success());

    fixture
        .make_command_due(&failed_dispatch_id, iot_server::core::auth::now_ms())
        .await;
    let loaded = iot_server::config::load(&fixture.config).unwrap();
    let storage = iot_server::storage::runtime::StorageRuntime::connect(&loaded)
        .await
        .unwrap();
    let (wake_tx, wake_rx) = mpsc::channel(1);
    drop(wake_tx);
    let dispatcher = iot_server::core::command::CommandDispatcher::new(
        storage.repositories.commands.clone(),
        Arc::new(FailedPublishChannel),
        loaded.config.commands.clone(),
        iot_server::core::command::PendingCommandAcks::default(),
        iot_server::core::command::CommandWakeReceiver::new(wake_rx, Arc::new(Notify::new())),
    );
    let before = fixture.command_state(&failed_dispatch_id).await;
    let dispatch_now = iot_server::core::auth::now_ms();
    dispatcher.run_once(dispatch_now).await.unwrap();
    let after = fixture.command_state(&failed_dispatch_id).await;
    assert_eq!(
        after.0, "pending",
        "failed publish does not complete a command"
    );
    assert_eq!(after.1, before.1 + 1, "failed publish records one attempt");
    assert!(after.2 > dispatch_now, "failed publish schedules a retry");
    storage.close().await;

    #[cfg(debug_assertions)]
    shutdown_deadline_does_not_ack_uncommitted_postgres_or_sqlite_work(&fixture, &device_b).await;
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

async fn assert_publish_denied(
    mqtt_addr: std::net::SocketAddr,
    device: &Device,
    topic: &str,
    retain: bool,
) {
    let (client, mut eventloop, _) =
        mqtt_connect(mqtt_addr, &device.id, &device.id, &device.token, true).await;
    mqtt_publish(&client, topic, QoS::AtLeastOnce, retain, b"{}").await;
    mqtt_wait_closed(&mut eventloop, Duration::from_secs(3)).await;
    drop(client);
}

async fn assert_retained_rejections_do_not_use_broker_storage(fixture: &Fixture, device: &Device) {
    let loaded = iot_server::config::load(&fixture.config).unwrap();
    let storage = iot_server::storage::runtime::StorageRuntime::connect(&loaded)
        .await
        .unwrap();
    let telemetry = &loaded.config.telemetry;
    let telemetry_cancel = tokio_util::sync::CancellationToken::new();
    let telemetry_ready = Arc::new(std::sync::atomic::AtomicBool::new(true));
    let (ingest, telemetry_task) = iot_server::core::telemetry::spawn_worker(
        storage.repositories.telemetry.clone(),
        iot_server::core::events::EventBus::new(),
        telemetry.queue_capacity as usize,
        telemetry.max_batch_bytes as usize,
        loaded.runtime_config.clone(),
        iot_server::core::telemetry::IngestWorkerControl {
            cancellation: telemetry_cancel.clone(),
            readiness: Arc::clone(&telemetry_ready),
        },
    );
    let limits = iot_server::core::telemetry::TelemetryPayload::try_from(telemetry).unwrap();
    let mut config = loaded.config.mqtt.clone();
    config.listen = "127.0.0.1:0".parse().unwrap();
    let mqtt = iot_server::mqtt::server::MqttServer::start_with_ingest(
        &config,
        Arc::new(std::sync::atomic::AtomicBool::new(false)),
        iot_server::mqtt::server::MqttRepositories {
            devices: storage.repositories.devices.clone(),
            commands: storage.repositories.commands.clone(),
            attributes: storage.repositories.attributes.clone(),
        },
        Arc::new(iot_server::core::presence::Presence::new()),
        (ingest, limits, telemetry.queue_capacity as usize),
    )
    .await
    .unwrap();
    let retained_before = mqtt.retained_count().await;
    let (client, mut eventloop, _) = mqtt_connect(
        mqtt.local_addr(),
        &device.id,
        &device.id,
        &device.token,
        true,
    )
    .await;
    let telemetry_ack_topic = format!("devices/{}/telemetry/ack", device.id);
    assert!(matches!(
        mqtt_subscribe(
            &client,
            &mut eventloop,
            &telemetry_ack_topic,
            QoS::AtLeastOnce,
        )
        .await,
        SubscribeReasonCode::Success(_)
    ));

    mqtt_publish(
        &client,
        &format!("devices/{}/status", device.id),
        QoS::AtLeastOnce,
        true,
        br#"{"status":"online"}"#,
    )
    .await;
    mqtt_wait_puback(&mut eventloop, Duration::from_secs(5)).await;
    let retained_after_allowed_status = mqtt.retained_count().await;
    assert_eq!(retained_after_allowed_status, retained_before + 1);

    mqtt_publish(
        &client,
        &format!("devices/{}/telemetry", device.id),
        QoS::AtLeastOnce,
        true,
        &telemetry_payload("retained-count-probe", 1.0),
    )
    .await;
    let retained_ack =
        mqtt_wait_telemetry_ack(&mut eventloop, &telemetry_ack_topic, Duration::from_secs(5)).await;
    assert_eq!(retained_ack["status"], "rejected");
    assert_eq!(retained_ack["code"], "retained_not_allowed");
    assert_eq!(mqtt.retained_count().await, retained_after_allowed_status);

    mqtt_publish(
        &client,
        &format!("devices/{}/attributes", device.id),
        QoS::AtLeastOnce,
        true,
        br#"{"values":{"firmware":"retained-count-probe"}}"#,
    )
    .await;
    mqtt_wait_closed(&mut eventloop, Duration::from_secs(3)).await;
    assert_eq!(mqtt.retained_count().await, retained_after_allowed_status);

    drop(client);
    mqtt.abort();
    drop(mqtt);
    telemetry_cancel.cancel();
    telemetry_task.await.unwrap();
    storage.close().await;
}

#[cfg(debug_assertions)]
fn paused_path(gate: &Path) -> PathBuf {
    let mut name = gate.as_os_str().to_owned();
    name.push(".paused");
    PathBuf::from(name)
}

#[cfg(debug_assertions)]
async fn wait_for_pause(gate: &Path) {
    timeout(Duration::from_secs(5), async {
        while !paused_path(gate).exists() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("telemetry writer reaches the pre-commit hook");
}

#[cfg(debug_assertions)]
async fn shutdown_deadline_does_not_ack_uncommitted_postgres_or_sqlite_work(
    fixture: &Fixture,
    device: &Device,
) {
    let gate = fixture._dir.path().join("a18-before-commit-deadline");
    std::fs::write(&gate, b"hold").unwrap();
    let mut server = fixture.start_paused(&gate).await;
    let (client, mut eventloop, _) = mqtt_connect(
        fixture.mqtt_addr,
        &device.id,
        &device.id,
        &device.token,
        true,
    )
    .await;
    let ack_topic = format!("devices/{}/telemetry/ack", device.id);
    assert!(matches!(
        mqtt_subscribe(&client, &mut eventloop, &ack_topic, QoS::AtLeastOnce).await,
        SubscribeReasonCode::Success(_)
    ));
    let event_id = format!("a18-shutdown-{}", Uuid::now_v7());
    mqtt_publish(
        &client,
        &format!("devices/{}/telemetry", device.id),
        QoS::AtLeastOnce,
        false,
        &telemetry_payload(&event_id, 23.0),
    )
    .await;
    mqtt_wait_puback(&mut eventloop, Duration::from_secs(5)).await;
    wait_for_pause(&gate).await;
    assert!(
        timeout(
            Duration::from_millis(100),
            mqtt_wait_telemetry_ack(&mut eventloop, &ack_topic, Duration::from_secs(5)),
        )
        .await
        .is_err(),
        "uncommitted work must not produce an application ACK"
    );

    let (monitor_started, monitor_ready) = oneshot::channel();
    let ack_monitor = tokio::spawn(async move {
        let _ = monitor_started.send(());
        loop {
            match eventloop.poll().await {
                Ok(Event::Incoming(Packet::Publish(publish))) if publish.topic == ack_topic => {
                    let ack: Value = serde_json::from_slice(&publish.payload)
                        .expect("parse application ACK while shutting down");
                    if ack["status"] == "stored" || ack["status"] == "duplicate" {
                        return Some(ack);
                    }
                }
                Ok(_) => {}
                Err(_) => return None,
            }
        }
    });
    monitor_ready.await.unwrap();

    let output = timeout(Duration::from_secs(10), server.terminate_with_output())
        .await
        .expect("server exits when the shutdown deadline is exceeded");
    assert!(
        !output.status.success(),
        "shutdown deadline must fail the process"
    );
    let logs = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        logs.contains("telemetry worker did not drain before shutdown deadline"),
        "missing shutdown deadline evidence: {logs}"
    );
    let unexpected_ack = timeout(Duration::from_secs(5), ack_monitor)
        .await
        .expect("MQTT event loop closes after the server exits")
        .unwrap();
    assert!(
        unexpected_ack.is_none(),
        "received a successful application ACK during shutdown: {}",
        unexpected_ack.unwrap()
    );
    assert_eq!(fixture.telemetry_count(&device.id, &event_id).await, 0);
    drop(client);
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
