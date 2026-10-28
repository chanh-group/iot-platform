//! Shared integration-test harness: isolated data dir, config, app, client.

#![allow(dead_code)]

use std::fs;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::Duration;

use iot_server::config::{self, LoadedConfig};
use iot_server::core::auth;
use iot_server::http::{AppState, router};
use iot_server::mqtt::server::MqttServer;
use iot_server::runtime::data_dir_lock::DataDirLock;
use iot_server::storage::sqlite::users;
use iot_server::storage::sqlite::{migrate, pool};
use reqwest::StatusCode;
use sqlx::SqlitePool;
use tempfile::TempDir;
use tokio::task::JoinHandle;

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
    mqtt: MqttServer,
    handle: JoinHandle<()>,
    _lock: DataDirLock,
    _dir: TempDir,
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

    pub fn abort_mqtt(&self) {
        self.mqtt.abort();
    }
}

impl Drop for TestApp {
    fn drop(&mut self) {
        self.mqtt.abort();
        self.handle.abort();
    }
}

pub struct TestAppBuilder {
    dashboard: bool,
    cookie_secure: bool,
    max_body_bytes: u64,
    login_max_attempts: u64,
}

impl Default for TestAppBuilder {
    fn default() -> Self {
        Self {
            dashboard: true,
            cookie_secure: false,
            max_body_bytes: 262_144,
            login_max_attempts: 5,
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

    pub fn max_body_bytes(mut self, bytes: u64) -> Self {
        self.max_body_bytes = bytes;
        self
    }

    pub fn login_max_attempts(mut self, attempts: u64) -> Self {
        self.login_max_attempts = attempts;
        self
    }

    pub async fn start(self) -> TestApp {
        let dir = TempDir::new().unwrap();
        let config_path = dir.path().join("conf.toml");
        let mqtt_port = free_port();
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
                 max_body_bytes = {}\n\
                 [mqtt]\n\
                 listen = \"127.0.0.1:{}\"\n\
                 [database]\n\
                 url = \"sqlite://./data/iot.db\"\n\
                 [auth]\n\
                 login_max_attempts = {}\n",
                self.dashboard,
                self.cookie_secure,
                self.max_body_bytes,
                mqtt_port,
                self.login_max_attempts,
            ),
        )
        .unwrap();

        let loaded = Arc::new(config::load_with_env(&config_path, |_| None).unwrap());
        let lock = DataDirLock::try_new(&loaded.data_dir).unwrap();
        let pool = pool::connect(&loaded).await.unwrap();
        migrate::run(&pool).await.unwrap();

        let mqtt_ready = Arc::new(AtomicBool::new(false));
        let mqtt = MqttServer::start(&loaded.config.mqtt, Arc::clone(&mqtt_ready))
            .await
            .unwrap();
        let mqtt_addr = mqtt.local_addr();
        let state = AppState::new(pool.clone(), Arc::clone(&loaded), mqtt_ready);
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
            mqtt,
            handle,
            _lock: lock,
            _dir: dir,
        }
    }
}

// Phase 2 adds MQTT and WebSocket helpers here.
