use std::fs;
use std::net::SocketAddr;
use std::sync::Arc;

use iot_server::config;
use iot_server::http::{AppState, router};
use iot_server::runtime::lock::DataDirLock;
use iot_server::storage::sqlite::{migrate, pool};
use reqwest::StatusCode;
use reqwest::header::CACHE_CONTROL;
use serde_json::{Value, json};
use sqlx::SqlitePool;
use tempfile::TempDir;
use tokio::task::JoinHandle;

struct TestServer {
    base: String,
    pool: SqlitePool,
    handle: JoinHandle<()>,
    _lock: DataDirLock,
    _dir: TempDir,
}

impl Drop for TestServer {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

impl TestServer {
    async fn start(dashboard: bool) -> Self {
        let dir = TempDir::new().unwrap();
        let config_path = dir.path().join("conf.toml");
        fs::write(
            &config_path,
            format!(
                "config_version = 1\n\
                 [server]\n\
                 data_dir = \"./data\"\n\
                 [http]\n\
                 listen = \"127.0.0.1:0\"\n\
                 dashboard = {dashboard}\n\
                 [database]\n\
                 url = \"sqlite://./data/iot.db\"\n"
            ),
        )
        .unwrap();

        let loaded = Arc::new(config::load_with_env(&config_path, |_| None).unwrap());
        let lock = DataDirLock::acquire(&loaded.data_dir).unwrap();
        let pool = pool::connect(&loaded).await.unwrap();
        migrate::run(&pool).await.unwrap();

        let state = AppState::new(pool.clone(), Arc::clone(&loaded));
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

        Self {
            base: format!("http://{addr}"),
            pool,
            handle,
            _lock: lock,
            _dir: dir,
        }
    }
}

async fn get(server: &TestServer, path: &str) -> reqwest::Response {
    reqwest::Client::new()
        .get(format!("{}{path}", server.base))
        .send()
        .await
        .unwrap()
}

async fn json_body(response: reqwest::Response) -> Value {
    response.json().await.unwrap()
}

#[tokio::test]
async fn healthz_is_public_and_readyz_tracks_the_database() {
    let server = TestServer::start(true).await;

    let health = get(&server, "/healthz").await;
    assert_eq!(health.status(), StatusCode::OK);
    assert_eq!(json_body(health).await, json!({ "status": "ok" }));

    let ready = get(&server, "/readyz").await;
    assert_eq!(ready.status(), StatusCode::OK);
    assert_eq!(
        json_body(ready).await,
        json!({ "status": "ready", "checks": { "database": "ok" } })
    );

    server.pool.close().await;

    let health = get(&server, "/healthz").await;
    assert_eq!(health.status(), StatusCode::OK);

    let ready = get(&server, "/readyz").await;
    assert_eq!(ready.status(), StatusCode::SERVICE_UNAVAILABLE);
    let body = ready.text().await.unwrap();
    assert!(body.contains(r#""status":"not_ready""#));
    assert!(!body.contains("sqlite"));
    assert!(!body.contains("SELECT"));
}

#[tokio::test]
async fn api_unknown_paths_and_methods_are_json_errors() {
    let server = TestServer::start(true).await;

    let response = get(&server, "/api/v1/nope").await;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    assert!(response.headers().contains_key("x-request-id"));
    assert_eq!(json_body(response).await["code"], "not_found");

    let response = get(&server, "/api/v1/auth/login").await;
    assert_eq!(response.status(), StatusCode::METHOD_NOT_ALLOWED);
    assert_eq!(json_body(response).await["code"], "method_not_allowed");
}

#[tokio::test]
async fn dashboard_serves_index_and_spa_fallback_but_not_assets() {
    let server = TestServer::start(true).await;

    let response = get(&server, "/").await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers().get(CACHE_CONTROL).unwrap(), "no-store");
    assert!(
        response
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .unwrap()
            .to_str()
            .unwrap()
            .starts_with("text/html")
    );
    assert!(response.text().await.unwrap().contains("IoT Server"));

    let response = get(&server, "/devices/abc").await;
    assert_eq!(response.status(), StatusCode::OK);
    assert!(response.text().await.unwrap().contains("IoT Server"));

    let response = get(&server, "/assets/app.js").await;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    assert_eq!(json_body(response).await["code"], "not_found");
}

#[tokio::test]
async fn dashboard_disabled_returns_404_but_health_still_works() {
    let server = TestServer::start(false).await;

    for path in ["/", "/devices/abc", "/assets/app.js"] {
        let response = get(&server, path).await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND, "{path}");
        assert_eq!(json_body(response).await["code"], "not_found", "{path}");
    }

    let response = get(&server, "/healthz").await;
    assert_eq!(response.status(), StatusCode::OK);
}
