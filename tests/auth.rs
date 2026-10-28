use std::fs;
use std::net::SocketAddr;
use std::sync::Arc;

use iot_server::config::{self, LoadedConfig};
use iot_server::core::auth;
use iot_server::http::{AppState, router};
use iot_server::runtime::lock::DataDirLock;
use iot_server::storage::sqlite::sessions;
use iot_server::storage::sqlite::users;
use iot_server::storage::sqlite::{migrate, pool};
use reqwest::StatusCode;
use reqwest::header::{CONTENT_TYPE, ORIGIN, RETRY_AFTER, SET_COOKIE};
use serde_json::{Value, json};
use sqlx::SqlitePool;
use tempfile::TempDir;
use tokio::task::JoinHandle;

const PASSWORD: &str = "correct horse battery staple";

#[derive(Clone, Copy)]
struct Settings {
    login_max_attempts: u64,
    login_window_ms: u64,
    password_min_length: u64,
    cookie_secure: bool,
    max_body_bytes: u64,
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            login_max_attempts: 5,
            login_window_ms: 60_000,
            password_min_length: 12,
            cookie_secure: false,
            max_body_bytes: 262_144,
        }
    }
}

struct TestServer {
    base: String,
    pool: SqlitePool,
    config: Arc<LoadedConfig>,
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
    async fn start(settings: Settings) -> Self {
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
                 max_body_bytes = {}\n\
                 cookie_secure = {}\n\
                 [database]\n\
                 url = \"sqlite://./data/iot.db\"\n\
                 [auth]\n\
                 session_ttl_ms = 86400000\n\
                 login_max_attempts = {}\n\
                 login_window_ms = {}\n\
                 password_min_length = {}\n",
                settings.max_body_bytes,
                settings.cookie_secure,
                settings.login_max_attempts,
                settings.login_window_ms,
                settings.password_min_length,
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
            config: loaded,
            handle,
            _lock: lock,
            _dir: dir,
        }
    }

    async fn create_admin(&self, username: &str, password: &str) {
        let hash = auth::hash_password(password).unwrap();
        let mut tx = self.pool.begin().await.unwrap();
        users::create_admin(&mut tx, username, &hash, auth::now_ms())
            .await
            .unwrap();
        tx.commit().await.unwrap();
    }
}

fn client() -> reqwest::Client {
    reqwest::Client::builder()
        .cookie_store(true)
        .build()
        .unwrap()
}

fn login_url(server: &TestServer) -> String {
    format!("{}/api/v1/auth/login", server.base)
}

async fn login(
    client: &reqwest::Client,
    server: &TestServer,
    username: &str,
    password: &str,
) -> reqwest::Response {
    client
        .post(login_url(server))
        .header("X-IoT-CSRF", "1")
        .json(&json!({ "username": username, "password": password }))
        .send()
        .await
        .unwrap()
}

async fn me(client: &reqwest::Client, server: &TestServer) -> reqwest::Response {
    client
        .get(format!("{}/api/v1/auth/me", server.base))
        .send()
        .await
        .unwrap()
}

async fn logout(client: &reqwest::Client, server: &TestServer) -> reqwest::Response {
    client
        .post(format!("{}/api/v1/auth/logout", server.base))
        .header("X-IoT-CSRF", "1")
        .header(CONTENT_TYPE, "application/json")
        .send()
        .await
        .unwrap()
}

async fn json_body(response: reqwest::Response) -> Value {
    response.json().await.unwrap()
}

#[tokio::test]
async fn no_default_admin_and_me_requires_session() {
    let server = TestServer::start(Settings::default()).await;
    let client = client();

    let users: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM users")
        .fetch_one(&server.pool)
        .await
        .unwrap();
    assert_eq!(users, 0);

    let response = login(&client, &server, "admin", "whatever-password").await;
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(json_body(response).await["code"], "invalid_credentials");

    let response = me(&client, &server).await;
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(json_body(response).await["code"], "unauthenticated");
}

#[tokio::test]
async fn login_me_logout_roundtrip_and_cookie_flags() {
    let server = TestServer::start(Settings::default()).await;
    server.create_admin("admin", PASSWORD).await;
    let client = client();

    let response = login(&client, &server, "admin", "wrong-password").await;
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    let failed_logins: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM audit_events WHERE action = 'auth.login'")
            .fetch_one(&server.pool)
            .await
            .unwrap();
    assert_eq!(failed_logins, 0);

    let response = login(&client, &server, "admin", PASSWORD).await;
    assert_eq!(response.status(), StatusCode::OK);
    let cookie = response
        .headers()
        .get(SET_COOKIE)
        .unwrap()
        .to_str()
        .unwrap()
        .to_owned();
    assert!(cookie.contains("HttpOnly"), "{cookie}");
    assert!(cookie.contains("SameSite=Strict"), "{cookie}");
    assert!(cookie.contains("Path=/"), "{cookie}");
    let max_age = server.config.config.auth.session_ttl_ms / 1000;
    assert!(cookie.contains(&format!("Max-Age={max_age}")), "{cookie}");
    assert!(!cookie.contains("Secure"), "{cookie}");
    let token = cookie
        .split(';')
        .next()
        .unwrap()
        .strip_prefix("iot_session=")
        .unwrap()
        .to_owned();
    let stored_hash: Vec<u8> = sqlx::query_scalar("SELECT token_hash FROM sessions")
        .fetch_one(&server.pool)
        .await
        .unwrap();
    assert_eq!(stored_hash, auth::hash_token(&token).to_vec());
    assert_ne!(stored_hash, token.as_bytes());

    let body = json_body(response).await;
    assert_eq!(body["user"]["username"], "admin");
    assert!(body["expires_at"].as_i64().unwrap() > auth::now_ms());

    let response = me(&client, &server).await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(json_body(response).await["username"], "admin");

    let actions: Vec<String> = sqlx::query_scalar("SELECT action FROM audit_events ORDER BY id")
        .fetch_all(&server.pool)
        .await
        .unwrap();
    assert!(actions.contains(&"admin.create".to_owned()), "{actions:?}");
    assert!(actions.contains(&"auth.login".to_owned()), "{actions:?}");

    let details: Vec<Option<String>> = sqlx::query_scalar("SELECT details_json FROM audit_events")
        .fetch_all(&server.pool)
        .await
        .unwrap();
    for details in details.into_iter().flatten() {
        assert!(!details.contains(PASSWORD), "{details}");
        assert!(!details.contains(&token), "{details}");
    }

    let response = logout(&client, &server).await;
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    let cleared = response
        .headers()
        .get(SET_COOKIE)
        .unwrap()
        .to_str()
        .unwrap()
        .to_owned();
    assert!(cleared.contains("Max-Age=0"), "{cleared}");

    let response = me(&client, &server).await;
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

    let actions: Vec<String> = sqlx::query_scalar("SELECT action FROM audit_events ORDER BY id")
        .fetch_all(&server.pool)
        .await
        .unwrap();
    assert!(actions.contains(&"auth.logout".to_owned()), "{actions:?}");
}

#[tokio::test]
async fn secure_cookie_when_configured() {
    let server = TestServer::start(Settings {
        cookie_secure: true,
        ..Settings::default()
    })
    .await;
    server.create_admin("admin", PASSWORD).await;
    let client = client();

    let response = login(&client, &server, "admin", PASSWORD).await;
    assert_eq!(response.status(), StatusCode::OK);
    let cookie = response
        .headers()
        .get(SET_COOKIE)
        .unwrap()
        .to_str()
        .unwrap();
    assert!(cookie.contains("; Secure"), "{cookie}");
}

#[tokio::test]
async fn csrf_content_type_and_origin_are_enforced() {
    let server = TestServer::start(Settings::default()).await;
    server.create_admin("admin", PASSWORD).await;
    let client = client();

    let response = client
        .post(login_url(&server))
        .json(&json!({ "username": "admin", "password": PASSWORD }))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    assert_eq!(json_body(response).await["code"], "csrf_failed");

    let response = client
        .post(login_url(&server))
        .header("X-IoT-CSRF", "1")
        .header(CONTENT_TYPE, "text/plain")
        .body(r#"{"username":"admin","password":"x"}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNSUPPORTED_MEDIA_TYPE);
    assert_eq!(json_body(response).await["code"], "unsupported_media_type");

    let response = client
        .post(login_url(&server))
        .header("X-IoT-CSRF", "1")
        .header(ORIGIN, "http://evil.example")
        .json(&json!({ "username": "admin", "password": PASSWORD }))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    assert_eq!(json_body(response).await["code"], "csrf_failed");

    let response = client
        .post(login_url(&server))
        .header("X-IoT-CSRF", "1")
        .header(ORIGIN, &server.base)
        .json(&json!({ "username": "admin", "password": PASSWORD }))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let response = me(&client, &server).await;
    assert_eq!(response.status(), StatusCode::OK);

    let response = client
        .post(format!("{}/api/v1/auth/logout", server.base))
        .header(CONTENT_TYPE, "application/json")
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    let response = me(&client, &server).await;
    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn login_rate_limit_returns_retry_after() {
    let server = TestServer::start(Settings {
        login_max_attempts: 2,
        login_window_ms: 60_000,
        ..Settings::default()
    })
    .await;
    server.create_admin("admin", PASSWORD).await;
    let client = client();

    for _ in 0..2 {
        let response = login(&client, &server, "admin", "wrong-password").await;
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    let response = login(&client, &server, "admin", PASSWORD).await;
    assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
    let retry_after: u64 = response
        .headers()
        .get(RETRY_AFTER)
        .unwrap()
        .to_str()
        .unwrap()
        .parse()
        .unwrap();
    assert!(retry_after >= 1);
    assert_eq!(json_body(response).await["code"], "rate_limited");

    let response = login(&client, &server, "other", PASSWORD).await;
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn last_seen_updates_at_most_once_per_five_minutes() {
    let server = TestServer::start(Settings::default()).await;
    server.create_admin("admin", PASSWORD).await;
    let client = client();

    let response = login(&client, &server, "admin", PASSWORD).await;
    assert_eq!(response.status(), StatusCode::OK);

    let session_id: String = sqlx::query_scalar("SELECT id FROM sessions")
        .fetch_one(&server.pool)
        .await
        .unwrap();
    let old = auth::now_ms() - 400_000;
    sqlx::query("UPDATE sessions SET last_seen_at = ? WHERE id = ?")
        .bind(old)
        .bind(&session_id)
        .execute(&server.pool)
        .await
        .unwrap();

    let response = me(&client, &server).await;
    assert_eq!(response.status(), StatusCode::OK);
    let first: i64 = sqlx::query_scalar("SELECT last_seen_at FROM sessions WHERE id = ?")
        .bind(&session_id)
        .fetch_one(&server.pool)
        .await
        .unwrap();
    assert!(first > old);

    let response = me(&client, &server).await;
    assert_eq!(response.status(), StatusCode::OK);
    let second: i64 = sqlx::query_scalar("SELECT last_seen_at FROM sessions WHERE id = ?")
        .bind(&session_id)
        .fetch_one(&server.pool)
        .await
        .unwrap();
    assert_eq!(second, first);
}

#[tokio::test]
async fn cleanup_removes_expired_and_old_revoked_sessions() {
    let server = TestServer::start(Settings::default()).await;
    server.create_admin("admin", PASSWORD).await;
    let client = client();
    let now = auth::now_ms();

    let response = login(&client, &server, "admin", PASSWORD).await;
    assert_eq!(response.status(), StatusCode::OK);
    sqlx::query("UPDATE sessions SET created_at = ?, expires_at = ?")
        .bind(now - 1_000)
        .bind(now - 1)
        .execute(&server.pool)
        .await
        .unwrap();
    assert_eq!(
        sessions::delete_expired(&server.pool, now).await.unwrap(),
        1
    );

    let response = login(&client, &server, "admin", PASSWORD).await;
    assert_eq!(response.status(), StatusCode::OK);
    sqlx::query("UPDATE sessions SET revoked_at = ?")
        .bind(now - 8 * 24 * 60 * 60 * 1000)
        .execute(&server.pool)
        .await
        .unwrap();
    assert_eq!(
        sessions::delete_expired(&server.pool, now).await.unwrap(),
        1
    );

    let response = login(&client, &server, "admin", PASSWORD).await;
    assert_eq!(response.status(), StatusCode::OK);
    sqlx::query("UPDATE sessions SET revoked_at = ?")
        .bind(now - 3_600_000)
        .execute(&server.pool)
        .await
        .unwrap();
    assert_eq!(
        sessions::delete_expired(&server.pool, now).await.unwrap(),
        0
    );
}

#[tokio::test]
async fn oversized_body_maps_to_payload_too_large() {
    let server = TestServer::start(Settings {
        max_body_bytes: 1_024,
        ..Settings::default()
    })
    .await;
    server.create_admin("admin", PASSWORD).await;
    let client = client();

    let response = client
        .post(login_url(&server))
        .header("X-IoT-CSRF", "1")
        .json(&json!({ "username": "admin", "password": "x".repeat(2_000) }))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
    assert_eq!(json_body(response).await["code"], "payload_too_large");
}
