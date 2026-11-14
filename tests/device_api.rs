mod common;

use std::time::Duration;
use std::{
    net::TcpListener,
    process::{Child, Command, Stdio},
};

use common::{TestApp, mqtt_connect, mqtt_connect_refused, mqtt_wait_closed};
use iot_server::core::auth;
use rumqttc::ConnectReturnCode;
use serde_json::{Value, json};
use sqlx::PgPool;
use tempfile::TempDir;

async fn admin_client(app: &TestApp, username: &str) -> reqwest::Client {
    app.create_admin(username, "device-test-password").await;
    let client = app.client();
    let response = client
        .post(format!("{}/api/v1/auth/login", app.base))
        .header("X-IoT-CSRF", "1")
        .json(&json!({"username": username, "password": "device-test-password"}))
        .send()
        .await
        .unwrap();
    assert!(response.status().is_success());
    client
}

#[tokio::test]
async fn device_create_patch_validate_and_audit() {
    let app = TestApp::start().await;
    let url = format!("{}/api/v1/devices", app.base);
    assert_eq!(
        reqwest::Client::new()
            .post(&url)
            .header("X-IoT-CSRF", "1")
            .json(&json!({"name":"unauthorized"}))
            .send()
            .await
            .unwrap()
            .status(),
        reqwest::StatusCode::UNAUTHORIZED
    );
    let http = admin_client(&app, "device-mutations").await;
    assert_eq!(
        http.post(&url)
            .json(&json!({"name":"no-csrf"}))
            .send()
            .await
            .unwrap()
            .status(),
        reqwest::StatusCode::FORBIDDEN
    );

    let created = http
        .post(&url)
        .header("X-IoT-CSRF", "1")
        .header("X-Request-ID", "device-create-123")
        .json(&json!({"name":"  温度センサー  ", "metadata":{"room":"A1"}, "enabled":false}))
        .send()
        .await
        .unwrap();
    assert_eq!(created.status(), reqwest::StatusCode::CREATED);
    let body: Value = created.json().await.unwrap();
    let device = &body["device"];
    let credential = &body["credential"];
    let id = device["id"].as_str().unwrap();
    let token = credential["token"].as_str().unwrap();
    assert!(auth::is_device_id(id));
    assert_eq!(device["name"], "温度センサー");
    assert_eq!(device["enabled"], false);
    assert_eq!(device["metadata"], json!({"room":"A1"}));
    assert_eq!(device["credential"]["id"], credential["id"]);
    assert!(token.starts_with("iot_"));
    assert_eq!(token.len(), 47);
    assert!(
        token[4..]
            .bytes()
            .all(|byte| { byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_') })
    );
    let stored_hash: Vec<u8> = sqlx::query_scalar(
        "SELECT token_hash FROM device_credentials WHERE id = ? AND status = 'active'",
    )
    .bind(credential["id"].as_str().unwrap())
    .fetch_one(&app.pool)
    .await
    .unwrap();
    assert_eq!(stored_hash, auth::hash_token(token));
    let audit: (String, String, Option<String>) = sqlx::query_as(
        "SELECT action, target_id, request_id FROM audit_events WHERE target_id = ? ORDER BY id DESC LIMIT 1",
    )
    .bind(id)
    .fetch_one(&app.pool)
    .await
    .unwrap();
    assert_eq!(audit.0, "device.create");
    assert_eq!(audit.1, id);
    assert_eq!(audit.2.as_deref(), Some("device-create-123"));
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM device_credentials WHERE device_id = ? AND status = 'active'",
        )
        .bind(id)
        .fetch_one(&app.pool)
        .await
        .unwrap(),
        1
    );

    let fetched: Value = http
        .get(format!("{url}/{id}"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(!fetched.to_string().contains(token));
    assert!(fetched["credential"].get("token").is_none());
    assert!(fetched.get("token_hash").is_none());
    let listed: Value = http.get(&url).send().await.unwrap().json().await.unwrap();
    assert!(!listed.to_string().contains(token));

    let patched = http
        .patch(format!("{url}/{id}"))
        .header("X-IoT-CSRF", "1")
        .header("X-Request-ID", "device-patch-123")
        .json(&json!({"name":"sensor-2", "metadata":{"zone":"B2"}, "enabled":true}))
        .send()
        .await
        .unwrap();
    assert_eq!(patched.status(), reqwest::StatusCode::OK);
    let patched: Value = patched.json().await.unwrap();
    assert_eq!(patched["name"], "sensor-2");
    assert_eq!(patched["metadata"], json!({"zone":"B2"}));
    assert_eq!(patched["enabled"], true);
    let audit_details: String = sqlx::query_scalar(
        "SELECT details_json FROM audit_events WHERE target_id = ? AND action = 'device.enable'",
    )
    .bind(id)
    .fetch_one(&app.pool)
    .await
    .unwrap();
    let audit_details: Value = serde_json::from_str(&audit_details).unwrap();
    assert_eq!(
        audit_details,
        json!({"name":true,"metadata":true,"enabled_from":false,"enabled_to":true})
    );
    let update = http
        .patch(format!("{url}/{id}"))
        .header("X-IoT-CSRF", "1")
        .json(&json!({"metadata":{"zone":"C3"}}))
        .send()
        .await
        .unwrap();
    assert_eq!(update.status(), reqwest::StatusCode::OK);
    let audit: (String, String) = sqlx::query_as(
        "SELECT action, details_json FROM audit_events WHERE target_id = ? ORDER BY id DESC LIMIT 1",
    )
    .bind(id)
    .fetch_one(&app.pool)
    .await
    .unwrap();
    assert_eq!(audit.0, "device.update");
    assert_eq!(
        serde_json::from_str::<Value>(&audit.1).unwrap(),
        json!({"name":false,"metadata":true})
    );

    let second = http
        .post(&url)
        .header("X-IoT-CSRF", "1")
        .json(&json!({"name":"another-device"}))
        .send()
        .await
        .unwrap();
    assert_eq!(second.status(), reqwest::StatusCode::CREATED);
    let second: Value = second.json().await.unwrap();
    assert_eq!(second["device"]["enabled"], true);
    assert_eq!(second["device"]["metadata"], json!({}));
    let duplicate_create = http
        .post(&url)
        .header("X-IoT-CSRF", "1")
        .json(&json!({"name":" SENSOR-2 "}))
        .send()
        .await
        .unwrap();
    assert_eq!(duplicate_create.status(), reqwest::StatusCode::CONFLICT);
    assert_eq!(
        duplicate_create.json::<Value>().await.unwrap()["code"],
        "device_name_conflict"
    );
    let second_id = second["device"]["id"].as_str().unwrap().to_owned();
    let duplicate = http
        .patch(format!("{url}/{second_id}"))
        .header("X-IoT-CSRF", "1")
        .json(&json!({"name":" sensor-2 "}))
        .send()
        .await
        .unwrap();
    assert_eq!(duplicate.status(), reqwest::StatusCode::CONFLICT);
    assert_eq!(
        duplicate.json::<Value>().await.unwrap()["code"],
        "device_name_conflict"
    );

    for invalid in [
        json!({"name":"  "}),
        json!({"name":"bad\nname"}),
        json!({"name":"x".repeat(65)}),
        json!({"name":"invalid-metadata", "metadata":null}),
        json!({"name":"large-metadata", "metadata":{"x":"x".repeat(8192)}}),
        json!({"name":"deep-metadata", "metadata":{"a":{"b":{"c":{"d":{"e":{"f":{"g":{"h":{"i":1}}}}}}}}}}),
        json!({"name":"unknown-field", "extra":true}),
    ] {
        let response = http
            .post(&url)
            .header("X-IoT-CSRF", "1")
            .json(&invalid)
            .send()
            .await
            .unwrap();
        assert_eq!(
            response.status(),
            reqwest::StatusCode::BAD_REQUEST,
            "{invalid}"
        );
        assert_eq!(
            response.json::<Value>().await.unwrap()["code"],
            "validation_error"
        );
    }
    for invalid in [json!({}), json!({"unknown":true}), json!({"metadata":null})] {
        let response = http
            .patch(format!("{url}/{id}"))
            .header("X-IoT-CSRF", "1")
            .json(&invalid)
            .send()
            .await
            .unwrap();
        assert_eq!(
            response.status(),
            reqwest::StatusCode::BAD_REQUEST,
            "{invalid}"
        );
        assert_eq!(
            response.json::<Value>().await.unwrap()["code"],
            "validation_error"
        );
    }
    let missing = http
        .patch(format!("{url}/missing"))
        .header("X-IoT-CSRF", "1")
        .json(&json!({"enabled":false}))
        .send()
        .await
        .unwrap();
    assert_eq!(missing.status(), reqwest::StatusCode::NOT_FOUND);
    assert_eq!(
        missing.json::<Value>().await.unwrap()["code"],
        "device_not_found"
    );
}

#[tokio::test]
async fn device_audit_failures_roll_back_create_and_patch() {
    let app = TestApp::start().await;
    let url = format!("{}/api/v1/devices", app.base);
    let http = admin_client(&app, "device-audit").await;
    sqlx::query(
        "CREATE TRIGGER fail_device_create_audit BEFORE INSERT ON audit_events
         WHEN NEW.action = 'device.create'
         BEGIN SELECT RAISE(FAIL, 'injected audit failure'); END",
    )
    .execute(&app.pool)
    .await
    .unwrap();
    let failed = http
        .post(&url)
        .header("X-IoT-CSRF", "1")
        .json(&json!({"name":"create-rollback"}))
        .send()
        .await
        .unwrap();
    assert_eq!(failed.status(), reqwest::StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM devices WHERE name = 'create-rollback'")
            .fetch_one(&app.pool)
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM device_credentials")
            .fetch_one(&app.pool)
            .await
            .unwrap(),
        0
    );
    sqlx::query("DROP TRIGGER fail_device_create_audit")
        .execute(&app.pool)
        .await
        .unwrap();

    let device = app.create_device("patch-rollback", true).await;
    sqlx::query(
        "CREATE TRIGGER fail_device_update_audit BEFORE INSERT ON audit_events
         WHEN NEW.action = 'device.disable'
         BEGIN SELECT RAISE(FAIL, 'injected audit failure'); END",
    )
    .execute(&app.pool)
    .await
    .unwrap();
    let failed = http
        .patch(format!("{url}/{}", device.id))
        .header("X-IoT-CSRF", "1")
        .json(&json!({"name":"must-rollback", "enabled":false}))
        .send()
        .await
        .unwrap();
    assert_eq!(failed.status(), reqwest::StatusCode::SERVICE_UNAVAILABLE);
    let row: (String, bool) = sqlx::query_as("SELECT name, enabled FROM devices WHERE id = ?")
        .bind(&device.id)
        .fetch_one(&app.pool)
        .await
        .unwrap();
    assert_eq!(row, ("patch-rollback".to_owned(), true));
}

#[tokio::test]
#[ignore = "requires disposable PostgreSQL fixture; run tests/device_api_postgres.sh"]
async fn device_audit_failures_roll_back_create_and_patch_postgres() {
    let fixture = DevicePostgresFixture::start().await;
    let url = std::env::var("IOT_TEST_POSTGRES_URL").unwrap();
    let pool = PgPool::connect(&url).await.unwrap();
    let http = &fixture.http;
    let devices_url = format!("{}/api/v1/devices", fixture.base);
    assert!(
        http.post(format!("{}/api/v1/auth/login", fixture.base))
            .header("X-IoT-CSRF", "1")
            .json(&json!({"username":"device-admin", "password":"device-test-password"}))
            .send()
            .await
            .unwrap()
            .status()
            .is_success()
    );
    sqlx::query(
        "CREATE FUNCTION fail_device_create_audit() RETURNS trigger LANGUAGE plpgsql AS $$
         BEGIN
             IF NEW.action = 'device.create' THEN RAISE EXCEPTION 'injected audit failure'; END IF;
             RETURN NEW;
         END $$",
    )
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(
        "CREATE TRIGGER fail_device_create_audit BEFORE INSERT ON audit_events
         FOR EACH ROW EXECUTE FUNCTION fail_device_create_audit()",
    )
    .execute(&pool)
    .await
    .unwrap();
    let failed = http
        .post(&devices_url)
        .header("X-IoT-CSRF", "1")
        .json(&json!({"name":"create-rollback"}))
        .send()
        .await
        .unwrap();
    assert_eq!(failed.status(), reqwest::StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM devices WHERE name = 'create-rollback'")
            .fetch_one(&pool)
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM device_credentials")
            .fetch_one(&pool)
            .await
            .unwrap(),
        0
    );
    sqlx::query("DROP TRIGGER fail_device_create_audit ON audit_events")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("DROP FUNCTION fail_device_create_audit()")
        .execute(&pool)
        .await
        .unwrap();

    let (device_id, _) = fixture.create_device("patch-rollback", true).await;
    sqlx::query(
        "CREATE FUNCTION fail_device_update_audit() RETURNS trigger LANGUAGE plpgsql AS $$
         BEGIN
             IF NEW.action = 'device.disable' THEN RAISE EXCEPTION 'injected audit failure'; END IF;
             RETURN NEW;
         END $$",
    )
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(
        "CREATE TRIGGER fail_device_update_audit BEFORE INSERT ON audit_events
         FOR EACH ROW EXECUTE FUNCTION fail_device_update_audit()",
    )
    .execute(&pool)
    .await
    .unwrap();
    let failed = http
        .patch(format!("{devices_url}/{device_id}"))
        .header("X-IoT-CSRF", "1")
        .json(&json!({"name":"must-rollback", "enabled":false}))
        .send()
        .await
        .unwrap();
    assert_eq!(failed.status(), reqwest::StatusCode::SERVICE_UNAVAILABLE);
    let row: (String, bool) = sqlx::query_as("SELECT name, enabled FROM devices WHERE id = $1")
        .bind(&device_id)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(row, ("patch-rollback".to_owned(), true));
    pool.close().await;
}

#[tokio::test]
async fn disabling_a_device_kicks_its_live_session_and_blocks_reconnect() {
    let app = TestApp::start().await;
    let url = format!("{}/api/v1/devices", app.base);
    let http = admin_client(&app, "device-disable").await;
    let created = http
        .post(&url)
        .header("X-IoT-CSRF", "1")
        .json(&json!({"name":"disable-through-api"}))
        .send()
        .await
        .unwrap();
    assert_eq!(created.status(), reqwest::StatusCode::CREATED);
    let created: Value = created.json().await.unwrap();
    let id = created["device"]["id"].as_str().unwrap().to_owned();
    let token = created["credential"]["token"].as_str().unwrap().to_owned();
    let (client, mut eventloop, _) = mqtt_connect(app.mqtt_addr, &id, &id, &token, true).await;

    let response = http
        .patch(format!("{url}/{id}"))
        .header("X-IoT-CSRF", "1")
        .json(&json!({"enabled":false}))
        .send()
        .await
        .unwrap();
    let status = response.status();
    let body = response.text().await.unwrap();
    assert_eq!(status, reqwest::StatusCode::OK, "{body}");
    assert_eq!(
        serde_json::from_str::<Value>(&body).unwrap()["enabled"],
        false
    );
    let audit: (String, String) = sqlx::query_as(
        "SELECT action, details_json FROM audit_events WHERE target_id = ? ORDER BY id DESC LIMIT 1",
    )
    .bind(&id)
    .fetch_one(&app.pool)
    .await
    .unwrap();
    assert_eq!(audit.0, "device.disable");
    assert_eq!(
        serde_json::from_str::<Value>(&audit.1).unwrap(),
        json!({"name":false,"metadata":false,"enabled_from":true,"enabled_to":false})
    );
    mqtt_wait_closed(&mut eventloop, Duration::from_secs(3)).await;
    assert_eq!(
        mqtt_connect_refused(app.mqtt_addr, &id, &id, &token).await,
        ConnectReturnCode::BadUserNamePassword
    );
    drop(client);
}

#[tokio::test]
async fn pagination_orders_equal_timestamps_by_descending_id() {
    let app = TestApp::start().await;
    app.create_admin("page-admin", "device-test-password").await;
    let mut expected = Vec::new();
    for name in ["page-a", "page-b", "page-c"] {
        expected.push(app.create_device(name, true).await.id);
    }
    sqlx::query("UPDATE devices SET created_at = 123456789")
        .execute(&app.pool)
        .await
        .unwrap();
    let http = app.client();
    assert!(
        http.post(format!("{}/api/v1/auth/login", app.base))
            .header("X-IoT-CSRF", "1")
            .json(&json!({"username":"page-admin", "password":"device-test-password"}))
            .send()
            .await
            .unwrap()
            .status()
            .is_success()
    );
    assert_equal_timestamp_page_order(&http, &app.base, &expected).await;
}

#[tokio::test]
#[ignore = "requires disposable PostgreSQL fixture; run tests/device_api_postgres.sh"]
async fn pagination_orders_equal_timestamps_by_descending_id_postgres() {
    let fixture = DevicePostgresFixture::start().await;
    assert!(
        fixture
            .http
            .post(format!("{}/api/v1/auth/login", fixture.base))
            .header("X-IoT-CSRF", "1")
            .json(&json!({"username":"device-admin", "password":"device-test-password"}))
            .send()
            .await
            .unwrap()
            .status()
            .is_success()
    );
    let mut expected = Vec::new();
    for name in ["page-a", "page-b", "page-c"] {
        expected.push(fixture.create_device(name, true).await.0);
    }
    let url = std::env::var("IOT_TEST_POSTGRES_URL").unwrap();
    let pool = PgPool::connect(&url).await.unwrap();
    sqlx::query("UPDATE devices SET created_at = 123456789 WHERE id = ANY($1)")
        .bind(&expected)
        .execute(&pool)
        .await
        .unwrap();
    pool.close().await;
    assert_equal_timestamp_page_order(&fixture.http, &fixture.base, &expected).await;
}

async fn assert_equal_timestamp_page_order(
    http: &reqwest::Client,
    base: &str,
    expected: &[String],
) {
    let mut expected = expected.to_vec();
    expected.sort_by(|a, b| b.cmp(a));
    let url = format!("{base}/api/v1/devices");
    let mut cursor = None;
    let mut actual = Vec::new();
    for _ in 0..3 {
        let mut request = http.get(&url).query(&[("limit", "1")]);
        if let Some(value) = &cursor {
            request = request.query(&[("cursor", value)]);
        }
        let response: Value = request.send().await.unwrap().json().await.unwrap();
        assert_eq!(response["items"].as_array().unwrap().len(), 1);
        actual.push(response["items"][0]["id"].as_str().unwrap().to_owned());
        cursor = response["next_cursor"].as_str().map(str::to_owned);
    }
    assert_eq!(actual, expected);
    assert!(cursor.is_none());
}

#[tokio::test]
async fn device_reads_use_live_presence_and_bound_pagination() {
    let app = TestApp::start().await;
    app.create_admin("device-admin", "device-test-password")
        .await;
    let first = app.create_device("first-sensor", true).await;
    let second = app.create_device("second-sensor", false).await;
    let http = app.client();
    let base = format!("{}/api/v1/devices", app.base);
    assert_eq!(
        reqwest::Client::new()
            .get(&base)
            .send()
            .await
            .unwrap()
            .status(),
        reqwest::StatusCode::UNAUTHORIZED
    );
    assert!(
        http.post(format!("{}/api/v1/auth/login", app.base))
            .header("X-IoT-CSRF", "1")
            .json(&json!({"username":"device-admin", "password":"device-test-password"}))
            .send()
            .await
            .unwrap()
            .status()
            .is_success()
    );

    assert_device_read_pagination(
        &http,
        &app.base,
        app.mqtt_addr,
        &first.id,
        &first.token,
        &second.id,
    )
    .await;
}

async fn assert_device_read_pagination(
    http: &reqwest::Client,
    server_base: &str,
    mqtt_addr: std::net::SocketAddr,
    first_id: &str,
    first_token: &str,
    second_id: &str,
) {
    let base = format!("{server_base}/api/v1/devices");
    let (_mqtt, _events, _) =
        common::mqtt_connect(mqtt_addr, first_id, first_id, first_token, true).await;
    let device: Value = http
        .get(format!("{base}/{first_id}"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(device["online"], true);
    assert_eq!(device["name"], "first-sensor");
    assert!(device["credential"]["id"].is_string());
    assert!(!device.to_string().contains(first_token));
    assert!(device["credential"].get("token").is_none());
    let missing = http.get(format!("{base}/missing")).send().await.unwrap();
    assert_eq!(missing.status(), reqwest::StatusCode::NOT_FOUND);
    assert_eq!(
        missing.json::<Value>().await.unwrap()["code"],
        "device_not_found"
    );

    let page: Value = http
        .get(&base)
        .query(&[("limit", "1"), ("q", "sensor")])
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(page["items"].as_array().unwrap().len(), 1);
    let cursor = page["next_cursor"].as_str().unwrap();
    let next: Value = http
        .get(&base)
        .query(&[("limit", "1"), ("q", "sensor"), ("cursor", cursor)])
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(next["items"].as_array().unwrap().len(), 1);
    assert_ne!(page["items"][0]["id"], next["items"][0]["id"]);
    assert!(next["next_cursor"].is_null());
    let mismatch = http
        .get(&base)
        .query(&[("q", "other"), ("cursor", cursor)])
        .send()
        .await
        .unwrap();
    assert_eq!(mismatch.status(), reqwest::StatusCode::BAD_REQUEST);
    assert_eq!(
        mismatch.json::<Value>().await.unwrap()["code"],
        "invalid_cursor"
    );
    let filtered: Value = http
        .get(&base)
        .query(&[("enabled", "false")])
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(filtered["items"].as_array().unwrap().len(), 1);
    assert_eq!(filtered["items"][0]["id"], second_id);
    assert_eq!(
        http.get(&base)
            .query(&[("limit", "201")])
            .send()
            .await
            .unwrap()
            .status(),
        reqwest::StatusCode::BAD_REQUEST
    );
}

#[tokio::test]
#[ignore = "run through tests/device_api_postgres.sh"]
async fn device_reads_use_live_presence_and_bound_pagination_postgres() {
    let fixture = DevicePostgresFixture::start().await;
    let http = fixture.http.clone();
    let unauthenticated = http
        .get(format!("{}/api/v1/devices", fixture.base))
        .send()
        .await
        .unwrap();
    assert_eq!(unauthenticated.status(), reqwest::StatusCode::UNAUTHORIZED);
    assert!(
        http.post(format!("{}/api/v1/auth/login", fixture.base))
            .header("X-IoT-CSRF", "1")
            .json(&json!({"username":"device-admin", "password":"device-test-password"}))
            .send()
            .await
            .unwrap()
            .status()
            .is_success()
    );
    let (first_id, first_token) = fixture.create_device("first-sensor", true).await;
    let (second_id, _) = fixture.create_device("second-sensor", false).await;
    assert_device_read_pagination(
        &http,
        &fixture.base,
        fixture.mqtt_addr,
        &first_id,
        &first_token,
        &second_id,
    )
    .await;
}

struct DevicePostgresFixture {
    _server: ChildGuard,
    _dir: TempDir,
    base: String,
    mqtt_addr: std::net::SocketAddr,
    http: reqwest::Client,
}

struct ChildGuard(Child);
impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

impl DevicePostgresFixture {
    async fn start() -> Self {
        const DATABASE: &str = "iot_test";
        let url = std::env::var("IOT_TEST_POSTGRES_URL").expect("fixture PostgreSQL URL");
        let container = std::env::var("IOT_TEST_POSTGRES_CONTAINER").expect("fixture container");
        let port: u16 = std::env::var("IOT_TEST_POSTGRES_PORT")
            .expect("mapped port")
            .parse()
            .expect("numeric mapped port");
        let (scheme, rest) = url.split_once("://").expect("PostgreSQL URL scheme");
        let (authority, query) = rest.split_once('?').expect("PostgreSQL URL query");
        let (credentials, endpoint) = authority.split_once('@').expect("PostgreSQL credentials");
        assert_eq!(scheme, "postgresql", "refusing unsafe PostgreSQL URL");
        assert!(
            credentials.starts_with("postgres:") && credentials.len() > 9,
            "refusing unsafe PostgreSQL credentials"
        );
        assert_eq!(
            endpoint,
            format!("127.0.0.1:{port}/{DATABASE}"),
            "refusing unsafe PostgreSQL endpoint"
        );
        assert_eq!(
            query, "sslmode=disable",
            "refusing unsafe PostgreSQL options"
        );
        let inspect = |format: &str| {
            Command::new("docker")
                .args(["inspect", "--format", format, &container])
                .output()
                .expect("docker inspect")
        };
        let output = inspect("{{.Config.Image}}");
        assert!(output.status.success(), "fixture container is unavailable");
        let image = String::from_utf8(output.stdout).unwrap();
        assert_eq!(
            image.trim(),
            "postgres:18.6-bookworm",
            "unexpected fixture image"
        );
        let label =
            String::from_utf8(inspect("{{ index .Config.Labels \"io.iot.test-fixture\" }}").stdout)
                .unwrap();
        assert_eq!(label.trim(), "device-api", "unexpected fixture label");
        let mapping = String::from_utf8(inspect("{{(index (index .NetworkSettings.Ports \"5432/tcp\") 0).HostIp}}:{{(index (index .NetworkSettings.Ports \"5432/tcp\") 0).HostPort}}").stdout).unwrap();
        assert_eq!(
            mapping.trim(),
            format!("127.0.0.1:{port}"),
            "unexpected fixture port mapping"
        );
        let pool = PgPool::connect(&url)
            .await
            .expect("connect fixture database");
        let (database, schema): (String, String) =
            sqlx::query_as("SELECT current_database(), current_schema()")
                .fetch_one(&pool)
                .await
                .expect("verify database before migration");
        assert_eq!(database, DATABASE, "refusing non-fixture database");
        assert_eq!(schema, "public", "refusing non-fixture schema");
        pool.close().await;
        let dir = tempfile::tempdir().expect("temp config directory");
        let http = free_port();
        let mqtt = free_port();
        let config = dir.path().join("conf.toml");
        std::fs::write(&config, format!("config_version=1\n[server]\ndata_dir='./data'\n[http]\nlisten='127.0.0.1:{http}'\n[mqtt]\nlisten='127.0.0.1:{mqtt}'\n[database]\nurl_env='IOT_TEST_POSTGRES_URL'\n")).unwrap();
        let binary = env!("CARGO_BIN_EXE_iot-server");
        assert!(
            Command::new(binary)
                .args(["migrate", "--config"])
                .arg(&config)
                .env("IOT_TEST_POSTGRES_URL", &url)
                .status()
                .expect("migrate")
                .success()
        );
        let password = dir.path().join("password");
        std::fs::write(&password, "device-test-password\n").unwrap();
        assert!(
            Command::new(binary)
                .args(["admin", "create", "--username", "device-admin", "--config"])
                .arg(&config)
                .args(["--password-file"])
                .arg(&password)
                .env("IOT_TEST_POSTGRES_URL", &url)
                .status()
                .expect("create admin")
                .success()
        );
        let server = ChildGuard(
            Command::new(binary)
                .args(["run", "--config"])
                .arg(config)
                .env("IOT_TEST_POSTGRES_URL", url)
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()
                .expect("start server"),
        );
        let base = format!("http://127.0.0.1:{http}");
        common::wait_ready(&reqwest::Client::new(), &base).await;
        Self {
            _server: server,
            _dir: dir,
            base,
            mqtt_addr: format!("127.0.0.1:{mqtt}").parse().unwrap(),
            http: reqwest::Client::builder()
                .cookie_store(true)
                .build()
                .unwrap(),
        }
    }

    async fn create_device(&self, name: &str, enabled: bool) -> (String, String) {
        let response: Value = self
            .http
            .post(format!("{}/api/v1/devices", self.base))
            .header("X-IoT-CSRF", "1")
            .json(&json!({"name":name,"enabled":enabled}))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        (
            response["device"]["id"].as_str().unwrap().to_owned(),
            response["credential"]["token"].as_str().unwrap().to_owned(),
        )
    }
}

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}
