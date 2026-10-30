mod common;

use std::time::Duration;

use common::{TestApp, mqtt_connect, mqtt_connect_refused, mqtt_wait_closed};
use iot_server::core::auth;
use rumqttc::ConnectReturnCode;
use serde_json::{Value, json};

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
    expected.sort_by(|a, b| b.cmp(a));
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
    let url = format!("{}/api/v1/devices", app.base);
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

    let (_mqtt, _events, _) =
        common::mqtt_connect(app.mqtt_addr, &first.id, &first.id, &first.token, true).await;
    let device: Value = http
        .get(format!("{base}/{}", first.id))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(device["online"], true);
    assert_eq!(device["name"], "first-sensor");
    assert!(device["credential"]["id"].is_string());
    assert!(!device.to_string().contains(&first.token));
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
    assert_eq!(filtered["items"][0]["id"], second.id);
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
