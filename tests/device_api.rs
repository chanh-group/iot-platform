mod common;

use common::TestApp;
use serde_json::{Value, json};

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
