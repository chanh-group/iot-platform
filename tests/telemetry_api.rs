mod common;

use iot_server::core::auth;
use iot_server::core::telemetry::{self, TelemetryPayload};
use iot_server::storage::sqlite::telemetry as telemetry_storage;
use serde_json::{Value, json};

use common::TestApp;

async fn logged_in_client(app: &TestApp) -> reqwest::Client {
    app.create_admin("telemetry-admin", "telemetry-test-password")
        .await;
    let client = app.client();
    let response = client
        .post(format!("{}/api/v1/auth/login", app.base))
        .header("X-IoT-CSRF", "1")
        .json(&json!({
            "username": "telemetry-admin",
            "password": "telemetry-test-password",
        }))
        .send()
        .await
        .unwrap();
    assert!(response.status().is_success());
    client
}

async fn insert_event(
    app: &TestApp,
    device_id: &str,
    event_id: &str,
    ts: i64,
    received_at: i64,
    values: Value,
) {
    let raw = json!({ "event_id": event_id, "ts": ts, "values": values })
        .to_string()
        .into_bytes();
    let limits = TelemetryPayload::try_from(&app.config.config.telemetry).unwrap();
    let event = telemetry::validate(&raw, received_at, limits).unwrap();
    assert_eq!(
        telemetry_storage::insert_batch(&app.pool, &[(device_id.to_owned(), event)])
            .await
            .unwrap(),
        [iot_server::storage::sqlite::telemetry::InsertResult::Stored]
    );
}

#[tokio::test]
async fn history_pages_are_stable_and_key_filter_projects_matching_values() {
    let app = TestApp::start().await;
    let client = logged_in_client(&app).await;
    let device = app.create_device("telemetry-api-history", true).await;
    let now = auth::now_ms();
    let ts = now - 1_000;
    insert_event(
        &app,
        &device.id,
        "history-1",
        ts,
        ts + 20,
        json!({ "x": 1, "y": 10 }),
    )
    .await;
    insert_event(
        &app,
        &device.id,
        "history-2",
        ts,
        ts + 40,
        json!({ "x": 2 }),
    )
    .await;
    insert_event(
        &app,
        &device.id,
        "history-3",
        ts,
        ts + 40,
        json!({ "x": 3 }),
    )
    .await;

    let endpoint = format!("{}/api/v1/devices/{}/telemetry", app.base, device.id);
    let first: Value = client
        .get(&endpoint)
        .query(&[
            ("from", ts.to_string()),
            ("to", (ts + 1).to_string()),
            ("limit", "1".to_owned()),
            ("keys", "x".to_owned()),
        ])
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(first["items"][0]["event_id"], "history-1");
    assert_eq!(first["items"][0]["values"], json!({ "x": 1 }));
    let cursor = first["next_cursor"].as_str().unwrap();

    let second: Value = client
        .get(&endpoint)
        .query(&[
            ("from", ts.to_string()),
            ("to", (ts + 1).to_string()),
            ("limit", "1".to_owned()),
            ("keys", "x".to_owned()),
            ("cursor", cursor.to_owned()),
        ])
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(second["items"][0]["event_id"], "history-2");
    let cursor = second["next_cursor"].as_str().unwrap();

    let third: Value = client
        .get(&endpoint)
        .query(&[
            ("from", ts.to_string()),
            ("to", (ts + 1).to_string()),
            ("limit", "1".to_owned()),
            ("keys", "x".to_owned()),
            ("cursor", cursor.to_owned()),
        ])
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(third["items"][0]["event_id"], "history-3");
    assert!(third["next_cursor"].is_null());
}

#[tokio::test]
async fn latest_uses_received_time_then_internal_id_as_tie_breakers() {
    let app = TestApp::start().await;
    let client = logged_in_client(&app).await;
    let device = app.create_device("telemetry-api-latest", true).await;
    let ts = auth::now_ms() - 1_000;
    insert_event(
        &app,
        &device.id,
        "latest-1",
        ts,
        ts + 10,
        json!({ "x": 1, "y": 4 }),
    )
    .await;
    insert_event(&app, &device.id, "latest-2", ts, ts + 20, json!({ "x": 2 })).await;
    insert_event(&app, &device.id, "latest-3", ts, ts + 20, json!({ "x": 3 })).await;

    let response: Value = client
        .get(format!(
            "{}/api/v1/devices/{}/telemetry/latest",
            app.base, device.id
        ))
        .query(&[("keys", "x,y")])
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(response["items"].as_array().unwrap().len(), 2);
    assert_eq!(response["items"][0]["key"], "y");
    assert_eq!(response["items"][0]["value"], 4);
    assert_eq!(response["items"][1]["key"], "x");
    assert_eq!(response["items"][1]["value"], 3);
    assert_eq!(response["items"][1]["event_id"], "latest-3");
}

#[tokio::test]
async fn missing_device_and_invalid_range_or_cursor_return_safe_errors() {
    let app = TestApp::start().await;
    let client = logged_in_client(&app).await;
    let url = format!("{}/api/v1/devices/missing-device/telemetry", app.base);
    let unauthenticated = reqwest::Client::new()
        .get(&url)
        .query(&[("from", "1"), ("to", "2")])
        .send()
        .await
        .unwrap();
    assert_eq!(unauthenticated.status(), reqwest::StatusCode::UNAUTHORIZED);

    let missing = client
        .get(&url)
        .query(&[("from", "1"), ("to", "2")])
        .send()
        .await
        .unwrap();
    assert_eq!(missing.status(), reqwest::StatusCode::NOT_FOUND);
    assert_eq!(
        missing.json::<Value>().await.unwrap()["code"],
        "device_not_found"
    );

    let device = app.create_device("telemetry-api-invalid", true).await;
    let endpoint = format!("{}/api/v1/devices/{}/telemetry", app.base, device.id);
    let invalid_range = client
        .get(&endpoint)
        .query(&[("from", "10"), ("to", "9")])
        .send()
        .await
        .unwrap();
    assert_eq!(invalid_range.status(), reqwest::StatusCode::BAD_REQUEST);
    assert_eq!(
        invalid_range.json::<Value>().await.unwrap()["code"],
        "validation_error"
    );
    let missing_range = client.get(&endpoint).send().await.unwrap();
    assert_eq!(missing_range.status(), reqwest::StatusCode::BAD_REQUEST);

    let invalid_cursor = client
        .get(&endpoint)
        .query(&[("from", "10"), ("to", "20"), ("cursor", "not-a-cursor")])
        .send()
        .await
        .unwrap();
    assert_eq!(invalid_cursor.status(), reqwest::StatusCode::BAD_REQUEST);
    let body: Value = invalid_cursor.json().await.unwrap();
    assert_eq!(body["code"], "invalid_cursor");
    assert!(!body.to_string().to_lowercase().contains("select"));

    let too_wide = client
        .get(&endpoint)
        .query(&[("from", "1"), ("to", "2678400002")])
        .send()
        .await
        .unwrap();
    assert_eq!(too_wide.status(), reqwest::StatusCode::BAD_REQUEST);

    let too_many_keys = client
        .get(&endpoint)
        .query(&[
            ("from", "1"),
            ("to", "2"),
            ("keys", "a,b,c,d,e,f,g,h,i,j,k,l,m,n,o,p,q"),
        ])
        .send()
        .await
        .unwrap();
    assert_eq!(too_many_keys.status(), reqwest::StatusCode::BAD_REQUEST);

    sqlx::query("DROP TABLE telemetry_events")
        .execute(&app.pool)
        .await
        .unwrap();
    let internal_error = client
        .get(&endpoint)
        .query(&[("from", "1"), ("to", "2")])
        .send()
        .await
        .unwrap();
    assert_eq!(
        internal_error.status(),
        reqwest::StatusCode::INTERNAL_SERVER_ERROR
    );
    let body = internal_error.text().await.unwrap().to_lowercase();
    assert!(!body.contains("select"));
    assert!(!body.contains("telemetry_events"));
}
