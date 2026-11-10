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
        [iot_server::core::telemetry::TelemetryInsertResult::Stored]
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

    let changed_to = client
        .get(&endpoint)
        .query(&[
            ("from", ts.to_string()),
            ("to", (ts + 2).to_string()),
            ("limit", "1".to_owned()),
            ("keys", "x".to_owned()),
            ("cursor", cursor.to_owned()),
        ])
        .send()
        .await
        .unwrap();
    assert_eq!(changed_to.status(), reqwest::StatusCode::BAD_REQUEST);
    assert_eq!(
        changed_to.json::<Value>().await.unwrap()["code"],
        "invalid_cursor"
    );

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
async fn commit_order_backfill_finds_late_events_and_paginates_by_checkpoint() {
    let app = TestApp::start().await;
    let client = logged_in_client(&app).await;
    let device = app.create_device("telemetry-received-backfill", true).await;
    let now = auth::now_ms();
    insert_event(
        &app,
        &device.id,
        "before-checkpoint",
        now - 300_000,
        now - 1,
        json!({"x": 0}),
    )
    .await;
    let checkpoint: i64 = sqlx::query_scalar(
        "SELECT internal_id FROM telemetry_events WHERE device_id = ? AND event_id = 'before-checkpoint'",
    )
    .bind(&device.id)
    .fetch_one(&app.pool)
    .await
    .unwrap();
    insert_event(
        &app,
        &device.id,
        "checkpoint-overlap",
        now - 300_000,
        now,
        json!({"x": 1}),
    )
    .await;
    insert_event(
        &app,
        &device.id,
        "late-event-1",
        now - 300_000,
        now + 1,
        json!({"x": 2}),
    )
    .await;
    insert_event(
        &app,
        &device.id,
        "late-event-2",
        now - 300_000,
        now + 2,
        json!({"x": 3}),
    )
    .await;

    let endpoint = format!("{}/api/v1/devices/{}/telemetry", app.base, device.id);
    let first: Value = client
        .get(&endpoint)
        .query(&[
            ("after_id", checkpoint.to_string()),
            ("limit", "2".to_owned()),
        ])
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(
        first["items"]
            .as_array()
            .unwrap()
            .iter()
            .map(|item| item["event_id"].as_str().unwrap())
            .collect::<Vec<_>>(),
        ["checkpoint-overlap", "late-event-1"]
    );
    let through_id = first["through_id"].as_i64().unwrap();
    assert!(through_id >= first["items"][1]["internal_id"].as_i64().unwrap());
    let cursor = first["next_cursor"].as_str().unwrap();

    let second: Value = client
        .get(&endpoint)
        .query(&[
            ("after_id", checkpoint.to_string()),
            ("limit", "2".to_owned()),
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
    assert_eq!(second["items"][0]["event_id"], "late-event-2");
    assert!(second["next_cursor"].is_null());
    assert_eq!(second["through_id"], through_id);

    for query in [
        vec![
            ("from", checkpoint.to_string()),
            ("to", now.to_string()),
            ("after_id", checkpoint.to_string()),
        ],
        vec![("after_id", "-1".to_owned())],
        vec![("after_id", (through_id + 1).to_string())],
    ] {
        let response = client.get(&endpoint).query(&query).send().await.unwrap();
        assert_eq!(response.status(), reqwest::StatusCode::BAD_REQUEST);
        assert_eq!(
            response.json::<Value>().await.unwrap()["code"],
            "validation_error"
        );
    }
}

#[tokio::test]
async fn starting_checkpoint_avoids_scanning_old_history_before_a_missed_event() {
    let app = TestApp::start().await;
    let client = logged_in_client(&app).await;
    let device = app.create_device("telemetry-api-large-history", true).await;
    let now = auth::now_ms();
    let old_timestamp = now - 40 * 24 * 60 * 60 * 1_000;
    let mut tx = app.pool.begin().await.unwrap();
    for index in 0..1_001_i64 {
        sqlx::query(
            "INSERT INTO telemetry_events
             (device_id, event_id, ts, received_at, values_json, payload_sha256)
             VALUES (?, ?, ?, ?, ?, ?)",
        )
        .bind(&device.id)
        .bind(format!("old-history-{index}"))
        .bind(old_timestamp - index)
        .bind(old_timestamp - index)
        .bind(r#"{"x":1}"#)
        .bind([3_u8; 32].as_slice())
        .execute(&mut *tx)
        .await
        .unwrap();
    }
    tx.commit().await.unwrap();

    let endpoint = format!("{}/api/v1/devices/{}/telemetry", app.base, device.id);
    let baseline: Value = client
        .get(&endpoint)
        .query(&[("after_id", "0"), ("limit", "1")])
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    let checkpoint = baseline["through_id"].as_i64().unwrap();
    assert_eq!(checkpoint, 1_001);
    assert!(baseline["next_cursor"].is_string());

    insert_event(
        &app,
        &device.id,
        "missed-near-now",
        now - 1_000,
        now,
        json!({"x": 2}),
    )
    .await;
    let recovered: Value = client
        .get(&endpoint)
        .query(&[("after_id", checkpoint.to_string())])
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(recovered["items"].as_array().unwrap().len(), 1);
    assert_eq!(recovered["items"][0]["event_id"], "missed-near-now");
    assert!(recovered["through_id"].as_i64().unwrap() > checkpoint);
}

#[tokio::test]
async fn starting_checkpoint_skips_large_old_history_and_recovers_a_new_event() {
    let app = TestApp::start().await;
    let client = logged_in_client(&app).await;
    let device = app
        .create_device("telemetry-api-large-bootstrap", true)
        .await;
    let now = auth::now_ms();
    let old_ts = now - 40 * 24 * 60 * 60 * 1_000;
    let mut tx = app.pool.begin().await.unwrap();
    for index in 0..1_001_i64 {
        sqlx::query(
            "INSERT INTO telemetry_events
             (device_id, event_id, ts, received_at, values_json, payload_sha256)
             VALUES (?, ?, ?, ?, ?, ?)",
        )
        .bind(&device.id)
        .bind(format!("old-history-{index}"))
        .bind(old_ts - index)
        .bind(old_ts - index)
        .bind(r#"{"x":1}"#)
        .bind([4_u8; 32].as_slice())
        .execute(&mut *tx)
        .await
        .unwrap();
    }
    tx.commit().await.unwrap();

    let endpoint = format!("{}/api/v1/devices/{}/telemetry", app.base, device.id);
    let baseline: Value = client
        .get(&endpoint)
        .query(&[("after_id", "0"), ("limit", "1")])
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    let checkpoint = baseline["through_id"].as_i64().unwrap();
    assert_eq!(checkpoint, 1_001);
    assert!(baseline["next_cursor"].is_string());

    insert_event(
        &app,
        &device.id,
        "missed-near-now",
        now - 1_000,
        now,
        json!({"x": 2}),
    )
    .await;
    let recovered: Value = client
        .get(&endpoint)
        .query(&[("after_id", checkpoint.to_string())])
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(recovered["items"].as_array().unwrap().len(), 1);
    assert_eq!(recovered["items"][0]["event_id"], "missed-near-now");
    assert!(recovered["through_id"].as_i64().unwrap() > checkpoint);
}

#[tokio::test]
async fn commit_checkpoint_does_not_advance_past_an_uncommitted_batch() {
    let app = TestApp::start().await;
    let client = logged_in_client(&app).await;
    let device = app
        .create_device("telemetry-api-pending-commit", true)
        .await;
    insert_event(
        &app,
        &device.id,
        "committed-before-pending",
        auth::now_ms(),
        auth::now_ms(),
        json!({"x": 1}),
    )
    .await;
    let checkpoint: i64 = sqlx::query_scalar("SELECT MAX(internal_id) FROM telemetry_events")
        .fetch_one(&app.pool)
        .await
        .unwrap();

    let mut conn = app.pool.acquire().await.unwrap();
    sqlx::query("BEGIN IMMEDIATE")
        .execute(&mut *conn)
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO telemetry_events
         (device_id, event_id, ts, received_at, values_json, payload_sha256)
         VALUES (?, ?, ?, ?, ?, ?)",
    )
    .bind(&device.id)
    .bind("waiting-for-commit")
    .bind(auth::now_ms() - 40 * 24 * 60 * 60 * 1_000)
    .bind(auth::now_ms() - 40 * 24 * 60 * 60 * 1_000)
    .bind(r#"{"x":2}"#)
    .bind([7_u8; 32].as_slice())
    .execute(&mut *conn)
    .await
    .unwrap();

    let endpoint = format!("{}/api/v1/devices/{}/telemetry", app.base, device.id);
    let before_commit: Value = client
        .get(&endpoint)
        .query(&[("after_id", checkpoint.to_string())])
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(before_commit["through_id"], checkpoint);
    assert!(before_commit["items"].as_array().unwrap().is_empty());

    sqlx::query("COMMIT").execute(&mut *conn).await.unwrap();
    let after_commit: Value = client
        .get(&endpoint)
        .query(&[("after_id", checkpoint.to_string())])
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(after_commit["items"][0]["event_id"], "waiting-for-commit");
    assert!(after_commit["through_id"].as_i64().unwrap() > checkpoint);
    assert!(
        after_commit["items"][0]["ts"].as_i64().unwrap()
            < auth::now_ms() - 31 * 24 * 60 * 60 * 1_000
    );
}

#[tokio::test]
async fn ignored_duplicate_and_conflict_advance_checkpoint_without_backfill_rows() {
    let app = TestApp::start().await;
    let client = logged_in_client(&app).await;
    let device = app
        .create_device("telemetry-api-ignored-inserts", true)
        .await;
    let endpoint = format!("{}/api/v1/devices/{}/telemetry", app.base, device.id);
    let ts = auth::now_ms();
    let limits = TelemetryPayload::try_from(&app.config.config.telemetry).unwrap();
    let original_raw = json!({
        "event_id": "ignored-insert",
        "ts": ts,
        "values": {"x": 1},
    })
    .to_string()
    .into_bytes();
    let original = telemetry::validate(&original_raw, ts, limits).unwrap();
    assert_eq!(
        telemetry_storage::insert_batch(&app.pool, &[(device.id.clone(), original)])
            .await
            .unwrap(),
        [iot_server::core::telemetry::TelemetryInsertResult::Stored]
    );

    let initial: Value = client
        .get(&endpoint)
        .query(&[("after_id", "0"), ("limit", "1")])
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    let initial_checkpoint = initial["through_id"].as_i64().unwrap();

    let duplicate = telemetry::validate(&original_raw, ts, limits).unwrap();
    assert_eq!(
        telemetry_storage::insert_batch(&app.pool, &[(device.id.clone(), duplicate)])
            .await
            .unwrap(),
        [iot_server::core::telemetry::TelemetryInsertResult::Duplicate]
    );
    let after_duplicate: Value = client
        .get(&endpoint)
        .query(&[("after_id", initial_checkpoint.to_string())])
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    let duplicate_checkpoint = after_duplicate["through_id"].as_i64().unwrap();
    assert!(duplicate_checkpoint > initial_checkpoint);
    assert!(after_duplicate["items"].as_array().unwrap().is_empty());

    let conflict_raw = json!({
        "event_id": "ignored-insert",
        "ts": ts,
        "values": {"x": 2},
    })
    .to_string()
    .into_bytes();
    let conflict = telemetry::validate(&conflict_raw, ts, limits).unwrap();
    assert_eq!(
        telemetry_storage::insert_batch(&app.pool, &[(device.id.clone(), conflict)])
            .await
            .unwrap(),
        [iot_server::core::telemetry::TelemetryInsertResult::Conflict]
    );
    let after_conflict: Value = client
        .get(&endpoint)
        .query(&[("after_id", initial_checkpoint.to_string())])
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    let safe_checkpoint = after_conflict["through_id"].as_i64().unwrap();
    assert!(safe_checkpoint > duplicate_checkpoint);
    assert!(after_conflict["items"].as_array().unwrap().is_empty());

    insert_event(
        &app,
        &device.id,
        "stored-after-ignored-inserts",
        auth::now_ms(),
        auth::now_ms(),
        json!({"x": 3}),
    )
    .await;
    let recovered: Value = client
        .get(&endpoint)
        .query(&[("after_id", safe_checkpoint.to_string())])
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(recovered["items"].as_array().unwrap().len(), 1);
    assert_eq!(
        recovered["items"][0]["event_id"],
        "stored-after-ignored-inserts"
    );
}

#[tokio::test]
async fn sqlite_commit_checkpoint_serializes_competing_writers_and_backfills_both() {
    let app = TestApp::start().await;
    let client = logged_in_client(&app).await;
    let device = app.create_device("telemetry-api-writer-order", true).await;
    let endpoint = format!("{}/api/v1/devices/{}/telemetry", app.base, device.id);

    let mut first = app.pool.acquire().await.unwrap();
    sqlx::query("BEGIN IMMEDIATE")
        .execute(&mut *first)
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO telemetry_events
         (device_id, event_id, ts, received_at, values_json, payload_sha256)
         VALUES (?, 'writer-one', ?, ?, '{\"x\":1}', ?)",
    )
    .bind(&device.id)
    .bind(auth::now_ms())
    .bind(auth::now_ms())
    .bind([1_u8; 32].as_slice())
    .execute(&mut *first)
    .await
    .unwrap();

    let mut second = app.pool.acquire().await.unwrap();
    sqlx::query("PRAGMA busy_timeout = 25")
        .execute(&mut *second)
        .await
        .unwrap();
    let lock_error = sqlx::query("BEGIN IMMEDIATE")
        .execute(&mut *second)
        .await
        .unwrap_err();
    assert!(
        lock_error.to_string().contains("locked"),
        "expected competing writer to be locked: {lock_error}"
    );

    let while_first_is_open: Value = client
        .get(&endpoint)
        .query(&[("after_id", "0"), ("limit", "1")])
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(while_first_is_open["through_id"], 0);
    assert!(while_first_is_open["items"].as_array().unwrap().is_empty());

    sqlx::query("COMMIT").execute(&mut *first).await.unwrap();
    insert_event(
        &app,
        &device.id,
        "writer-two",
        auth::now_ms(),
        auth::now_ms(),
        json!({"x": 2}),
    )
    .await;

    let first_page: Value = client
        .get(&endpoint)
        .query(&[("after_id", "0"), ("limit", "1")])
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(first_page["items"][0]["event_id"], "writer-one");
    let first_id = first_page["items"][0]["internal_id"].as_i64().unwrap();
    let through_id = first_page["through_id"].as_i64().unwrap();
    assert!(through_id > first_id);

    let second_page: Value = client
        .get(&endpoint)
        .query(&[
            ("after_id", "0"),
            ("limit", "1"),
            ("cursor", first_page["next_cursor"].as_str().unwrap()),
        ])
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(second_page["items"][0]["event_id"], "writer-two");
    let second_id = second_page["items"][0]["internal_id"].as_i64().unwrap();
    assert!(second_id > first_id && second_id <= through_id);
    assert_eq!(second_page["through_id"], through_id);
    assert!(second_page["next_cursor"].is_null());

    sqlx::query("BEGIN IMMEDIATE")
        .execute(&mut *first)
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO telemetry_events
         (device_id, event_id, ts, received_at, values_json, payload_sha256)
         VALUES (?, 'writer-rolled-back', ?, ?, '{\"x\":3}', ?)",
    )
    .bind(&device.id)
    .bind(auth::now_ms())
    .bind(auth::now_ms())
    .bind([3_u8; 32].as_slice())
    .execute(&mut *first)
    .await
    .unwrap();
    let lock_error = sqlx::query("BEGIN IMMEDIATE")
        .execute(&mut *second)
        .await
        .unwrap_err();
    assert!(
        lock_error.to_string().contains("locked"),
        "expected competing writer to be locked: {lock_error}"
    );

    let while_rollback_is_open: Value = client
        .get(&endpoint)
        .query(&[("after_id", through_id.to_string())])
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(while_rollback_is_open["through_id"], through_id);
    assert!(
        while_rollback_is_open["items"]
            .as_array()
            .unwrap()
            .is_empty()
    );

    sqlx::query("ROLLBACK").execute(&mut *first).await.unwrap();
    insert_event(
        &app,
        &device.id,
        "writer-after-rollback",
        auth::now_ms(),
        auth::now_ms(),
        json!({"x": 4}),
    )
    .await;

    let after_rollback: Value = client
        .get(&endpoint)
        .query(&[("after_id", through_id.to_string())])
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(after_rollback["items"].as_array().unwrap().len(), 1);
    assert_eq!(
        after_rollback["items"][0]["event_id"],
        "writer-after-rollback"
    );
}

#[tokio::test]
async fn latest_uses_received_time_then_internal_id_as_tie_breakers() {
    let app = TestApp::start().await;
    let client = logged_in_client(&app).await;
    let device = app.create_device("telemetry-api-latest", true).await;
    let ts = auth::now_ms() - 1_000;
    insert_event(&app, &device.id, "latest-1", ts, ts + 10, json!({ "z": 4 })).await;
    insert_event(
        &app,
        &device.id,
        "latest-2",
        ts + 20,
        ts + 30,
        json!({ "a": 2 }),
    )
    .await;
    insert_event(
        &app,
        &device.id,
        "latest-3",
        ts + 20,
        ts + 30,
        json!({ "a": 3 }),
    )
    .await;
    insert_event(
        &app,
        &device.id,
        "latest-4",
        ts + 20,
        ts + 25,
        json!({ "a": 4 }),
    )
    .await;

    let response: Value = client
        .get(format!(
            "{}/api/v1/devices/{}/telemetry/latest",
            app.base, device.id
        ))
        .query(&[("keys", "a,z")])
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(response["items"].as_array().unwrap().len(), 2);
    assert_eq!(response["items"][0]["key"], "a");
    assert_eq!(response["items"][0]["value"], 3);
    assert_eq!(response["items"][0]["event_id"], "latest-3");
    assert_eq!(response["items"][1]["key"], "z");
    assert_eq!(response["items"][1]["value"], 4);
    assert!(
        response["items"][0]["ts"].as_i64().unwrap() > response["items"][1]["ts"].as_i64().unwrap()
    );
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
