mod common;

use std::time::Duration;

use common::{TestApp, mqtt_connect, mqtt_publish, mqtt_subscribe, mqtt_wait_telemetry_ack};
use serde_json::Value;
use sha2::{Digest, Sha256};

async fn publish(app: &TestApp, device_id: &str, token: &str, raw: &[u8]) -> Value {
    publish_with_retain(app, device_id, token, raw, false).await
}

async fn publish_with_retain(
    app: &TestApp,
    device_id: &str,
    token: &str,
    raw: &[u8],
    retain: bool,
) -> Value {
    let (client, mut eventloop, _) =
        mqtt_connect(app.mqtt_addr, device_id, device_id, token, true).await;
    let ack_topic = format!("devices/{device_id}/telemetry/ack");
    mqtt_subscribe(
        &client,
        &mut eventloop,
        &ack_topic,
        rumqttc::QoS::AtLeastOnce,
    )
    .await;
    mqtt_publish(
        &client,
        &format!("devices/{device_id}/telemetry"),
        rumqttc::QoS::AtLeastOnce,
        retain,
        raw,
    )
    .await;
    mqtt_wait_telemetry_ack(&mut eventloop, &ack_topic, Duration::from_secs(3)).await
}

async fn wait_for_rows(app: &TestApp, device_id: &str, count: i64) {
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let rows: i64 =
                sqlx::query_scalar("SELECT COUNT(*) FROM telemetry_events WHERE device_id = ?")
                    .bind(device_id)
                    .fetch_one(&app.pool)
                    .await
                    .unwrap();
            if rows == count {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn valid_telemetry_is_stored_with_server_timestamp_and_raw_hash() {
    let app = TestApp::start().await;
    let device = app.create_device("telemetry-flow", true).await;
    let raw = br#"{"event_id":"evt-flow","values":{"temperature":27.5}}"#;
    let ack = publish(&app, &device.id, &device.token, raw).await;
    assert_eq!(ack["status"], "stored");
    assert!(ack["code"].is_null());
    wait_for_rows(&app, &device.id, 1).await;

    let row: (i64, i64, String, Vec<u8>) = sqlx::query_as(
        "SELECT ts, received_at, values_json, payload_sha256 FROM telemetry_events
         WHERE device_id = ? AND event_id = 'evt-flow'",
    )
    .bind(&device.id)
    .fetch_one(&app.pool)
    .await
    .unwrap();
    assert_eq!(row.0, ack["received_at"]);
    assert_eq!(row.1, ack["received_at"]);
    assert_eq!(
        serde_json::from_str::<Value>(&row.2).unwrap()["temperature"],
        27.5
    );
    assert_eq!(row.3, Sha256::digest(raw).as_slice());
}

#[tokio::test]
async fn byte_identical_retry_deduplicates_but_changed_bytes_conflict() {
    let app = TestApp::start().await;
    let device = app.create_device("telemetry-dedup", true).await;
    let first = br#"{"event_id":"evt-retry","values":{"x":1}}"#;
    assert_eq!(
        publish(&app, &device.id, &device.token, first).await["status"],
        "stored"
    );
    wait_for_rows(&app, &device.id, 1).await;

    assert_eq!(
        publish(&app, &device.id, &device.token, first).await["status"],
        "duplicate"
    );
    wait_for_rows(&app, &device.id, 1).await;
    let changed = br#"{ "event_id":"evt-retry","values":{"x":1}}"#;
    let ack = publish(&app, &device.id, &device.token, changed).await;
    assert_eq!(ack["status"], "rejected");
    assert_eq!(ack["code"], "event_id_conflict");
    wait_for_rows(&app, &device.id, 1).await;

    let hash: Vec<u8> = sqlx::query_scalar(
        "SELECT payload_sha256 FROM telemetry_events WHERE device_id = ? AND event_id = 'evt-retry'",
    )
    .bind(&device.id)
    .fetch_one(&app.pool)
    .await
    .unwrap();
    assert_eq!(hash, Sha256::digest(first).as_slice());
}

#[tokio::test]
async fn invalid_payload_is_not_persisted() {
    let app = TestApp::start().await;
    let device = app.create_device("telemetry-invalid", true).await;
    let ack = publish(
        &app,
        &device.id,
        &device.token,
        br#"{"event_id":"evt-bad","values":{"x":null}}"#,
    )
    .await;
    assert_eq!(ack["status"], "rejected");
    assert_eq!(ack["code"], "invalid_payload");
    tokio::time::sleep(Duration::from_millis(50)).await;
    let rows: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM telemetry_events WHERE device_id = ?")
        .bind(&device.id)
        .fetch_one(&app.pool)
        .await
        .unwrap();
    assert_eq!(rows, 0);
}

#[tokio::test]
async fn retained_telemetry_is_rejected_before_broker_storage() {
    let app = TestApp::start().await;
    let device = app.create_device("telemetry-retained", true).await;
    let raw = br#"{"event_id":"evt-retained","values":{"x":1}}"#;
    let ack = publish_with_retain(&app, &device.id, &device.token, raw, true).await;

    assert_eq!(ack["status"], "rejected");
    assert_eq!(ack["code"], "retained_not_allowed");
    let rows: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM telemetry_events WHERE device_id = ?")
        .bind(&device.id)
        .fetch_one(&app.pool)
        .await
        .unwrap();
    assert_eq!(rows, 0);
    assert_eq!(app.retained_count().await, 0);
}

#[tokio::test]
async fn missing_id_nested_values_long_strings_key_limit_and_skew_are_rejected() {
    let app = TestApp::start().await;
    let future_ts = iot_server::core::auth::now_ms() + 600_000;
    let long_value = "x".repeat(4_097);
    let keys = (0..129)
        .map(|key| format!("\"k{key}\":{key}"))
        .collect::<Vec<_>>()
        .join(",");
    let cases = [
        (
            br#"{"values":{"x":1}}"#.to_vec(),
            Value::Null,
            "missing_event_id",
        ),
        (
            br#"{"event_id":"nested","values":{"x":{"y":1}}}"#.to_vec(),
            serde_json::json!("nested"),
            "invalid_payload",
        ),
        (
            format!(r#"{{"event_id":"long","values":{{"x":"{long_value}"}}}}"#).into_bytes(),
            serde_json::json!("long"),
            "invalid_payload",
        ),
        (
            format!(r#"{{"event_id":"keys","values":{{{keys}}}}}"#).into_bytes(),
            serde_json::json!("keys"),
            "invalid_payload",
        ),
        (
            format!(r#"{{"event_id":"future","ts":{future_ts},"values":{{"x":1}}}}"#).into_bytes(),
            serde_json::json!("future"),
            "invalid_timestamp",
        ),
    ];

    for (index, (raw, event_id, code)) in cases.into_iter().enumerate() {
        let device = app
            .create_device(&format!("telemetry-validation-{index}"), true)
            .await;
        let ack = publish(&app, &device.id, &device.token, &raw).await;
        assert_eq!(ack["status"], "rejected", "{event_id}");
        assert_eq!(ack["event_id"], event_id);
        assert_eq!(ack["code"], code);
    }

    let rows: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM telemetry_events")
        .fetch_one(&app.pool)
        .await
        .unwrap();
    assert_eq!(rows, 0);
}
