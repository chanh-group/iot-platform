mod common;

use std::time::Duration;

use common::{TestApp, mqtt_connect, mqtt_publish, mqtt_subscribe, mqtt_wait_telemetry_ack};
use iot_server::config::schema::TelemetryConfig;
use rumqttc::{Event, Packet, QoS};
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

#[tokio::test]
async fn full_ingest_queue_returns_retryable_without_success_ack_and_exports_metric() {
    const METRICS_TOKEN: &str = "queue-full-metrics-test-token-long-enough-012345";
    let telemetry_config = TelemetryConfig {
        queue_capacity: 16,
        batch_size: 1,
        flush_interval_ms: 10,
        ..TelemetryConfig::default()
    };
    let app = TestApp::builder()
        .metrics_token(METRICS_TOKEN)
        .telemetry_config(telemetry_config)
        .start()
        .await;
    assert_eq!(app.config.config.telemetry.queue_capacity, 16);
    let device = app.create_device("telemetry-queue-full", true).await;
    sqlx::query(
        "CREATE TRIGGER fail_telemetry BEFORE INSERT ON telemetry_events
         BEGIN SELECT RAISE(FAIL, 'injected commit failure'); END",
    )
    .execute(&app.pool)
    .await
    .unwrap();

    let (client, mut eventloop, _) =
        mqtt_connect(app.mqtt_addr, &device.id, &device.id, &device.token, true).await;
    let ack_topic = format!("devices/{}/telemetry/ack", device.id);
    let telemetry_topic = format!("devices/{}/telemetry", device.id);
    mqtt_subscribe(&client, &mut eventloop, &ack_topic, QoS::AtLeastOnce).await;

    let mut acknowledgements = Vec::<Value>::new();
    for index in 0..64 {
        let payload = format!(r#"{{"event_id":"queue-full-{index}","values":{{"x":{index}}}}}"#);
        mqtt_publish(
            &client,
            &telemetry_topic,
            QoS::AtLeastOnce,
            false,
            payload.as_bytes(),
        )
        .await;
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                match eventloop.poll().await.unwrap() {
                    Event::Incoming(Packet::PubAck(_)) => break,
                    Event::Incoming(Packet::Publish(packet)) if packet.topic == ack_topic => {
                        acknowledgements.push(serde_json::from_slice(&packet.payload).unwrap());
                    }
                    _ => {}
                }
            }
        })
        .await
        .expect("receive MQTT PUBACK before timeout");
    }
    tokio::time::timeout(Duration::from_secs(3), async {
        while !acknowledgements
            .iter()
            .any(|ack| ack["code"] == "storage_unavailable")
        {
            if let Event::Incoming(Packet::Publish(packet)) = eventloop.poll().await.unwrap()
                && packet.topic == ack_topic
            {
                acknowledgements.push(serde_json::from_slice(&packet.payload).unwrap());
            }
        }
    })
    .await
    .expect("failed commit should return storage_unavailable");
    tokio::time::timeout(Duration::from_secs(3), async {
        while !acknowledgements
            .iter()
            .any(|ack| ack["code"] == "queue_full")
        {
            if let Event::Incoming(Packet::Publish(packet)) = eventloop.poll().await.unwrap()
                && packet.topic == ack_topic
            {
                acknowledgements.push(serde_json::from_slice(&packet.payload).unwrap());
            }
        }
    })
    .await
    .expect("queue_full ACK must not wait for storage recovery");
    assert!(
        !acknowledgements
            .iter()
            .any(|ack| ack["status"] == "stored" || ack["status"] == "duplicate"),
        "a failed commit must never produce a success ACK"
    );

    let metrics = reqwest::Client::new()
        .get(format!("{}/metrics", app.base))
        .bearer_auth(METRICS_TOKEN)
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    let queue_full_count = metrics
        .lines()
        .find_map(|line| line.strip_prefix("iot_ingest_rejected_total{reason=\"queue_full\"} "))
        .and_then(|count| count.parse::<u64>().ok())
        .unwrap_or_default();
    assert!(
        queue_full_count > 0,
        "queue rejection metric must be exported"
    );
    assert!(
        metrics
            .lines()
            .any(|line| line == "iot_telemetry_queue_depth 16"),
        "queue depth must stop at the configured capacity"
    );

    let rows: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM telemetry_events WHERE device_id = ?")
        .bind(&device.id)
        .fetch_one(&app.pool)
        .await
        .unwrap();
    assert_eq!(rows, 0);

    let readiness = reqwest::Client::new()
        .get(format!("{}/readyz", app.base))
        .send()
        .await
        .unwrap();
    assert_eq!(readiness.status(), reqwest::StatusCode::SERVICE_UNAVAILABLE);
    let readiness: Value = readiness.json().await.unwrap();
    assert_eq!(readiness["checks"]["database"], "ok");
    assert_eq!(readiness["checks"]["ingest"], "degraded");

    let queue_full_ids: Vec<_> = acknowledgements
        .iter()
        .filter(|ack| ack["code"] == "queue_full")
        .map(|ack| ack["event_id"].as_str().unwrap().to_owned())
        .collect();
    assert!(!queue_full_ids.is_empty());
    for event_id in queue_full_ids {
        assert!(
            acknowledgements.iter().any(|ack| {
                ack["event_id"] == event_id
                    && ack["status"] == "retryable"
                    && ack["code"] == "queue_full"
            }),
            "queue-full event must receive only a retryable ACK"
        );
        assert!(
            !acknowledgements.iter().any(|ack| {
                ack["event_id"] == event_id
                    && (ack["status"] == "stored" || ack["status"] == "duplicate")
            }),
            "queue-full event must never receive a success ACK"
        );
        let rows: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM telemetry_events WHERE device_id = ? AND event_id = ?",
        )
        .bind(&device.id)
        .bind(&event_id)
        .fetch_one(&app.pool)
        .await
        .unwrap();
        assert_eq!(rows, 0, "queue-full event must not be written");
    }
    sqlx::query("DROP TRIGGER fail_telemetry")
        .execute(&app.pool)
        .await
        .unwrap();
}
