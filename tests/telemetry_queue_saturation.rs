mod common;

use std::time::Duration;

use common::{TestApp, mqtt_connect, mqtt_publish, mqtt_subscribe};
use iot_server::config::schema::TelemetryConfig;
use rumqttc::{Event, Packet, QoS};
use serde_json::Value;

// Metrics are process-wide, so this test must not share a binary with other TestApps.
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
