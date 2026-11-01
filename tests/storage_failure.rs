mod common;

use std::time::Duration;

use common::{
    TestApp, mqtt_connect, mqtt_publish, mqtt_subscribe, mqtt_wait_puback, mqtt_wait_telemetry_ack,
};
use iot_server::config::schema::TelemetryConfig;
use rumqttc::QoS;
use serde_json::json;

fn test_telemetry_config(queue_capacity: u64) -> TelemetryConfig {
    TelemetryConfig {
        queue_capacity,
        batch_size: 1,
        flush_interval_ms: 10,
        ..TelemetryConfig::default()
    }
}

async fn start_app(queue_capacity: u64) -> TestApp {
    TestApp::builder()
        .telemetry_config(test_telemetry_config(queue_capacity))
        .start()
        .await
}

async fn connect_telemetry(
    app: &TestApp,
    name: &str,
) -> (rumqttc::AsyncClient, rumqttc::EventLoop, String, String) {
    let device = app.create_device(name, true).await;
    let (client, mut eventloop, _) =
        mqtt_connect(app.mqtt_addr, &device.id, &device.id, &device.token, true).await;
    let ack_topic = format!("devices/{}/telemetry/ack", device.id);
    let telemetry_topic = format!("devices/{}/telemetry", device.id);
    mqtt_subscribe(&client, &mut eventloop, &ack_topic, QoS::AtLeastOnce).await;
    (client, eventloop, ack_topic, telemetry_topic)
}

async fn readiness_status(app: &TestApp) -> reqwest::StatusCode {
    reqwest::Client::new()
        .get(format!("{}/readyz", app.base))
        .send()
        .await
        .expect("readiness request")
        .status()
}

async fn set_max_page_count(app: &TestApp, page_count: i64) {
    let mut connections = Vec::new();
    for _ in 0..5 {
        let mut connection = app.pool.acquire().await.expect("acquire SQLite connection");
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "PRAGMA max_page_count = {page_count}"
        )))
        .execute(&mut *connection)
        .await
        .expect("set SQLite page limit");
        connections.push(connection);
    }
}

fn large_telemetry(event_id: &str) -> Vec<u8> {
    let value = "x".repeat(4_000);
    let values = (0..2)
        .map(|index| (format!("key{index}"), json!(value.clone())))
        .collect::<serde_json::Map<_, _>>();
    serde_json::to_vec(&json!({"event_id": event_id, "values": values}))
        .expect("serialize telemetry")
}

#[tokio::test]
async fn database_failure_never_publishes_stored_and_recovers() {
    let app = start_app(16).await;
    let (client, mut eventloop, ack_topic, telemetry_topic) =
        connect_telemetry(&app, "storage-db-failure").await;
    sqlx::query(
        "CREATE TRIGGER fail_telemetry BEFORE INSERT ON telemetry_events
         BEGIN SELECT RAISE(FAIL, 'injected database failure'); END",
    )
    .execute(&app.pool)
    .await
    .expect("install database failure trigger");

    mqtt_publish(
        &client,
        &telemetry_topic,
        QoS::AtLeastOnce,
        false,
        br#"{"event_id":"db-failure","values":{"x":1}}"#,
    )
    .await;
    let failed_ack =
        mqtt_wait_telemetry_ack(&mut eventloop, &ack_topic, Duration::from_secs(3)).await;
    assert_eq!(failed_ack["status"], "retryable");
    assert_eq!(failed_ack["code"], "storage_unavailable");
    assert_eq!(
        readiness_status(&app).await,
        reqwest::StatusCode::SERVICE_UNAVAILABLE
    );

    sqlx::query("DROP TRIGGER fail_telemetry")
        .execute(&app.pool)
        .await
        .expect("remove database failure trigger");
    mqtt_publish(
        &client,
        &telemetry_topic,
        QoS::AtLeastOnce,
        false,
        br#"{"event_id":"db-recovery","values":{"x":2}}"#,
    )
    .await;
    let stored = mqtt_wait_telemetry_ack(&mut eventloop, &ack_topic, Duration::from_secs(5)).await;
    assert_eq!(stored["status"], "stored");
    assert_eq!(readiness_status(&app).await, reqwest::StatusCode::OK);
}

#[tokio::test]
async fn sqlite_full_never_panics_or_publishes_stored_and_recovers() {
    let app = start_app(16).await;
    let (client, mut eventloop, ack_topic, telemetry_topic) =
        connect_telemetry(&app, "storage-sqlite-full").await;
    let page_count: i64 = sqlx::query_scalar("PRAGMA page_count")
        .fetch_one(&app.pool)
        .await
        .expect("read SQLite page count");
    set_max_page_count(&app, page_count).await;

    let disk_full_payload = large_telemetry("disk-full");
    mqtt_publish(
        &client,
        &telemetry_topic,
        QoS::AtLeastOnce,
        false,
        &disk_full_payload,
    )
    .await;
    if let Ok(failed_ack) = tokio::time::timeout(
        Duration::from_secs(3),
        mqtt_wait_telemetry_ack(&mut eventloop, &ack_topic, Duration::from_secs(1)),
    )
    .await
    {
        assert_ne!(failed_ack["status"], "stored");
    }
    assert_eq!(
        readiness_status(&app).await,
        reqwest::StatusCode::SERVICE_UNAVAILABLE
    );

    set_max_page_count(&app, 1_000_000).await;
    let recovery_payload = large_telemetry("disk-recovery");
    mqtt_publish(
        &client,
        &telemetry_topic,
        QoS::AtLeastOnce,
        false,
        &recovery_payload,
    )
    .await;
    let stored = mqtt_wait_telemetry_ack(&mut eventloop, &ack_topic, Duration::from_secs(5)).await;
    assert_eq!(stored["status"], "stored");
    assert_eq!(readiness_status(&app).await, reqwest::StatusCode::OK);
}

#[tokio::test]
async fn saturated_ingest_queue_stays_bounded_and_exports_rejection_metric() {
    const METRICS_TOKEN: &str = "storage-queue-metrics-token-long-enough-012345";
    let app = TestApp::builder()
        .metrics_token(METRICS_TOKEN)
        .telemetry_config(test_telemetry_config(16))
        .start()
        .await;
    let (client, mut eventloop, _, telemetry_topic) =
        connect_telemetry(&app, "storage-queue-full").await;
    sqlx::query(
        "CREATE TRIGGER fail_telemetry BEFORE INSERT ON telemetry_events
         BEGIN SELECT RAISE(FAIL, 'injected queue saturation failure'); END",
    )
    .execute(&app.pool)
    .await
    .expect("install queue saturation trigger");

    for index in 0..64 {
        let payload = format!(r#"{{"event_id":"queue-{index}","values":{{"x":{index}}}}}"#);
        mqtt_publish(
            &client,
            &telemetry_topic,
            QoS::AtLeastOnce,
            false,
            payload.as_bytes(),
        )
        .await;
        mqtt_wait_puback(&mut eventloop, Duration::from_secs(2)).await;
    }

    let metrics = reqwest::Client::new()
        .get(format!("{}/metrics", app.base))
        .bearer_auth(METRICS_TOKEN)
        .send()
        .await
        .expect("metrics request")
        .text()
        .await
        .expect("metrics body");
    let queue_full_count = metrics
        .lines()
        .find_map(|line| line.strip_prefix("iot_ingest_rejected_total{reason=\"queue_full\"} "))
        .and_then(|count| count.parse::<u64>().ok())
        .unwrap_or_default();
    assert!(queue_full_count > 0);
    let queue_depth = metrics
        .lines()
        .find_map(|line| line.strip_prefix("iot_telemetry_queue_depth "))
        .and_then(|depth| depth.parse::<u64>().ok())
        .expect("queue depth metric");
    assert!(queue_depth <= 16);
    assert_eq!(
        readiness_status(&app).await,
        reqwest::StatusCode::SERVICE_UNAVAILABLE
    );

    let rows: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM telemetry_events")
        .fetch_one(&app.pool)
        .await
        .expect("count telemetry rows");
    assert_eq!(rows, 0);
}
