mod common;

use common::{TestApp, mqtt_connect, mqtt_subscribe, mqtt_wait_closed, mqtt_wait_publish};
use iot_server::observability::metrics::Metrics;
use rumqttc::{Event, Packet, QoS};
use std::time::Duration;

static RETAINED_CAP_TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

async fn publish_qos1(
    client: &rumqttc::AsyncClient,
    eventloop: &mut rumqttc::EventLoop,
    topic: &str,
    retain: bool,
    payload: &[u8],
) {
    client
        .publish(topic, QoS::AtLeastOnce, retain, payload)
        .await
        .expect("queue MQTT publish");
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if matches!(
                eventloop.poll().await.expect("poll MQTT event"),
                Event::Incoming(Packet::PubAck(_))
            ) {
                break;
            }
        }
    })
    .await
    .expect("receive MQTT PUBACK before timeout");
}

#[tokio::test]
async fn attributes_are_latest_per_key_and_invalid_or_retained_updates_are_dropped() {
    let app = TestApp::start().await;
    let device = app.create_device("attributes-flow", true).await;
    let topic = format!("devices/{}/attributes", device.id);
    let (client, mut eventloop, _) =
        common::mqtt_connect(app.mqtt_addr, &device.id, &device.id, &device.token, true).await;

    publish_qos1(
        &client,
        &mut eventloop,
        &topic,
        false,
        br#"{"values":{"firmware":"1.0","interval_s":30}}"#,
    )
    .await;
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let count: i64 =
                sqlx::query_scalar("SELECT COUNT(*) FROM device_attributes WHERE device_id = ?")
                    .bind(&device.id)
                    .fetch_one(&app.pool)
                    .await
                    .expect("query initial attribute count");
            if count == 2 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("initial attributes should be stored before timeout");

    publish_qos1(
        &client,
        &mut eventloop,
        &topic,
        false,
        br#"{"values":{"firmware":"2.0"}}"#,
    )
    .await;
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let value: Option<String> = sqlx::query_scalar(
                "SELECT value_json FROM device_attributes WHERE device_id = ? AND key = 'firmware'",
            )
            .bind(&device.id)
            .fetch_optional(&app.pool)
            .await
            .expect("query latest firmware attribute");
            if value.as_deref() == Some(r#""2.0""#) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("firmware update should be stored before timeout");
    let rows: Vec<(String, String)> = sqlx::query_as(
        "SELECT key, value_json FROM device_attributes WHERE device_id = ? ORDER BY key",
    )
    .bind(&device.id)
    .fetch_all(&app.pool)
    .await
    .expect("query latest attributes");
    assert_eq!(
        rows,
        [
            (String::from("firmware"), String::from(r#""2.0""#)),
            (String::from("interval_s"), String::from("30"))
        ],
        "updating one attribute must preserve the latest value of other keys"
    );

    publish_qos1(
        &client,
        &mut eventloop,
        &topic,
        false,
        br#"{"values":{"invalid":null}}"#,
    )
    .await;
    let count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM device_attributes WHERE device_id = ?")
            .bind(&device.id)
            .fetch_one(&app.pool)
            .await
            .expect("query attributes after invalid update");
    assert_eq!(count, 2, "invalid attribute payload must not add a row");

    client
        .publish(&topic, QoS::AtLeastOnce, true, br#"{"values":{"x":1}}"#)
        .await
        .expect("queue retained attributes publish");
    common::mqtt_wait_closed(&mut eventloop, Duration::from_secs(2)).await;
    assert_eq!(
        app.retained_count().await,
        0,
        "retained attributes must be rejected before storage"
    );
}

#[tokio::test]
async fn retained_replace_clear_and_caps_keep_accounting_consistent() {
    let _guard = RETAINED_CAP_TEST_LOCK.lock().await;
    let app = TestApp::builder().retained_limits(1, 5).start().await;
    let device = app.create_device("retained-cap", true).await;
    let status = format!("devices/{}/status", device.id);
    let data = format!("devices/{}/data/x", device.id);
    let (client, mut eventloop, _) =
        common::mqtt_connect(app.mqtt_addr, &device.id, &device.id, &device.token, true).await;

    publish_qos1(&client, &mut eventloop, &status, true, b"1234").await;
    assert_eq!(app.retained_count().await, 1, "status should be retained");
    assert!(
        app.presence
            .is_online(&device.id)
            .expect("presence state should remain healthy"),
        "retaining status must not change MQTT presence"
    );

    // Replacement subtracts the previous payload before applying the byte cap.
    publish_qos1(&client, &mut eventloop, &status, true, b"12345").await;
    assert_eq!(
        app.retained_count().await,
        1,
        "replacing the same topic must not consume another topic slot"
    );

    publish_qos1(&client, &mut eventloop, &status, true, b"").await;
    assert_eq!(
        app.retained_count().await,
        0,
        "clearing retained status removes it"
    );
    publish_qos1(&client, &mut eventloop, &data, true, b"12345").await;
    assert_eq!(
        app.retained_count().await,
        1,
        "clearing must release the topic slot"
    );

    client
        .publish(&status, QoS::AtLeastOnce, true, b"x")
        .await
        .expect("queue over-cap retained publish");
    common::mqtt_wait_closed(&mut eventloop, Duration::from_secs(2)).await;
    assert_eq!(
        app.retained_count().await,
        1,
        "over-cap publish must not replace or add retained data"
    );
}

#[tokio::test]
async fn retained_byte_cap_rejects_oversized_replacement_and_counts_it() {
    let _guard = RETAINED_CAP_TEST_LOCK.lock().await;
    let app = TestApp::builder().retained_limits(2, 5).start().await;
    let rejected_before = Metrics::global().retained_cap_rejected_total.get();
    let device = app.create_device("retained-byte-cap", true).await;
    let status = format!("devices/{}/status", device.id);
    let (client, mut eventloop, _) =
        mqtt_connect(app.mqtt_addr, &device.id, &device.id, &device.token, true).await;

    publish_qos1(&client, &mut eventloop, &status, true, b"12345").await;
    client
        .publish(&status, QoS::AtLeastOnce, true, b"123456")
        .await
        .expect("queue retained payload over byte cap");
    mqtt_wait_closed(&mut eventloop, Duration::from_secs(2)).await;

    assert_eq!(app.retained_count().await, 1);
    assert!(
        Metrics::global().retained_cap_rejected_total.get() == rejected_before + 1,
        "byte-cap rejection must increment the retained rejection metric"
    );
    drop(client);
    drop(eventloop);

    let (client, mut eventloop, _) =
        mqtt_connect(app.mqtt_addr, &device.id, &device.id, &device.token, true).await;
    assert_eq!(
        mqtt_subscribe(&client, &mut eventloop, &status, QoS::AtLeastOnce).await,
        rumqttc::SubscribeReasonCode::Success(QoS::AtLeastOnce)
    );
    let retained = mqtt_wait_publish(&mut eventloop, Duration::from_secs(2)).await;
    assert_eq!(retained.payload.as_ref(), b"12345");
    assert!(retained.retain);
}

#[tokio::test]
async fn retained_state_is_empty_after_broker_restart() {
    let mut app = TestApp::start().await;
    let device = app.create_device("retained-restart", true).await;
    let status = format!("devices/{}/status", device.id);
    let (client, mut eventloop, _) =
        common::mqtt_connect(app.mqtt_addr, &device.id, &device.id, &device.token, true).await;

    publish_qos1(&client, &mut eventloop, &status, true, b"online").await;
    assert_eq!(
        app.retained_count().await,
        1,
        "publish should populate retained storage"
    );
    drop(eventloop);
    drop(client);

    app.restart_mqtt().await;
    assert_eq!(
        app.retained_count().await,
        0,
        "a fresh broker context must start without retained messages"
    );
}
