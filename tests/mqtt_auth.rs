//! P2-02/P2-04: device auth hook, protocol gating, presence generation (A03).

mod common;

use std::time::Duration;

use common::{
    TestApp, connack_code, mqtt_connect, mqtt_connect_refused, mqtt_wait_closed, raw_connack,
    raw_publish_after_refused_connect, wait_until,
};
use iot_server::config::schema::TelemetryConfig;
use rumqttc::ConnectReturnCode;

const WAIT: Duration = Duration::from_secs(5);

#[tokio::test]
async fn valid_token_connects_and_marks_presence() {
    let server = TestApp::start().await;
    let device = server.create_device("sensor-ok", true).await;

    let (client, eventloop, session_present) = mqtt_connect(
        server.mqtt_addr,
        &device.id,
        &device.id,
        &device.token,
        true,
    )
    .await;
    assert!(!session_present);

    wait_until(
        || server.presence.is_online(&device.id),
        WAIT,
        "device to become online",
    )
    .await;
    assert!(server.last_seen(&device.id).await.is_some());

    drop(client);
    drop(eventloop);
    wait_until(
        || !server.presence.is_online(&device.id),
        WAIT,
        "device to become offline",
    )
    .await;
}

#[tokio::test]
async fn bad_credentials_are_all_refused_with_the_same_code() {
    let server = TestApp::builder()
        .telemetry_config(TelemetryConfig {
            queue_capacity: 16,
            batch_size: 1,
            flush_interval_ms: 10,
            ..TelemetryConfig::default()
        })
        .start()
        .await;
    let device = server.create_device("sensor-refused", true).await;
    let disabled = server.create_device("sensor-disabled", false).await;
    let revoked = server.create_device("sensor-revoked", true).await;
    server.revoke_active_credential(&revoked.id).await;
    let rotated = server.create_device("sensor-rotated", true).await;
    let rotated_new_token = server.rotate_credential(&rotated.id).await;
    let unknown = uuid::Uuid::now_v7().to_string();
    let other = uuid::Uuid::now_v7().to_string();

    for (label, client_id, username, token) in [
        (
            "wrong token",
            device.id.as_str(),
            device.id.as_str(),
            "iot_wrong_token",
        ),
        (
            "unknown device",
            unknown.as_str(),
            unknown.as_str(),
            "iot_x",
        ),
        (
            "disabled device",
            disabled.id.as_str(),
            disabled.id.as_str(),
            disabled.token.as_str(),
        ),
        (
            "revoked credential",
            revoked.id.as_str(),
            revoked.id.as_str(),
            revoked.token.as_str(),
        ),
        (
            "rotated credential",
            rotated.id.as_str(),
            rotated.id.as_str(),
            rotated.token.as_str(),
        ),
        (
            "client id mismatch",
            other.as_str(),
            device.id.as_str(),
            device.token.as_str(),
        ),
        (
            "non-canonical device id",
            device.id.as_str(),
            device.id.to_uppercase().as_str(),
            device.token.as_str(),
        ),
    ] {
        assert_eq!(
            mqtt_connect_refused(server.mqtt_addr, client_id, username, token).await,
            ConnectReturnCode::BadUserNamePassword,
            "{label}"
        );
    }

    for device_id in [&device.id, &disabled.id] {
        let token = if device_id == &device.id {
            "iot_wrong_token"
        } else {
            disabled.token.as_str()
        };
        assert!(
            !raw_publish_after_refused_connect(
                server.mqtt_addr,
                device_id,
                device_id,
                token,
                &format!("devices/{device_id}/telemetry"),
                br#"{"event_id":"unauthorized-attempt","values":{"x":1}}"#,
            )
            .await,
            "refused authentication must not receive a PUBACK"
        );
    }
    tokio::time::sleep(Duration::from_millis(50)).await;
    for device_id in [&device.id, &disabled.id] {
        let rows: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM telemetry_events WHERE device_id = ?")
                .bind(device_id)
                .fetch_one(&server.pool)
                .await
                .unwrap();
        assert_eq!(
            rows, 0,
            "refused MQTT authentication must not store telemetry"
        );
    }

    let (client, eventloop, _) = mqtt_connect(
        server.mqtt_addr,
        &rotated.id,
        &rotated.id,
        &rotated_new_token,
        true,
    )
    .await;
    drop(client);
    drop(eventloop);
    wait_until(
        || server.presence.online_count() == 0,
        WAIT,
        "presence to drain",
    )
    .await;
}

#[tokio::test]
async fn mqtt_31_and_mqtt_5_are_refused_at_the_wire() {
    let server = TestApp::start().await;
    let device = server.create_device("sensor-proto", true).await;

    let v3 = raw_connack(server.mqtt_addr, 3, &device.id, &device.id, &device.token).await;
    assert_eq!(connack_code(&v3, 3), 0x05);

    let v5 = raw_connack(server.mqtt_addr, 5, &device.id, &device.id, &device.token).await;
    assert_eq!(connack_code(&v5, 5), 0x87);
    assert_eq!(server.presence.online_count(), 0);
}

#[tokio::test]
async fn duplicate_client_id_uses_session_generation() {
    let server = TestApp::start().await;
    let device = server.create_device("sensor-dup", true).await;

    let (client1, mut eventloop1, _) = mqtt_connect(
        server.mqtt_addr,
        &device.id,
        &device.id,
        &device.token,
        true,
    )
    .await;
    wait_until(
        || server.presence.is_online(&device.id),
        WAIT,
        "first connection online",
    )
    .await;

    let (client2, eventloop2, session_present) = mqtt_connect(
        server.mqtt_addr,
        &device.id,
        &device.id,
        &device.token,
        true,
    )
    .await;
    assert!(!session_present, "clean session must not be resumed");
    mqtt_wait_closed(&mut eventloop1, WAIT).await;

    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(
        server.presence.is_online(&device.id),
        "disconnect of the replaced session must not clear the new one"
    );

    drop(client2);
    drop(eventloop2);
    wait_until(
        || !server.presence.is_online(&device.id),
        WAIT,
        "second connection offline",
    )
    .await;
    assert!(server.last_seen(&device.id).await.is_some());

    drop(client1);
}
