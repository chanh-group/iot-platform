//! P2-02/P2-04: device auth hook, protocol gating, presence generation (A03).

mod common;
#[path = "common/mqtt_auth_postgres.rs"]
mod postgres;

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
    let presence = server.presence.clone();
    let online_id = device.id.clone();
    let pool = server.pool.clone();
    let last_seen_id = device.id.clone();
    valid_token_presence_body(
        server.mqtt_addr,
        &device.id,
        &device.token,
        move || {
            let presence = presence.clone();
            let id = online_id.clone();
            async move {
                presence
                    .is_online(&id)
                    .expect("presence state should remain healthy")
            }
        },
        move || {
            let pool = pool.clone();
            let id = last_seen_id.clone();
            async move {
                sqlx::query_scalar("SELECT last_seen_at FROM devices WHERE id = ?")
                    .bind(id)
                    .fetch_optional(&pool)
                    .await
                    .unwrap()
            }
        },
    )
    .await;
}

#[tokio::test]
#[ignore = "requires disposable PostgreSQL fixture; run tests/mqtt_auth_postgres.sh"]
async fn valid_token_connects_and_marks_presence_postgres() {
    let server = postgres::PostgresApp::start().await;
    let device = server.create_device("sensor-ok").await;
    let online_server = server.clone_for_queries();
    let online_id = device.id.clone();
    let last_seen_server = server.clone_for_queries();
    let last_seen_id = device.id.clone();
    valid_token_presence_body(
        server.mqtt_addr(),
        &device.id,
        &device.token,
        move || {
            let server = online_server.clone();
            let id = online_id.clone();
            async move { server.is_online(&id).await }
        },
        move || {
            let server = last_seen_server.clone();
            let id = last_seen_id.clone();
            async move { server.last_seen(&id).await }
        },
    )
    .await;
}

async fn valid_token_presence_body<Online, OnlineFuture, LastSeen, LastSeenFuture>(
    address: std::net::SocketAddr,
    device_id: &str,
    token: &str,
    is_online: Online,
    last_seen: LastSeen,
) where
    Online: Fn() -> OnlineFuture,
    OnlineFuture: std::future::Future<Output = bool>,
    LastSeen: Fn() -> LastSeenFuture,
    LastSeenFuture: std::future::Future<Output = Option<i64>>,
{
    let (client, eventloop, session_present) =
        mqtt_connect(address, device_id, device_id, token, true).await;
    assert!(!session_present);
    wait_online(&is_online, true, "device to become online").await;
    assert!(last_seen().await.is_some());

    drop(client);
    drop(eventloop);
    wait_online(&is_online, false, "device to become offline").await;
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
        || {
            server
                .presence
                .online_count()
                .expect("presence state should remain healthy")
                == 0
        },
        WAIT,
        "presence to drain",
    )
    .await;
}

#[tokio::test]
async fn mqtt_31_and_mqtt_5_are_refused_at_the_wire() {
    let server = TestApp::start().await;
    let device = server.create_device("sensor-proto", true).await;

    assert_mqtt_versions_refused(server.mqtt_addr, &device.id, &device.token).await;
    assert_eq!(server.presence.online_count().unwrap(), 0);
}

#[tokio::test]
#[ignore = "requires disposable PostgreSQL fixture; run tests/mqtt_auth_postgres.sh"]
async fn mqtt_31_and_mqtt_5_are_refused_at_the_wire_postgres() {
    let server = postgres::PostgresApp::start().await;
    let device = server.create_device("sensor-proto").await;

    assert_mqtt_versions_refused(server.mqtt_addr(), &device.id, &device.token).await;
    server.assert_no_devices_online().await;
}

async fn assert_mqtt_versions_refused(address: std::net::SocketAddr, device_id: &str, token: &str) {
    let v3 = raw_connack(address, 3, device_id, device_id, token).await;
    assert_eq!(connack_code(&v3, 3), 0x05);

    let v5 = raw_connack(address, 5, device_id, device_id, token).await;
    assert_eq!(connack_code(&v5, 5), 0x87);
}

#[tokio::test]
async fn duplicate_client_id_uses_session_generation() {
    let server = TestApp::start().await;
    let device = server.create_device("sensor-dup", true).await;

    let presence = server.presence.clone();
    let online_id = device.id.clone();
    let pool = server.pool.clone();
    let last_seen_id = device.id.clone();
    duplicate_client_id_uses_session_generation_body(
        server.mqtt_addr,
        &device.id,
        &device.token,
        move || {
            let presence = presence.clone();
            let id = online_id.clone();
            async move {
                presence
                    .is_online(&id)
                    .expect("presence state should remain healthy")
            }
        },
        move || {
            let pool = pool.clone();
            let id = last_seen_id.clone();
            async move {
                sqlx::query_scalar("SELECT last_seen_at FROM devices WHERE id = ?")
                    .bind(id)
                    .fetch_optional(&pool)
                    .await
                    .unwrap()
            }
        },
    )
    .await;
}

#[tokio::test]
#[ignore = "requires disposable PostgreSQL fixture; run tests/mqtt_auth_postgres.sh"]
async fn duplicate_client_id_uses_session_generation_postgres() {
    let server = postgres::PostgresApp::start().await;
    let device = server.create_device("sensor-dup").await;
    let online_server = server.clone_for_queries();
    let online_id = device.id.clone();
    let last_seen_server = server.clone_for_queries();
    let last_seen_id = device.id.clone();
    duplicate_client_id_uses_session_generation_body(
        server.mqtt_addr(),
        &device.id,
        &device.token,
        move || {
            let server = online_server.clone();
            let id = online_id.clone();
            async move { server.is_online(&id).await }
        },
        move || {
            let server = last_seen_server.clone();
            let id = last_seen_id.clone();
            async move { server.last_seen(&id).await }
        },
    )
    .await;
}

async fn duplicate_client_id_uses_session_generation_body<
    Online,
    OnlineFuture,
    LastSeen,
    LastSeenFuture,
>(
    mqtt_addr: std::net::SocketAddr,
    device_id: &str,
    token: &str,
    is_online: Online,
    last_seen: LastSeen,
) where
    Online: Fn() -> OnlineFuture,
    OnlineFuture: std::future::Future<Output = bool>,
    LastSeen: Fn() -> LastSeenFuture,
    LastSeenFuture: std::future::Future<Output = Option<i64>>,
{
    let (client1, mut eventloop1, _) =
        mqtt_connect(mqtt_addr, device_id, device_id, token, true).await;
    wait_online(&is_online, true, "first connection online").await;

    let (client2, eventloop2, session_present) =
        mqtt_connect(mqtt_addr, device_id, device_id, token, true).await;
    assert!(!session_present, "clean session must not be resumed");
    mqtt_wait_closed(&mut eventloop1, WAIT).await;

    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(
        is_online().await,
        "disconnect of the replaced session must not clear the new one"
    );

    drop(client2);
    drop(eventloop2);
    wait_online(&is_online, false, "second connection offline").await;
    assert!(last_seen().await.is_some());

    drop(client1);
}

async fn wait_online<F, Fut>(is_online: &F, expected: bool, message: &str)
where
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    tokio::time::timeout(WAIT, async {
        while is_online().await != expected {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("timed out waiting for {message}"));
}
