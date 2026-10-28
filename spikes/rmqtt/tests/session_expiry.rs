mod common;

use common::*;
use rmqtt_spike::{Event, SpikeConfig};
use rumqttc::SubscribeReasonCode;
use std::time::Duration;

#[tokio::test]
async fn session_expiry() {
    let server = start_with(SpikeConfig {
        session_expiry: Duration::from_secs(2),
        ..config()
    })
    .await;
    let commands = format!("devices/{DEVICE_A}/commands");

    let (client, mut eventloop, session_present) =
        connect_ok(&server, DEVICE_A, DEVICE_A, TOKEN_A, false).await;
    assert!(!session_present);
    let codes = subscribe(&client, &mut eventloop, &commands).await;
    assert_eq!(
        codes,
        vec![SubscribeReasonCode::Success(rumqttc::QoS::AtLeastOnce)]
    );

    drop(client);
    drop(eventloop);
    wait_event(
        &server,
        |event| matches!(event, Event::Disconnected { client_id, .. } if client_id == DEVICE_A),
        Duration::from_secs(5),
    )
    .await;
    wait_sessions(&server, 1, Duration::from_secs(5)).await;

    // Reconnect inside the 2 s window: session and subscription survive.
    let (client2, mut eventloop2, session_present) =
        connect_ok(&server, DEVICE_A, DEVICE_A, TOKEN_A, false).await;
    assert!(session_present);
    server
        .publish_server(DEVICE_A, &commands, b"persisted")
        .await
        .unwrap();
    let publish = wait_publish(&mut eventloop2, Duration::from_secs(5)).await;
    assert_eq!(publish.payload.as_ref(), b"persisted");

    drop(client2);
    drop(eventloop2);
    wait_event(
        &server,
        |event| matches!(event, Event::Disconnected { client_id, .. } if client_id == DEVICE_A),
        Duration::from_secs(5),
    )
    .await;
    wait_sessions(&server, 0, Duration::from_secs(6)).await;

    // After expiry a clean_session=0 reconnect starts a brand-new session.
    let (client3, eventloop3, session_present) =
        connect_ok(&server, DEVICE_A, DEVICE_A, TOKEN_A, false).await;
    assert!(!session_present);

    drop(client3);
    drop(eventloop3);
}
