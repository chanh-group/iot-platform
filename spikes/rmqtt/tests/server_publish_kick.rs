mod common;

use common::*;
use rmqtt_spike::Event;
use rumqttc::ConnectReturnCode;
use std::time::Duration;

#[tokio::test]
async fn server_publish_kick() {
    let server = start().await;
    let commands = format!("devices/{DEVICE_A}/commands");
    let (client, mut eventloop, _) = connect_ok(&server, DEVICE_A, DEVICE_A, TOKEN_A, true).await;
    let codes = subscribe(&client, &mut eventloop, &commands).await;
    assert_eq!(
        codes,
        vec![rumqttc::SubscribeReasonCode::Success(
            rumqttc::QoS::AtLeastOnce
        )]
    );

    server
        .publish_server(DEVICE_A, &commands, b"reboot")
        .await
        .unwrap();
    let publish = wait_publish(&mut eventloop, Duration::from_secs(5)).await;
    assert_eq!(publish.topic, commands);
    assert_eq!(publish.payload.as_ref(), b"reboot");
    assert_eq!(publish.qos, rumqttc::QoS::AtLeastOnce);
    assert!(!publish.retain);

    // Duplicate client id: the second connection replaces the first.
    let (client2, mut eventloop2, session_present) =
        connect_ok(&server, DEVICE_A, DEVICE_A, TOKEN_A, true).await;
    assert!(!session_present);
    wait_closed(&mut eventloop, Duration::from_secs(5)).await;
    wait_sessions(&server, 1, Duration::from_secs(5)).await;

    let generations: Vec<i64> = server
        .events()
        .await
        .into_iter()
        .filter_map(|event| match event {
            Event::Connected {
                client_id,
                created_at,
                ..
            } if client_id == DEVICE_A => Some(created_at),
            _ => None,
        })
        .collect();
    assert!(
        generations.len() >= 2,
        "expected two connection generations: {generations:?}"
    );
    assert_ne!(
        generations[0], generations[1],
        "session id must change per connection"
    );

    server.kick(DEVICE_A).await.unwrap();
    wait_closed(&mut eventloop2, Duration::from_secs(5)).await;
    wait_sessions(&server, 0, Duration::from_secs(5)).await;

    // Rotate the credential, then prove the old token is refused and the new
    // token works. Product code will call kick after the DB transaction.
    let new_token = "iot_spike_rotated_token_for_device_a_000000";
    server.rotate(DEVICE_A, new_token).await;
    assert_eq!(
        connect_refused(&server, DEVICE_A, DEVICE_A, TOKEN_A).await,
        ConnectReturnCode::BadUserNamePassword
    );
    let (client3, mut eventloop3, _) =
        connect_ok(&server, DEVICE_A, DEVICE_A, new_token, true).await;
    let codes = subscribe(&client3, &mut eventloop3, &commands).await;
    assert_eq!(
        codes,
        vec![rumqttc::SubscribeReasonCode::Success(
            rumqttc::QoS::AtLeastOnce
        )]
    );
    server
        .publish_server(DEVICE_A, &commands, b"rotated")
        .await
        .unwrap();
    let publish = wait_publish(&mut eventloop3, Duration::from_secs(5)).await;
    assert_eq!(publish.payload.as_ref(), b"rotated");

    drop(client);
    drop(client2);
    drop(client3);
    drop(eventloop3);
}
