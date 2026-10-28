mod common;

use common::*;
use rumqttc::{AsyncClient, LastWill, QoS, SubscribeReasonCode};
use std::time::Duration;

#[tokio::test]
async fn qos_smoke() {
    let server = start().await;
    let (client, mut eventloop, _) = connect_ok(&server, DEVICE_A, DEVICE_A, TOKEN_A, true).await;
    let filter = format!("devices/{DEVICE_A}/data/#");
    let codes = subscribe_with_qos(&client, &mut eventloop, &filter, QoS::ExactlyOnce).await;
    assert_eq!(codes, vec![SubscribeReasonCode::Success(QoS::ExactlyOnce)]);

    for (topic, qos, payload) in [
        (
            format!("devices/{DEVICE_A}/data/q0"),
            QoS::AtMostOnce,
            b"zero".as_slice(),
        ),
        (
            format!("devices/{DEVICE_A}/data/q1"),
            QoS::AtLeastOnce,
            b"one".as_slice(),
        ),
        (
            format!("devices/{DEVICE_A}/data/q2"),
            QoS::ExactlyOnce,
            b"two".as_slice(),
        ),
    ] {
        client.publish(&topic, qos, false, payload).await.unwrap();
        let publish = wait_publish(&mut eventloop, Duration::from_secs(5)).await;
        assert_eq!(publish.topic, topic);
        assert_eq!(publish.payload.as_ref(), payload);
        assert_eq!(publish.qos, qos);
    }

    drop(client);
    drop(eventloop);
}

#[tokio::test]
async fn last_will() {
    let server = start().await;
    let status = format!("devices/{DEVICE_A}/status");
    let mut options = options_for(server.addr(), DEVICE_A, DEVICE_A, TOKEN_A, false);
    options.set_last_will(LastWill::new(&status, b"offline", QoS::AtLeastOnce, true));
    let (client, mut eventloop) = AsyncClient::new(options, 16);
    assert!(!wait_connack(&mut eventloop).await);

    // No MQTT DISCONNECT: the socket drop must publish the will.
    drop(client);
    drop(eventloop);
    wait_event(
        &server,
        |event| matches!(event, rmqtt_spike::Event::Disconnected { client_id, .. } if client_id == DEVICE_A),
        Duration::from_secs(5),
    )
    .await;
    tokio::time::sleep(Duration::from_millis(300)).await;

    let (client2, mut eventloop2, _) = connect_ok(&server, DEVICE_A, DEVICE_A, TOKEN_A, true).await;
    let codes = subscribe(&client2, &mut eventloop2, &status).await;
    assert_eq!(codes, vec![SubscribeReasonCode::Success(QoS::AtLeastOnce)]);
    let publish = wait_publish(&mut eventloop2, Duration::from_secs(5)).await;
    assert_eq!(publish.topic, status);
    assert_eq!(publish.payload.as_ref(), b"offline");
    assert!(publish.retain);

    drop(client2);
    drop(eventloop2);
}
