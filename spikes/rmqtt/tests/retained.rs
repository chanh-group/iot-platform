mod common;

use common::*;
use rmqtt_spike::{SpikeConfig, SpikeServer};
use rumqttc::{AsyncClient, EventLoop, SubscribeReasonCode};
use std::time::Duration;

#[tokio::test]
async fn retained() {
    let server = start().await;
    let status = format!("devices/{DEVICE_A}/status");
    let (client, mut eventloop, _) = connect_ok(&server, DEVICE_A, DEVICE_A, TOKEN_A, true).await;

    publish_qos1(&client, &mut eventloop, &status, true, b"online").await;
    assert_eq!(server.retained_topics().await, 1);
    assert_eq!(server.retained_bytes().await, 6);
    assert_eq!(server.retained_count().await, 1);

    let codes = subscribe(&client, &mut eventloop, &status).await;
    assert_eq!(
        codes,
        vec![SubscribeReasonCode::Success(rumqttc::QoS::AtLeastOnce)]
    );
    let publish = wait_publish(&mut eventloop, Duration::from_secs(5)).await;
    assert_eq!(publish.payload.as_ref(), b"online");
    assert!(publish.retain);

    publish_qos1(&client, &mut eventloop, &status, true, b"offline").await;
    assert_eq!(server.retained_topics().await, 1);
    assert_eq!(server.retained_bytes().await, 7);
    assert_eq!(server.retained_count().await, 1);

    let codes = subscribe(&client, &mut eventloop, &status).await;
    assert_eq!(
        codes,
        vec![SubscribeReasonCode::Success(rumqttc::QoS::AtLeastOnce)]
    );
    let publish = wait_publish(&mut eventloop, Duration::from_secs(5)).await;
    assert_eq!(publish.payload.as_ref(), b"offline");

    publish_qos1(&client, &mut eventloop, &status, true, b"").await;
    assert_eq!(server.retained_topics().await, 0);
    assert_eq!(server.retained_bytes().await, 0);
    assert_eq!(server.retained_count().await, 0);

    let codes = subscribe(&client, &mut eventloop, &status).await;
    assert_eq!(
        codes,
        vec![SubscribeReasonCode::Success(rumqttc::QoS::AtLeastOnce)]
    );
    expect_no_publish(&mut eventloop, Duration::from_millis(400)).await;

    drop(client);
    drop(eventloop);
    server.abort();
    tokio::time::sleep(Duration::from_millis(200)).await;

    retained_cap_and_restart().await;
}

async fn retained_cap_and_restart() {
    let cap_config = SpikeConfig {
        max_retained_messages: 1,
        max_retained_bytes: 8,
        ..config()
    };
    let server = SpikeServer::start(cap_config.clone()).await.unwrap();
    let addr = server.addr();
    let status = format!("devices/{DEVICE_A}/status");
    let data = format!("devices/{DEVICE_A}/data/x");
    let (client, mut eventloop, _) = connect_ok(&server, DEVICE_A, DEVICE_A, TOKEN_A, true).await;

    publish_qos1(&client, &mut eventloop, &status, true, b"1234").await;
    assert_eq!(server.retained_topics().await, 1);
    assert_eq!(server.retained_bytes().await, 4);
    assert_eq!(server.retained_count().await, 1);

    // Topic-count cap: this publish is rejected before storage.
    let acked = publish_maybe_closed(&client, &mut eventloop, &data, true, b"5678").await;
    assert!(!acked, "over-cap retained publish must not be acknowledged");
    assert_eq!(server.retained_topics().await, 1);
    assert_eq!(server.retained_bytes().await, 4);
    assert_eq!(server.retained_count().await, 1);

    let (client, mut eventloop) = reconnect(&server).await;
    publish_qos1(&client, &mut eventloop, &status, true, b"").await;
    assert_eq!(server.retained_topics().await, 0);
    assert_eq!(server.retained_bytes().await, 0);
    assert_eq!(server.retained_count().await, 0);

    // Byte cap: 9 > 8 is rejected.
    publish_maybe_closed(&client, &mut eventloop, &status, true, b"123456789").await;
    assert_eq!(server.retained_topics().await, 0);
    assert_eq!(server.retained_bytes().await, 0);
    assert_eq!(server.retained_count().await, 0);

    let (client, mut eventloop) = reconnect(&server).await;
    publish_qos1(&client, &mut eventloop, &data, true, b"abcd").await;
    assert_eq!(server.retained_topics().await, 1);
    assert_eq!(server.retained_bytes().await, 4);
    assert_eq!(server.retained_count().await, 1);

    drop(client);
    drop(eventloop);
    server.abort();
    tokio::time::sleep(Duration::from_millis(300)).await;

    // MVP scope: retained state is process memory only and must not survive.
    let restarted = SpikeServer::start_on(addr, cap_config).await.unwrap();
    assert_eq!(restarted.retained_topics().await, 0);
    assert_eq!(restarted.retained_bytes().await, 0);
    assert_eq!(restarted.retained_count().await, 0);
    let (_client, _eventloop, _) = connect_ok(&restarted, DEVICE_A, DEVICE_A, TOKEN_A, true).await;
    restarted.abort();
}

async fn reconnect(server: &SpikeServer) -> (AsyncClient, EventLoop) {
    let (client, eventloop, _) = connect_ok(server, DEVICE_A, DEVICE_A, TOKEN_A, true).await;
    (client, eventloop)
}
