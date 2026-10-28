//! P2-03: default-deny ACL matrix over a real broker (T12–T14, A04).

mod common;

use std::time::Duration;

use common::{TestApp, TestDevice, mqtt_connect, mqtt_next, mqtt_wait_closed, raw_publish_denied};
use rumqttc::{AsyncClient, Event as MqttEvent, EventLoop, Packet, QoS, SubscribeReasonCode};

const WAIT: Duration = Duration::from_secs(5);

#[tokio::test]
async fn own_topics_are_allowed_for_pub_and_sub() {
    let server = TestApp::start().await;
    let device = server.create_device("acl-own", true).await;
    let (client, mut eventloop, _) = mqtt_connect(
        server.mqtt_addr,
        &device.id,
        &device.id,
        &device.token,
        true,
    )
    .await;

    for filter in [
        format!("devices/{}/telemetry/ack", device.id),
        format!("devices/{}/commands", device.id),
        format!("devices/{}/status", device.id),
        format!("devices/{}/data/#", device.id),
        format!("devices/{}/data/sensors/+", device.id),
    ] {
        assert_eq!(
            subscribe(&client, &mut eventloop, &filter).await,
            SubscribeReasonCode::Success(QoS::AtLeastOnce),
            "{filter}"
        );
    }

    for topic in [
        format!("devices/{}/telemetry", device.id),
        format!("devices/{}/attributes", device.id),
        format!("devices/{}/commands/ack", device.id),
        format!("devices/{}/status", device.id),
        format!("devices/{}/data/sensors/room", device.id),
    ] {
        publish_ok(&client, &mut eventloop, &topic).await;
    }
}

#[tokio::test]
async fn subscribe_outside_own_prefix_is_refused_and_keeps_the_connection() {
    let server = TestApp::start().await;
    let a = server.create_device("acl-sub-a", true).await;
    let b = server.create_device("acl-sub-b", true).await;
    let (client, mut eventloop, _) =
        mqtt_connect(server.mqtt_addr, &a.id, &a.id, &a.token, true).await;

    // T13/T14 plus every filter that would widen the read set.
    for filter in [
        format!("devices/{}/commands", b.id),
        format!("devices/{}/data/#", b.id),
        "devices/+/telemetry".to_owned(),
        "devices/#".to_owned(),
        a.id.clone(),
        format!("devices/{}/#", a.id),
        format!("devices/{}/telemetry", a.id),
        format!("devices/{}/attributes", a.id),
        "$SYS/#".to_owned(),
        format!("$share/group/devices/{}/data/#", a.id),
    ] {
        assert_eq!(
            subscribe(&client, &mut eventloop, &filter).await,
            SubscribeReasonCode::Failure,
            "{filter}"
        );
    }

    // Denied filters do not close the connection.
    assert_eq!(
        subscribe(&client, &mut eventloop, &format!("devices/{}/data/#", a.id)).await,
        SubscribeReasonCode::Success(QoS::AtLeastOnce)
    );
}

#[tokio::test]
async fn cross_device_publish_is_denied_not_routed_and_disconnects() {
    let server = TestApp::start().await;
    let a = server.create_device("acl-pub-a", true).await;
    let b = server.create_device("acl-pub-b", true).await;

    let (b_client, mut b_eventloop, _) =
        mqtt_connect(server.mqtt_addr, &b.id, &b.id, &b.token, true).await;
    assert_eq!(
        subscribe(
            &b_client,
            &mut b_eventloop,
            &format!("devices/{}/data/#", b.id)
        )
        .await,
        SubscribeReasonCode::Success(QoS::AtLeastOnce)
    );

    // T12: A cannot write into B's namespace and B never sees the message.
    let (a_client, mut a_eventloop, _) =
        mqtt_connect(server.mqtt_addr, &a.id, &a.id, &a.token, true).await;
    a_client
        .publish(
            &format!("devices/{}/data/attack", b.id),
            QoS::AtLeastOnce,
            false,
            b"{}".as_slice(),
        )
        .await
        .unwrap();
    mqtt_wait_closed(&mut a_eventloop, WAIT).await;
    assert!(
        tokio::time::timeout(Duration::from_millis(300), b_eventloop.poll())
            .await
            .is_err(),
        "denied publish must not be routed to device B"
    );
}

#[tokio::test]
async fn server_only_and_system_topics_are_denied_for_publish() {
    let server = TestApp::start().await;
    let device = server.create_device("acl-deny", true).await;

    for topic in [
        format!("devices/{}/telemetry/ack", device.id),
        format!("devices/{}/commands", device.id),
        "$SYS/brokers".to_owned(),
        format!("$share/group/devices/{}/data/x", device.id),
    ] {
        assert_publish_denied(&server, &device, &topic).await;
    }

    // Wildcards in a publish topic never reach the broker through rumqttc.
    for topic in [
        format!("devices/{}/data/+", device.id),
        format!("devices/{}/telemetry/#", device.id),
    ] {
        assert!(
            raw_publish_denied(
                server.mqtt_addr,
                &device.id,
                &device.id,
                &device.token,
                &topic,
                false,
            )
            .await,
            "{topic}"
        );
    }
}

#[tokio::test]
async fn retained_publish_to_server_topics_is_denied() {
    let server = TestApp::start().await;
    let device = server.create_device("acl-retained", true).await;
    let (client, mut eventloop, _) = mqtt_connect(
        server.mqtt_addr,
        &device.id,
        &device.id,
        &device.token,
        true,
    )
    .await;

    client
        .publish(
            &format!("devices/{}/commands", device.id),
            QoS::AtLeastOnce,
            true,
            b"{}".as_slice(),
        )
        .await
        .unwrap();
    mqtt_wait_closed(&mut eventloop, WAIT).await;
}

async fn subscribe(
    client: &AsyncClient,
    eventloop: &mut EventLoop,
    filter: &str,
) -> SubscribeReasonCode {
    client.subscribe(filter, QoS::AtLeastOnce).await.unwrap();
    loop {
        if let MqttEvent::Incoming(Packet::SubAck(ack)) = mqtt_next(eventloop, WAIT).await.unwrap()
        {
            return ack.return_codes[0];
        }
    }
}

async fn publish_ok(client: &AsyncClient, eventloop: &mut EventLoop, topic: &str) {
    client
        .publish(topic, QoS::AtLeastOnce, false, b"{}".as_slice())
        .await
        .unwrap();
    loop {
        if let MqttEvent::Incoming(Packet::PubAck(_)) = mqtt_next(eventloop, WAIT).await.unwrap() {
            return;
        }
    }
}

async fn assert_publish_denied(server: &TestApp, device: &TestDevice, topic: &str) {
    let (client, mut eventloop, _) = mqtt_connect(
        server.mqtt_addr,
        &device.id,
        &device.id,
        &device.token,
        true,
    )
    .await;
    client
        .publish(topic, QoS::AtLeastOnce, false, b"{}".as_slice())
        .await
        .unwrap();
    mqtt_wait_closed(&mut eventloop, WAIT).await;
    drop(client);
}
