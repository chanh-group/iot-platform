mod common;

use common::*;
use rmqtt_spike::Event;
use rumqttc::SubscribeReasonCode;
use std::time::Duration;

#[tokio::test]
async fn acl_cross_device() {
    let server = start().await;
    let (client_a, mut loop_a, _) = connect_ok(&server, DEVICE_A, DEVICE_A, TOKEN_A, true).await;
    let (client_b, mut loop_b, _) = connect_ok(&server, DEVICE_B, DEVICE_B, TOKEN_B, true).await;

    let denied = subscribe(
        &client_a,
        &mut loop_a,
        &format!("devices/{DEVICE_B}/commands"),
    )
    .await;
    assert_eq!(denied, vec![SubscribeReasonCode::Failure]);
    let denied = subscribe(&client_a, &mut loop_a, "devices/+/telemetry").await;
    assert_eq!(denied, vec![SubscribeReasonCode::Failure]);

    let allowed = subscribe(
        &client_a,
        &mut loop_a,
        &format!("devices/{DEVICE_A}/commands"),
    )
    .await;
    assert_eq!(
        allowed,
        vec![SubscribeReasonCode::Success(rumqttc::QoS::AtLeastOnce)]
    );
    let allowed = subscribe(
        &client_b,
        &mut loop_b,
        &format!("devices/{DEVICE_B}/data/#"),
    )
    .await;
    assert_eq!(
        allowed,
        vec![SubscribeReasonCode::Success(rumqttc::QoS::AtLeastOnce)]
    );

    let attack = format!("devices/{DEVICE_B}/data/attack");
    publish_maybe_closed(&client_a, &mut loop_a, &attack, false, b"denied").await;

    wait_event(
        &server,
        |event| {
            matches!(
                event,
                Event::PublishAcl {
                    topic,
                    allowed: false,
                    ingest: false,
                    ..
                } if topic == &attack
            )
        },
        Duration::from_secs(5),
    )
    .await;

    expect_no_publish(&mut loop_b, Duration::from_millis(500)).await;

    let control = format!("devices/{DEVICE_B}/data/control");
    server
        .publish_server(DEVICE_B, &control, b"ok")
        .await
        .unwrap();
    let publish = wait_publish(&mut loop_b, Duration::from_secs(5)).await;
    assert_eq!(publish.topic, control);
    assert_eq!(publish.payload.as_ref(), b"ok");

    let events = server.events().await;
    let publish_index = events
        .iter()
        .position(|event| matches!(event, Event::Publish { topic, .. } if topic == &attack))
        .expect("MessagePublish hook must run for the denied publish");
    let acl_index = events
        .iter()
        .position(|event| matches!(event, Event::PublishAcl { topic, .. } if topic == &attack))
        .expect("publish ACL hook must run");
    assert!(
        publish_index < acl_index,
        "RMQTT runs MessagePublish before MessagePublishCheckAcl: {events:?}"
    );
    assert!(!events.iter().any(|event| matches!(
        event,
        Event::PublishAcl {
            topic,
            ingest: true,
            ..
        } if topic == &attack
    )));

    drop(client_a);
    drop(loop_a);
    drop(client_b);
    drop(loop_b);
}
