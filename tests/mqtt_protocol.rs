mod common;

use std::time::Duration;

use common::{
    TestApp, mqtt_connect, mqtt_connect_refused, mqtt_next, mqtt_options, mqtt_publish,
    mqtt_subscribe, mqtt_wait_closed, mqtt_wait_puback, mqtt_wait_publish, wait_until,
};
use iot_server::config::schema::MqttConfig;
use rumqttc::{
    AsyncClient, ConnectReturnCode, Event as MqttEvent, LastWill, Packet, QoS, SubscribeReasonCode,
};

const WAIT: Duration = Duration::from_secs(5);

async fn next_telemetry_ack(
    eventloop: &mut rumqttc::EventLoop,
    ack_topic: &str,
    timeout: Duration,
) -> rumqttc::Publish {
    tokio::time::timeout(timeout, async {
        loop {
            if let MqttEvent::Incoming(Packet::Publish(publish)) = eventloop.poll().await.unwrap()
                && publish.topic == ack_topic
            {
                return publish;
            }
        }
    })
    .await
    .expect("receive telemetry ACK before timeout")
}

async fn publish_telemetry_and_wait_for_puback(
    client: &AsyncClient,
    eventloop: &mut rumqttc::EventLoop,
    topic: &str,
    ack_topic: &str,
    payload: &[u8],
    acknowledgements: &mut Vec<rumqttc::Publish>,
) {
    client
        .publish(topic, QoS::AtLeastOnce, false, payload)
        .await
        .unwrap();
    tokio::time::timeout(WAIT, async {
        loop {
            match eventloop.poll().await.unwrap() {
                MqttEvent::Incoming(Packet::PubAck(_)) => return,
                MqttEvent::Incoming(Packet::Publish(publish)) if publish.topic == ack_topic => {
                    acknowledgements.push(publish);
                }
                _ => {}
            }
        }
    })
    .await
    .expect("receive telemetry publish PUBACK before timeout");
}

#[tokio::test]
async fn qos_levels_and_own_device_wildcard_round_trip() {
    let app = TestApp::start().await;
    let device = app.create_device("protocol-qos", true).await;
    let (client, mut eventloop, _) =
        mqtt_connect(app.mqtt_addr, &device.id, &device.id, &device.token, true).await;
    let filter = format!("devices/{}/data/#", device.id);

    assert_eq!(
        mqtt_subscribe(&client, &mut eventloop, &filter, QoS::ExactlyOnce).await,
        SubscribeReasonCode::Success(QoS::ExactlyOnce)
    );

    for (qos, suffix, payload) in [
        (QoS::AtMostOnce, "qos0", b"zero".as_slice()),
        (QoS::AtLeastOnce, "qos1", b"one".as_slice()),
        (QoS::ExactlyOnce, "qos2", b"two".as_slice()),
    ] {
        let topic = format!("devices/{}/data/{suffix}", device.id);
        mqtt_publish(&client, &topic, qos, false, payload).await;
        let received = mqtt_wait_publish(&mut eventloop, WAIT).await;
        assert_eq!(received.topic, topic);
        assert_eq!(received.payload.as_ref(), payload);
        assert_eq!(received.qos, qos);
    }
}

#[tokio::test]
async fn max_inflight_per_client_limits_unacknowledged_qos1_deliveries() {
    let config = MqttConfig {
        max_inflight_per_client: 1,
        ..MqttConfig::default()
    };
    let app = TestApp::builder().mqtt_config(config).start().await;
    assert_eq!(app.config.config.mqtt.max_inflight_per_client, 1);
    let device = app.create_device("protocol-inflight", true).await;
    let ack_topic = format!("devices/{}/telemetry/ack", device.id);
    let telemetry_topic = format!("devices/{}/telemetry", device.id);

    let mut options = mqtt_options(app.mqtt_addr, &device.id, &device.id, &device.token, true);
    options.set_manual_acks(true);
    let (client, mut eventloop) = AsyncClient::new(options, 32);
    assert!(matches!(
        mqtt_next(&mut eventloop, WAIT).await.unwrap(),
        MqttEvent::Incoming(Packet::ConnAck(ack)) if ack.code == ConnectReturnCode::Success
    ));
    assert_eq!(
        mqtt_subscribe(&client, &mut eventloop, &ack_topic, QoS::AtLeastOnce).await,
        SubscribeReasonCode::Success(QoS::AtLeastOnce)
    );

    let mut acknowledgements = Vec::new();
    for index in 0..3 {
        let payload = format!(r#"{{"event_id":"inflight-{index}","values":{{"x":{index}}}}}"#);
        publish_telemetry_and_wait_for_puback(
            &client,
            &mut eventloop,
            &telemetry_topic,
            &ack_topic,
            payload.as_bytes(),
            &mut acknowledgements,
        )
        .await;
    }

    tokio::time::timeout(WAIT, async {
        loop {
            let count: i64 =
                sqlx::query_scalar("SELECT COUNT(*) FROM telemetry_events WHERE device_id = ?")
                    .bind(&device.id)
                    .fetch_one(&app.pool)
                    .await
                    .unwrap();
            if count == 3 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("all three telemetry events should commit");

    if acknowledgements.is_empty() {
        acknowledgements.push(next_telemetry_ack(&mut eventloop, &ack_topic, WAIT).await);
    }
    assert_eq!(
        acknowledgements.len(),
        1,
        "only one QoS 1 delivery may remain unacknowledged"
    );
    let first = acknowledgements.pop().unwrap();
    assert_eq!(first.qos, QoS::AtLeastOnce);

    assert!(
        tokio::time::timeout(
            Duration::from_millis(500),
            next_telemetry_ack(&mut eventloop, &ack_topic, WAIT),
        )
        .await
        .is_err(),
        "broker must wait for PUBACK before sending another QoS 1 message"
    );

    client.ack(&first).await.unwrap();
    let second = next_telemetry_ack(&mut eventloop, &ack_topic, WAIT).await;
    assert_ne!(second.pkid, first.pkid);
    assert!(
        tokio::time::timeout(
            Duration::from_millis(500),
            next_telemetry_ack(&mut eventloop, &ack_topic, WAIT),
        )
        .await
        .is_err(),
        "the next inflight slot must remain occupied until its PUBACK"
    );

    client.ack(&second).await.unwrap();
    let third = next_telemetry_ack(&mut eventloop, &ack_topic, WAIT).await;
    assert_ne!(third.pkid, second.pkid);
    client.ack(&third).await.unwrap();
}

#[tokio::test]
async fn retained_last_will_is_replayed_after_ungraceful_disconnect() {
    let app = TestApp::start().await;
    let device = app.create_device("protocol-will", true).await;
    let status = format!("devices/{}/status", device.id);
    let mut options = mqtt_options(app.mqtt_addr, &device.id, &device.id, &device.token, false);
    options.set_last_will(LastWill::new(&status, b"offline", QoS::AtLeastOnce, true));
    let (client, mut eventloop) = AsyncClient::new(options, 16);
    assert!(matches!(
        mqtt_next(&mut eventloop, WAIT).await.unwrap(),
        MqttEvent::Incoming(Packet::ConnAck(ack)) if ack.code == ConnectReturnCode::Success
    ));

    drop(client);
    drop(eventloop);
    wait_until(
        || {
            !app.presence
                .is_online(&device.id)
                .expect("presence state should remain healthy")
        },
        WAIT,
        "device disconnect",
    )
    .await;
    tokio::time::timeout(WAIT, async {
        while app.retained_count().await != 1 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the broker should store the retained will");

    let (client, mut eventloop, _) =
        mqtt_connect(app.mqtt_addr, &device.id, &device.id, &device.token, true).await;
    assert_eq!(
        mqtt_subscribe(&client, &mut eventloop, &status, QoS::AtLeastOnce).await,
        SubscribeReasonCode::Success(QoS::AtLeastOnce)
    );
    let will = mqtt_wait_publish(&mut eventloop, WAIT).await;
    assert_eq!(will.topic, status);
    assert_eq!(will.payload.as_ref(), b"offline");
    assert!(will.retain);
}

#[tokio::test]
async fn persistent_session_reconnects_then_expires() {
    let config = MqttConfig {
        max_session_expiry_ms: 500,
        ..MqttConfig::default()
    };
    let app = TestApp::builder().mqtt_config(config).start().await;
    let device = app.create_device("protocol-session", true).await;
    let filter = format!("devices/{}/data/#", device.id);

    let (client, mut eventloop, present) =
        mqtt_connect(app.mqtt_addr, &device.id, &device.id, &device.token, false).await;
    assert!(!present);
    assert_eq!(
        mqtt_subscribe(&client, &mut eventloop, &filter, QoS::AtLeastOnce).await,
        SubscribeReasonCode::Success(QoS::AtLeastOnce)
    );
    drop(client);
    drop(eventloop);
    wait_until(
        || {
            !app.presence
                .is_online(&device.id)
                .expect("presence state should remain healthy")
        },
        WAIT,
        "session disconnect",
    )
    .await;

    let (client, mut eventloop, present) =
        mqtt_connect(app.mqtt_addr, &device.id, &device.id, &device.token, false).await;
    assert!(
        present,
        "an unexpired clean_session=false session should resume"
    );
    let topic = format!("devices/{}/data/resumed", device.id);
    mqtt_publish(&client, &topic, QoS::AtLeastOnce, false, b"resumed").await;
    assert_eq!(
        mqtt_wait_publish(&mut eventloop, WAIT)
            .await
            .payload
            .as_ref(),
        b"resumed"
    );
    drop(client);
    drop(eventloop);
    wait_until(
        || {
            !app.presence
                .is_online(&device.id)
                .expect("presence state should remain healthy")
        },
        WAIT,
        "second session disconnect",
    )
    .await;

    tokio::time::sleep(Duration::from_millis(700)).await;
    let (_client, _eventloop, present) =
        mqtt_connect(app.mqtt_addr, &device.id, &device.id, &device.token, false).await;
    assert!(!present, "an expired persistent session must be discarded");
}

#[tokio::test]
async fn broker_restart_discards_persistent_sessions() {
    let mut app = TestApp::start().await;
    let device = app.create_device("protocol-restart", true).await;
    let filter = format!("devices/{}/data/#", device.id);
    let (client, mut eventloop, _) =
        mqtt_connect(app.mqtt_addr, &device.id, &device.id, &device.token, false).await;
    mqtt_subscribe(&client, &mut eventloop, &filter, QoS::AtLeastOnce).await;
    drop(client);
    drop(eventloop);
    wait_until(
        || {
            !app.presence
                .is_online(&device.id)
                .expect("presence state should remain healthy")
        },
        WAIT,
        "session disconnect",
    )
    .await;

    app.restart_mqtt().await;
    let (_client, _eventloop, present) =
        mqtt_connect(app.mqtt_addr, &device.id, &device.id, &device.token, false).await;
    assert!(
        !present,
        "broker sessions are in-memory and must not survive restart"
    );
}

#[tokio::test]
async fn subscription_limit_is_enforced_by_the_listener() {
    let config = MqttConfig {
        max_subscriptions_per_client: 1,
        ..MqttConfig::default()
    };
    let app = TestApp::builder().mqtt_config(config).start().await;
    let device = app.create_device("protocol-sub-limit", true).await;
    let (client, mut eventloop, _) =
        mqtt_connect(app.mqtt_addr, &device.id, &device.id, &device.token, true).await;

    let status = format!("devices/{}/status", device.id);
    let data = format!("devices/{}/data/#", device.id);
    assert_eq!(
        mqtt_subscribe(&client, &mut eventloop, &status, QoS::AtLeastOnce).await,
        SubscribeReasonCode::Success(QoS::AtLeastOnce)
    );
    assert_eq!(
        mqtt_subscribe(&client, &mut eventloop, &data, QoS::AtLeastOnce).await,
        SubscribeReasonCode::Failure
    );
}

#[tokio::test]
async fn topic_level_limit_is_enforced_by_the_listener() {
    let config = MqttConfig {
        max_topic_levels: 4,
        ..MqttConfig::default()
    };
    let app = TestApp::builder().mqtt_config(config).start().await;
    let device = app.create_device("protocol-topic-limit", true).await;
    let (client, mut eventloop, _) =
        mqtt_connect(app.mqtt_addr, &device.id, &device.id, &device.token, true).await;
    let filter = format!("devices/{}/data/#", device.id);
    let too_deep_filter = format!("devices/{}/data/x/y/#", device.id);
    assert_eq!(
        mqtt_subscribe(&client, &mut eventloop, &too_deep_filter, QoS::AtLeastOnce).await,
        SubscribeReasonCode::Failure
    );
    mqtt_subscribe(&client, &mut eventloop, &filter, QoS::AtLeastOnce).await;

    let allowed = format!("devices/{}/data/x", device.id);
    mqtt_publish(&client, &allowed, QoS::AtLeastOnce, false, b"ok").await;
    assert_eq!(mqtt_wait_publish(&mut eventloop, WAIT).await.topic, allowed);

    let too_deep = format!("devices/{}/data/x/y", device.id);
    mqtt_publish(&client, &too_deep, QoS::AtLeastOnce, false, b"deny").await;
    mqtt_wait_closed(&mut eventloop, WAIT).await;
}

#[tokio::test]
async fn packet_limit_is_enforced_by_the_listener() {
    let config = MqttConfig {
        max_packet_bytes: 1_024,
        ..MqttConfig::default()
    };
    let app = TestApp::builder().mqtt_config(config).start().await;
    let device = app.create_device("protocol-packet-limit", true).await;
    let (client, mut eventloop, _) =
        mqtt_connect(app.mqtt_addr, &device.id, &device.id, &device.token, true).await;
    let topic = format!("devices/{}/data/large", device.id);
    let payload = [b'x'; 2_048];

    mqtt_publish(&client, &topic, QoS::AtLeastOnce, false, &payload).await;
    mqtt_wait_closed(&mut eventloop, WAIT).await;
}

#[tokio::test]
async fn client_id_limit_rejects_only_when_the_canonical_id_exceeds_it() {
    let device_id = uuid::Uuid::now_v7().to_string();
    assert_eq!(device_id.len(), 36);
    let token = format!(
        "iot_{}",
        iot_server::core::auth::generate_session_token().unwrap()
    );
    // A below-schema test value isolates the broker limit from UUID authentication.
    let restrictive_config = MqttConfig {
        max_client_id_len: 35,
        ..MqttConfig::default()
    };
    let mut app = TestApp::builder()
        .mqtt_config(restrictive_config)
        .start()
        .await;
    let device = app
        .create_device_with_identity(&device_id, &token, "protocol-client-id-limit", true)
        .await;

    mqtt_connect_refused(app.mqtt_addr, &device.id, &device.id, &device.token).await;

    let permissive_config = MqttConfig {
        max_client_id_len: 36,
        ..MqttConfig::default()
    };
    app.restart_mqtt_with_config(permissive_config).await;
    let (client, eventloop, _) =
        mqtt_connect(app.mqtt_addr, &device.id, &device.id, &device.token, true).await;
    drop(client);
    drop(eventloop);
}

#[tokio::test]
async fn connection_limit_rejects_an_additional_device() {
    let config = MqttConfig {
        max_connections: 1,
        ..MqttConfig::default()
    };
    let app = TestApp::builder().mqtt_config(config).start().await;
    let first = app.create_device("protocol-connection-a", true).await;
    let second = app.create_device("protocol-connection-b", true).await;
    let (_client, _eventloop, _) =
        mqtt_connect(app.mqtt_addr, &first.id, &first.id, &first.token, true).await;
    assert_eq!(
        mqtt_connect_refused(app.mqtt_addr, &second.id, &second.id, &second.token).await,
        ConnectReturnCode::NotAuthorized,
        "the listener should explicitly refuse a connection above its configured cap"
    );
}

#[tokio::test]
async fn offline_session_queue_obeys_its_configured_cap() {
    let config = MqttConfig {
        max_queued_messages_per_client: 1,
        ..MqttConfig::default()
    };
    let app = TestApp::builder().mqtt_config(config).start().await;
    let device = app.create_device("protocol-queue-limit", true).await;
    let ack_topic = format!("devices/{}/telemetry/ack", device.id);
    let (client, mut eventloop, _) =
        mqtt_connect(app.mqtt_addr, &device.id, &device.id, &device.token, false).await;
    mqtt_subscribe(&client, &mut eventloop, &ack_topic, QoS::AtLeastOnce).await;

    let mut lock = app.pool.acquire().await.unwrap();
    sqlx::query("BEGIN IMMEDIATE")
        .execute(&mut *lock)
        .await
        .unwrap();
    for event_id in ["queued-1", "queued-2"] {
        let raw = format!(r#"{{"event_id":"{event_id}","values":{{"x":1}}}}"#);
        let topic = format!("devices/{}/telemetry", device.id);
        mqtt_publish(&client, &topic, QoS::AtLeastOnce, false, raw.as_bytes()).await;
        mqtt_wait_puback(&mut eventloop, WAIT).await;
    }

    drop(client);
    drop(eventloop);
    wait_until(
        || {
            !app.presence
                .is_online(&device.id)
                .expect("presence state should remain healthy")
        },
        WAIT,
        "session disconnect before commit",
    )
    .await;
    sqlx::query("COMMIT").execute(&mut *lock).await.unwrap();
    drop(lock);

    tokio::time::timeout(WAIT, async {
        loop {
            let rows: i64 =
                sqlx::query_scalar("SELECT COUNT(*) FROM telemetry_events WHERE device_id = ?")
                    .bind(&device.id)
                    .fetch_one(&app.pool)
                    .await
                    .unwrap();
            if rows == 2 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("both publishes should commit after releasing the test lock");
    tokio::time::sleep(Duration::from_millis(100)).await;

    let (client, mut eventloop, present) =
        mqtt_connect(app.mqtt_addr, &device.id, &device.id, &device.token, false).await;
    assert!(present, "the persistent subscription should resume");
    let queued = mqtt_wait_publish(&mut eventloop, WAIT).await;
    assert_eq!(queued.topic, ack_topic);
    let extra = tokio::time::timeout(Duration::from_millis(250), async {
        loop {
            if let MqttEvent::Incoming(Packet::Publish(publish)) = eventloop.poll().await.unwrap()
                && publish.topic == ack_topic
            {
                return;
            }
        }
    })
    .await;
    assert!(
        extra.is_err(),
        "the offline queue must retain no more than one ACK"
    );
    drop(client);
}
