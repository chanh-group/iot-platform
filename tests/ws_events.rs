mod common;

use std::time::Duration;

use common::{
    TestApp, TestWebSocket, mqtt_connect, mqtt_next, mqtt_options, mqtt_subscribe,
    mqtt_wait_puback, mqtt_wait_publish, ws_connect, ws_session_cookie,
};
use futures_util::{SinkExt, StreamExt};
use iot_server::core::auth;
use rumqttc::{AsyncClient, Event as MqttEvent, LastWill, Packet, QoS, SubscribeReasonCode};
use serde_json::{Value, json};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::HeaderValue;
use tokio_tungstenite::tungstenite::http::header::{COOKIE, ORIGIN};
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
use uuid::Uuid;

async fn next_json(socket: &mut TestWebSocket) -> Value {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let Some(Ok(Message::Text(text))) = socket.next().await {
                return serde_json::from_str(text.as_str()).unwrap();
            }
        }
    })
    .await
    .expect("receive WebSocket event")
}

async fn expect_no_message(socket: &mut TestWebSocket) {
    assert!(
        tokio::time::timeout(Duration::from_millis(150), socket.next())
            .await
            .is_err(),
        "an event should not be duplicated or routed to this subscription"
    );
}

async fn subscribe(socket: &mut TestWebSocket, channels: Vec<String>) {
    socket
        .send(Message::Text(
            json!({"action":"subscribe", "channels":channels})
                .to_string()
                .into(),
        ))
        .await
        .unwrap();
    assert_eq!(next_json(socket).await["type"], "subscribed");
}

async fn create_command(
    client: &reqwest::Client,
    app: &TestApp,
    device_id: &str,
) -> reqwest::Response {
    client
        .post(format!("{}/api/v1/devices/{device_id}/commands", app.base))
        .header("X-IoT-CSRF", "1")
        .json(&json!({"name":"reboot"}))
        .send()
        .await
        .unwrap()
}

async fn create_short_command(
    client: &reqwest::Client,
    app: &TestApp,
    device_id: &str,
) -> reqwest::Response {
    client
        .post(format!("{}/api/v1/devices/{device_id}/commands", app.base))
        .header("X-IoT-CSRF", "1")
        .json(&json!({"name":"reboot", "ttl_ms":1000}))
        .send()
        .await
        .unwrap()
}

#[tokio::test]
async fn routes_committed_and_reported_events_once_with_redacted_system_messages() {
    let app = TestApp::start().await;
    let (client, cookie) = ws_session_cookie(&app).await;
    let online_device = app.create_device("ws-events-online", true).await;
    let offline_device = app.create_device("ws-events-offline", true).await;
    let mut socket = ws_connect(&app, &cookie).await;
    assert_eq!(next_json(&mut socket).await["type"], "hello");
    let too_many_channels = (0..9)
        .map(|_| format!("device:{}", Uuid::now_v7()))
        .collect::<Vec<_>>();
    socket
        .send(Message::Text(
            json!({"action":"subscribe", "channels":too_many_channels})
                .to_string()
                .into(),
        ))
        .await
        .unwrap();
    assert_eq!(next_json(&mut socket).await["code"], "max_subscriptions");
    subscribe(
        &mut socket,
        vec![
            "overview".to_owned(),
            format!("device:{}", online_device.id),
            format!("device:{}", offline_device.id),
        ],
    )
    .await;

    let status_topic = format!("devices/{}/status", online_device.id);
    let will_ts = iot_server::core::auth::now_ms();
    let mut options = mqtt_options(
        app.mqtt_addr,
        &online_device.id,
        &online_device.id,
        &online_device.token,
        true,
    );
    options.set_last_will(LastWill::new(
        status_topic.clone(),
        json!({"status":"offline", "ts":will_ts})
            .to_string()
            .into_bytes(),
        QoS::AtLeastOnce,
        true,
    ));
    let (device_client, mut eventloop) = AsyncClient::new(options, 32);
    assert!(matches!(
        mqtt_next(&mut eventloop, Duration::from_secs(5))
            .await
            .unwrap(),
        MqttEvent::Incoming(Packet::ConnAck(_))
    ));
    let presence = next_json(&mut socket).await;
    assert_eq!(presence["type"], "presence");
    assert_eq!(presence["device_id"], online_device.id);
    assert_eq!(presence["data"]["online"], true);
    assert!(presence["data"]["last_seen_at"].as_i64().is_some());
    expect_no_message(&mut socket).await;

    let online_status = json!({"status":"online", "ts":will_ts + 1}).to_string();
    device_client
        .publish(&status_topic, QoS::AtLeastOnce, true, online_status)
        .await
        .unwrap();
    mqtt_wait_puback(&mut eventloop, Duration::from_secs(3)).await;
    let reported = next_json(&mut socket).await;
    assert_eq!(reported["type"], "reported_status");
    assert_eq!(reported["data"]["status"], "online");
    assert_eq!(reported["data"]["ts"], will_ts + 1);
    assert!(app.presence.is_online(&online_device.id).unwrap());
    expect_no_message(&mut socket).await;

    let settings = client
        .patch(format!("{}/api/v1/config/runtime", app.base))
        .header("X-IoT-CSRF", "1")
        .json(&json!({
            "expected_revision": 0,
            "settings": {"logging":{"level":"debug"}},
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(settings.status(), reqwest::StatusCode::OK);
    let config_event = next_json(&mut socket).await;
    assert_eq!(config_event["type"], "config");
    assert_eq!(config_event["data"]["revision"], 1);
    expect_no_message(&mut socket).await;

    app.events.publish_queue_full();
    let system = next_json(&mut socket).await;
    assert_eq!(system["type"], "system");
    assert_eq!(system["data"]["kind"], "queue_full");
    assert_eq!(system["data"]["severity"], "warning");
    assert_eq!(system["data"]["message"], "Telemetry ingest queue is full");
    assert!(system.get("device_id").is_none());
    assert!(!system.to_string().contains("sqlite"));
    expect_no_message(&mut socket).await;

    drop(device_client);
    drop(eventloop);
    let mut got_offline_presence = false;
    let mut got_offline_status = false;
    let disconnect_events = tokio::time::timeout(Duration::from_secs(5), async {
        while !got_offline_presence || !got_offline_status {
            let event = next_json(&mut socket).await;
            match event["type"].as_str() {
                Some("presence") if event["data"]["online"] == false => {
                    got_offline_presence = true;
                }
                Some("reported_status") if event["data"]["status"] == "offline" => {
                    assert_eq!(event["data"]["ts"], will_ts);
                    got_offline_status = true;
                }
                _ => {}
            }
        }
    })
    .await;
    assert!(
        disconnect_events.is_ok(),
        "disconnect event evidence incomplete: presence={got_offline_presence}, reported_status={got_offline_status}"
    );

    let command = create_command(&client, &app, &offline_device.id).await;
    assert_eq!(command.status(), reqwest::StatusCode::ACCEPTED);
    let body: Value = command.json().await.unwrap();
    let command_event = next_json(&mut socket).await;
    assert_eq!(command_event["type"], "command");
    assert_eq!(command_event["device_id"], offline_device.id);
    assert_eq!(command_event["data"]["command_id"], body["command_id"]);
    assert_eq!(command_event["data"]["status"], "pending");
    assert_eq!(command_event["data"]["state_version"], 1);
    expect_no_message(&mut socket).await;

    let (command_client, mut command_loop, _) = mqtt_connect(
        app.mqtt_addr,
        &offline_device.id,
        &offline_device.id,
        &offline_device.token,
        true,
    )
    .await;
    let command_presence = next_json(&mut socket).await;
    assert_eq!(command_presence["type"], "presence");
    assert_eq!(command_presence["device_id"], offline_device.id);
    assert_eq!(command_presence["data"]["online"], true);
    let command_topic = format!("devices/{}/commands", offline_device.id);
    assert_eq!(
        mqtt_subscribe(
            &command_client,
            &mut command_loop,
            &command_topic,
            QoS::AtLeastOnce
        )
        .await,
        SubscribeReasonCode::Success(QoS::AtLeastOnce)
    );
    let delivered_command = mqtt_wait_publish(&mut command_loop, Duration::from_secs(3)).await;
    let dispatched_event = next_json(&mut socket).await;
    assert_eq!(dispatched_event["type"], "command");
    assert_eq!(dispatched_event["data"]["command_id"], body["command_id"]);
    assert_eq!(dispatched_event["data"]["status"], "dispatched");
    assert_eq!(dispatched_event["data"]["state_version"], 2);
    assert_eq!(
        serde_json::from_slice::<Value>(&delivered_command.payload).unwrap()["command_id"],
        body["command_id"]
    );

    let ack_topic = format!("devices/{}/commands/ack", offline_device.id);
    let ack_payload = json!({
        "command_id": body["command_id"],
        "status": "succeeded",
    })
    .to_string();
    command_client
        .publish(&ack_topic, QoS::AtLeastOnce, false, ack_payload)
        .await
        .unwrap();
    mqtt_wait_puback(&mut command_loop, Duration::from_secs(3)).await;
    let acknowledged = next_json(&mut socket).await;
    assert_eq!(acknowledged["type"], "command");
    assert_eq!(acknowledged["data"]["command_id"], body["command_id"]);
    assert_eq!(acknowledged["data"]["status"], "succeeded");
    assert_eq!(acknowledged["data"]["state_version"], 3);

    let expiring = create_short_command(&client, &app, &offline_device.id).await;
    assert_eq!(expiring.status(), reqwest::StatusCode::ACCEPTED);
    let expiring_body: Value = expiring.json().await.unwrap();
    let pending_expiry = next_json(&mut socket).await;
    assert_eq!(pending_expiry["type"], "command");
    assert_eq!(
        pending_expiry["data"]["command_id"],
        expiring_body["command_id"]
    );
    assert_eq!(pending_expiry["data"]["status"], "pending");
    assert_eq!(pending_expiry["data"]["state_version"], 1);
    let expired = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let event = next_json(&mut socket).await;
            if event["type"] == "command"
                && event["data"]["command_id"] == expiring_body["command_id"]
                && event["data"]["status"] == "expired"
            {
                break event;
            }
            assert_eq!(
                event["type"], "command",
                "unexpected event while expiring command"
            );
        }
    })
    .await
    .expect("expired command transition should be broadcast");
    assert_eq!(expired["data"]["state_version"], 3);
    assert!(expired["data"]["completed_at"].as_i64().is_some());

    sqlx::query("DROP TABLE audit_events")
        .execute(&app.pool)
        .await
        .unwrap();
    let failed_command = create_command(&client, &app, &offline_device.id).await;
    assert_eq!(
        failed_command.status(),
        reqwest::StatusCode::SERVICE_UNAVAILABLE
    );
    expect_no_message(&mut socket).await;
    drop(command_client);
    drop(command_loop);
}

#[tokio::test]
async fn replaced_session_does_not_emit_offline_presence_for_its_stale_generation() {
    let app = TestApp::start().await;
    let (_, cookie) = ws_session_cookie(&app).await;
    let device = app.create_device("ws-events-generation", true).await;
    let mut socket = ws_connect(&app, &cookie).await;
    assert_eq!(next_json(&mut socket).await["type"], "hello");
    subscribe(
        &mut socket,
        vec!["overview".to_owned(), format!("device:{}", device.id)],
    )
    .await;

    let (first_client, mut first_loop) = AsyncClient::new(
        mqtt_options(app.mqtt_addr, &device.id, &device.id, &device.token, true),
        32,
    );
    assert!(matches!(
        mqtt_next(&mut first_loop, Duration::from_secs(5))
            .await
            .unwrap(),
        MqttEvent::Incoming(Packet::ConnAck(_))
    ));
    assert_eq!(next_json(&mut socket).await["data"]["online"], true);

    let (second_client, mut second_loop) = AsyncClient::new(
        mqtt_options(app.mqtt_addr, &device.id, &device.id, &device.token, true),
        32,
    );
    assert!(matches!(
        mqtt_next(&mut second_loop, Duration::from_secs(5))
            .await
            .unwrap(),
        MqttEvent::Incoming(Packet::ConnAck(_))
    ));
    let mut saw_online_after_replacement = false;
    let mut saw_offline_after_replacement = false;
    while let Ok(Some(Ok(Message::Text(text)))) =
        tokio::time::timeout(Duration::from_millis(300), socket.next()).await
    {
        let event: Value = serde_json::from_str(text.as_str()).unwrap();
        if event["type"] == "presence" {
            if event["data"]["online"] == true {
                saw_online_after_replacement = true;
            } else {
                saw_offline_after_replacement = true;
            }
        }
    }
    assert!(!saw_offline_after_replacement || saw_online_after_replacement);
    assert!(app.presence.is_online(&device.id).unwrap());

    drop(first_client);
    drop(first_loop);
    expect_no_message(&mut socket).await;
    assert!(app.presence.is_online(&device.id).unwrap());

    drop(second_client);
    drop(second_loop);
    let offline = next_json(&mut socket).await;
    assert_eq!(offline["type"], "presence");
    assert_eq!(offline["data"]["online"], false);
}

#[tokio::test]
async fn presence_events_are_ordered_before_advisory_last_seen_writes() {
    let app = TestApp::start().await;
    let (_, cookie) = ws_session_cookie(&app).await;
    let device = app.create_device("ws-events-last-seen", true).await;
    let mut socket = ws_connect(&app, &cookie).await;
    assert_eq!(next_json(&mut socket).await["type"], "hello");
    subscribe(
        &mut socket,
        vec!["overview".to_owned(), format!("device:{}", device.id)],
    )
    .await;

    let last_seen_before: Option<i64> =
        sqlx::query_scalar("SELECT last_seen_at FROM devices WHERE id = ?")
            .bind(&device.id)
            .fetch_one(&app.pool)
            .await
            .unwrap();
    let mut blocker = app.pool.acquire().await.unwrap();
    sqlx::query("BEGIN IMMEDIATE")
        .execute(&mut *blocker)
        .await
        .unwrap();
    let (mqtt_client, mut eventloop) = AsyncClient::new(
        mqtt_options(app.mqtt_addr, &device.id, &device.id, &device.token, true),
        32,
    );
    let eventloop_task = tokio::spawn(async move { while eventloop.poll().await.is_ok() {} });

    let online = tokio::time::timeout(Duration::from_secs(2), next_json(&mut socket))
        .await
        .expect("connect presence event must not wait for last_seen storage");
    assert_eq!(online["type"], "presence");
    assert_eq!(online["device_id"], device.id);
    assert_eq!(online["data"]["online"], true);
    assert!(app.presence.is_online(&device.id).unwrap());
    let last_seen_while_blocked: Option<i64> =
        sqlx::query_scalar("SELECT last_seen_at FROM devices WHERE id = ?")
            .bind(&device.id)
            .fetch_one(&app.pool)
            .await
            .unwrap();
    assert_eq!(last_seen_while_blocked, last_seen_before);

    drop(mqtt_client);
    eventloop_task.abort();
    let _ = eventloop_task.await;
    let disconnect_while_blocked =
        tokio::time::timeout(Duration::from_millis(300), socket.next()).await;
    let offline_seen = match disconnect_while_blocked {
        Ok(Some(Ok(Message::Text(text)))) => {
            let event: Value = serde_json::from_str(text.as_str()).unwrap();
            assert_eq!(event["type"], "presence");
            assert_eq!(event["data"]["online"], false);
            true
        }
        Ok(Some(other)) => panic!("unexpected WebSocket event while closing device: {other:?}"),
        Ok(None) | Err(_) => false,
    };
    sqlx::query("ROLLBACK")
        .execute(&mut *blocker)
        .await
        .unwrap();

    if !offline_seen {
        let offline = next_json(&mut socket).await;
        assert_eq!(offline["type"], "presence");
        assert_eq!(offline["device_id"], device.id);
        assert_eq!(offline["data"]["online"], false);
    }
    assert!(!app.presence.is_online(&device.id).unwrap());
    expect_no_message(&mut socket).await;
}

#[tokio::test]
async fn expired_session_closes_websocket_and_reconnect_is_unauthorized() {
    let app = TestApp::start().await;
    let (client, cookie) = ws_session_cookie(&app).await;
    let token = cookie.strip_prefix("iot_session=").unwrap();
    let token_hash = auth::hash_token(token);
    sqlx::query("UPDATE sessions SET expires_at = ? WHERE token_hash = ?")
        .bind(auth::now_ms() + 300)
        .bind(token_hash.as_slice())
        .execute(&app.pool)
        .await
        .unwrap();

    let mut socket = ws_connect(&app, &cookie).await;
    assert_eq!(next_json(&mut socket).await["type"], "hello");
    let close = tokio::time::timeout(Duration::from_secs(2), socket.next())
        .await
        .expect("WebSocket should close when the session expires")
        .expect("WebSocket should return a close frame")
        .unwrap();
    assert!(matches!(
        close,
        Message::Close(Some(frame)) if frame.code == CloseCode::Library(4401)
    ));

    let response = client
        .get(format!("{}/api/v1/auth/me", app.base))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn websocket_connection_limit_is_enforced_before_upgrade() {
    let app = TestApp::builder().websocket_limits(1, 16).start().await;
    let (_, cookie) = ws_session_cookie(&app).await;
    let mut first = ws_connect(&app, &cookie).await;
    assert_eq!(next_json(&mut first).await["type"], "hello");

    let authority = app.base.trim_start_matches("http://");
    let mut request = format!("ws://{authority}/api/v1/events")
        .into_client_request()
        .unwrap();
    request
        .headers_mut()
        .insert(COOKIE, HeaderValue::from_str(&cookie).unwrap());
    request
        .headers_mut()
        .insert(ORIGIN, HeaderValue::from_str(&app.base).unwrap());
    let response = tokio_tungstenite::connect_async(request).await;
    let Err(tokio_tungstenite::tungstenite::Error::Http(response)) = response else {
        panic!("second WebSocket must be rejected before upgrade");
    };
    assert_eq!(response.status(), reqwest::StatusCode::SERVICE_UNAVAILABLE);
    first.close(None).await.unwrap();
}

#[tokio::test]
async fn websocket_rejects_messages_over_the_configured_protocol_limit() {
    let app = TestApp::start().await;
    let (_, cookie) = ws_session_cookie(&app).await;
    let mut socket = ws_connect(&app, &cookie).await;
    assert_eq!(next_json(&mut socket).await["type"], "hello");
    socket
        .send(Message::Text("x".repeat(4_097).into()))
        .await
        .unwrap();

    let response = tokio::time::timeout(Duration::from_secs(2), socket.next())
        .await
        .expect("oversized message should be handled within the send deadline");
    match response {
        Some(Ok(Message::Text(text))) => {
            assert_eq!(
                serde_json::from_str::<Value>(text.as_str()).unwrap()["code"],
                "message_too_large"
            );
        }
        Some(Ok(Message::Close(_))) | Some(Err(_)) | None => {}
        Some(Ok(_)) => panic!("unexpected response to oversized WebSocket message"),
    }
}
