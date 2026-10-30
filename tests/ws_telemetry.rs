mod common;

use std::time::Duration;

use common::{TestApp, mqtt_connect};
use futures_util::{SinkExt, StreamExt};
use reqwest::header::{COOKIE, ORIGIN, SET_COOKIE};
use rumqttc::{Event, Packet, QoS};
use serde_json::{Value, json};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::HeaderValue;
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;

async fn session_cookie(app: &TestApp) -> (reqwest::Client, String) {
    app.create_admin("ws-admin", "websocket-test-password")
        .await;
    let client = app.client();
    let response = client
        .post(format!("{}/api/v1/auth/login", app.base))
        .header("X-IoT-CSRF", "1")
        .json(&json!({
            "username": "ws-admin",
            "password": "websocket-test-password",
        }))
        .send()
        .await
        .unwrap();
    assert!(response.status().is_success());
    let cookie = response
        .headers()
        .get(SET_COOKIE)
        .unwrap()
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_owned();
    (client, cookie)
}

async fn connect_ws(
    app: &TestApp,
    cookie: &str,
) -> tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>> {
    let authority = app.base.trim_start_matches("http://");
    let mut request = format!("ws://{authority}/api/v1/events")
        .into_client_request()
        .unwrap();
    request
        .headers_mut()
        .insert(COOKIE, HeaderValue::from_str(cookie).unwrap());
    request
        .headers_mut()
        .insert(ORIGIN, HeaderValue::from_str(&app.base).unwrap());
    tokio_tungstenite::connect_async(request).await.unwrap().0
}

async fn publish(app: &TestApp, device_id: &str, token: &str, event_id: &str, raw: &[u8]) {
    let (client, mut eventloop, _) =
        mqtt_connect(app.mqtt_addr, device_id, device_id, token, true).await;
    let ack_topic = format!("devices/{device_id}/telemetry/ack");
    client
        .subscribe(&ack_topic, QoS::AtLeastOnce)
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if matches!(
                eventloop.poll().await.unwrap(),
                Event::Incoming(Packet::SubAck(_))
            ) {
                break;
            }
        }
    })
    .await
    .unwrap();
    client
        .publish(
            format!("devices/{device_id}/telemetry"),
            QoS::AtLeastOnce,
            false,
            raw,
        )
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if let Event::Incoming(Packet::Publish(packet)) = eventloop.poll().await.unwrap()
                && packet.topic == ack_topic
            {
                let ack: Value = serde_json::from_slice(&packet.payload).unwrap();
                assert_eq!(ack["event_id"], event_id);
                break;
            }
        }
    })
    .await
    .unwrap();
}

async fn next_json(
    socket: &mut tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
) -> Value {
    let Some(Ok(Message::Text(text))) = socket.next().await else {
        panic!("expected websocket JSON message");
    };
    serde_json::from_str(text.as_str()).unwrap()
}

#[tokio::test]
async fn subscribed_clients_receive_only_committed_telemetry_and_refetch_after_reconnect() {
    let app = TestApp::start().await;
    let (client, cookie) = session_cookie(&app).await;
    let device = app.create_device("ws-telemetry", true).await;
    let mut socket = connect_ws(&app, &cookie).await;

    let hello = next_json(&mut socket).await;
    assert_eq!(hello["type"], "hello");
    assert_eq!(hello["max_subscriptions"], 8);

    socket
        .send(Message::Text(
            json!({"action":"subscribe", "channels":["device:not-a-uuid"]})
                .to_string()
                .into(),
        ))
        .await
        .unwrap();
    assert_eq!(next_json(&mut socket).await["code"], "invalid_channel");

    socket
        .send(Message::Text(
            json!({"action":"subscribe", "channels":[format!("device:{}", device.id)]})
                .to_string()
                .into(),
        ))
        .await
        .unwrap();
    assert_eq!(next_json(&mut socket).await["type"], "subscribed");
    socket
        .send(Message::Text(json!({"action":"ping"}).to_string().into()))
        .await
        .unwrap();
    assert_eq!(next_json(&mut socket).await["type"], "pong");

    publish(
        &app,
        &device.id,
        &device.token,
        "invalid-ws-event",
        br#"{"event_id":"invalid-ws-event","values":{"x":null}}"#,
    )
    .await;
    assert!(
        tokio::time::timeout(Duration::from_millis(150), socket.next())
            .await
            .is_err()
    );

    publish(
        &app,
        &device.id,
        &device.token,
        "committed-ws-event",
        br#"{"event_id":"committed-ws-event","values":{"temperature":24.5}}"#,
    )
    .await;
    let event = next_json(&mut socket).await;
    assert_eq!(event["type"], "telemetry");
    assert_eq!(event["device_id"], device.id);
    assert_eq!(event["data"]["event_id"], "committed-ws-event");
    assert_eq!(event["data"]["values"]["temperature"], 24.5);
    let stored: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM telemetry_events WHERE device_id = ? AND event_id = ?",
    )
    .bind(&device.id)
    .bind("committed-ws-event")
    .fetch_one(&app.pool)
    .await
    .unwrap();
    assert_eq!(stored, 1);
    socket
        .send(Message::Text(
            json!({"action":"unsubscribe", "channels":[format!("device:{}", device.id)]})
                .to_string()
                .into(),
        ))
        .await
        .unwrap();
    assert_eq!(next_json(&mut socket).await["type"], "unsubscribed");

    socket.close(None).await.unwrap();
    let mut reconnected = connect_ws(&app, &cookie).await;
    assert_eq!(next_json(&mut reconnected).await["type"], "hello");
    assert!(
        tokio::time::timeout(Duration::from_millis(100), reconnected.next())
            .await
            .is_err()
    );

    let ts = event["data"]["ts"].as_i64().unwrap();
    let history: Value = client
        .get(format!(
            "{}/api/v1/devices/{}/telemetry",
            app.base, device.id
        ))
        .query(&[("from", ts.to_string()), ("to", (ts + 1).to_string())])
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(history["items"][0]["event_id"], "committed-ws-event");
    let logout = client
        .post(format!("{}/api/v1/auth/logout", app.base))
        .header("X-IoT-CSRF", "1")
        .header(reqwest::header::CONTENT_TYPE, "application/json")
        .send()
        .await
        .unwrap();
    assert_eq!(logout.status(), reqwest::StatusCode::NO_CONTENT);
    let closed = tokio::time::timeout(Duration::from_secs(2), reconnected.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(matches!(
        closed,
        Message::Close(Some(frame)) if frame.code == CloseCode::Library(4401)
    ));
}

#[tokio::test]
async fn failed_telemetry_commit_is_not_broadcast() {
    let app = TestApp::start().await;
    let (_, cookie) = session_cookie(&app).await;
    let device = app.create_device("ws-commit-failure", true).await;
    let mut socket = connect_ws(&app, &cookie).await;
    assert_eq!(next_json(&mut socket).await["type"], "hello");
    socket
        .send(Message::Text(
            json!({"action":"subscribe", "channels":[format!("device:{}", device.id)]})
                .to_string()
                .into(),
        ))
        .await
        .unwrap();
    assert_eq!(next_json(&mut socket).await["type"], "subscribed");

    sqlx::query(
        "CREATE TRIGGER fail_telemetry BEFORE INSERT ON telemetry_events
         BEGIN SELECT RAISE(FAIL, 'injected commit failure'); END",
    )
    .execute(&app.pool)
    .await
    .unwrap();
    publish(
        &app,
        &device.id,
        &device.token,
        "failed-commit-event",
        br#"{"event_id":"failed-commit-event","values":{"x":1}}"#,
    )
    .await;
    assert!(
        tokio::time::timeout(Duration::from_millis(200), socket.next())
            .await
            .is_err()
    );
    let stored: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM telemetry_events WHERE device_id = ? AND event_id = ?",
    )
    .bind(&device.id)
    .bind("failed-commit-event")
    .fetch_one(&app.pool)
    .await
    .unwrap();
    assert_eq!(stored, 0);
}
