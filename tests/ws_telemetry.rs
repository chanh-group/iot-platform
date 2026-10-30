mod common;

use std::time::Duration;

use common::{TestApp, mqtt_connect, ws_connect, ws_session_cookie};
use futures_util::{SinkExt, StreamExt};
use iot_server::core::events::TelemetryCommitted;
use iot_server::observability::metrics::Metrics;
use rumqttc::{Event, Packet, QoS};
use serde_json::{Value, json};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;

async fn publish(app: &TestApp, device_id: &str, token: &str, event_id: &str, raw: &[u8]) -> Value {
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
                break ack;
            }
        }
    })
    .await
    .unwrap()
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
    let (client, cookie) = ws_session_cookie(&app).await;
    let device = app.create_device("ws-telemetry", true).await;
    let mut socket = ws_connect(&app, &cookie).await;

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

    let rejected = publish(
        &app,
        &device.id,
        &device.token,
        "invalid-ws-event",
        br#"{"event_id":"invalid-ws-event","values":{"x":null}}"#,
    )
    .await;
    assert_eq!(rejected["status"], "rejected");
    assert_eq!(rejected["code"], "invalid_payload");
    assert!(
        tokio::time::timeout(Duration::from_millis(150), socket.next())
            .await
            .is_err()
    );

    let stored_ack = publish(
        &app,
        &device.id,
        &device.token,
        "committed-ws-event",
        br#"{"event_id":"committed-ws-event","values":{"temperature":24.5}}"#,
    )
    .await;
    assert_eq!(stored_ack["status"], "stored");
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
    let mut reconnected = ws_connect(&app, &cookie).await;
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
    let (_, cookie) = ws_session_cookie(&app).await;
    let device = app.create_device("ws-commit-failure", true).await;
    let mut socket = ws_connect(&app, &cookie).await;
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
    let failed_ack = publish(
        &app,
        &device.id,
        &device.token,
        "failed-commit-event",
        br#"{"event_id":"failed-commit-event","values":{"x":1}}"#,
    )
    .await;
    assert_eq!(failed_ack["status"], "retryable");
    assert_eq!(failed_ack["code"], "storage_unavailable");
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

    let readiness = reqwest::Client::new()
        .get(format!("{}/readyz", app.base))
        .send()
        .await
        .unwrap();
    assert_eq!(readiness.status(), reqwest::StatusCode::SERVICE_UNAVAILABLE);
    let readiness: Value = readiness.json().await.unwrap();
    assert_eq!(readiness["checks"]["database"], "ok");
    assert_eq!(readiness["checks"]["ingest"], "degraded");
}

#[tokio::test]
async fn slow_websocket_is_closed_and_releases_its_connection_slot() {
    let app = TestApp::builder().websocket_limits(1, 16).start().await;
    let dropped_before = Metrics::global().ws_dropped_total.get();
    let (_, cookie) = ws_session_cookie(&app).await;
    let device = app.create_device("ws-slow-consumer", true).await;
    let mut slow = ws_connect(&app, &cookie).await;
    assert_eq!(next_json(&mut slow).await["type"], "hello");
    slow.send(Message::Text(
        json!({"action":"subscribe", "channels":[format!("device:{}", device.id)]})
            .to_string()
            .into(),
    ))
    .await
    .unwrap();
    assert_eq!(next_json(&mut slow).await["type"], "subscribed");

    let payload = "x".repeat(32 * 1024);
    for event_id in 0_i64..512 {
        app.events.publish(TelemetryCommitted {
            id: event_id.to_string(),
            ts: event_id,
            device_id: device.id.clone(),
            data: json!({"payload":payload.clone()}),
        });
    }

    let close_code = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            match slow.next().await {
                Some(Ok(Message::Close(Some(frame)))) => return frame.code,
                Some(Ok(_)) => {}
                Some(Err(error)) => panic!("slow websocket failed before close: {error}"),
                None => panic!("slow websocket ended without a close frame"),
            }
        }
    })
    .await
    .expect("slow websocket should be closed");
    assert_eq!(close_code, CloseCode::Again);
    assert_eq!(Metrics::global().ws_dropped_total.get() - dropped_before, 1);

    let mut replacement = ws_connect(&app, &cookie).await;
    assert_eq!(next_json(&mut replacement).await["type"], "hello");
    replacement.close(None).await.unwrap();
}

#[tokio::test]
async fn request_responses_timeout_when_client_does_not_read_and_release_the_slot() {
    let app = TestApp::builder().websocket_limits(1, 16).start().await;
    let (_, cookie) = ws_session_cookie(&app).await;
    let mut slow = ws_connect(&app, &cookie).await;
    assert_eq!(next_json(&mut slow).await["type"], "hello");

    let channels: Vec<_> = (1..=8)
        .map(|id| format!("device:00000000-0000-7000-8000-{id:012x}"))
        .collect();
    let request = Message::Text(
        json!({"action":"subscribe", "channels":channels})
            .to_string()
            .into(),
    );
    let started = std::time::Instant::now();
    let server_closed = tokio::time::timeout(Duration::from_secs(15), async {
        for _ in 0..100_000 {
            if slow.send(request.clone()).await.is_err() {
                return true;
            }
        }
        false
    })
    .await
    .expect("request flood should be bounded by the server's write deadline");
    assert!(
        server_closed,
        "server kept accepting requests without draining replies"
    );
    assert!(started.elapsed() >= Duration::from_millis(800));

    let close = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            match slow.next().await {
                Some(Ok(Message::Close(frame))) => return frame.map(|frame| frame.code),
                Some(Ok(_)) => {}
                Some(Err(_)) | None => return None,
            }
        }
    })
    .await
    .expect("slow request client should be disconnected within the deadline");
    assert!(close.is_none_or(|code| code == CloseCode::Again));

    let mut replacement = ws_connect(&app, &cookie).await;
    assert_eq!(next_json(&mut replacement).await["type"], "hello");
    replacement.close(None).await.unwrap();
}
