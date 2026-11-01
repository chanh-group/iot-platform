use std::error::Error;
use std::io::{BufRead, Write};
use std::time::Duration;

use rumqttc::{AsyncClient, ConnectReturnCode, ConnectionError, Event, MqttOptions, Packet, QoS};
use serde_json::{Value, json};
use tokio::sync::mpsc;

#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        emit(json!({ "event": "error", "message": error.to_string() }));
        std::process::exit(1);
    }
}

async fn run() -> Result<(), Box<dyn Error>> {
    let mut args = std::env::args().skip(1);
    let host = args.next().ok_or("missing MQTT host")?;
    let port = args.next().ok_or("missing MQTT port")?.parse::<u16>()?;
    let device_id = args.next().ok_or("missing device id")?;
    let token = args.next().ok_or("missing device token")?;

    let mut options = MqttOptions::new(&device_id, host, port);
    options
        .set_keep_alive(Duration::from_secs(30))
        .set_clean_session(true)
        .set_credentials(&device_id, token);
    let (client, mut event_loop) = AsyncClient::new(options, 32);

    let (input_tx, mut input_rx) = mpsc::channel::<String>(8);
    std::thread::spawn(move || {
        for line in std::io::stdin().lock().lines() {
            let Ok(line) = line else { break };
            if input_tx.blocking_send(line).is_err() {
                break;
            }
        }
    });
    let (event_tx, mut event_rx) = mpsc::channel::<Result<Event, ConnectionError>>(32);
    let poll_task = tokio::spawn(async move {
        loop {
            let event = event_loop.poll().await;
            let failed = event.is_err();
            if event_tx.send(event).await.is_err() || failed {
                break;
            }
        }
    });

    let mut connected = false;
    let mut pending_subscriptions = Vec::new();
    loop {
        tokio::select! {
            line = input_rx.recv() => {
                let Some(line) = line else { break };
                let command: Value = serde_json::from_str(&line)?;
                let id = command.get("id").cloned().unwrap_or(Value::Null);
                match command.get("action").and_then(Value::as_str) {
                    Some("subscribe") => {
                        let topic = command.get("topic").and_then(Value::as_str).ok_or("subscribe missing topic")?;
                        client.subscribe(topic, QoS::AtLeastOnce).await?;
                        pending_subscriptions.push(id);
                    }
                    Some("publish") => {
                        let topic = command.get("topic").and_then(Value::as_str).ok_or("publish missing topic")?;
                        let payload = serde_json::to_vec(command.get("payload").ok_or("publish missing payload")?)?;
                        client.publish(topic, QoS::AtMostOnce, false, payload).await?;
                        emit(json!({ "id": id, "ok": true }));
                    }
                    Some("close") => {
                        client.disconnect().await?;
                        emit(json!({ "id": id, "ok": true }));
                        break;
                    }
                    _ => emit(json!({ "id": id, "error": "unknown MQTT helper action" })),
                }
            }
            event = event_rx.recv() => {
                let Some(event) = event else { break };
                match event {
                    Ok(Event::Incoming(Packet::ConnAck(ack))) => {
                        let code = ack.code as u8;
                        emit(json!({ "event": "connack", "code": code }));
                        if ack.code == ConnectReturnCode::Success {
                            connected = true;
                        } else {
                            break;
                        }
                    }
                    Ok(Event::Incoming(Packet::SubAck(ack))) => {
                        let id = pending_subscriptions.first().cloned().unwrap_or(Value::Null);
                        if !pending_subscriptions.is_empty() {
                            pending_subscriptions.remove(0);
                        }
                        let accepted = !ack.return_codes.is_empty()
                            && ack.return_codes.iter().all(|code| {
                                *code != rumqttc::SubscribeReasonCode::Failure
                            });
                        emit(json!({ "id": id, "ok": accepted }));
                    }
                    Ok(Event::Incoming(Packet::Publish(publish))) => {
                        emit(json!({
                            "event": "publish",
                            "topic": publish.topic,
                            "payload": String::from_utf8_lossy(&publish.payload),
                        }));
                    }
                    Ok(Event::Incoming(Packet::Disconnect)) => {
                        emit(json!({ "event": "disconnected" }));
                        break;
                    }
                    Ok(_) => {}
                    Err(ConnectionError::ConnectionRefused(code)) if !connected => {
                        emit(json!({ "event": "connack", "code": code as u8 }));
                        break;
                    }
                    Err(error) if connected => {
                        emit(json!({ "event": "disconnected", "message": error.to_string() }));
                        break;
                    }
                    Err(error) => return Err(Box::new(error)),
                }
            }
        }
    }

    poll_task.abort();
    Ok(())
}

fn emit(value: Value) {
    let stdout = std::io::stdout();
    let mut output = stdout.lock();
    if serde_json::to_writer(&mut output, &value).is_ok() {
        let _ = output.write_all(b"\n");
        let _ = output.flush();
    }
}
