//! Authenticated telemetry event WebSocket (`02` §4.8).

use std::collections::BTreeSet;
use std::time::Duration;

use axum::extract::State;
use axum::extract::ws::{CloseFrame, Message, WebSocket, WebSocketUpgrade};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::Response;
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::sync::{mpsc, watch};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::core::domain_error::DomainError;
use crate::core::events::RealtimeEvent;
use crate::observability::metrics::Metrics;
use crate::observability::request_id::RequestId;

use super::AppState;
use super::api_error::ApiError;
use super::auth::AuthSession;

const MAX_SUBSCRIPTIONS: usize = 8;
const MESSAGE_LIMIT: usize = 4_096;
const WS_SEND_TIMEOUT: Duration = Duration::from_secs(1);
const SERVER_PING_INTERVAL: Duration = Duration::from_secs(30);
const PONG_TIMEOUT: Duration = Duration::from_secs(60);

#[derive(Debug, Deserialize)]
#[serde(tag = "action", rename_all = "lowercase")]
enum ClientMessage {
    Subscribe { channels: Vec<String> },
    Unsubscribe { channels: Vec<String> },
    Ping,
}

pub async fn upgrade(
    State(state): State<AppState>,
    RequestId(request_id): RequestId,
    headers: HeaderMap,
    AuthSession(session): AuthSession,
    ws: WebSocketUpgrade,
) -> Result<Response, ApiError> {
    if !valid_origin(&headers) {
        return Err(ApiError::new(
            StatusCode::FORBIDDEN,
            "forbidden",
            "invalid origin".to_owned(),
            request_id,
        ));
    }
    let session_revocations = state.events.subscribe_session_revocations();
    let session_live: bool = sqlx::query_scalar(
        "SELECT EXISTS(
             SELECT 1 FROM sessions
             WHERE id = ? AND revoked_at IS NULL AND expires_at > ?
         )",
    )
    .bind(&session.id)
    .bind(crate::core::auth::now_ms())
    .fetch_one(&state.pool)
    .await
    .map_err(|error| {
        ApiError::from_domain(DomainError::Internal(error.into()), request_id.clone())
    })?;
    if !session_live {
        return Err(ApiError::from_domain(DomainError::Unauthorized, request_id));
    }
    let permit = state
        .ws_connections
        .clone()
        .try_acquire_owned()
        .map_err(|_| {
            ApiError::new(
                StatusCode::SERVICE_UNAVAILABLE,
                "ws_capacity_exceeded",
                "websocket capacity exceeded".to_owned(),
                request_id,
            )
        })?;
    let queue_capacity =
        usize::try_from(state.config.config.http.ws_queue_capacity).unwrap_or(MESSAGE_LIMIT);
    let session_id = session.id;
    let expires_at = session.expires_at;
    Ok(ws
        .max_message_size(MESSAGE_LIMIT)
        .max_frame_size(MESSAGE_LIMIT)
        .on_upgrade(move |socket| {
            run(
                socket,
                state,
                permit,
                queue_capacity,
                session_id,
                expires_at,
                session_revocations,
            )
        }))
}

fn valid_origin(headers: &HeaderMap) -> bool {
    let Some(host) = headers
        .get(header::HOST)
        .and_then(|value| value.to_str().ok())
    else {
        return false;
    };
    let Some(origin) = headers
        .get(header::ORIGIN)
        .and_then(|value| value.to_str().ok())
    else {
        return false;
    };
    let Some(authority) = origin
        .strip_prefix("http://")
        .or_else(|| origin.strip_prefix("https://"))
    else {
        return false;
    };
    !authority.is_empty()
        && !authority.contains(['/', '?', '#', '@'])
        && authority.eq_ignore_ascii_case(host)
}

struct ClientMetric;

impl ClientMetric {
    fn new() -> Self {
        Metrics::global().ws_clients.inc();
        Self
    }
}

impl Drop for ClientMetric {
    fn drop(&mut self) {
        Metrics::global().ws_clients.dec();
    }
}

async fn run(
    mut socket: WebSocket,
    state: AppState,
    _permit: tokio::sync::OwnedSemaphorePermit,
    queue_capacity: usize,
    session_id: String,
    expires_at: i64,
    mut session_revocations: tokio::sync::broadcast::Receiver<String>,
) {
    let _metric = ClientMetric::new();
    if !send_json(
        &mut socket,
        &json!({
            "type": "hello",
            "server_time": crate::core::auth::now_ms(),
            "max_subscriptions": MAX_SUBSCRIPTIONS,
        }),
    )
    .await
    {
        return;
    }

    let (subscriptions_tx, subscriptions_rx) = watch::channel(BTreeSet::new());
    let (outbound_tx, mut outbound_rx) = mpsc::channel::<Value>(queue_capacity.max(1));
    let slow_consumer = CancellationToken::new();
    let cancel_slow_consumer = slow_consumer.clone();
    let cancel_producer = CancellationToken::new();
    let producer_cancel = cancel_producer.clone();
    let mut events = state.events.subscribe_realtime();
    let producer = tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = producer_cancel.cancelled() => break,
                received = events.recv() => match received {
                    Ok(event) => {
                        if is_subscribed(&event, &subscriptions_rx.borrow())
                            && !send_event(&outbound_tx, event)
                        {
                            Metrics::global().ws_dropped_total.inc();
                            cancel_slow_consumer.cancel();
                            break;
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                        Metrics::global().ws_dropped_total.inc();
                        cancel_slow_consumer.cancel();
                        break;
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                }
            }
        }
    });

    let start = Instant::now();
    let mut last_pong = start;
    let mut ping = tokio::time::interval_at(start + SERVER_PING_INTERVAL, SERVER_PING_INTERVAL);
    let expires_in_ms = expires_at
        .saturating_sub(crate::core::auth::now_ms())
        .max(0);
    let session_expiry = tokio::time::sleep(Duration::from_millis(
        u64::try_from(expires_in_ms).unwrap_or(u64::MAX),
    ));
    tokio::pin!(session_expiry);
    loop {
        tokio::select! {
            revocation = session_revocations.recv() => match revocation {
                Ok(revoked_id) if revoked_id == session_id => {
                    close(&mut socket, 4401, "session_revoked").await;
                    break;
                }
                Ok(_) => {},
                Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                    close(&mut socket, 4401, "session_state_lost").await;
                    break;
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
            },
            _ = &mut session_expiry => {
                close(&mut socket, 4401, "session_expired").await;
                break;
            }
            incoming = socket.recv() => match incoming {
                Some(Ok(Message::Text(text))) => {
                    if text.len() > MESSAGE_LIMIT {
                        if !send_error(&mut socket, "message_too_large", "message exceeds 4096 bytes").await {
                            close(&mut socket, 1013, "slow_consumer").await;
                            break;
                        }
                        continue;
                    }
                    match serde_json::from_str::<ClientMessage>(text.as_str()) {
                        Ok(ClientMessage::Subscribe { channels }) => {
                            let Some(channels) = normalize_channels(&channels) else {
                                if !send_error(&mut socket, "invalid_channel", "invalid channel").await {
                                    close(&mut socket, 1013, "slow_consumer").await;
                                    break;
                                }
                                continue;
                            };
                            let mut current = subscriptions_tx.borrow().clone();
                            current.extend(channels.iter().cloned());
                            if current.len() > MAX_SUBSCRIPTIONS {
                                if !send_error(&mut socket, "max_subscriptions", "at most 8 channels are allowed").await {
                                    close(&mut socket, 1013, "slow_consumer").await;
                                    break;
                                }
                                continue;
                            }
                            subscriptions_tx.send_replace(current);
                            if !send_json(&mut socket, &json!({"type":"subscribed", "channels":channels})).await {
                                close(&mut socket, 1013, "slow_consumer").await;
                                break;
                            }
                        }
                        Ok(ClientMessage::Unsubscribe { channels }) => {
                            let Some(channels) = normalize_channels(&channels) else {
                                if !send_error(&mut socket, "invalid_channel", "invalid channel").await {
                                    close(&mut socket, 1013, "slow_consumer").await;
                                    break;
                                }
                                continue;
                            };
                            let mut current = subscriptions_tx.borrow().clone();
                            for channel in &channels {
                                current.remove(channel);
                            }
                            subscriptions_tx.send_replace(current);
                            if !send_json(&mut socket, &json!({"type":"unsubscribed", "channels":channels})).await {
                                close(&mut socket, 1013, "slow_consumer").await;
                                break;
                            }
                        }
                        Ok(ClientMessage::Ping) => {
                            if !send_json(&mut socket, &json!({"type":"pong", "ts":crate::core::auth::now_ms()})).await {
                                close(&mut socket, 1013, "slow_consumer").await;
                                break;
                            }
                        }
                        Err(_) => {
                            if !send_error(&mut socket, "invalid_message", "invalid websocket message").await {
                                close(&mut socket, 1013, "slow_consumer").await;
                                break;
                            }
                        }
                    }
                }
                Some(Ok(Message::Ping(payload))) => {
                    if !send_message(&mut socket, Message::Pong(payload)).await {
                        close(&mut socket, 1013, "slow_consumer").await;
                        break;
                    }
                }
                Some(Ok(Message::Pong(_))) => last_pong = Instant::now(),
                Some(Ok(Message::Close(_))) | None | Some(Err(_)) => break,
                Some(Ok(Message::Binary(_))) => {
                    if !send_error(&mut socket, "invalid_message", "text messages are required").await {
                        close(&mut socket, 1013, "slow_consumer").await;
                        break;
                    }
                }
            },
            outbound = outbound_rx.recv() => match outbound {
                Some(message) => {
                    let sent = tokio::select! {
                        biased;
                        _ = slow_consumer.cancelled() => None,
                        sent = send_json(&mut socket, &message) => Some(sent),
                    };
                    if sent != Some(true) {
                        if sent.is_some() {
                            Metrics::global().ws_dropped_total.inc();
                        }
                        close(&mut socket, 1013, "slow_consumer").await;
                        break;
                    }
                }
                None => break,
            },
            _ = slow_consumer.cancelled() => {
                close(&mut socket, 1013, "slow_consumer").await;
                break;
            }
            _ = ping.tick() => {
                if Instant::now().duration_since(last_pong) >= PONG_TIMEOUT {
                    close(&mut socket, 1001, "pong_timeout").await;
                    break;
                }
                if !send_message(&mut socket, Message::Ping(Vec::new().into())).await {
                    close(&mut socket, 1013, "slow_consumer").await;
                    break;
                }
            }
        }
    }
    cancel_producer.cancel();
    producer.abort();
}

fn is_subscribed(event: &RealtimeEvent, channels: &BTreeSet<String>) -> bool {
    let overview = channels.contains("overview");
    match event {
        RealtimeEvent::Telemetry(event) => {
            channels.contains(&format!("device:{}", event.device_id))
        }
        RealtimeEvent::Command(event) => {
            overview || channels.contains(&format!("device:{}", event.device_id))
        }
        RealtimeEvent::Presence(event) => {
            overview || channels.contains(&format!("device:{}", event.device_id))
        }
        RealtimeEvent::ReportedStatus(event) => {
            channels.contains(&format!("device:{}", event.device_id))
        }
        RealtimeEvent::Config(_) | RealtimeEvent::System(_) => overview,
    }
}

fn send_event(sender: &mpsc::Sender<Value>, event: RealtimeEvent) -> bool {
    let payload = match event {
        RealtimeEvent::Telemetry(event) => json!({
            "type": "telemetry",
            "id": event.id,
            "ts": event.ts,
            "device_id": event.device_id,
            "data": event.data,
        }),
        RealtimeEvent::Command(event) => json!({
            "type": "command",
            "id": event.id,
            "ts": event.ts,
            "device_id": event.device_id,
            "data": {
                "command_id": event.command_id,
                "device_id": event.device_id,
                "status": event.status.as_str(),
                "state_version": event.state_version,
                "completed_at": event.completed_at,
                "error": event.error,
            },
        }),
        RealtimeEvent::Presence(event) => json!({
            "type": "presence",
            "id": event.id,
            "ts": event.ts,
            "device_id": event.device_id,
            "data": {
                "online": event.online,
                "last_seen_at": event.last_seen_at,
            },
        }),
        RealtimeEvent::ReportedStatus(event) => json!({
            "type": "reported_status",
            "id": event.id,
            "ts": event.ts,
            "device_id": event.device_id,
            "data": {"status": event.status, "ts": event.ts},
        }),
        RealtimeEvent::Config(event) => json!({
            "type": "config",
            "id": event.id,
            "ts": event.ts,
            "data": {"revision": event.revision},
        }),
        RealtimeEvent::System(event) => json!({
            "type": "system",
            "id": event.id,
            "ts": event.ts,
            "data": {
                "kind": event.kind,
                "severity": event.severity,
                "message": event.message,
            },
        }),
    };
    sender.try_send(payload).is_ok()
}

fn normalize_channels(channels: &[String]) -> Option<Vec<String>> {
    let mut normalized = Vec::with_capacity(channels.len());
    for channel in channels {
        if channel == "overview" {
            normalized.push(channel.clone());
            continue;
        }
        let id = channel.strip_prefix("device:")?;
        normalized.push(format!("device:{}", Uuid::parse_str(id).ok()?));
    }
    normalized.sort();
    normalized.dedup();
    Some(normalized)
}

async fn send_json(socket: &mut WebSocket, value: &Value) -> bool {
    let Ok(text) = serde_json::to_string(value) else {
        return false;
    };
    send_message(socket, Message::Text(text.into())).await
}

async fn send_error(socket: &mut WebSocket, code: &str, message: &str) -> bool {
    send_json(
        socket,
        &json!({"type":"error", "code":code, "message":message}),
    )
    .await
}

async fn send_message(socket: &mut WebSocket, message: Message) -> bool {
    matches!(
        tokio::time::timeout(WS_SEND_TIMEOUT, socket.send(message)).await,
        Ok(Ok(()))
    )
}

async fn close(socket: &mut WebSocket, code: u16, reason: &'static str) {
    let _ = send_message(
        socket,
        Message::Close(Some(CloseFrame {
            code,
            reason: reason.into(),
        })),
    )
    .await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::events::{
        CommandChanged, ConfigChanged, PresenceChanged, ReportedStatus, SystemEvent,
        TelemetryCommitted,
    };
    use axum::http::HeaderValue;

    #[test]
    fn origin_must_match_host_without_path_or_user_info() {
        let mut headers = HeaderMap::new();
        headers.insert(header::HOST, HeaderValue::from_static("example.test:8080"));
        headers.insert(
            header::ORIGIN,
            HeaderValue::from_static("http://EXAMPLE.test:8080"),
        );
        assert!(valid_origin(&headers));
        headers.insert(
            header::ORIGIN,
            HeaderValue::from_static("http://example.test:8080/path"),
        );
        assert!(!valid_origin(&headers));
        headers.insert(
            header::ORIGIN,
            HeaderValue::from_static("http://user@example.test:8080"),
        );
        assert!(!valid_origin(&headers));
    }

    #[test]
    fn channel_validation_canonicalizes_uuid_and_rejects_unknown_channels() {
        let channel = "device:019c0e1e-0000-7000-8000-00000000000a".to_owned();
        assert_eq!(
            normalize_channels(std::slice::from_ref(&channel)),
            Some(vec![channel])
        );
        assert!(normalize_channels(&["device:not-a-uuid".to_owned()]).is_none());
    }

    #[test]
    fn event_channels_match_contract_and_dual_subscription_sends_once() {
        let overview = BTreeSet::from(["overview".to_owned()]);
        let device = BTreeSet::from(["device:device-a".to_owned()]);
        let other_device = BTreeSet::from(["device:device-b".to_owned()]);
        let both = BTreeSet::from(["overview".to_owned(), "device:device-a".to_owned()]);
        let command = RealtimeEvent::Command(CommandChanged {
            id: "event-id".to_owned(),
            ts: 1,
            command_id: "command-id".to_owned(),
            device_id: "device-a".to_owned(),
            status: crate::core::command::CommandStatus::Pending,
            state_version: 1,
            completed_at: None,
            error: None,
        });
        let config = RealtimeEvent::Config(ConfigChanged {
            id: "config-id".to_owned(),
            ts: 1,
            revision: 1,
        });
        let telemetry = RealtimeEvent::Telemetry(TelemetryCommitted {
            id: "telemetry-id".to_owned(),
            ts: 1,
            device_id: "device-a".to_owned(),
            data: json!({}),
        });
        let presence = RealtimeEvent::Presence(PresenceChanged {
            id: "presence-id".to_owned(),
            ts: 1,
            device_id: "device-a".to_owned(),
            online: true,
            last_seen_at: 1,
        });
        let reported_status = RealtimeEvent::ReportedStatus(ReportedStatus {
            id: "reported-id".to_owned(),
            ts: 1,
            device_id: "device-a".to_owned(),
            status: "online",
        });
        let system = RealtimeEvent::System(SystemEvent {
            id: "system-id".to_owned(),
            ts: 1,
            kind: "queue_full",
            severity: "warning",
            message: "queue full",
        });

        assert!(is_subscribed(&command, &overview));
        assert!(is_subscribed(&command, &device));
        assert!(!is_subscribed(&command, &other_device));
        assert!(is_subscribed(&config, &overview));
        assert!(!is_subscribed(&config, &device));
        assert!(is_subscribed(&presence, &overview));
        assert!(is_subscribed(&presence, &device));
        assert!(is_subscribed(&reported_status, &device));
        assert!(!is_subscribed(&reported_status, &overview));
        assert!(is_subscribed(&system, &overview));
        assert!(!is_subscribed(&system, &device));
        assert!(!is_subscribed(&telemetry, &overview));
        assert!(is_subscribed(&telemetry, &device));

        let (sender, mut receiver) = mpsc::channel(2);
        assert!(is_subscribed(&command, &both));
        assert!(send_event(&sender, command));
        assert_eq!(receiver.try_recv().unwrap()["type"], "command");
        assert!(receiver.try_recv().is_err());
    }

    #[test]
    fn full_client_queue_rejects_the_newest_event() {
        let (sender, _receiver) = mpsc::channel(1);
        sender.try_send(json!({"queued":true})).unwrap();
        assert!(!send_event(
            &sender,
            RealtimeEvent::Telemetry(TelemetryCommitted {
                id: "event-id".to_owned(),
                ts: 1,
                device_id: "device-id".to_owned(),
                data: json!({}),
            })
        ));
    }
}
