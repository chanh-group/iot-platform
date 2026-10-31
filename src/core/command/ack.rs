//! Bounded command ACK validation and persistence worker.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::core::auth;
use crate::core::events::EventBus;
use crate::observability::metrics::Metrics;

use super::{CommandRepository, CommandRepositoryError};

pub const ACK_QUEUE_CAPACITY: usize = 4_096;
const MAX_RESULT_BYTES: usize = 16_384;
const MAX_ERROR_CHARS: usize = 512;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum CommandAckStatus {
    Succeeded,
    Failed,
}

impl CommandAckStatus {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
        }
    }
}

/// Validated device result. Device identity is intentionally carried separately.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CommandAck {
    pub command_id: String,
    pub status: CommandAckStatus,
    #[serde(
        default,
        deserialize_with = "deserialize_result",
        skip_serializing_if = "Option::is_none"
    )]
    pub result: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

fn deserialize_result<'de, D>(deserializer: D) -> Result<Option<Value>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    Value::deserialize(deserializer).map(Some)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum CommandAckError {
    #[error("invalid command ACK")]
    Invalid,
}

/// Parses and validates one ACK without retaining the raw MQTT payload.
pub fn validate_ack(payload: &[u8]) -> Result<CommandAck, CommandAckError> {
    let ack: CommandAck = serde_json::from_slice(payload).map_err(|_| CommandAckError::Invalid)?;
    if !auth::is_device_id(&ack.command_id)
        || ack.result.as_ref().is_some_and(|result| {
            serde_json::to_vec(result).map_or(true, |bytes| bytes.len() > MAX_RESULT_BYTES)
        })
        || ack
            .error
            .as_ref()
            .is_some_and(|error| error.chars().count() > MAX_ERROR_CHARS)
    {
        return Err(CommandAckError::Invalid);
    }
    Ok(ack)
}

struct QueuedAck {
    device_id: String,
    ack: CommandAck,
    received_at: i64,
    _pending: AckPendingPermit,
}

#[derive(Default)]
struct PendingAckState {
    commands: HashMap<String, usize>,
    unregistered: usize,
}

struct AckPendingPermit {
    pending: PendingCommandAcks,
    command_id: String,
}

impl AckPendingPermit {
    fn new(pending: PendingCommandAcks, command_id: String) -> Self {
        Self {
            pending,
            command_id,
        }
    }
}

impl Drop for AckPendingPermit {
    fn drop(&mut self) {
        if let Ok(mut state) = self.pending.0.lock()
            && let Some(count) = state.commands.get_mut(&self.command_id)
        {
            *count -= 1;
            if *count == 0 {
                state.commands.remove(&self.command_id);
            }
        }
    }
}

/// Bounded count of accepted ACKs waiting for or undergoing SQL persistence.
#[derive(Clone, Default)]
pub struct PendingCommandAcks(Arc<Mutex<PendingAckState>>);

impl PendingCommandAcks {
    fn begin(&self) -> bool {
        let Ok(mut state) = self.0.lock() else {
            return false;
        };
        state.unregistered += 1;
        true
    }

    fn register(&self, command_id: &str) -> Option<AckPendingPermit> {
        let Ok(mut state) = self.0.lock() else {
            return None;
        };
        *state.commands.entry(command_id.to_owned()).or_default() += 1;
        Some(AckPendingPermit::new(self.clone(), command_id.to_owned()))
    }

    fn finish_unregistered(&self) {
        if let Ok(mut state) = self.0.lock() {
            state.unregistered -= 1;
        }
    }

    /// Returns pending ACK command IDs; `None` asks the expiry sweep to defer.
    pub(crate) fn command_ids(&self) -> Option<Vec<String>> {
        let state = self.0.lock().ok()?;
        if state.unregistered != 0 {
            return None;
        }
        Some(state.commands.keys().cloned().collect())
    }
}

/// Non-blocking producer for the bounded MQTT command ACK queue.
#[derive(Clone)]
pub struct CommandAckHandle {
    sender: mpsc::Sender<QueuedAck>,
    pending: PendingCommandAcks,
}

impl CommandAckHandle {
    /// Returns the shared pending counter so the expiry sweep can defer safely.
    #[must_use]
    pub fn pending(&self) -> PendingCommandAcks {
        self.pending.clone()
    }

    /// Captures receipt time, validates, and enqueues without waiting for SQLite.
    pub fn try_send(&self, device_id: &str, payload: &[u8]) {
        // Mark the hook in flight before reading time so an expiry sweep cannot miss it.
        if !self.pending.begin() {
            Metrics::global().command_ack_dropped_total.inc();
            tracing::warn!("command ACK pending registry unavailable; ACK dropped");
            return;
        }
        let received_at = auth::now_ms();
        let ack = match validate_ack(payload) {
            Ok(ack) => ack,
            Err(_) => {
                self.pending.finish_unregistered();
                Metrics::global().command_ack_invalid_total.inc();
                tracing::warn!("invalid command ACK ignored");
                return;
            }
        };
        let Some(pending) = self.pending.register(&ack.command_id) else {
            self.pending.finish_unregistered();
            Metrics::global().command_ack_dropped_total.inc();
            tracing::warn!("command ACK pending registry unavailable; ACK dropped");
            return;
        };
        self.pending.finish_unregistered();
        if self
            .sender
            .try_send(QueuedAck {
                device_id: device_id.to_owned(),
                ack,
                received_at,
                _pending: pending,
            })
            .is_err()
        {
            Metrics::global().command_ack_dropped_total.inc();
            tracing::warn!("command ACK queue full or closed; ACK dropped");
        }
    }
}

/// Starts the sole command ACK persistence worker.
pub fn spawn_ack_worker(
    repository: Arc<dyn CommandRepository>,
    events: EventBus,
    cancel: CancellationToken,
) -> (CommandAckHandle, JoinHandle<()>) {
    let (sender, mut receiver) = mpsc::channel::<QueuedAck>(ACK_QUEUE_CAPACITY);
    let pending = PendingCommandAcks::default();
    let task = tokio::spawn(async move {
        loop {
            tokio::select! {
                biased;
                _ = cancel.cancelled() => {
                    receiver.close();
                    break;
                }
                queued = receiver.recv() => {
                    let Some(queued) = queued else { break };
                    process_ack(repository.as_ref(), &events, queued).await;
                }
            }
        }
        while let Some(queued) = receiver.recv().await {
            process_ack(repository.as_ref(), &events, queued).await;
        }
    });
    (CommandAckHandle { sender, pending }, task)
}

async fn process_ack(repository: &dyn CommandRepository, events: &EventBus, queued: QueuedAck) {
    match repository
        .apply_ack(&queued.device_id, &queued.ack, queued.received_at)
        .await
    {
        Ok(super::AckApplyOutcome::WrongDevice) => {
            tracing::warn!("cross-device command ACK ignored");
        }
        Ok(super::AckApplyOutcome::UnknownCommand) => {
            tracing::warn!("unknown command ACK ignored");
        }
        Ok(super::AckApplyOutcome::Applied) => {
            publish_command_event(repository, events, &queued.ack.command_id).await;
        }
        Ok(super::AckApplyOutcome::Duplicate | super::AckApplyOutcome::LateRecorded) => {}
        Ok(super::AckApplyOutcome::ExpiredAndLateRecorded) => {
            Metrics::global().commands_expired_total.inc();
            publish_command_event(repository, events, &queued.ack.command_id).await;
        }
        Err(CommandRepositoryError::StorageUnavailable) => {
            Metrics::global().command_ack_storage_errors_total.inc();
            tracing::warn!("cannot persist command ACK; device may retry");
        }
        Err(_) => {
            Metrics::global().command_ack_storage_errors_total.inc();
            tracing::warn!("command ACK persistence failed");
        }
    }
}

async fn publish_command_event(
    repository: &dyn CommandRepository,
    events: &EventBus,
    command_id: &str,
) {
    match repository.get(command_id).await {
        Ok(Some(command)) => events.publish_command(
            &command.id,
            &command.device_id,
            command.status,
            command.completed_at,
            command.error.as_deref(),
        ),
        Ok(None) => tracing::warn!("command missing after ACK state transition"),
        Err(error) => tracing::warn!(%error, "cannot read command after persisted ACK"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use uuid::Uuid;

    #[test]
    fn validates_only_contract_ack_status_and_limits() {
        let command_id = Uuid::now_v7().to_string();
        for payload in [
            json!({"command_id":command_id,"status":"pending"}).to_string(),
            json!({"command_id":"not-a-uuid","status":"succeeded"}).to_string(),
            json!({"command_id":command_id,"status":"succeeded","error":"x".repeat(513)}).to_string(),
            json!({"command_id":command_id,"status":"succeeded","result":"x".repeat(MAX_RESULT_BYTES)}).to_string(),
            json!({"command_id":command_id,"status":"succeeded","extra":true}).to_string(),
        ] {
            assert!(validate_ack(payload.as_bytes()).is_err(), "{payload}");
        }
        let valid = json!({
            "command_id": command_id,
            "status": "failed",
            "result": {"attempt": 2},
            "error": "device error",
        })
        .to_string();
        assert_eq!(
            validate_ack(valid.as_bytes()).unwrap().status,
            CommandAckStatus::Failed
        );
        let null_result = json!({
            "command_id": Uuid::now_v7().to_string(),
            "status": "succeeded",
            "result": null,
        })
        .to_string();
        assert_eq!(
            validate_ack(null_result.as_bytes()).unwrap().result,
            Some(Value::Null)
        );
    }

    #[test]
    fn queue_full_drops_ack_without_unbounded_work() {
        let (sender, _receiver) = mpsc::channel(ACK_QUEUE_CAPACITY);
        let handle = CommandAckHandle {
            sender,
            pending: PendingCommandAcks::default(),
        };
        let before = Metrics::global().command_ack_dropped_total.get();
        let id = Uuid::now_v7().to_string();
        let payload = json!({"command_id":id,"status":"succeeded"}).to_string();
        for _ in 0..=ACK_QUEUE_CAPACITY {
            handle.try_send("device", payload.as_bytes());
        }
        assert_eq!(
            Metrics::global().command_ack_dropped_total.get(),
            before + 1
        );
    }

    #[test]
    fn invalid_ack_increments_its_metric() {
        let (sender, _receiver) = mpsc::channel(1);
        let handle = CommandAckHandle {
            sender,
            pending: PendingCommandAcks::default(),
        };
        let before = Metrics::global().command_ack_invalid_total.get();
        handle.try_send("device", b"{}");
        assert_eq!(
            Metrics::global().command_ack_invalid_total.get(),
            before + 1
        );
    }
}
