//! Bounded best-effort broadcasts of committed application events.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use tokio::sync::broadcast;
use uuid::Uuid;

use super::command::CommandStatus;
use super::telemetry::ValidatedTelemetry;

pub const EVENT_CAPACITY: usize = 1_024;

#[derive(Clone, Debug)]
pub struct TelemetryCommitted {
    pub id: String,
    pub ts: i64,
    pub device_id: String,
    pub data: serde_json::Value,
}

#[derive(Clone, Debug)]
pub struct CommandChanged {
    pub id: String,
    pub ts: i64,
    pub command_id: String,
    pub device_id: String,
    pub status: CommandStatus,
    pub state_version: u8,
    pub completed_at: Option<i64>,
    pub error: Option<String>,
}

#[derive(Clone, Debug)]
pub struct PresenceChanged {
    pub id: String,
    pub ts: i64,
    pub device_id: String,
    pub online: bool,
    pub last_seen_at: i64,
}

#[derive(Clone, Debug)]
pub struct ReportedStatus {
    pub id: String,
    pub ts: i64,
    pub device_id: String,
    pub status: &'static str,
}

#[derive(Clone, Debug)]
pub struct ConfigChanged {
    pub id: String,
    pub ts: i64,
    pub revision: u64,
}

#[derive(Clone, Debug)]
pub struct SystemEvent {
    pub id: String,
    pub ts: i64,
    pub kind: &'static str,
    pub severity: &'static str,
    pub message: &'static str,
}

#[derive(Clone, Debug)]
pub enum RealtimeEvent {
    Telemetry(TelemetryCommitted),
    Command(CommandChanged),
    Presence(PresenceChanged),
    ReportedStatus(ReportedStatus),
    Config(ConfigChanged),
    System(SystemEvent),
}

impl TelemetryCommitted {
    pub fn from_telemetry(device_id: &str, event: &ValidatedTelemetry) -> Self {
        Self {
            id: Uuid::now_v7().to_string(),
            ts: event.received_at,
            device_id: device_id.to_owned(),
            data: serde_json::json!({
                "event_id": event.event_id,
                "ts": event.ts,
                "received_at": event.received_at,
                "values": event.values,
            }),
        }
    }
}

#[derive(Clone)]
pub struct EventBus {
    realtime: broadcast::Sender<RealtimeEvent>,
    session_revocations: broadcast::Sender<String>,
    config_revisions: broadcast::Sender<u64>,
    queue_full_announced: Arc<AtomicBool>,
}

impl EventBus {
    #[must_use]
    pub fn new() -> Self {
        let (realtime, _) = broadcast::channel(EVENT_CAPACITY);
        let (session_revocations, _) = broadcast::channel(EVENT_CAPACITY);
        let (config_revisions, _) = broadcast::channel(EVENT_CAPACITY);
        Self {
            realtime,
            session_revocations,
            config_revisions,
            queue_full_announced: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn publish(&self, event: TelemetryCommitted) {
        let _ = self.realtime.send(RealtimeEvent::Telemetry(event));
    }

    pub fn subscribe_realtime(&self) -> broadcast::Receiver<RealtimeEvent> {
        self.realtime.subscribe()
    }

    pub fn subscribe_session_revocations(&self) -> broadcast::Receiver<String> {
        self.session_revocations.subscribe()
    }

    pub fn publish_session_revocation(&self, session_id: &str) {
        let _ = self.session_revocations.send(session_id.to_owned());
    }

    pub fn subscribe_config_revisions(&self) -> broadcast::Receiver<u64> {
        self.config_revisions.subscribe()
    }

    pub fn publish_config_revision(&self, revision: u64) {
        let _ = self.config_revisions.send(revision);
        let _ = self.realtime.send(RealtimeEvent::Config(ConfigChanged {
            id: Uuid::now_v7().to_string(),
            ts: crate::core::auth::now_ms(),
            revision,
        }));
    }

    pub fn publish_command(
        &self,
        command_id: &str,
        device_id: &str,
        status: CommandStatus,
        completed_at: Option<i64>,
        error: Option<&str>,
    ) {
        let _ = self.realtime.send(RealtimeEvent::Command(CommandChanged {
            id: Uuid::now_v7().to_string(),
            ts: crate::core::auth::now_ms(),
            command_id: command_id.to_owned(),
            device_id: device_id.to_owned(),
            state_version: match status {
                CommandStatus::Pending => 1,
                CommandStatus::Dispatched => 2,
                CommandStatus::Succeeded | CommandStatus::Failed | CommandStatus::Expired => 3,
            },
            status,
            completed_at,
            error: error.map(str::to_owned),
        }));
    }

    pub fn publish_presence(&self, device_id: &str, online: bool, last_seen_at: i64) {
        let _ = self.realtime.send(RealtimeEvent::Presence(PresenceChanged {
            id: Uuid::now_v7().to_string(),
            ts: last_seen_at,
            device_id: device_id.to_owned(),
            online,
            last_seen_at,
        }));
    }

    pub fn publish_reported_status(&self, device_id: &str, status: &'static str, ts: i64) {
        let _ = self
            .realtime
            .send(RealtimeEvent::ReportedStatus(ReportedStatus {
                id: Uuid::now_v7().to_string(),
                ts,
                device_id: device_id.to_owned(),
                status,
            }));
    }

    pub fn publish_queue_full(&self) {
        if self.realtime.receiver_count() == 0
            || self
                .queue_full_announced
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                .is_err()
        {
            return;
        }
        if !self.publish_system("queue_full", "warning", "Telemetry ingest queue is full") {
            self.reset_queue_full();
        }
    }

    pub(crate) fn reset_queue_full(&self) {
        self.queue_full_announced.store(false, Ordering::Release);
    }

    pub fn publish_storage_unavailable(&self) {
        let _ = self.publish_system("storage_unavailable", "error", "Storage is unavailable");
    }

    fn publish_system(
        &self,
        kind: &'static str,
        severity: &'static str,
        message: &'static str,
    ) -> bool {
        self.realtime
            .send(RealtimeEvent::System(SystemEvent {
                id: Uuid::now_v7().to_string(),
                ts: crate::core::auth::now_ms(),
                kind,
                severity,
                message,
            }))
            .is_ok()
    }
}

impl Default for EventBus {
    fn default() -> Self {
        Self::new()
    }
}
