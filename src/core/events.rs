//! Bounded best-effort broadcasts of committed application events.

use tokio::sync::broadcast;
use uuid::Uuid;

use super::telemetry::ValidatedTelemetry;

pub const EVENT_CAPACITY: usize = 1_024;

#[derive(Clone, Debug)]
pub struct TelemetryCommitted {
    pub id: String,
    pub ts: i64,
    pub device_id: String,
    pub data: serde_json::Value,
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
    telemetry: broadcast::Sender<TelemetryCommitted>,
    session_revocations: broadcast::Sender<String>,
}

impl EventBus {
    #[must_use]
    pub fn new() -> Self {
        let (telemetry, _) = broadcast::channel(EVENT_CAPACITY);
        let (session_revocations, _) = broadcast::channel(EVENT_CAPACITY);
        Self {
            telemetry,
            session_revocations,
        }
    }

    pub fn subscribe(&self) -> broadcast::Receiver<TelemetryCommitted> {
        self.telemetry.subscribe()
    }

    pub fn publish(&self, event: TelemetryCommitted) {
        let _ = self.telemetry.send(event);
    }

    pub fn subscribe_session_revocations(&self) -> broadcast::Receiver<String> {
        self.session_revocations.subscribe()
    }

    pub fn publish_session_revocation(&self, session_id: &str) {
        let _ = self.session_revocations.send(session_id.to_owned());
    }
}

impl Default for EventBus {
    fn default() -> Self {
        Self::new()
    }
}
