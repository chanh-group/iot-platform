//! Broker-originated publishes to device sessions.

use rmqtt::context::ServerContext;
use rmqtt::types::{CodecPublish, From, Id, Publish, QoS};
use tokio::sync::mpsc;

use crate::core::telemetry::IngestResult;
use crate::observability::metrics::Metrics;

const ACK_QUEUE_CAPACITY: usize = 256;

enum AckOutcome {
    Immediate {
        status: &'static str,
        code: Option<&'static str>,
    },
    Ingest(IngestResult),
}

struct Ack {
    device_id: String,
    event_id: Option<String>,
    received_at: i64,
    outcome: AckOutcome,
}

/// Non-blocking publisher interface backed by a bounded ACK queue.
#[derive(Clone)]
pub(crate) struct AckHandle(mpsc::Sender<Ack>);

impl AckHandle {
    pub(crate) fn send(
        &self,
        device_id: String,
        event_id: Option<String>,
        status: &'static str,
        received_at: i64,
        code: Option<&'static str>,
    ) {
        self.try_send(Ack {
            device_id,
            event_id,
            received_at,
            outcome: AckOutcome::Immediate { status, code },
        });
    }

    pub(crate) fn send_after_ingest(
        &self,
        device_id: String,
        event_id: String,
        received_at: i64,
        result: IngestResult,
    ) {
        self.try_send(Ack {
            device_id,
            event_id: Some(event_id),
            received_at,
            outcome: AckOutcome::Ingest(result),
        });
    }

    fn try_send(&self, ack: Ack) {
        if self.0.try_send(ack).is_err() {
            Metrics::global().telemetry_ack_dropped_total.inc();
            tracing::warn!("telemetry ACK queue full or closed; ACK dropped");
        }
    }
}

#[derive(Clone)]
pub(crate) struct ServerPublisher(ServerContext);

impl ServerPublisher {
    pub(crate) fn new(context: ServerContext) -> Self {
        Self(context)
    }

    pub(crate) async fn telemetry_ack(
        &self,
        device_id: &str,
        payload: &[u8],
    ) -> anyhow::Result<()> {
        let id = Id::new(self.0.node.id(), 0, None, None, device_id.into(), None);
        let from_id = Id::new(self.0.node.id(), 0, None, None, "$server".into(), None);
        let topic = format!("devices/{device_id}/telemetry/ack");
        let publish = Publish::new(
            Box::new(CodecPublish {
                dup: false,
                retain: false,
                qos: QoS::AtLeastOnce,
                topic: topic.into(),
                packet_id: None,
                payload: payload.to_vec().into(),
                properties: None,
            }),
            None,
            None,
            None,
        );
        self.0
            .extends
            .shared()
            .await
            .entry(id)
            .publish(From::from_system(from_id), publish)
            .await
            .map_err(|(_, _, reason)| anyhow::anyhow!("telemetry ACK publish failed: {reason:?}"))
    }
}

/// Starts the sole ACK publisher; a full queue drops the ACK so device retries remain safe.
pub(crate) fn spawn_ack_worker(
    publisher: ServerPublisher,
) -> (AckHandle, tokio::task::JoinHandle<()>) {
    let (sender, mut receiver) = mpsc::channel::<Ack>(ACK_QUEUE_CAPACITY);
    let task = tokio::spawn(async move {
        while let Some(ack) = receiver.recv().await {
            let (status, code) = match ack.outcome {
                AckOutcome::Immediate { status, code } => (status, code),
                AckOutcome::Ingest(result) => match result.await {
                    Ok(Ok(crate::core::telemetry::TelemetryInsertResult::Stored)) => {
                        ("stored", None)
                    }
                    Ok(Ok(crate::core::telemetry::TelemetryInsertResult::Duplicate)) => {
                        ("duplicate", None)
                    }
                    Ok(Ok(crate::core::telemetry::TelemetryInsertResult::Conflict)) => {
                        ("rejected", Some("event_id_conflict"))
                    }
                    Ok(Err(())) | Err(_) => ("retryable", Some("storage_unavailable")),
                },
            };
            let payload = serde_json::json!({
                "event_id": ack.event_id,
                "status": status,
                "received_at": ack.received_at,
                "code": code,
            });
            if let Err(error) = publisher
                .telemetry_ack(&ack.device_id, payload.to_string().as_bytes())
                .await
            {
                tracing::warn!(%error, device_id = %ack.device_id, "cannot publish telemetry ACK");
            }
        }
    });
    (AckHandle(sender), task)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn full_ack_queue_drops_new_ack_without_spawning_tasks() {
        let (sender, receiver) = mpsc::channel(1);
        let ack = AckHandle(sender);
        let dropped_before = Metrics::global().telemetry_ack_dropped_total.get();

        ack.send(
            "device".to_owned(),
            Some("event-1".to_owned()),
            "stored",
            1,
            None,
        );
        ack.send(
            "device".to_owned(),
            Some("event-2".to_owned()),
            "stored",
            1,
            None,
        );

        assert_eq!(receiver.len(), 1);
        assert_eq!(
            Metrics::global().telemetry_ack_dropped_total.get(),
            dropped_before + 1
        );
    }
}
