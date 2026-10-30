//! Broker-originated publishes to device sessions.

use std::sync::Arc;

use rmqtt::context::ServerContext;
use rmqtt::types::{CodecPublish, From, Id, Publish, QoS};
use tokio::sync::OwnedSemaphorePermit;
use tokio::sync::{Semaphore, mpsc};

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
    _permit: Option<OwnedSemaphorePermit>,
}

/// Non-blocking publisher interface backed by bounded ACK queues. Immediate ACKs bypass ingest waits,
/// but share the total capacity and can still be dropped when all permits are occupied.
#[derive(Clone)]
pub(crate) struct AckHandle {
    immediate: mpsc::Sender<Ack>,
    ingest: mpsc::Sender<Ack>,
    permits: Arc<Semaphore>,
}

impl AckHandle {
    pub(crate) fn send(
        &self,
        device_id: String,
        event_id: Option<String>,
        status: &'static str,
        received_at: i64,
        code: Option<&'static str>,
    ) {
        self.try_send(
            &self.immediate,
            Ack {
                device_id,
                event_id,
                received_at,
                outcome: AckOutcome::Immediate { status, code },
                _permit: None,
            },
        );
    }

    pub(crate) fn send_after_ingest(
        &self,
        device_id: String,
        event_id: String,
        received_at: i64,
        result: IngestResult,
    ) {
        self.try_send(
            &self.ingest,
            Ack {
                device_id,
                event_id: Some(event_id),
                received_at,
                outcome: AckOutcome::Ingest(result),
                _permit: None,
            },
        );
    }

    fn try_send(&self, sender: &mpsc::Sender<Ack>, ack: Ack) {
        let Ok(permit) = Arc::clone(&self.permits).try_acquire_owned() else {
            Metrics::global().telemetry_ack_dropped_total.inc();
            tracing::warn!("telemetry ACK queue full or closed; ACK dropped");
            return;
        };
        let mut ack = ack;
        ack._permit = Some(permit);
        if sender.try_send(ack).is_err() {
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
    // Immediate rejection ACKs must not wait behind ingest results during a DB outage.
    let (immediate_sender, immediate_receiver) = mpsc::channel::<Ack>(ACK_QUEUE_CAPACITY);
    let (ingest_sender, ingest_receiver) = mpsc::channel::<Ack>(ACK_QUEUE_CAPACITY);
    // Share one cap so the separate lanes do not double ACK memory.
    let permits = Arc::new(Semaphore::new(ACK_QUEUE_CAPACITY));
    let task = tokio::spawn(async move {
        tokio::join!(
            run_ack_worker(immediate_receiver, publisher.clone()),
            run_ack_worker(ingest_receiver, publisher),
        );
    });
    (
        AckHandle {
            immediate: immediate_sender,
            ingest: ingest_sender,
            permits,
        },
        task,
    )
}

async fn run_ack_worker(mut receiver: mpsc::Receiver<Ack>, publisher: ServerPublisher) {
    while let Some(ack) = receiver.recv().await {
        let (status, code) = match ack.outcome {
            AckOutcome::Immediate { status, code } => (status, code),
            AckOutcome::Ingest(result) => match result.await {
                Ok(Ok(crate::core::telemetry::TelemetryInsertResult::Stored)) => ("stored", None),
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
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn full_ack_queue_drops_new_ack_without_spawning_tasks() {
        let (sender, receiver) = mpsc::channel(1);
        let (ingest, ingest_receiver) = mpsc::channel(1);
        let permits = Arc::new(Semaphore::new(3));
        let ack = AckHandle {
            immediate: sender,
            ingest,
            permits,
        };
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
        let (_, first_result) = tokio::sync::oneshot::channel();
        ack.send_after_ingest("device".to_owned(), "event-3".to_owned(), 1, first_result);
        let (_, second_result) = tokio::sync::oneshot::channel();
        ack.send_after_ingest("device".to_owned(), "event-4".to_owned(), 1, second_result);

        assert_eq!(receiver.len(), 1);
        assert_eq!(ingest_receiver.len(), 1);
        assert_eq!(
            Metrics::global().telemetry_ack_dropped_total.get(),
            dropped_before + 2
        );
    }
}
