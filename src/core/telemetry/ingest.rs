use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use sqlx::SqlitePool;
use tokio::sync::{mpsc, oneshot};

use crate::core::events::{EventBus, TelemetryCommitted};
use crate::observability::metrics::{IngestRejectReason, Metrics};
use crate::storage::sqlite::telemetry::{self, InsertResult};

use super::ValidatedTelemetry;

struct Work {
    device_id: String,
    event: ValidatedTelemetry,
    response: Option<oneshot::Sender<Result<InsertResult, ()>>>,
}

/// Handle used to submit validated telemetry to the bounded ingest queue.
#[derive(Clone)]
pub struct IngestHandle(mpsc::Sender<Work>);

/// Completion receiver for a submitted telemetry event.
pub type IngestResult = oneshot::Receiver<Result<InsertResult, ()>>;

/// Why a telemetry event could not be submitted to the ingest queue.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IngestSubmitError {
    /// The bounded queue has no available capacity.
    Full,
    /// The ingest worker has stopped.
    Closed,
}

impl IngestHandle {
    /// Queues without waiting, so the broker callback never blocks on ingest.
    pub fn try_submit(
        &self,
        device_id: String,
        event: ValidatedTelemetry,
    ) -> Result<IngestResult, IngestSubmitError> {
        let (response, receiver) = oneshot::channel();
        self.0
            .try_send(Work {
                device_id,
                event,
                response: Some(response),
            })
            .map_err(|error| match error {
                mpsc::error::TrySendError::Full(_) => {
                    Metrics::global().reject_ingest(IngestRejectReason::QueueFull);
                    IngestSubmitError::Full
                }
                mpsc::error::TrySendError::Closed(_) => IngestSubmitError::Closed,
            })?;
        Metrics::global().ingest_accepted_total.inc();
        Metrics::global()
            .telemetry_queue_depth
            .set((self.0.max_capacity() - self.0.capacity()) as u64);
        Ok(receiver)
    }
}

/// Starts one bounded queue and its single SQLite batch writer.
pub fn spawn_worker(
    pool: SqlitePool,
    events: EventBus,
    capacity: usize,
    batch_size: usize,
    max_batch_bytes: usize,
    flush_interval: Duration,
    readiness: Arc<AtomicBool>,
) -> (IngestHandle, tokio::task::JoinHandle<()>) {
    let (sender, receiver) = mpsc::channel(capacity);
    let handle = IngestHandle(sender);
    let task = tokio::spawn(run_worker(
        pool,
        events,
        receiver,
        batch_size,
        max_batch_bytes,
        flush_interval,
        readiness,
    ));
    (handle, task)
}

async fn run_worker(
    pool: SqlitePool,
    events: EventBus,
    mut receiver: mpsc::Receiver<Work>,
    batch_size: usize,
    max_batch_bytes: usize,
    flush_interval: Duration,
    readiness: Arc<AtomicBool>,
) {
    let mut pending = None;
    loop {
        let first = match pending.take() {
            Some(work) => work,
            None => match receiver.recv().await {
                Some(work) => work,
                None => break,
            },
        };
        let mut batch = vec![first];
        let mut bytes = batch[0].event.raw_bytes;
        Metrics::global()
            .telemetry_queue_depth
            .set((receiver.max_capacity() - receiver.capacity()) as u64);
        let deadline = tokio::time::sleep(flush_interval);
        tokio::pin!(deadline);
        while batch.len() < batch_size {
            tokio::select! {
                _ = &mut deadline => break,
                work = receiver.recv() => match work {
                    Some(work) if bytes.saturating_add(work.event.raw_bytes) <= max_batch_bytes => {
                        bytes += work.event.raw_bytes;
                        batch.push(work);
                        Metrics::global().telemetry_queue_depth.set(
                            (receiver.max_capacity() - receiver.capacity()) as u64,
                        );
                    }
                    Some(work) => {
                        pending = Some(work);
                        Metrics::global().telemetry_queue_depth.set(
                            (receiver.max_capacity() - receiver.capacity()) as u64,
                        );
                        break;
                    }
                    None => break,
                }
            }
        }

        let rows: Vec<_> = batch
            .iter()
            .map(|work| (work.device_id.clone(), work.event.clone()))
            .collect();
        let started = std::time::Instant::now();
        let mut backoff = Duration::from_millis(100);
        let mut notified_storage_error = false;
        let results = loop {
            match telemetry::insert_batch(&pool, &rows).await {
                Ok(results) => break results,
                Err(error) => {
                    readiness.store(false, Ordering::Release);
                    Metrics::global().telemetry_commit_errors_total.inc();
                    tracing::error!(%error, "telemetry batch commit failed; retrying");
                    if !notified_storage_error {
                        for work in &mut batch {
                            if let Some(response) = work.response.take() {
                                let _ = response.send(Err(()));
                            }
                        }
                        notified_storage_error = true;
                    }
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(Duration::from_secs(5));
                }
            }
        };
        readiness.store(true, Ordering::Release);
        Metrics::global().observe_telemetry_commit(
            started.elapsed().as_micros().min(u128::from(u64::MAX)) as u64,
        );
        for (work, result) in batch.into_iter().zip(results) {
            if result == InsertResult::Stored {
                events.publish(TelemetryCommitted::from_telemetry(
                    &work.device_id,
                    &work.event,
                ));
            }
            if let Some(response) = work.response {
                let _ = response.send(Ok(result));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_json::{Map, json};

    use super::*;

    fn event() -> ValidatedTelemetry {
        ValidatedTelemetry {
            event_id: "evt-1".to_owned(),
            ts: 1,
            received_at: 1,
            values: Map::from_iter([("x".to_owned(), json!(1))]),
            payload_sha256: [0; 32],
            raw_bytes: 1,
        }
    }

    #[test]
    fn queue_rejects_without_waiting_when_full() {
        let (sender, _receiver) = mpsc::channel(1);
        let ingest = IngestHandle(sender);
        assert!(ingest.try_submit("device".to_owned(), event()).is_ok());
        assert!(matches!(
            ingest.try_submit("device".to_owned(), event()),
            Err(IngestSubmitError::Full)
        ));
    }
}
