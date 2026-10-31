use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;

use crate::config::runtime::RuntimeConfigStore;
use crate::core::events::{EventBus, TelemetryCommitted};
use crate::observability::metrics::{IngestRejectReason, Metrics};

use super::{TelemetryInsertResult, TelemetryRepository, ValidatedTelemetry};

struct Work {
    device_id: String,
    event: ValidatedTelemetry,
    response: Option<oneshot::Sender<Result<TelemetryInsertResult, ()>>>,
}

/// Handle used to submit validated telemetry to the bounded ingest queue.
#[derive(Clone)]
pub struct IngestHandle(mpsc::Sender<Work>);

/// Completion receiver for a submitted telemetry event.
pub type IngestResult = oneshot::Receiver<Result<TelemetryInsertResult, ()>>;

/// Shutdown and readiness controls for the ingestion worker.
pub struct IngestWorkerControl {
    /// Stops accepting new queue items and drains items already accepted.
    pub cancellation: CancellationToken,
    /// Indicates whether database commits are currently healthy.
    pub readiness: Arc<AtomicBool>,
}

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

/// Starts one bounded queue and its single batch writer.
pub fn spawn_worker(
    repository: Arc<dyn TelemetryRepository>,
    events: EventBus,
    capacity: usize,
    max_batch_bytes: usize,
    runtime_config: RuntimeConfigStore,
    control: IngestWorkerControl,
) -> (IngestHandle, tokio::task::JoinHandle<()>) {
    let (sender, receiver) = mpsc::channel(capacity);
    let handle = IngestHandle(sender);
    let task = tokio::spawn(run_worker(
        repository,
        events,
        receiver,
        max_batch_bytes,
        runtime_config,
        control,
    ));
    (handle, task)
}

async fn run_worker(
    repository: Arc<dyn TelemetryRepository>,
    events: EventBus,
    mut receiver: mpsc::Receiver<Work>,
    max_batch_bytes: usize,
    runtime_config: RuntimeConfigStore,
    control: IngestWorkerControl,
) {
    let IngestWorkerControl {
        cancellation: cancel,
        readiness,
    } = control;
    let mut pending: Option<Work> = None;
    let mut draining = false;
    loop {
        let first = match pending.take() {
            Some(work) => Some(work),
            None if draining => receiver.recv().await,
            None => tokio::select! {
                biased;
                _ = cancel.cancelled() => {
                    receiver.close();
                    draining = true;
                    receiver.recv().await
                }
                work = receiver.recv() => work,
            },
        };
        let Some(first) = first else { break };
        let settings = runtime_config.load();
        let batch_size = settings.config.telemetry.batch_size as usize;
        let flush_interval = Duration::from_millis(settings.config.telemetry.flush_interval_ms);
        let mut batch = vec![first];
        let mut bytes = batch[0].event.raw_bytes;
        Metrics::global()
            .telemetry_queue_depth
            .set((receiver.max_capacity() - receiver.capacity()) as u64);
        let deadline = tokio::time::sleep(flush_interval);
        tokio::pin!(deadline);
        while batch.len() < batch_size {
            tokio::select! {
                biased;
                _ = cancel.cancelled(), if !draining => {
                    receiver.close();
                    draining = true;
                    break;
                }
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
            match repository.insert_batch(&rows).await {
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
            if result == TelemetryInsertResult::Stored {
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
    use std::sync::atomic::AtomicUsize;

    use serde_json::{Map, json};
    use tempfile::TempDir;
    use tokio::sync::{Notify, mpsc};

    use super::*;
    use crate::config;

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

    struct RecordingRepository {
        calls: mpsc::Sender<usize>,
        call_count: AtomicUsize,
        first_call_entered: Notify,
        release_first_call: Notify,
    }

    #[async_trait::async_trait]
    impl TelemetryRepository for RecordingRepository {
        async fn insert_batch(
            &self,
            events: &[(String, ValidatedTelemetry)],
        ) -> anyhow::Result<Vec<TelemetryInsertResult>> {
            self.calls.send(events.len()).await?;
            if self.call_count.fetch_add(1, Ordering::Relaxed) == 0 {
                self.first_call_entered.notify_one();
                self.release_first_call.notified().await;
            }
            Ok(vec![TelemetryInsertResult::Stored; events.len()])
        }
    }

    #[tokio::test]
    async fn worker_reads_batch_and_flush_settings_at_each_batch() {
        let dir = TempDir::new().unwrap();
        let config_path = dir.path().join("conf.toml");
        std::fs::write(
            &config_path,
            "config_version = 1\n\
             [server]\n\
             data_dir = \"./data\"\n\
             [database]\n\
             url = \"sqlite://./data/test.db\"\n\
             [telemetry]\n\
             batch_size = 1\n\
             flush_interval_ms = 1000\n",
        )
        .unwrap();
        let loaded = config::load(&config_path).unwrap();
        let (calls, mut call_results) = mpsc::channel(4);
        let repository = Arc::new(RecordingRepository {
            calls,
            call_count: AtomicUsize::new(0),
            first_call_entered: Notify::new(),
            release_first_call: Notify::new(),
        });
        let (handle, task) = spawn_worker(
            repository.clone(),
            EventBus::new(),
            8,
            65_536,
            loaded.runtime_config.clone(),
            IngestWorkerControl {
                cancellation: CancellationToken::new(),
                readiness: Arc::new(AtomicBool::new(true)),
            },
        );

        handle.try_submit("device".to_owned(), event()).unwrap();
        repository.first_call_entered.notified().await;
        loaded
            .runtime_config
            .update(
                0,
                &json!({"telemetry":{"batch_size":2,"flush_interval_ms":20}}),
            )
            .await
            .unwrap();
        repository.release_first_call.notify_one();
        assert_eq!(call_results.recv().await, Some(1));

        handle.try_submit("device".to_owned(), event()).unwrap();
        handle.try_submit("device".to_owned(), event()).unwrap();
        assert_eq!(call_results.recv().await, Some(2));

        handle.try_submit("device".to_owned(), event()).unwrap();
        let flushed = tokio::time::timeout(Duration::from_millis(500), call_results.recv())
            .await
            .expect("updated flush interval should apply to the next batch");
        assert_eq!(flushed, Some(1));
        drop(handle);
        task.abort();
        let _ = task.await;
    }
}
