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
pub struct IngestHandle {
    sender: mpsc::Sender<Work>,
    events: EventBus,
    queue_episode: Arc<std::sync::Mutex<()>>,
}

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
    pub(crate) fn event_bus(&self) -> EventBus {
        self.events.clone()
    }

    /// Queues without waiting, so the broker callback never blocks on ingest.
    pub fn try_submit(
        &self,
        device_id: String,
        event: ValidatedTelemetry,
    ) -> Result<IngestResult, IngestSubmitError> {
        let (response, receiver) = oneshot::channel();
        let Ok(_episode) = self.queue_episode.lock() else {
            tracing::error!("ingest queue episode lock is poisoned");
            return Err(IngestSubmitError::Closed);
        };
        match self.sender.try_send(Work {
            device_id,
            event,
            response: Some(response),
        }) {
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Full(_)) => {
                Metrics::global().reject_ingest(IngestRejectReason::QueueFull);
                self.events.publish_queue_full();
                return Err(IngestSubmitError::Full);
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                return Err(IngestSubmitError::Closed);
            }
        }
        Metrics::global().ingest_accepted_total.inc();
        Metrics::global()
            .telemetry_queue_depth
            .set((self.sender.max_capacity() - self.sender.capacity()) as u64);
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
    let queue_episode = Arc::new(std::sync::Mutex::new(()));
    let handle = IngestHandle {
        sender,
        events: events.clone(),
        queue_episode: Arc::clone(&queue_episode),
    };
    let task = tokio::spawn(run_worker(
        repository,
        events,
        receiver,
        max_batch_bytes,
        runtime_config,
        control,
        queue_episode,
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
    queue_episode: Arc<std::sync::Mutex<()>>,
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
                    if readiness.swap(false, Ordering::AcqRel) {
                        events.publish_storage_unavailable();
                    }
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
        reset_queue_full_if_drained(&receiver, &queue_episode, &events);
    }
}

fn reset_queue_full_if_drained(
    receiver: &mpsc::Receiver<Work>,
    queue_episode: &std::sync::Mutex<()>,
    events: &EventBus,
) {
    let Ok(_episode) = queue_episode.lock() else {
        return;
    };
    if receiver.capacity() == receiver.max_capacity() {
        events.reset_queue_full();
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
    fn queue_full_alert_coalesces_per_drain_and_counts_each_rejection() {
        let (sender, mut receiver) = mpsc::channel(2);
        let events = EventBus::new();
        let mut realtime = events.subscribe_realtime();
        let queue_episode = Arc::new(std::sync::Mutex::new(()));
        let ingest = IngestHandle {
            sender,
            events: events.clone(),
            queue_episode: Arc::clone(&queue_episode),
        };
        let rejected_before = Metrics::global().ingest_rejected_total.get();
        assert!(ingest.try_submit("device".to_owned(), event()).is_ok());
        assert!(ingest.try_submit("device".to_owned(), event()).is_ok());
        for _ in 0..4 {
            assert!(matches!(
                ingest.try_submit("device".to_owned(), event()),
                Err(IngestSubmitError::Full)
            ));
        }
        let crate::core::events::RealtimeEvent::System(system) = realtime.try_recv().unwrap()
        else {
            panic!("queue-full rejection should publish one system event");
        };
        assert_eq!(system.kind, "queue_full");
        assert!(realtime.try_recv().is_err());

        assert!(receiver.try_recv().is_ok());
        assert!(receiver.try_recv().is_ok());
        assert!(ingest.try_submit("device".to_owned(), event()).is_ok());
        assert!(ingest.try_submit("device".to_owned(), event()).is_ok());
        assert!(matches!(
            ingest.try_submit("device".to_owned(), event()),
            Err(IngestSubmitError::Full)
        ));
        assert!(
            realtime.try_recv().is_err(),
            "producers must not rearm the alert before the drain is synchronized"
        );

        assert!(receiver.try_recv().is_ok());
        assert!(receiver.try_recv().is_ok());
        reset_queue_full_if_drained(&receiver, &queue_episode, &events);
        assert!(ingest.try_submit("device".to_owned(), event()).is_ok());
        assert!(ingest.try_submit("device".to_owned(), event()).is_ok());
        assert!(matches!(
            ingest.try_submit("device".to_owned(), event()),
            Err(IngestSubmitError::Full)
        ));
        let crate::core::events::RealtimeEvent::System(system) = realtime.try_recv().unwrap()
        else {
            panic!("a drained queue should rearm one system event for the next episode");
        };
        assert_eq!(system.kind, "queue_full");
        assert!(realtime.try_recv().is_err());
        assert_eq!(
            Metrics::global().ingest_rejected_total.get(),
            rejected_before + 6,
            "the metric counts each rejection despite coalesced system events"
        );
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
