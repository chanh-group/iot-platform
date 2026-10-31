//! Bounded SQL-backed command delivery and recovery loop.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use serde::Serialize;
use tokio::sync::{Notify, mpsc};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::config::schema::CommandsConfig;
use crate::core::auth;
use crate::core::device_channel::DeviceChannel;
use crate::core::events::EventBus;
use crate::observability::metrics::Metrics;

use super::{Command, CommandRepository, CommandStatus, PendingCommandAcks, expire_due};

pub const DISPATCH_BATCH_SIZE: i64 = 100;

/// Bounded reconnect events plus the coalesced signal for queue overflow.
pub struct CommandWakeReceiver {
    receiver: mpsc::Receiver<String>,
    overflow: Arc<Notify>,
    events: EventBus,
}

impl CommandWakeReceiver {
    #[must_use]
    pub fn new(receiver: mpsc::Receiver<String>, overflow: Arc<Notify>) -> Self {
        Self::with_events(receiver, overflow, EventBus::new())
    }

    /// Builds the wake receiver with the shared real-time event bus.
    pub fn with_events(
        receiver: mpsc::Receiver<String>,
        overflow: Arc<Notify>,
        events: EventBus,
    ) -> Self {
        Self {
            receiver,
            overflow,
            events,
        }
    }
}

#[derive(Serialize)]
struct CommandMessage<'a> {
    command_id: &'a str,
    name: &'a str,
    params: &'a serde_json::Value,
    created_at: i64,
    expires_at: i64,
}

/// Drives due, bounded batches; SQL remains the source of truth after restart.
pub struct CommandDispatcher {
    repository: Arc<dyn CommandRepository>,
    channel: Arc<dyn DeviceChannel>,
    config: CommandsConfig,
    pending_acks: PendingCommandAcks,
    wakes: CommandWakeReceiver,
    events: EventBus,
}

impl CommandDispatcher {
    #[must_use]
    pub fn new(
        repository: Arc<dyn CommandRepository>,
        channel: Arc<dyn DeviceChannel>,
        config: CommandsConfig,
        pending_acks: PendingCommandAcks,
        wakes: CommandWakeReceiver,
    ) -> Self {
        let events = wakes.events.clone();
        Self {
            repository,
            channel,
            config,
            pending_acks,
            wakes,
            events,
        }
    }

    /// Processes one bounded expiry and due-command batch at the supplied time.
    pub async fn run_once(&self, now: i64) -> anyhow::Result<bool> {
        let mut wrote = expire_due(
            self.repository.as_ref(),
            now,
            DISPATCH_BATCH_SIZE,
            &self.pending_acks,
            &self.events,
        )
        .await?
            > 0;
        let commands = self.repository.due(now, DISPATCH_BATCH_SIZE).await?;
        for command in commands {
            wrote |= self.dispatch(command, now).await?;
        }
        Ok(wrote)
    }

    async fn dispatch(&self, command: Command, now: i64) -> anyhow::Result<bool> {
        match self.channel.is_online(&command.device_id).await {
            Ok(true) => {}
            Ok(false) | Err(_) => {
                let next_attempt_at = now.saturating_add(
                    i64::try_from(
                        self.config
                            .dispatch_interval_ms
                            .max(self.config.retry_initial_ms),
                    )
                    .unwrap_or(i64::MAX),
                );
                self.repository
                    .defer_offline(&command.id, now, next_attempt_at)
                    .await?;
                return Ok(true);
            }
        }

        let payload = serde_json::to_vec(&CommandMessage {
            command_id: &command.id,
            name: &command.name,
            params: &command.params,
            created_at: command.created_at,
            expires_at: command.expires_at,
        })?;
        let topic = format!("devices/{}/commands", command.device_id);
        if !self
            .repository
            .can_dispatch(&command.id, auth::now_ms())
            .await?
        {
            return Ok(false);
        }
        let attempted_at = auth::now_ms();
        let published = match self
            .channel
            .publish_if_online(&command.device_id, &topic, &payload)
            .await
        {
            Ok(()) => true,
            Err(_) => {
                Metrics::global().command_publish_errors_total.inc();
                tracing::warn!("command publish failed; command remains recoverable");
                false
            }
        };
        let finished_at = auth::now_ms();
        let next_attempt_at = finished_at.saturating_add(retry_delay_ms(
            self.config.retry_initial_ms,
            self.config.retry_max_ms,
            command.attempts,
        ));
        let updated = self
            .repository
            .record_dispatch_attempt(
                &command.id,
                attempted_at,
                finished_at,
                next_attempt_at,
                published,
            )
            .await?;
        if command.status == CommandStatus::Pending
            && let Some(updated) = updated
            && updated.status == CommandStatus::Dispatched
        {
            self.events.publish_command(
                &updated.id,
                &updated.device_id,
                updated.status,
                updated.completed_at,
                updated.error.as_deref(),
            );
        }
        if command.status != CommandStatus::Pending && !published {
            tracing::debug!("command resend will retry with the same command ID");
        }
        Ok(true)
    }

    /// Runs immediately on startup, then polls at the configured interval.
    pub async fn run(mut self, cancel: CancellationToken, readiness: Arc<AtomicBool>) {
        readiness.store(true, Ordering::Release);
        let _readiness = CommandReadiness(Arc::clone(&readiness));
        let interval = Duration::from_millis(self.config.dispatch_interval_ms);
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut wakes_open = true;
        loop {
            let event = tokio::select! {
                _ = cancel.cancelled() => break,
                _ = self.wakes.overflow.notified() => DispatcherEvent::WakeOverflow,
                device_id = self.wakes.receiver.recv(), if wakes_open => DispatcherEvent::Wake(device_id),
                _ = ticker.tick() => DispatcherEvent::Tick,
            };
            match event {
                DispatcherEvent::Tick => self.run_tick(&readiness).await,
                DispatcherEvent::Wake(Some(device_id)) => {
                    self.wake_device(&device_id, &readiness).await;
                }
                DispatcherEvent::Wake(None) => wakes_open = false,
                DispatcherEvent::WakeOverflow => self.wake_overflow(&readiness).await,
            }
        }
    }

    async fn wake_device(&self, device_id: &str, readiness: &AtomicBool) {
        let now = auth::now_ms();
        match self.repository.wake_device(device_id, now).await {
            Ok(woken) => {
                if woken > 0 {
                    readiness.store(true, Ordering::Release);
                }
                self.run_tick(readiness).await;
            }
            Err(error) => {
                self.storage_failed(error, readiness);
            }
        }
    }

    async fn wake_overflow(&self, readiness: &AtomicBool) {
        let scheduled_at = auth::now_ms();
        let mut woke_any = false;
        loop {
            match self
                .repository
                .wake_all(scheduled_at, DISPATCH_BATCH_SIZE)
                .await
            {
                Ok(0) => break,
                Ok(_) => woke_any = true,
                Err(error) => {
                    self.storage_failed(error, readiness);
                    return;
                }
            }
        }
        if !woke_any {
            return;
        }
        readiness.store(true, Ordering::Release);
        loop {
            let expiry_now = auth::now_ms();
            match expire_due(
                self.repository.as_ref(),
                expiry_now,
                DISPATCH_BATCH_SIZE,
                &self.pending_acks,
                &self.events,
            )
            .await
            {
                Ok(expired) if expired > 0 => readiness.store(true, Ordering::Release),
                Ok(_) => {}
                Err(error) => {
                    self.storage_failed(error, readiness);
                    return;
                }
            }
            let now = auth::now_ms();
            match self
                .repository
                .due_at(scheduled_at, now, DISPATCH_BATCH_SIZE)
                .await
            {
                Ok(commands) => {
                    let batch_len = commands.len();
                    for command in commands {
                        if let Err(error) = self.dispatch(command, now).await {
                            self.storage_failed(error, readiness);
                            return;
                        }
                    }
                    if batch_len < usize::try_from(DISPATCH_BATCH_SIZE).unwrap_or(100) {
                        break;
                    }
                }
                Err(error) => {
                    self.storage_failed(error, readiness);
                    return;
                }
            }
        }
    }

    async fn run_tick(&self, readiness: &AtomicBool) {
        match self.run_once(auth::now_ms()).await {
            Ok(true) => readiness.store(true, Ordering::Release),
            Ok(false) => {}
            Err(error) => self.storage_failed(error, readiness),
        }
    }

    fn storage_failed(&self, error: impl std::fmt::Display, readiness: &AtomicBool) {
        if readiness.swap(false, Ordering::AcqRel) {
            self.events.publish_storage_unavailable();
        }
        Metrics::global().command_storage_errors_total.inc();
        tracing::warn!(%error, "command dispatcher storage operation failed; retrying");
    }
}

enum DispatcherEvent {
    Tick,
    Wake(Option<String>),
    WakeOverflow,
}

struct CommandReadiness(Arc<AtomicBool>);

impl Drop for CommandReadiness {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}

/// Spawns the supervised dispatcher task.
pub fn spawn_dispatcher(
    repository: Arc<dyn CommandRepository>,
    channel: Arc<dyn DeviceChannel>,
    config: CommandsConfig,
    pending_acks: PendingCommandAcks,
    wakes: CommandWakeReceiver,
    cancel: CancellationToken,
    readiness: Arc<AtomicBool>,
) -> JoinHandle<()> {
    tokio::spawn(
        CommandDispatcher::new(repository, channel, config, pending_acks, wakes)
            .run(cancel, readiness),
    )
}

fn retry_delay_ms(initial_ms: u64, maximum_ms: u64, attempts: i64) -> i64 {
    let exponent = u32::try_from(attempts.max(0)).unwrap_or(u32::MAX).min(63);
    let delay = initial_ms
        .checked_mul(1_u64 << exponent)
        .unwrap_or(maximum_ms)
        .min(maximum_ms);
    i64::try_from(delay).unwrap_or(i64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::device_channel::DeviceChannelError;
    use sqlx::SqlitePool;

    #[test]
    fn retries_use_capped_exponential_backoff() {
        assert_eq!(retry_delay_ms(100, 1_000, 0), 100);
        assert_eq!(retry_delay_ms(100, 1_000, 2), 400);
        assert_eq!(retry_delay_ms(100, 1_000, 20), 1_000);
    }

    struct OfflineChannel;

    #[async_trait::async_trait]
    impl DeviceChannel for OfflineChannel {
        async fn publish(
            &self,
            _device_id: &str,
            _topic: &str,
            _payload: &[u8],
        ) -> Result<(), DeviceChannelError> {
            Err(DeviceChannelError::Unavailable)
        }

        async fn kick(&self, _device_id: &str) -> Result<(), DeviceChannelError> {
            Ok(())
        }

        async fn is_online(&self, _device_id: &str) -> Result<bool, DeviceChannelError> {
            Ok(false)
        }
    }

    #[tokio::test]
    async fn cancellation_joins_dispatcher_and_clears_readiness() {
        let repository = Arc::new(
            crate::storage::sqlite::commands::SqliteCommandRepository::new(
                SqlitePool::connect_lazy("sqlite::memory:").unwrap(),
            ),
        );
        let readiness = Arc::new(AtomicBool::new(false));
        let cancel = CancellationToken::new();
        let task = spawn_dispatcher(
            repository,
            Arc::new(OfflineChannel),
            CommandsConfig::default(),
            PendingCommandAcks::default(),
            closed_wakes(),
            cancel.clone(),
            Arc::clone(&readiness),
        );
        cancel.cancel();
        tokio::time::timeout(Duration::from_secs(1), task)
            .await
            .expect("dispatcher should stop on cancellation")
            .expect("dispatcher task should not panic");
        assert!(!readiness.load(Ordering::Acquire));
    }

    fn closed_wakes() -> CommandWakeReceiver {
        let (sender, receiver) = mpsc::channel(1);
        drop(sender);
        CommandWakeReceiver::new(receiver, Arc::new(Notify::new()))
    }
}
