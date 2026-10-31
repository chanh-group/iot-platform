//! Lifecycle wrapper around RMQTT's in-process broker.

use std::collections::{HashSet, VecDeque};
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use rmqtt::context::ServerContext;
use rmqtt::hook::Type;
use rmqtt::server::MqttServer as RmqttServer;
use sqlx::SqlitePool;
use tokio::sync::mpsc::OwnedPermit;
use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore, mpsc, oneshot};
use tokio::task::{JoinHandle, JoinSet};
use tokio_util::sync::CancellationToken;

use crate::config::schema::MqttConfig;
use crate::core::command::{self, CommandWakeReceiver, PendingCommandAcks};
use crate::core::device_channel::DeviceChannel;
use crate::core::presence::Presence;
use crate::core::telemetry::{IngestHandle, TelemetryPayload};
use crate::observability::metrics::Metrics;
use crate::storage::sqlite::attributes as attribute_storage;
use crate::storage::sqlite::commands::SqliteCommandRepository;

use super::acl::AclHooks;
use super::auth::AuthHooks;
use super::config;
use super::presence::{ConnectionLimiter, DeviceAdmission, DeviceAdmissionGuard, PresenceHooks};
use super::retained::BudgetedRetainStorage;
use super::server_publish::{ServerPublisher, spawn_ack_worker};

/// Running RMQTT listener and the channel used by core services.
pub struct MqttServer {
    address: std::net::SocketAddr,
    readiness: Arc<AtomicBool>,
    connections: Arc<ConnectionLimiter>,
    context: ServerContext,
    publisher: ServerPublisher,
    device_kicks: DeviceKickQueue,
    task: Option<JoinHandle<anyhow::Result<()>>>,
    kick_task: JoinHandle<()>,
    ack_task: Option<JoinHandle<()>>,
    ack_cancel: Option<CancellationToken>,
    command_ack_task: Option<JoinHandle<()>>,
    command_ack_cancel: CancellationToken,
    command_ack_pending: PendingCommandAcks,
    command_wakes: Option<mpsc::Receiver<String>>,
    command_wake_overflow: Arc<Notify>,
    attributes_task: Option<JoinHandle<()>>,
}

/// Reports fatal MQTT failures through the synchronized readiness path.
#[derive(Clone)]
pub struct MqttFailure(Arc<ConnectionLimiter>);

impl MqttFailure {
    /// Marks MQTT unavailable and signals the supervisor to stop the listener.
    pub fn report_failure(&self, error: impl std::fmt::Display) {
        self.0.report_failure(error);
    }
}

/// HTTP-facing controls backed by the running MQTT listener.
#[derive(Clone)]
pub struct MqttControl {
    pub(crate) failure: MqttFailure,
    pub(crate) device_admission: DeviceAdmission,
    pub(crate) device_kicks: DeviceKickQueue,
}

const DEVICE_KICK_QUEUE_CAPACITY: usize = 32;
const COMMAND_WAKE_QUEUE_CAPACITY: usize = 4_096;
const DEVICE_KICK_CONCURRENCY: usize = 8;
const DEVICE_KICK_ATTEMPTS: usize = 3;
const DEVICE_KICK_TIMEOUT: Duration = Duration::from_secs(3);

#[derive(Clone)]
pub(crate) struct DeviceKickQueue {
    sender: mpsc::Sender<QueuedDeviceKick>,
    slots: Arc<Semaphore>,
}

pub(crate) struct DeviceKickPermit {
    sender: OwnedPermit<QueuedDeviceKick>,
    slot: OwnedSemaphorePermit,
}

struct QueuedDeviceKick {
    job: DeviceKickJob,
    _slot: OwnedSemaphorePermit,
}

pub(crate) enum DeviceKickRecovery {
    Disable,
    Rotate { previous_credential_id: String },
    Presence,
}

struct DeviceKickJob {
    device_id: String,
    admission: DeviceAdmissionGuard,
    committed: oneshot::Receiver<bool>,
    recovery: DeviceKickRecovery,
    completion: oneshot::Sender<Result<(), ()>>,
}

impl DeviceKickQueue {
    pub(crate) fn try_reserve(&self) -> Result<DeviceKickPermit, ()> {
        let slot = Arc::clone(&self.slots)
            .try_acquire_owned()
            .map_err(|_| ())?;
        let sender = self.sender.clone().try_reserve_owned().map_err(|_| ())?;
        Ok(DeviceKickPermit { sender, slot })
    }

    pub(crate) async fn reserve(&self) -> Result<DeviceKickPermit, ()> {
        let slot = Arc::clone(&self.slots)
            .acquire_owned()
            .await
            .map_err(|_| ())?;
        let sender = self.sender.clone().reserve_owned().await.map_err(|_| ())?;
        Ok(DeviceKickPermit { sender, slot })
    }
}

impl DeviceKickPermit {
    pub(crate) fn enqueue(
        self,
        device_id: String,
        admission: DeviceAdmissionGuard,
        committed: oneshot::Receiver<bool>,
        recovery: DeviceKickRecovery,
    ) -> oneshot::Receiver<Result<(), ()>> {
        let (completion, result) = oneshot::channel();
        self.sender.send(QueuedDeviceKick {
            job: DeviceKickJob {
                device_id,
                admission,
                committed,
                recovery,
                completion,
            },
            _slot: self.slot,
        });
        result
    }
}

pub(crate) fn spawn_device_kick_worker(
    pool: SqlitePool,
    primary: Arc<dyn DeviceChannel>,
    emergency: Arc<dyn DeviceChannel>,
    failure: MqttFailure,
) -> (DeviceKickQueue, JoinHandle<()>) {
    let (sender, mut receiver) = mpsc::channel(DEVICE_KICK_QUEUE_CAPACITY);
    let slots = Arc::new(Semaphore::new(DEVICE_KICK_QUEUE_CAPACITY));
    let task = tokio::spawn(async move {
        let mut active = JoinSet::new();
        let mut pending = VecDeque::new();
        let mut active_devices = HashSet::new();
        let mut receiver_closed = false;

        loop {
            while active.len() < DEVICE_KICK_CONCURRENCY {
                let mut pending_devices = HashSet::new();
                let Some(index) = pending.iter().position(|queued: &QueuedDeviceKick| {
                    !active_devices.contains(&queued.job.device_id)
                        && pending_devices.insert(queued.job.device_id.clone())
                }) else {
                    break;
                };
                let Some(QueuedDeviceKick { job, _slot }) = pending.remove(index) else {
                    break;
                };
                drop(_slot);
                let device_id = job.device_id.clone();
                active_devices.insert(device_id.clone());
                let pool = pool.clone();
                let primary = Arc::clone(&primary);
                let emergency = Arc::clone(&emergency);
                let failure = failure.clone();
                active.spawn(async move {
                    process_device_kick_job(pool, primary, emergency, failure, job).await;
                    device_id
                });
            }

            if receiver_closed && pending.is_empty() && active.is_empty() {
                break;
            }

            tokio::select! {
                result = active.join_next(), if !active.is_empty() => {
                    match result {
                        Some(Ok(device_id)) => { active_devices.remove(&device_id); }
                        Some(Err(error)) => {
                            failure.report_failure(error);
                            break;
                        }
                        None => {}
                    }
                }
                job = receiver.recv(), if !receiver_closed && pending.len() < DEVICE_KICK_QUEUE_CAPACITY => {
                    match job {
                        Some(job) => pending.push_back(job),
                        None => receiver_closed = true,
                    }
                }
            }
        }
    });
    (DeviceKickQueue { sender, slots }, task)
}

async fn process_device_kick_job(
    pool: SqlitePool,
    primary: Arc<dyn DeviceChannel>,
    emergency: Arc<dyn DeviceChannel>,
    failure: MqttFailure,
    job: DeviceKickJob,
) {
    let DeviceKickJob {
        device_id,
        admission,
        committed,
        recovery,
        completion,
    } = job;
    let committed = committed.await;
    let mut fatal_failure = false;
    let mut state_unknown = false;
    let (recovered_kick, enabled) = match recover_device_kick(&pool, &device_id, &recovery).await {
        Ok(state) => state,
        Err(error) => {
            tracing::error!(%error, "cannot determine whether a device mutation committed");
            let _ = admission.fail_closed(error);
            fatal_failure = true;
            state_unknown = true;
            (true, false)
        }
    };
    if !state_unknown
        && (!enabled || matches!(recovery, DeviceKickRecovery::Presence))
        && let Err(error) = admission.set_device_enabled(false)
    {
        failure.report_failure(error);
        fatal_failure = true;
    }
    let should_kick = committed.unwrap_or(recovered_kick);
    if !should_kick {
        drop(admission);
        let result = if fatal_failure { Err(()) } else { Ok(()) };
        let _ = completion.send(result);
        return;
    }

    if kick_with_retries(primary.as_ref(), &device_id).await {
        drop(admission);
        let result = if fatal_failure { Err(()) } else { Ok(()) };
        let _ = completion.send(result);
        return;
    }

    Metrics::global().mqtt_kick_failures_total.inc();
    if !kick_with_retries(emergency.as_ref(), &device_id).await {
        tracing::error!("emergency MQTT session kick also failed");
    }
    let _ = admission.fail_closed("device session kick failed after retries");
    drop(admission);
    let _ = completion.send(Err(()));
}

async fn recover_device_kick(
    pool: &SqlitePool,
    device_id: &str,
    recovery: &DeviceKickRecovery,
) -> Result<(bool, bool), sqlx::Error> {
    let device = sqlx::query_as::<_, (bool, Option<String>)>(
        "SELECT d.enabled, c.id
         FROM devices d
         LEFT JOIN device_credentials c ON c.device_id = d.id AND c.status = 'active'
         WHERE d.id = ?",
    )
    .bind(device_id)
    .fetch_optional(pool)
    .await?;
    let Some((enabled, active_credential_id)) = device else {
        return Ok((true, false));
    };
    let should_kick = match recovery {
        DeviceKickRecovery::Disable => !enabled,
        DeviceKickRecovery::Rotate {
            previous_credential_id,
        } => active_credential_id.as_deref() != Some(previous_credential_id),
        DeviceKickRecovery::Presence => true,
    };
    Ok((should_kick, enabled))
}

async fn kick_with_retries(channel: &dyn DeviceChannel, device_id: &str) -> bool {
    for attempt in 0..DEVICE_KICK_ATTEMPTS {
        match tokio::time::timeout(DEVICE_KICK_TIMEOUT, channel.kick(device_id)).await {
            Ok(Ok(())) => return true,
            Ok(Err(error)) => {
                tracing::warn!(%error, attempt = attempt + 1, "MQTT session kick failed")
            }
            Err(_) => tracing::warn!(attempt = attempt + 1, "MQTT session kick timed out"),
        }
        if attempt + 1 < DEVICE_KICK_ATTEMPTS {
            tokio::time::sleep(Duration::from_millis(50 * (attempt as u64 + 1))).await;
        }
    }
    false
}

struct ListenerStoppedGuard(Arc<ConnectionLimiter>);

impl Drop for ListenerStoppedGuard {
    fn drop(&mut self) {
        self.0.listener_stopped();
    }
}

impl MqttServer {
    /// Returns the failure signal and device operations for HTTP handlers.
    #[must_use]
    pub fn control(&self) -> MqttControl {
        MqttControl {
            failure: MqttFailure(Arc::clone(&self.connections)),
            device_admission: self.connections.admission(),
            device_kicks: self.device_kicks.clone(),
        }
    }

    /// Starts RMQTT in the current process and reports its bound address.
    ///
    /// Auth and presence hooks are registered before the listener runs.
    pub async fn start(
        config: &MqttConfig,
        readiness: Arc<AtomicBool>,
        pool: SqlitePool,
        presence: Arc<Presence>,
    ) -> anyhow::Result<Self> {
        Self::start_inner(config, readiness, pool, presence, None, None, None).await
    }

    /// Starts the broker with bounded telemetry ingestion enabled.
    pub async fn start_with_ingest(
        config: &MqttConfig,
        readiness: Arc<AtomicBool>,
        pool: SqlitePool,
        presence: Arc<Presence>,
        ingest: IngestHandle,
        limits: TelemetryPayload,
        attribute_queue_capacity: usize,
    ) -> anyhow::Result<Self> {
        Self::start_inner(
            config,
            readiness,
            pool,
            presence,
            Some((ingest, limits, attribute_queue_capacity)),
            None,
            None,
        )
        .await
    }

    #[cfg(test)]
    async fn start_with_test_kicker(
        config: &MqttConfig,
        readiness: Arc<AtomicBool>,
        pool: SqlitePool,
        presence: Arc<Presence>,
        kicker: Arc<dyn DeviceChannel>,
    ) -> anyhow::Result<Self> {
        Self::start_inner(config, readiness, pool, presence, None, Some(kicker), None).await
    }

    #[cfg(test)]
    async fn start_with_test_kickers(
        config: &MqttConfig,
        readiness: Arc<AtomicBool>,
        pool: SqlitePool,
        presence: Arc<Presence>,
        primary: Arc<dyn DeviceChannel>,
        emergency: Arc<dyn DeviceChannel>,
    ) -> anyhow::Result<Self> {
        Self::start_inner(
            config,
            readiness,
            pool,
            presence,
            None,
            Some(primary),
            Some(emergency),
        )
        .await
    }

    async fn start_inner(
        config: &MqttConfig,
        readiness: Arc<AtomicBool>,
        pool: SqlitePool,
        presence: Arc<Presence>,
        ingest: Option<(IngestHandle, TelemetryPayload, usize)>,
        test_kicker: Option<Arc<dyn DeviceChannel>>,
        test_emergency_kicker: Option<Arc<dyn DeviceChannel>>,
    ) -> anyhow::Result<Self> {
        let listener = config::listener(config)?;
        let address = listener.local_addr()?;
        let max_connections = usize::try_from(config.max_connections)
            .map_err(|_| anyhow::anyhow!("mqtt.max_connections does not fit usize"))?;
        let max_topic_levels = usize::try_from(config.max_topic_levels)
            .map_err(|_| anyhow::anyhow!("mqtt.max_topic_levels does not fit usize"))?;
        let presence_failure = CancellationToken::new();
        let readiness_transition = Arc::new(Mutex::new(()));
        let connections = Arc::new(ConnectionLimiter::new(
            max_connections,
            Arc::clone(&readiness),
            readiness_transition,
            presence_failure.clone(),
        ));
        let context = ServerContext::new().busy_check_enable(false).build().await;
        let max_retained_messages = usize::try_from(config.max_retained_messages)
            .map_err(|_| anyhow::anyhow!("mqtt.max_retained_messages does not fit usize"))?;
        let max_retained_bytes = usize::try_from(config.max_retained_bytes)
            .map_err(|_| anyhow::anyhow!("mqtt.max_retained_bytes does not fit usize"))?;
        *context.extends.retain_mut().await = Box::new(BudgetedRetainStorage::new(
            max_retained_messages,
            max_retained_bytes,
        ));
        let retained_context = context.clone();
        let publisher = ServerPublisher::new(
            context.clone(),
            Arc::clone(&presence),
            Arc::clone(&connections),
        );
        let command_repository = Arc::new(SqliteCommandRepository::new(pool.clone()));
        let command_ack_cancel = CancellationToken::new();
        let (command_ack, command_ack_task) =
            command::spawn_ack_worker(command_repository, command_ack_cancel.clone());
        let command_ack_pending = command_ack.pending();
        let (command_wake_sender, command_wakes) = mpsc::channel(COMMAND_WAKE_QUEUE_CAPACITY);
        let command_wake_overflow = Arc::new(Notify::new());
        let failure = MqttFailure(Arc::clone(&connections));
        let (device_kicks, kick_task) = spawn_device_kick_worker(
            pool.clone(),
            test_kicker.unwrap_or_else(|| Arc::new(publisher.clone())),
            test_emergency_kicker.unwrap_or_else(|| Arc::new(publisher.clone())),
            failure,
        );

        let register = context.extends.hook_mgr().register();
        register
            .add_priority(
                Type::ClientAuthenticate,
                10,
                Box::new(AuthHooks::new(pool.clone(), Arc::clone(&connections))),
            )
            .await;
        let (acl_hooks, ack_task, ack_cancel, attributes_task) = match ingest {
            Some((ingest, limits, attribute_queue_capacity)) => {
                let (attributes, attributes_task) =
                    attribute_storage::spawn_worker(pool.clone(), attribute_queue_capacity);
                let ack_cancel = CancellationToken::new();
                let (ack, task) = spawn_ack_worker(publisher.clone(), ack_cancel.clone());
                (
                    AclHooks::with_ingest(
                        Arc::clone(&connections),
                        ingest,
                        attributes,
                        limits,
                        ack,
                        command_ack.clone(),
                        max_topic_levels,
                    ),
                    Some(task),
                    Some(ack_cancel),
                    Some(attributes_task),
                )
            }
            None => (
                AclHooks::with_command_ack(Arc::clone(&connections), command_ack, max_topic_levels),
                None,
                None,
                None,
            ),
        };
        register
            .add_priority(Type::MessagePublishCheckAcl, 10, Box::new(acl_hooks))
            .await;
        register
            .add_priority(
                Type::ClientSubscribeCheckAcl,
                10,
                Box::new(AclHooks::new(Arc::clone(&connections), max_topic_levels)),
            )
            .await;
        let presence_hooks = PresenceHooks::new(
            pool.clone(),
            presence,
            Arc::clone(&connections),
            device_kicks.clone(),
            command_wake_sender,
            Arc::clone(&command_wake_overflow),
        );
        register
            .add_priority(Type::ClientConnected, 10, Box::new(presence_hooks.clone()))
            .await;
        register
            .add_priority(
                Type::ClientDisconnected,
                10,
                Box::new(presence_hooks.clone()),
            )
            .await;
        register
            .add_priority(Type::SessionSubscribed, 10, Box::new(presence_hooks))
            .await;
        register.start().await;

        let task_connections = Arc::clone(&connections);
        let task = tokio::spawn(async move {
            let _stopped = ListenerStoppedGuard(task_connections);
            RmqttServer::new(context)
                .listener(listener)
                .build()
                .run()
                .await
        });

        connections.publish_ready();
        Ok(Self {
            address,
            readiness,
            connections,
            context: retained_context,
            publisher,
            device_kicks,
            task: Some(task),
            kick_task,
            ack_task,
            ack_cancel,
            command_ack_task: Some(command_ack_task),
            command_ack_cancel,
            command_ack_pending,
            command_wakes: Some(command_wakes),
            command_wake_overflow,
            attributes_task,
        })
    }

    /// Address actually bound by RMQTT, including a dynamically assigned port.
    pub fn local_addr(&self) -> std::net::SocketAddr {
        self.address
    }

    /// Returns the broker-backed channel used by the command dispatcher.
    #[must_use]
    pub fn device_channel(&self) -> Arc<dyn DeviceChannel> {
        Arc::new(self.publisher.clone())
    }

    /// Returns the pending ACK count shared with the dispatcher expiry sweep.
    #[must_use]
    pub fn pending_command_acks(&self) -> PendingCommandAcks {
        self.command_ack_pending.clone()
    }

    /// Transfers the bounded device-reconnect wake queue to the dispatcher.
    pub fn take_command_wakes(&mut self) -> CommandWakeReceiver {
        let wakes = self.command_wakes.take().unwrap_or_else(|| {
            let (sender, receiver) = mpsc::channel(1);
            drop(sender);
            receiver
        });
        CommandWakeReceiver::new(wakes, Arc::clone(&self.command_wake_overflow))
    }

    /// Number of retained broker messages, used to verify retained-topic policy.
    pub async fn retained_count(&self) -> isize {
        self.context.extends.retain().await.count().await
    }

    /// Waits for an unexpected broker stop.
    pub async fn wait(&mut self) -> anyhow::Result<()> {
        let Some(task) = self.task.as_mut() else {
            anyhow::bail!("MQTT listener task was already joined");
        };
        let result = wait_for_task_or_mqtt_failure(
            task,
            &mut self.kick_task,
            &mut self.ack_task,
            &mut self.command_ack_task,
            self.connections.failure(),
        )
        .await;
        if self.task.as_ref().is_some_and(JoinHandle::is_finished) {
            self.task.take();
        }
        if self.ack_task.as_ref().is_some_and(JoinHandle::is_finished) {
            self.ack_task.take();
        }
        if self
            .command_ack_task
            .as_ref()
            .is_some_and(JoinHandle::is_finished)
        {
            self.command_ack_task.take();
        }
        if result.is_err() {
            self.stop_listener().await;
        }
        result
    }

    #[cfg(test)]
    pub(crate) fn abort_command_ack_worker_for_test(&self) {
        if let Some(task) = &self.command_ack_task {
            task.abort();
        }
    }

    /// Stops the MQTT listener. Device sessions must be kicked before shutdown.
    pub fn abort(&self) {
        self.readiness.store(false, Ordering::Release);
        if let Some(cancel) = &self.ack_cancel {
            cancel.cancel();
        }
        self.command_ack_cancel.cancel();
        if let Some(task) = &self.task {
            task.abort();
        }
        if let Some(task) = &self.ack_task {
            task.abort();
        }
        if let Some(task) = &self.command_ack_task {
            task.abort();
        }
        if let Some(task) = &self.attributes_task {
            task.abort();
        }
    }

    /// Stops the listener first so hook producers release their bounded queues.
    pub async fn stop_listener(&mut self) {
        self.readiness.store(false, Ordering::Release);
        if let Some(task) = self.task.take() {
            if !task.is_finished() {
                task.abort();
            }
            match task.await {
                Ok(Ok(())) => {}
                Ok(Err(error)) => tracing::warn!(%error, "MQTT listener failed during shutdown"),
                Err(error) if !error.is_cancelled() => {
                    tracing::warn!(%error, "MQTT listener task failed during shutdown");
                }
                Err(_) => {}
            }
        }
        if let Some(task) = self.attributes_task.take() {
            task.abort();
            let _ = task.await;
        }
        if let Some(cancel) = &self.ack_cancel {
            cancel.cancel();
        }
        self.command_ack_cancel.cancel();
    }

    /// Drains MQTT workers after listener hooks have stopped producing messages.
    pub async fn drain_workers(&mut self, timeout: Duration) -> bool {
        if let Some(cancel) = &self.ack_cancel {
            cancel.cancel();
        }
        self.command_ack_cancel.cancel();
        let deadline = Instant::now() + timeout;
        let mut completed = true;
        for mut task in [self.ack_task.take(), self.command_ack_task.take()]
            .into_iter()
            .flatten()
        {
            let remaining = deadline.saturating_duration_since(Instant::now());
            match tokio::time::timeout(remaining, &mut task).await {
                Ok(Ok(())) => {}
                Ok(Err(error)) => {
                    tracing::warn!(%error, "MQTT worker failed during shutdown");
                    completed = false;
                }
                Err(_) => {
                    task.abort();
                    let _ = task.await;
                    completed = false;
                }
            }
        }
        completed
    }
}

async fn wait_for_task_or_mqtt_failure(
    task: &mut JoinHandle<anyhow::Result<()>>,
    kick_task: &mut JoinHandle<()>,
    ack_task: &mut Option<JoinHandle<()>>,
    command_ack_task: &mut Option<JoinHandle<()>>,
    mqtt_failure: &CancellationToken,
) -> anyhow::Result<()> {
    let result = tokio::select! {
        biased;
        _ = mqtt_failure.cancelled() => None,
        result = &mut *task => Some(result),
        result = &mut *kick_task => {
            match result {
                Ok(()) => anyhow::bail!("MQTT device kick worker stopped unexpectedly"),
                Err(error) => return Err(error.into()),
            }
        }
        result = wait_optional_task(ack_task) => {
            result?;
            anyhow::bail!("MQTT telemetry ACK worker stopped unexpectedly")
        }
        result = wait_optional_task(command_ack_task) => {
            result?;
            anyhow::bail!("MQTT command ACK worker stopped unexpectedly")
        }
    };
    match result {
        None => {
            task.abort();
            let _ = task.await;
            anyhow::bail!("MQTT failure; stopping listener")
        }
        Some(result) => result.map_err(anyhow::Error::from)??,
    }
    Ok(())
}

async fn wait_optional_task(
    task: &mut Option<JoinHandle<()>>,
) -> Result<(), tokio::task::JoinError> {
    match task {
        Some(task) => task.await,
        None => std::future::pending().await,
    }
}

impl Drop for MqttServer {
    fn drop(&mut self) {
        self.abort();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::SocketAddr;
    use std::sync::Mutex as StdMutex;
    use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
    use std::time::Duration;
    use tokio::net::{TcpListener, TcpStream};

    use crate::core::auth;
    use crate::core::device_channel::DeviceChannelError;
    use crate::core::events::EventBus;
    use crate::http::{AppState, router};
    use rumqttc::{
        AsyncClient, ConnectReturnCode, Event as MqttEvent, EventLoop, MqttOptions, Packet, QoS,
    };
    use tokio::sync::{Notify, watch};

    struct DeviceHttpFixture {
        pool: SqlitePool,
        base: String,
        id: String,
        token: String,
        readiness: Arc<AtomicBool>,
        presence: Arc<Presence>,
        mqtt: MqttServer,
        http: JoinHandle<()>,
    }

    impl Drop for DeviceHttpFixture {
        fn drop(&mut self) {
            self.http.abort();
        }
    }

    async fn start_device_http(
        request_timeout_ms: u64,
        kicker: Arc<dyn DeviceChannel>,
        emergency_kicker: Option<Arc<dyn DeviceChannel>>,
    ) -> DeviceHttpFixture {
        let pool = SqlitePool::connect("sqlite::memory:").await.unwrap();
        crate::storage::sqlite::migrate::run(&pool).await.unwrap();
        let now = auth::now_ms();
        sqlx::query("INSERT INTO users (id, username, password_hash, enabled, created_at, updated_at) VALUES ('user', 'admin', 'hash', 1, ?, ?)")
            .bind(now).bind(now).execute(&pool).await.unwrap();
        sqlx::query("INSERT INTO sessions (id, user_id, token_hash, created_at, last_seen_at, expires_at) VALUES ('session', 'user', ?, ?, ?, ?)")
            .bind(auth::hash_token("test-session").as_slice()).bind(now).bind(now).bind(now + 60_000).execute(&pool).await.unwrap();
        let id = uuid::Uuid::now_v7().to_string();
        let token = format!("iot_{}", auth::generate_session_token().unwrap());
        sqlx::query(
            "INSERT INTO devices (id, name, metadata_json, enabled, created_at, updated_at)
             VALUES (?, 'test-device', '{}', 1, ?, ?)",
        )
        .bind(&id)
        .bind(now)
        .bind(now)
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO device_credentials (id, device_id, token_hash, status, created_at)
             VALUES ('credential', ?, ?, 'active', ?)",
        )
        .bind(&id)
        .bind(auth::hash_token(&token).as_slice())
        .bind(now)
        .execute(&pool)
        .await
        .unwrap();

        let readiness = Arc::new(AtomicBool::new(false));
        let presence = Arc::new(Presence::new());
        let config = MqttConfig {
            listen: "127.0.0.1:0".parse().unwrap(),
            ..MqttConfig::default()
        };
        let mqtt = match emergency_kicker {
            Some(emergency) => {
                MqttServer::start_with_test_kickers(
                    &config,
                    Arc::clone(&readiness),
                    pool.clone(),
                    Arc::clone(&presence),
                    kicker,
                    emergency,
                )
                .await
            }
            None => {
                MqttServer::start_with_test_kicker(
                    &config,
                    Arc::clone(&readiness),
                    pool.clone(),
                    Arc::clone(&presence),
                    kicker,
                )
                .await
            }
        }
        .unwrap();
        let mut loaded = crate::config::load(std::path::Path::new("conf.toml")).unwrap();
        loaded.config.http.request_timeout_ms = request_timeout_ms;
        let state = AppState::new(
            pool.clone(),
            Arc::new(loaded),
            crate::http::ServiceReadiness {
                mqtt: Arc::clone(&readiness),
                telemetry: Arc::new(AtomicBool::new(true)),
                commands: Arc::new(AtomicBool::new(true)),
            },
            EventBus::new(),
            Arc::clone(&presence),
            mqtt.control(),
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let http = tokio::spawn(async move {
            axum::serve(
                listener,
                router::build(state).into_make_service_with_connect_info::<SocketAddr>(),
            )
            .await
            .unwrap();
        });
        DeviceHttpFixture {
            pool,
            base: format!("http://{address}"),
            id,
            token,
            readiness,
            presence,
            mqtt,
            http,
        }
    }

    fn disable_request(fixture: &DeviceHttpFixture) -> reqwest::RequestBuilder {
        disable_request_for(fixture, &fixture.id)
    }

    fn disable_request_for(
        fixture: &DeviceHttpFixture,
        device_id: &str,
    ) -> reqwest::RequestBuilder {
        reqwest::Client::new()
            .patch(format!("{}/api/v1/devices/{device_id}", fixture.base))
            .header("Cookie", "iot_session=test-session")
            .header("X-IoT-CSRF", "1")
            .json(&serde_json::json!({"enabled":false}))
    }

    fn rotate_request(fixture: &DeviceHttpFixture) -> reqwest::RequestBuilder {
        rotate_request_for(fixture, &fixture.id)
    }

    fn rotate_request_for(fixture: &DeviceHttpFixture, device_id: &str) -> reqwest::RequestBuilder {
        reqwest::Client::new()
            .post(format!(
                "{}/api/v1/devices/{device_id}/credentials/rotate",
                fixture.base
            ))
            .header("Cookie", "iot_session=test-session")
            .header("X-IoT-CSRF", "1")
            .json(&serde_json::json!({}))
    }

    fn enqueue_test_kick(
        fixture: &DeviceHttpFixture,
        device_id: &str,
    ) -> oneshot::Receiver<Result<(), ()>> {
        let admission = fixture
            .mqtt
            .connections
            .admission()
            .block(device_id)
            .unwrap();
        let permit = fixture.mqtt.device_kicks.try_reserve().unwrap();
        let (committed, decision) = oneshot::channel();
        let completion = permit.enqueue(
            device_id.to_owned(),
            admission,
            decision,
            DeviceKickRecovery::Disable,
        );
        committed.send(true).unwrap();
        completion
    }

    async fn connect_test_device(
        address: std::net::SocketAddr,
        device_id: &str,
        token: &str,
    ) -> (AsyncClient, EventLoop) {
        let mut options = MqttOptions::new(device_id, address.ip().to_string(), address.port());
        options
            .set_keep_alive(Duration::from_secs(30))
            .set_credentials(device_id, token);
        let (client, mut eventloop) = AsyncClient::new(options, 16);
        let connack = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                match eventloop.poll().await {
                    Ok(MqttEvent::Incoming(Packet::ConnAck(ack))) => break ack.code,
                    Ok(_) => {}
                    Err(error) => panic!("MQTT connect failed: {error:?}"),
                }
            }
        })
        .await
        .unwrap();
        assert_eq!(connack, ConnectReturnCode::Success);
        (client, eventloop)
    }

    async fn subscribe_test_device(client: &AsyncClient, eventloop: &mut EventLoop, topic: &str) {
        client.subscribe(topic, QoS::AtLeastOnce).await.unwrap();
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if matches!(
                    eventloop.poll().await.unwrap(),
                    MqttEvent::Incoming(Packet::SubAck(_))
                ) {
                    break;
                }
            }
        })
        .await
        .unwrap();
    }

    async fn refused_test_device(
        address: SocketAddr,
        device_id: &str,
        token: &str,
    ) -> ConnectReturnCode {
        let mut options = MqttOptions::new(device_id, address.ip().to_string(), address.port());
        options.set_credentials(device_id, token);
        let (client, mut eventloop) = AsyncClient::new(options, 8);
        let result = tokio::time::timeout(Duration::from_secs(5), eventloop.poll())
            .await
            .unwrap();
        drop(client);
        match result {
            Err(error) => match error {
                rumqttc::ConnectionError::ConnectionRefused(code) => code,
                other => panic!("expected refused connect, got {other:?}"),
            },
            Ok(other) => panic!("expected refused connect, got {other:?}"),
        }
    }

    async fn wait_test_device_closed(eventloop: &mut EventLoop) {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if eventloop.poll().await.is_err() {
                    break;
                }
            }
        })
        .await
        .expect("MQTT connection must close");
    }

    struct RecordingKick(Arc<StdMutex<Vec<&'static str>>>);

    #[async_trait::async_trait]
    impl DeviceChannel for RecordingKick {
        async fn publish(
            &self,
            _device_id: &str,
            _topic: &str,
            _payload: &[u8],
        ) -> Result<(), DeviceChannelError> {
            Err(DeviceChannelError::Unavailable)
        }

        async fn kick(&self, _device_id: &str) -> Result<(), DeviceChannelError> {
            self.0.lock().unwrap().push("kick");
            Ok(())
        }

        async fn is_online(&self, _device_id: &str) -> Result<bool, DeviceChannelError> {
            Ok(false)
        }
    }

    #[tokio::test]
    async fn disable_waits_for_every_pre_authorized_handshake_before_commit_and_kick() {
        let order = Arc::new(StdMutex::new(Vec::new()));
        let kicker = Arc::new(RecordingKick(Arc::clone(&order)));
        let fixture = start_device_http(5_000, kicker, None).await;
        assert!(fixture.mqtt.connections.try_reserve(&fixture.id).unwrap());

        let request = disable_request(&fixture);
        let response_task = tokio::spawn(async move { request.send().await.unwrap() });
        tokio::time::timeout(Duration::from_secs(2), async {
            while !fixture
                .mqtt
                .connections
                .device_is_blocked_for_test(&fixture.id)
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("disable must close admission before its transaction");
        let enabled: bool = sqlx::query_scalar("SELECT enabled FROM devices WHERE id = ?")
            .bind(&fixture.id)
            .fetch_one(&fixture.pool)
            .await
            .unwrap();
        assert!(
            enabled,
            "SQL must remain enabled while an allowed handshake is pending"
        );
        assert!(!fixture.mqtt.connections.try_reserve(&fixture.id).unwrap());

        order.lock().unwrap().push("connected");
        fixture
            .mqtt
            .connections
            .connected(&fixture.id, "controlled-session")
            .unwrap();
        let response = tokio::time::timeout(Duration::from_secs(3), response_task)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(response.status(), axum::http::StatusCode::OK);
        assert_eq!(*order.lock().unwrap(), ["connected", "kick"]);
        assert_eq!(
            refused_test_device(fixture.mqtt.local_addr(), &fixture.id, &fixture.token).await,
            ConnectReturnCode::BadUserNamePassword
        );
    }

    #[tokio::test]
    async fn rotate_waits_for_pre_authorized_handshake_before_commit_and_kick() {
        let order = Arc::new(StdMutex::new(Vec::new()));
        let kicker = Arc::new(RecordingKick(Arc::clone(&order)));
        let fixture = start_device_http(5_000, kicker, None).await;
        assert!(fixture.mqtt.connections.try_reserve(&fixture.id).unwrap());

        let response_task = tokio::spawn(rotate_request(&fixture).send());
        tokio::time::timeout(Duration::from_secs(2), async {
            while !fixture
                .mqtt
                .connections
                .device_is_blocked_for_test(&fixture.id)
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("rotation must close admission before its transaction");
        assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT status FROM device_credentials WHERE id = 'credential'",
            )
            .fetch_one(&fixture.pool)
            .await
            .unwrap(),
            "active",
            "SQL must remain unchanged until an authorized handshake resolves"
        );
        assert!(!fixture.mqtt.connections.try_reserve(&fixture.id).unwrap());

        fixture
            .mqtt
            .connections
            .connected(&fixture.id, "controlled-session")
            .unwrap();
        let response = tokio::time::timeout(Duration::from_secs(3), response_task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(response.status(), axum::http::StatusCode::OK);
        assert_eq!(*order.lock().unwrap(), ["kick"]);
        assert_eq!(
            refused_test_device(fixture.mqtt.local_addr(), &fixture.id, &fixture.token).await,
            ConnectReturnCode::BadUserNamePassword
        );
    }

    #[tokio::test]
    async fn device_channel_publishes_server_qos1_non_retained_and_reads_presence() {
        let kicker = Arc::new(RecordingKick(Arc::new(StdMutex::new(Vec::new()))));
        let fixture = start_device_http(5_000, kicker, None).await;
        let publisher = ServerPublisher::new(
            fixture.mqtt.context.clone(),
            Arc::clone(&fixture.presence),
            Arc::clone(&fixture.mqtt.connections),
        );
        assert!(
            !DeviceChannel::is_online(&publisher, &fixture.id)
                .await
                .unwrap()
        );
        let (client, mut eventloop) =
            connect_test_device(fixture.mqtt.local_addr(), &fixture.id, &fixture.token).await;
        tokio::time::timeout(Duration::from_secs(2), async {
            while !fixture.presence.is_online(&fixture.id).unwrap() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(
            DeviceChannel::is_online(&publisher, &fixture.id)
                .await
                .unwrap()
        );

        let topic = format!("devices/{}/data/server-channel", fixture.id);
        subscribe_test_device(&client, &mut eventloop, &topic).await;
        DeviceChannel::publish(&publisher, &fixture.id, &topic, b"server-message")
            .await
            .unwrap();
        let publish = tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if let MqttEvent::Incoming(Packet::Publish(publish)) =
                    eventloop.poll().await.unwrap()
                {
                    break publish;
                }
            }
        })
        .await
        .unwrap();
        assert_eq!(publish.payload.as_ref(), b"server-message");
        assert_eq!(publish.qos, QoS::AtLeastOnce);
        assert!(!publish.retain);
    }

    #[tokio::test]
    async fn http_timeout_does_not_cancel_a_committed_kick_and_acl_blocks_telemetry() {
        let started = Arc::new(Notify::new());
        let (release, receiver) = watch::channel(false);
        let kicker = Arc::new(BlockingKick {
            started: Arc::clone(&started),
            release: StdMutex::new(receiver),
        });
        let mut fixture = start_device_http(1_000, kicker, None).await;
        let (client, mut eventloop) =
            connect_test_device(fixture.mqtt.local_addr(), &fixture.id, &fixture.token).await;
        let request = disable_request(&fixture);
        let response_task = tokio::spawn(async move { request.send().await.unwrap() });
        tokio::time::timeout(Duration::from_secs(2), started.notified())
            .await
            .expect("supervised kick worker must start");

        let response = tokio::time::timeout(Duration::from_secs(3), response_task)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(response.status(), axum::http::StatusCode::REQUEST_TIMEOUT);
        let enabled: bool = sqlx::query_scalar("SELECT enabled FROM devices WHERE id = ?")
            .bind(&fixture.id)
            .fetch_one(&fixture.pool)
            .await
            .unwrap();
        assert!(!enabled);
        assert!(!fixture.mqtt.kick_task.is_finished());
        assert!(fixture.readiness.load(Ordering::Acquire));

        client
            .publish(
                format!("devices/{}/telemetry", fixture.id),
                QoS::AtLeastOnce,
                false,
                br#"{"event_id":"after-disable","values":{"x":1}}"#.as_slice(),
            )
            .await
            .unwrap();
        wait_test_device_closed(&mut eventloop).await;
        release.send(true).unwrap();
        assert!(
            tokio::time::timeout(Duration::from_secs(5), fixture.mqtt.wait())
                .await
                .unwrap()
                .is_err()
        );
        assert!(!fixture.readiness.load(Ordering::Acquire));
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM telemetry_events WHERE device_id = ?",
            )
            .bind(&fixture.id)
            .fetch_one(&fixture.pool)
            .await
            .unwrap(),
            0
        );
    }

    #[tokio::test]
    async fn rotate_timeout_keeps_kick_running_and_retry_recovers_one_time_token() {
        let started = Arc::new(Notify::new());
        let (release, receiver) = watch::channel(false);
        let kicker = Arc::new(BlockingOnceKick {
            started: Arc::clone(&started),
            release: StdMutex::new(receiver),
            first: AtomicBool::new(true),
        });
        let fixture = start_device_http(1_000, kicker, None).await;
        let (old_client, mut old_eventloop) =
            connect_test_device(fixture.mqtt.local_addr(), &fixture.id, &fixture.token).await;
        let request = rotate_request(&fixture);
        let response_task = tokio::spawn(async move { request.send().await.unwrap() });
        tokio::time::timeout(Duration::from_secs(2), started.notified())
            .await
            .expect("committed rotation must reach the supervised kick worker");

        let response = tokio::time::timeout(Duration::from_secs(3), response_task)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(response.status(), axum::http::StatusCode::REQUEST_TIMEOUT);
        assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT status FROM device_credentials WHERE id = 'credential'",
            )
            .fetch_one(&fixture.pool)
            .await
            .unwrap(),
            "revoked"
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM device_credentials WHERE device_id = ? AND status = 'active'",
            )
            .bind(&fixture.id)
            .fetch_one(&fixture.pool)
            .await
            .unwrap(),
            1
        );
        assert_eq!(
            refused_test_device(fixture.mqtt.local_addr(), &fixture.id, &fixture.token).await,
            ConnectReturnCode::BadUserNamePassword
        );
        let publisher = ServerPublisher::new(
            fixture.mqtt.context.clone(),
            Arc::new(Presence::new()),
            Arc::clone(&fixture.mqtt.connections),
        );
        assert!(
            publisher
                .publish(
                    &fixture.id,
                    &format!("devices/{}/commands", fixture.id),
                    b"fenced"
                )
                .await
                .is_err()
        );

        let retry = rotate_request(&fixture);
        let retry_task = tokio::spawn(async move { retry.send().await.unwrap() });
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let audits: i64 = sqlx::query_scalar(
                    "SELECT COUNT(*) FROM audit_events
                     WHERE target_id = ? AND action = 'credential.rotate'",
                )
                .bind(&fixture.id)
                .fetch_one(&fixture.pool)
                .await
                .unwrap();
                if audits == 2 {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("a second rotation must commit while the first broker kick is blocked");
        assert!(!retry_task.is_finished());
        release.send(true).unwrap();
        let response = retry_task.await.unwrap();
        assert_eq!(response.status(), axum::http::StatusCode::OK);
        let body: serde_json::Value = response.json().await.unwrap();
        let recovered_token = body["credential"]["token"].as_str().unwrap();
        assert_eq!(
            refused_test_device(fixture.mqtt.local_addr(), &fixture.id, &fixture.token).await,
            ConnectReturnCode::BadUserNamePassword
        );
        let (_new_client, _new_eventloop) =
            connect_test_device(fixture.mqtt.local_addr(), &fixture.id, recovered_token).await;
        wait_test_device_closed(&mut old_eventloop).await;
        assert!(fixture.readiness.load(Ordering::Acquire));
        drop(old_client);
    }

    #[tokio::test]
    async fn concurrent_rotations_return_tokens_in_serialized_commit_order() {
        let started = Arc::new(Notify::new());
        let (release, receiver) = watch::channel(false);
        let kicker = Arc::new(BlockingOnceKick {
            started: Arc::clone(&started),
            release: StdMutex::new(receiver),
            first: AtomicBool::new(true),
        });
        let fixture = start_device_http(5_000, kicker, None).await;
        let first = tokio::spawn(rotate_request(&fixture).send());
        tokio::time::timeout(Duration::from_secs(2), started.notified())
            .await
            .expect("first rotation must be waiting in its broker kick");

        let second = tokio::spawn(rotate_request(&fixture).send());
        let second_committed = tokio::time::timeout(Duration::from_millis(200), async {
            loop {
                let audits: i64 = sqlx::query_scalar(
                    "SELECT COUNT(*) FROM audit_events
                     WHERE target_id = ? AND action = 'credential.rotate'",
                )
                .bind(&fixture.id)
                .fetch_one(&fixture.pool)
                .await
                .unwrap();
                if audits == 2 {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await;
        assert!(
            second_committed.is_err(),
            "second rotation must not commit while the first request awaits kick"
        );
        assert!(!second.is_finished());

        release.send(true).unwrap();
        let first_response = first.await.unwrap().unwrap();
        assert_eq!(first_response.status(), axum::http::StatusCode::OK);
        let first_body: serde_json::Value = first_response.json().await.unwrap();
        let first_id = first_body["credential"]["id"].as_str().unwrap().to_owned();
        let first_token = first_body["credential"]["token"]
            .as_str()
            .unwrap()
            .to_owned();

        let second_response = second.await.unwrap().unwrap();
        assert_eq!(second_response.status(), axum::http::StatusCode::OK);
        let second_body: serde_json::Value = second_response.json().await.unwrap();
        let second_id = second_body["credential"]["id"].as_str().unwrap();
        let second_token = second_body["credential"]["token"].as_str().unwrap();
        assert_ne!(first_token, second_token);

        let first_record: (String, Vec<u8>) =
            sqlx::query_as("SELECT status, token_hash FROM device_credentials WHERE id = ?")
                .bind(&first_id)
                .fetch_one(&fixture.pool)
                .await
                .unwrap();
        assert_eq!(first_record.0, "revoked");
        assert_eq!(first_record.1, auth::hash_token(&first_token));
        let second_record: (String, Vec<u8>, Option<String>) = sqlx::query_as(
            "SELECT status, token_hash, rotated_from FROM device_credentials WHERE id = ?",
        )
        .bind(second_id)
        .fetch_one(&fixture.pool)
        .await
        .unwrap();
        assert_eq!(second_record.0, "active");
        assert_eq!(second_record.1, auth::hash_token(second_token));
        assert_eq!(second_record.2.as_deref(), Some(first_id.as_str()));
    }

    #[tokio::test]
    async fn rotating_an_unrelated_device_does_not_wait_for_a_blocked_kick() {
        let started = Arc::new(Notify::new());
        let (release, receiver) = watch::channel(false);
        let kicker = Arc::new(BlockingOnceKick {
            started: Arc::clone(&started),
            release: StdMutex::new(receiver),
            first: AtomicBool::new(true),
        });
        let fixture = start_device_http(5_000, kicker, None).await;
        let device_b = uuid::Uuid::now_v7().to_string();
        let token_b = format!("iot_{}", auth::generate_session_token().unwrap());
        let now = auth::now_ms();
        sqlx::query(
            "INSERT INTO devices (id, name, metadata_json, enabled, created_at, updated_at)
             VALUES (?, 'other-device', '{}', 1, ?, ?)",
        )
        .bind(&device_b)
        .bind(now)
        .bind(now)
        .execute(&fixture.pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO device_credentials (id, device_id, token_hash, status, created_at)
             VALUES ('credential-b', ?, ?, 'active', ?)",
        )
        .bind(&device_b)
        .bind(auth::hash_token(&token_b).as_slice())
        .bind(now)
        .execute(&fixture.pool)
        .await
        .unwrap();

        let device_a_request = rotate_request_for(&fixture, &fixture.id);
        let device_a = tokio::spawn(async move { device_a_request.send().await.unwrap() });
        tokio::time::timeout(Duration::from_secs(2), started.notified())
            .await
            .expect("device A kick must block");

        let device_b_request = rotate_request_for(&fixture, &device_b);
        let device_b_response =
            tokio::time::timeout(Duration::from_secs(2), device_b_request.send())
                .await
                .expect("device B rotation must not wait for device A")
                .unwrap();
        assert_eq!(device_b_response.status(), axum::http::StatusCode::OK);
        let device_b_body: serde_json::Value = device_b_response.json().await.unwrap();
        let new_token_b = device_b_body["credential"]["token"].as_str().unwrap();
        assert_ne!(new_token_b, token_b);
        assert!(!device_a.is_finished());

        release.send(true).unwrap();
        let device_a_response = device_a.await.unwrap();
        assert_eq!(device_a_response.status(), axum::http::StatusCode::OK);
    }

    #[tokio::test]
    async fn failed_rotate_kicks_leave_sql_enabled_but_fence_the_live_session() {
        let primary = Arc::new(FailingKick(AtomicUsize::new(0)));
        let emergency = Arc::new(FailingKick(AtomicUsize::new(0)));
        let mut fixture = start_device_http(
            5_000,
            Arc::clone(&primary) as Arc<dyn DeviceChannel>,
            Some(Arc::clone(&emergency) as Arc<dyn DeviceChannel>),
        )
        .await;
        let (client, mut eventloop) =
            connect_test_device(fixture.mqtt.local_addr(), &fixture.id, &fixture.token).await;

        let response = rotate_request(&fixture).send().await.unwrap();
        assert_eq!(
            response.status(),
            axum::http::StatusCode::SERVICE_UNAVAILABLE
        );
        assert_eq!(
            response.json::<serde_json::Value>().await.unwrap()["code"],
            "device_kick_failed"
        );
        assert_eq!(
            primary.0.load(AtomicOrdering::Relaxed),
            DEVICE_KICK_ATTEMPTS
        );
        assert_eq!(
            emergency.0.load(AtomicOrdering::Relaxed),
            DEVICE_KICK_ATTEMPTS
        );
        assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT status FROM device_credentials WHERE id = 'credential'",
            )
            .fetch_one(&fixture.pool)
            .await
            .unwrap(),
            "revoked"
        );
        assert!(
            sqlx::query_scalar::<_, bool>("SELECT enabled FROM devices WHERE id = ?")
                .bind(&fixture.id)
                .fetch_one(&fixture.pool)
                .await
                .unwrap(),
            "SQL enabled state must not change on a failed rotate kick"
        );
        assert!(!fixture.readiness.load(Ordering::Acquire));
        let publisher = ServerPublisher::new(
            fixture.mqtt.context.clone(),
            Arc::new(Presence::new()),
            Arc::clone(&fixture.mqtt.connections),
        );
        assert!(
            publisher
                .publish(
                    &fixture.id,
                    &format!("devices/{}/commands", fixture.id),
                    b"fenced"
                )
                .await
                .is_err()
        );
        client
            .publish(
                format!("devices/{}/status", fixture.id),
                QoS::AtLeastOnce,
                false,
                b"after-rotate-failure".as_slice(),
            )
            .await
            .unwrap();
        wait_test_device_closed(&mut eventloop).await;
        assert!(fixture.mqtt.wait().await.is_err());
        drop(client);
    }

    #[tokio::test]
    async fn both_failed_kicks_fence_a_live_mqtt_session_in_both_directions() {
        let primary = Arc::new(FailingKick(AtomicUsize::new(0)));
        let emergency = Arc::new(FailingKick(AtomicUsize::new(0)));
        let mut fixture = start_device_http(
            5_000,
            Arc::clone(&primary) as Arc<dyn DeviceChannel>,
            Some(Arc::clone(&emergency) as Arc<dyn DeviceChannel>),
        )
        .await;
        let (client, mut eventloop) =
            connect_test_device(fixture.mqtt.local_addr(), &fixture.id, &fixture.token).await;
        let topic = format!("devices/{}/commands", fixture.id);
        subscribe_test_device(&client, &mut eventloop, &topic).await;
        let publisher = ServerPublisher::new(
            fixture.mqtt.context.clone(),
            Arc::new(Presence::new()),
            Arc::clone(&fixture.mqtt.connections),
        );
        publisher
            .publish(&fixture.id, &topic, b"before-disable")
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if let MqttEvent::Incoming(Packet::Publish(publish)) =
                    eventloop.poll().await.unwrap()
                {
                    assert_eq!(publish.payload.as_ref(), b"before-disable");
                    break;
                }
            }
        })
        .await
        .unwrap();

        let response = disable_request(&fixture).send().await.unwrap();
        assert_eq!(
            response.status(),
            axum::http::StatusCode::SERVICE_UNAVAILABLE
        );
        assert_eq!(
            response.json::<serde_json::Value>().await.unwrap()["code"],
            "device_kick_failed"
        );
        assert_eq!(
            primary.0.load(AtomicOrdering::Relaxed),
            DEVICE_KICK_ATTEMPTS
        );
        assert_eq!(
            emergency.0.load(AtomicOrdering::Relaxed),
            DEVICE_KICK_ATTEMPTS
        );

        assert!(
            publisher
                .publish(&fixture.id, &topic, b"after-disable")
                .await
                .is_err()
        );
        assert!(
            tokio::time::timeout(Duration::from_millis(150), async {
                loop {
                    if let MqttEvent::Incoming(Packet::Publish(_)) = eventloop.poll().await.unwrap()
                    {
                        break;
                    }
                }
            })
            .await
            .is_err(),
            "disabled device must not receive server messages"
        );

        client
            .publish(
                format!("devices/{}/status", fixture.id),
                QoS::AtLeastOnce,
                false,
                b"after-disable".as_slice(),
            )
            .await
            .unwrap();
        wait_test_device_closed(&mut eventloop).await;
        assert!(!fixture.readiness.load(Ordering::Acquire));
        assert!(fixture.mqtt.wait().await.is_err());
        drop(client);
    }

    #[tokio::test]
    async fn queued_jobs_for_one_device_do_not_block_an_unrelated_device() {
        let started = Arc::new(Notify::new());
        let (release, receiver) = watch::channel(false);
        let kicker = Arc::new(SelectiveBlockingKick {
            block_first: AtomicBool::new(true),
            started: Arc::clone(&started),
            release: StdMutex::new(receiver),
        });
        let fixture =
            start_device_http(5_000, Arc::clone(&kicker) as Arc<dyn DeviceChannel>, None).await;
        let device_a = fixture.id.clone();
        let first = enqueue_test_kick(&fixture, &device_a);
        tokio::time::timeout(Duration::from_secs(2), started.notified())
            .await
            .expect("first device kick must block");

        let same_device_jobs: Vec<_> = (0..DEVICE_KICK_CONCURRENCY)
            .map(|_| enqueue_test_kick(&fixture, &device_a))
            .collect();
        tokio::time::timeout(Duration::from_secs(2), async {
            while fixture.mqtt.device_kicks.sender.capacity() != DEVICE_KICK_QUEUE_CAPACITY {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("all same-device jobs must be buffered before device B is queued");

        let device_b = uuid::Uuid::now_v7().to_string();
        let unrelated = enqueue_test_kick(&fixture, &device_b);
        assert!(
            tokio::time::timeout(Duration::from_secs(2), unrelated)
                .await
                .expect("device B must be scheduled behind queued A jobs")
                .unwrap()
                .is_ok()
        );

        release.send(true).unwrap();
        assert!(first.await.unwrap().is_ok());
        for job in same_device_jobs {
            assert!(job.await.unwrap().is_ok());
        }
    }

    struct BlockingKick {
        started: Arc<Notify>,
        release: StdMutex<watch::Receiver<bool>>,
    }

    struct BlockingOnceKick {
        started: Arc<Notify>,
        release: StdMutex<watch::Receiver<bool>>,
        first: AtomicBool,
    }

    #[async_trait::async_trait]
    impl DeviceChannel for BlockingOnceKick {
        async fn publish(
            &self,
            _device_id: &str,
            _topic: &str,
            _payload: &[u8],
        ) -> Result<(), DeviceChannelError> {
            Err(DeviceChannelError::Unavailable)
        }

        async fn kick(&self, _device_id: &str) -> Result<(), DeviceChannelError> {
            if self.first.swap(false, AtomicOrdering::Relaxed) {
                self.started.notify_one();
                let mut release = self.release.lock().unwrap().clone();
                while !*release.borrow() {
                    if release.changed().await.is_err() {
                        break;
                    }
                }
            }
            Ok(())
        }

        async fn is_online(&self, _device_id: &str) -> Result<bool, DeviceChannelError> {
            Ok(true)
        }
    }

    #[async_trait::async_trait]
    impl DeviceChannel for BlockingKick {
        async fn publish(
            &self,
            _device_id: &str,
            _topic: &str,
            _payload: &[u8],
        ) -> Result<(), DeviceChannelError> {
            Err(DeviceChannelError::Unavailable)
        }

        async fn kick(&self, _device_id: &str) -> Result<(), DeviceChannelError> {
            self.started.notify_one();
            let mut release = self.release.lock().unwrap().clone();
            while !*release.borrow() {
                if release.changed().await.is_err() {
                    break;
                }
            }
            Err(DeviceChannelError::Unavailable)
        }

        async fn is_online(&self, _device_id: &str) -> Result<bool, DeviceChannelError> {
            Ok(true)
        }
    }

    struct SelectiveBlockingKick {
        block_first: AtomicBool,
        started: Arc<Notify>,
        release: StdMutex<watch::Receiver<bool>>,
    }

    #[async_trait::async_trait]
    impl DeviceChannel for SelectiveBlockingKick {
        async fn publish(
            &self,
            _device_id: &str,
            _topic: &str,
            _payload: &[u8],
        ) -> Result<(), DeviceChannelError> {
            Err(DeviceChannelError::Unavailable)
        }

        async fn kick(&self, _device_id: &str) -> Result<(), DeviceChannelError> {
            if self.block_first.swap(false, AtomicOrdering::Relaxed) {
                self.started.notify_one();
                let mut release = self.release.lock().unwrap().clone();
                while !*release.borrow() {
                    if release.changed().await.is_err() {
                        break;
                    }
                }
            }
            Ok(())
        }

        async fn is_online(&self, _device_id: &str) -> Result<bool, DeviceChannelError> {
            Ok(true)
        }
    }

    struct FailingKick(AtomicUsize);

    #[async_trait::async_trait]
    impl DeviceChannel for FailingKick {
        async fn publish(
            &self,
            _device_id: &str,
            _topic: &str,
            _payload: &[u8],
        ) -> Result<(), DeviceChannelError> {
            Err(DeviceChannelError::Unavailable)
        }

        async fn kick(&self, _device_id: &str) -> Result<(), DeviceChannelError> {
            self.0.fetch_add(1, AtomicOrdering::Relaxed);
            Err(DeviceChannelError::Unavailable)
        }

        async fn is_online(&self, _device_id: &str) -> Result<bool, DeviceChannelError> {
            Ok(true)
        }
    }
    #[tokio::test]
    async fn http_presence_failure_stops_mqtt_listener() {
        use crate::core::{auth, events::EventBus};
        use crate::http::{AppState, router};
        use axum::http::StatusCode;

        let pool = SqlitePool::connect("sqlite::memory:").await.unwrap();
        crate::storage::sqlite::migrate::run(&pool).await.unwrap();
        let now = auth::now_ms();
        sqlx::query("INSERT INTO users (id, username, password_hash, enabled, created_at, updated_at) VALUES ('user', 'admin', 'hash', 1, ?, ?)")
            .bind(now).bind(now).execute(&pool).await.unwrap();
        sqlx::query("INSERT INTO sessions (id, user_id, token_hash, created_at, last_seen_at, expires_at) VALUES ('session', 'user', ?, ?, ?, ?)")
            .bind(auth::hash_token("test-session").as_slice()).bind(now).bind(now).bind(now + 60_000).execute(&pool).await.unwrap();
        sqlx::query("INSERT INTO devices (id, name, enabled, created_at, updated_at) VALUES ('device', 'test', 1, ?, ?)")
            .bind(now).bind(now).execute(&pool).await.unwrap();
        sqlx::query("INSERT INTO device_credentials (id, device_id, token_hash, status, created_at) VALUES ('credential', 'device', ?, 'active', ?)")
            .bind([0_u8; 32].as_slice()).bind(now).execute(&pool).await.unwrap();

        let readiness = Arc::new(AtomicBool::new(false));
        let presence = Arc::new(Presence::new());
        let config = MqttConfig {
            listen: "127.0.0.1:0".parse().unwrap(),
            ..MqttConfig::default()
        };
        let mut mqtt = MqttServer::start(
            &config,
            Arc::clone(&readiness),
            pool.clone(),
            Arc::clone(&presence),
        )
        .await
        .unwrap();
        let address = mqtt.local_addr();
        let loaded = crate::config::load(std::path::Path::new("conf.toml")).unwrap();
        let state = AppState::new(
            pool,
            Arc::new(loaded),
            crate::http::ServiceReadiness {
                mqtt: Arc::clone(&readiness),
                telemetry: Arc::new(AtomicBool::new(true)),
                commands: Arc::new(AtomicBool::new(true)),
            },
            EventBus::new(),
            Arc::clone(&presence),
            mqtt.control(),
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let http_addr = listener.local_addr().unwrap();
        let http =
            tokio::spawn(async move { axum::serve(listener, router::build(state)).await.unwrap() });
        presence.poison_for_test();
        let response = reqwest::Client::new()
            .get(format!("http://{http_addr}/api/v1/devices/device"))
            .header("Cookie", "iot_session=test-session")
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert!(!readiness.load(Ordering::Acquire));
        assert!(
            tokio::time::timeout(Duration::from_secs(2), mqtt.wait())
                .await
                .unwrap()
                .is_err()
        );
        assert!(TcpStream::connect(address).await.is_err());
        http.abort();
    }

    fn spawn_panicking_listener(connections: Arc<ConnectionLimiter>) -> JoinHandle<()> {
        tokio::spawn(async move {
            let _stopped = ListenerStoppedGuard(connections);
            panic!("simulated RMQTT listener panic");
        })
    }

    #[tokio::test]
    async fn mqtt_failure_aborts_a_pending_task() {
        // Isolate the select branch; a real RMQTT listener is covered below.
        let mut task = tokio::spawn(std::future::pending::<anyhow::Result<()>>());
        let mqtt_failure = CancellationToken::new();
        mqtt_failure.cancel();

        let mut kick_task = tokio::spawn(std::future::pending::<()>());
        let mut ack_task = None;
        let mut command_ack_task = Some(tokio::spawn(std::future::pending::<()>()));
        let result = wait_for_task_or_mqtt_failure(
            &mut task,
            &mut kick_task,
            &mut ack_task,
            &mut command_ack_task,
            &mqtt_failure,
        )
        .await;

        assert!(result.is_err());
        assert!(task.is_finished());
    }

    #[tokio::test]
    async fn listener_panic_before_startup_publish_keeps_readiness_down() {
        let readiness = Arc::new(AtomicBool::new(false));
        let connections = Arc::new(ConnectionLimiter::new(
            1,
            Arc::clone(&readiness),
            Arc::new(Mutex::new(())),
            CancellationToken::new(),
        ));

        assert!(
            spawn_panicking_listener(Arc::clone(&connections))
                .await
                .unwrap_err()
                .is_panic()
        );
        connections.publish_ready();

        assert!(!readiness.load(Ordering::Acquire));
    }

    #[tokio::test]
    async fn listener_panic_after_startup_publish_drops_readiness() {
        let readiness = Arc::new(AtomicBool::new(false));
        let connections = Arc::new(ConnectionLimiter::new(
            1,
            Arc::clone(&readiness),
            Arc::new(Mutex::new(())),
            CancellationToken::new(),
        ));
        connections.publish_ready();
        assert!(readiness.load(Ordering::Acquire));

        assert!(
            spawn_panicking_listener(connections)
                .await
                .unwrap_err()
                .is_panic()
        );

        assert!(!readiness.load(Ordering::Acquire));
    }

    #[tokio::test]
    async fn presence_failure_stops_real_rmqtt_listener() {
        let config = MqttConfig {
            listen: "127.0.0.1:0".parse().unwrap(),
            ..MqttConfig::default()
        };
        let readiness = Arc::new(AtomicBool::new(false));
        let pool = SqlitePool::connect_lazy("sqlite::memory:").unwrap();
        let mut server = MqttServer::start(
            &config,
            Arc::clone(&readiness),
            pool,
            Arc::new(Presence::new()),
        )
        .await
        .unwrap();
        let address = server.local_addr();

        let probe = TcpStream::connect(address).await.unwrap();
        drop(probe);
        server.connections.poison_state_for_test();
        assert!(server.connections.try_reserve("device-a").is_err());

        assert!(!readiness.load(Ordering::Acquire));
        assert!(server.wait().await.is_err());
        assert!(TcpStream::connect(address).await.is_err());
    }
}
