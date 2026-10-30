//! Lifecycle wrapper around RMQTT's in-process broker.

use std::collections::{HashSet, VecDeque};
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use rmqtt::context::ServerContext;
use rmqtt::hook::Type;
use rmqtt::server::MqttServer as RmqttServer;
use sqlx::SqlitePool;
use tokio::sync::mpsc::OwnedPermit;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot};
use tokio::task::{JoinHandle, JoinSet};
use tokio_util::sync::CancellationToken;

use crate::config::schema::MqttConfig;
use crate::core::device_channel::DeviceChannel;
use crate::core::presence::Presence;
use crate::core::telemetry::{IngestHandle, TelemetryPayload};
use crate::observability::metrics::Metrics;
use crate::storage::sqlite::attributes as attribute_storage;

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
    device_kicks: DeviceKickQueue,
    task: JoinHandle<anyhow::Result<()>>,
    kick_task: JoinHandle<()>,
    ack_task: Option<JoinHandle<()>>,
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

struct DeviceKickJob {
    device_id: String,
    admission: DeviceAdmissionGuard,
    committed: oneshot::Receiver<bool>,
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
    ) -> oneshot::Receiver<Result<(), ()>> {
        let (completion, result) = oneshot::channel();
        self.sender.send(QueuedDeviceKick {
            job: DeviceKickJob {
                device_id,
                admission,
                committed,
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
        completion,
    } = job;
    let should_kick = match committed.await {
        Ok(committed) => {
            if let Err(error) = admission.set_device_enabled(!committed) {
                failure.report_failure(error);
            }
            committed
        }
        Err(_) => {
            match sqlx::query_scalar::<_, bool>("SELECT enabled FROM devices WHERE id = ?")
                .bind(&device_id)
                .fetch_optional(&pool)
                .await
            {
                Ok(enabled) => {
                    let enabled = enabled.unwrap_or(true);
                    if let Err(error) = admission.set_device_enabled(enabled) {
                        failure.report_failure(error);
                    }
                    !enabled
                }
                Err(error) => {
                    tracing::error!(%error, "cannot determine whether a device disable committed");
                    if let Err(error) = admission.set_device_enabled(false) {
                        failure.report_failure(error);
                    }
                    true
                }
            }
        }
    };
    if !should_kick {
        drop(admission);
        let _ = completion.send(Ok(()));
        return;
    }

    if kick_with_retries(primary.as_ref(), &device_id).await {
        drop(admission);
        let _ = completion.send(Ok(()));
        return;
    }

    Metrics::global().mqtt_kick_failures_total.inc();
    if !kick_with_retries(emergency.as_ref(), &device_id).await {
        tracing::error!("emergency MQTT session kick also failed");
    }
    failure.report_failure("device session kick failed after retries");
    drop(admission);
    let _ = completion.send(Err(()));
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
        let (acl_hooks, ack_task, attributes_task) = match ingest {
            Some((ingest, limits, attribute_queue_capacity)) => {
                let (attributes, attributes_task) =
                    attribute_storage::spawn_worker(pool.clone(), attribute_queue_capacity);
                let (ack, task) = spawn_ack_worker(publisher.clone());
                (
                    AclHooks::with_ingest(
                        Arc::clone(&connections),
                        ingest,
                        attributes,
                        limits,
                        ack,
                        max_topic_levels,
                    ),
                    Some(task),
                    Some(attributes_task),
                )
            }
            None => (
                AclHooks::new(Arc::clone(&connections), max_topic_levels),
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
            pool,
            presence,
            Arc::clone(&connections),
            device_kicks.clone(),
        );
        register
            .add_priority(Type::ClientConnected, 10, Box::new(presence_hooks.clone()))
            .await;
        register
            .add_priority(Type::ClientDisconnected, 10, Box::new(presence_hooks))
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
            device_kicks,
            task,
            kick_task,
            ack_task,
            attributes_task,
        })
    }

    /// Address actually bound by RMQTT, including a dynamically assigned port.
    pub fn local_addr(&self) -> std::net::SocketAddr {
        self.address
    }

    /// Number of retained broker messages, used to verify retained-topic policy.
    pub async fn retained_count(&self) -> isize {
        self.context.extends.retain().await.count().await
    }

    /// Waits for an unexpected broker stop.
    pub async fn wait(&mut self) -> anyhow::Result<()> {
        let result = wait_for_task_or_mqtt_failure(
            &mut self.task,
            &mut self.kick_task,
            self.connections.failure(),
        )
        .await;
        if result.is_err() {
            self.abort();
        }
        result
    }

    /// Stops the MQTT listener. Device sessions must be kicked before shutdown.
    pub fn abort(&self) {
        self.readiness.store(false, Ordering::Release);
        self.task.abort();
        if let Some(task) = &self.ack_task {
            task.abort();
        }
        if let Some(task) = &self.attributes_task {
            task.abort();
        }
    }
}

async fn wait_for_task_or_mqtt_failure(
    task: &mut JoinHandle<anyhow::Result<()>>,
    kick_task: &mut JoinHandle<()>,
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
            Arc::clone(&readiness),
            Arc::new(AtomicBool::new(true)),
            EventBus::new(),
            presence,
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
        let completion = permit.enqueue(device_id.to_owned(), admission, decision);
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
            Arc::clone(&readiness),
            Arc::new(AtomicBool::new(true)),
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
        let result = wait_for_task_or_mqtt_failure(&mut task, &mut kick_task, &mqtt_failure).await;

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
