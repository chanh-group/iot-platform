//! Lifecycle wrapper around RMQTT's in-process broker.

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};

use rmqtt::context::ServerContext;
use rmqtt::hook::Type;
use rmqtt::server::MqttServer as RmqttServer;
use sqlx::SqlitePool;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::config::schema::MqttConfig;
use crate::core::presence::Presence;
use crate::core::telemetry::{IngestHandle, TelemetryPayload};
use crate::storage::sqlite::attributes as attribute_storage;

use super::acl::AclHooks;
use super::auth::AuthHooks;
use super::config;
use super::presence::{ConnectionLimiter, PresenceHooks};
use super::retained::BudgetedRetainStorage;
use super::server_publish::{ServerPublisher, spawn_ack_worker};

/// Running RMQTT listener and the channel used by core services.
pub struct MqttServer {
    address: std::net::SocketAddr,
    readiness: Arc<AtomicBool>,
    connections: Arc<ConnectionLimiter>,
    context: ServerContext,
    task: JoinHandle<anyhow::Result<()>>,
    ack_task: Option<JoinHandle<()>>,
    attributes_task: Option<JoinHandle<()>>,
}

struct ListenerStoppedGuard(Arc<ConnectionLimiter>);

impl Drop for ListenerStoppedGuard {
    fn drop(&mut self) {
        self.0.listener_stopped();
    }
}

impl MqttServer {
    /// Starts RMQTT in the current process and reports its bound address.
    ///
    /// Auth and presence hooks are registered before the listener runs.
    pub async fn start(
        config: &MqttConfig,
        readiness: Arc<AtomicBool>,
        pool: SqlitePool,
        presence: Arc<Presence>,
    ) -> anyhow::Result<Self> {
        Self::start_inner(config, readiness, pool, presence, None).await
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
        )
        .await
    }

    async fn start_inner(
        config: &MqttConfig,
        readiness: Arc<AtomicBool>,
        pool: SqlitePool,
        presence: Arc<Presence>,
        ingest: Option<(IngestHandle, TelemetryPayload, usize)>,
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
        let publisher = ServerPublisher::new(context.clone());

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
                let (ack, task) = spawn_ack_worker(publisher);
                (
                    AclHooks::with_ingest(ingest, attributes, limits, ack, max_topic_levels),
                    Some(task),
                    Some(attributes_task),
                )
            }
            None => (AclHooks::new(max_topic_levels), None, None),
        };
        register
            .add_priority(Type::MessagePublishCheckAcl, 10, Box::new(acl_hooks))
            .await;
        register
            .add_priority(
                Type::ClientSubscribeCheckAcl,
                10,
                Box::new(AclHooks::new(max_topic_levels)),
            )
            .await;
        let presence_hooks = PresenceHooks::new(pool, presence, Arc::clone(&connections));
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
            task,
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
        let result =
            wait_for_task_or_presence_failure(&mut self.task, self.connections.failure()).await;
        if result.is_err() {
            self.abort();
        }
        result
    }

    /// Stops accepting MQTT connections. Session kicking is added with auth/presence.
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

async fn wait_for_task_or_presence_failure(
    task: &mut JoinHandle<anyhow::Result<()>>,
    presence_failure: &CancellationToken,
) -> anyhow::Result<()> {
    let result = tokio::select! {
        biased;
        _ = presence_failure.cancelled() => None,
        result = &mut *task => Some(result),
    };
    match result {
        None => {
            task.abort();
            let _ = task.await;
            anyhow::bail!("MQTT presence state mutex poisoned; stopping listener")
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
    use tokio::net::TcpStream;

    fn spawn_panicking_listener(connections: Arc<ConnectionLimiter>) -> JoinHandle<()> {
        tokio::spawn(async move {
            let _stopped = ListenerStoppedGuard(connections);
            panic!("simulated RMQTT listener panic");
        })
    }

    #[tokio::test]
    async fn unit_test_presence_failure_aborts_a_pending_task() {
        // Isolate the select branch; a real RMQTT listener is covered below.
        let mut task = tokio::spawn(std::future::pending::<anyhow::Result<()>>());
        let presence_failure = CancellationToken::new();
        presence_failure.cancel();

        let result = wait_for_task_or_presence_failure(&mut task, &presence_failure).await;

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
