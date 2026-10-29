//! Lifecycle wrapper around RMQTT's in-process broker.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use rmqtt::context::ServerContext;
use rmqtt::hook::Type;
use rmqtt::server::MqttServer as RmqttServer;
use sqlx::SqlitePool;
use tokio::task::JoinHandle;

use crate::config::schema::MqttConfig;
use crate::core::presence::Presence;
use crate::core::telemetry::{IngestHandle, TelemetryPayload};
use crate::storage::sqlite::attributes as attribute_storage;

use super::acl::AclHooks;
use super::auth::AuthHooks;
use super::config;
use super::presence::PresenceHooks;
use super::retained::BudgetedRetainStorage;
use super::server_publish::{ServerPublisher, spawn_ack_worker};

/// Running RMQTT listener and the channel used by core services.
pub struct MqttServer {
    address: std::net::SocketAddr,
    readiness: Arc<AtomicBool>,
    context: ServerContext,
    task: JoinHandle<anyhow::Result<()>>,
    ack_task: Option<JoinHandle<()>>,
    attributes_task: Option<JoinHandle<()>>,
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
                Box::new(AuthHooks::new(pool.clone())),
            )
            .await;
        let (acl_hooks, ack_task, attributes_task) = match ingest {
            Some((ingest, limits, attribute_queue_capacity)) => {
                let (attributes, attributes_task) =
                    attribute_storage::spawn_worker(pool.clone(), attribute_queue_capacity);
                let (ack, task) = spawn_ack_worker(publisher);
                (
                    AclHooks::with_ingest(ingest, attributes, limits, ack),
                    Some(task),
                    Some(attributes_task),
                )
            }
            None => (AclHooks::new(), None, None),
        };
        register
            .add_priority(Type::MessagePublishCheckAcl, 10, Box::new(acl_hooks))
            .await;
        register
            .add_priority(Type::ClientSubscribeCheckAcl, 10, Box::new(AclHooks::new()))
            .await;
        let presence_hooks = PresenceHooks::new(pool, presence);
        register
            .add_priority(Type::ClientConnected, 10, Box::new(presence_hooks.clone()))
            .await;
        register
            .add_priority(Type::ClientDisconnected, 10, Box::new(presence_hooks))
            .await;
        register.start().await;

        let task_readiness = Arc::clone(&readiness);
        let task = tokio::spawn(async move {
            let result = RmqttServer::new(context)
                .listener(listener)
                .build()
                .run()
                .await;
            task_readiness.store(false, Ordering::Release);
            result
        });

        readiness.store(true, Ordering::Release);
        Ok(Self {
            address,
            readiness,
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
        (&mut self.task).await.map_err(anyhow::Error::from)??;
        Ok(())
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

impl Drop for MqttServer {
    fn drop(&mut self) {
        self.abort();
    }
}
