//! Lifecycle wrapper around RMQTT's in-process broker.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use rmqtt::context::ServerContext;
use rmqtt::server::MqttServer as RmqttServer;
use tokio::task::JoinHandle;

use crate::config::schema::MqttConfig;

use super::config;

/// Running RMQTT listener and the channel used by core services.
pub struct MqttServer {
    address: std::net::SocketAddr,
    readiness: Arc<AtomicBool>,
    task: JoinHandle<anyhow::Result<()>>,
}

impl MqttServer {
    /// Starts RMQTT in the current process and reports its bound address.
    pub async fn start(config: &MqttConfig, readiness: Arc<AtomicBool>) -> anyhow::Result<Self> {
        let listener = config::listener(config)?;
        let address = listener.local_addr()?;
        let context = ServerContext::new().busy_check_enable(false).build().await;
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
            task,
        })
    }

    /// Address actually bound by RMQTT, including a dynamically assigned port.
    pub fn local_addr(&self) -> std::net::SocketAddr {
        self.address
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
    }
}

impl Drop for MqttServer {
    fn drop(&mut self) {
        self.abort();
    }
}
