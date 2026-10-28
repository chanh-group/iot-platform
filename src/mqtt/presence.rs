//! Maps RMQTT session lifecycle into the presence registry (`02` §2.10, §3.4).

use std::sync::Arc;

use async_trait::async_trait;
use rmqtt::hook::{Handler, HookResult, Parameter, ReturnType};
use sqlx::SqlitePool;

use crate::core::auth;
use crate::core::presence::Presence;
use crate::storage::sqlite::devices;

/// Keeps online state in RAM and `devices.last_seen_at` at connect/disconnect.
#[derive(Clone)]
pub struct PresenceHooks {
    pool: SqlitePool,
    presence: Arc<Presence>,
}

impl PresenceHooks {
    /// Hooks share the registry used by the rest of the server.
    #[must_use]
    pub fn new(pool: SqlitePool, presence: Arc<Presence>) -> Self {
        Self { pool, presence }
    }

    async fn record_last_seen(&self, device_id: &str) {
        // shortcut: failures are logged only; metric/readiness wiring lands with the
        // `/metrics` exporter in P2-13.
        if let Err(error) = devices::update_last_seen(&self.pool, device_id, auth::now_ms()).await {
            tracing::warn!(%error, %device_id, "cannot update devices.last_seen_at");
        }
    }
}

#[async_trait]
impl Handler for PresenceHooks {
    async fn hook(&self, param: &Parameter, _acc: Option<HookResult>) -> ReturnType {
        match param {
            Parameter::ClientConnected(session) => {
                let device_id: &str = &session.id.client_id;
                self.presence.connect(device_id, &session.id.to_string());
                self.record_last_seen(device_id).await;
            }
            Parameter::ClientDisconnected(session, _reason) => {
                let device_id: &str = &session.id.client_id;
                self.presence.disconnect(device_id, &session.id.to_string());
                self.record_last_seen(device_id).await;
            }
            _ => {}
        }
        (true, None)
    }
}
