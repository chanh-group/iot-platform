//! Maps RMQTT session lifecycle into the presence registry (`02` §2.10, §3.4).

use std::collections::HashMap;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use rmqtt::hook::{Handler, HookResult, Parameter, ReturnType};
use sqlx::SqlitePool;

use crate::core::auth;
use crate::core::presence::Presence;
use crate::observability::metrics::Metrics;
use crate::storage::sqlite::devices;

const CONNECTION_RESERVATION_TTL: Duration = Duration::from_secs(30);

#[derive(Default)]
struct ConnectionState {
    active: HashMap<String, String>,
    pending: HashMap<String, Instant>,
}

/// Bounds authenticated connections, including handshakes not yet in presence.
pub(crate) struct ConnectionLimiter {
    maximum: usize,
    state: Mutex<ConnectionState>,
}

impl ConnectionLimiter {
    #[must_use]
    pub(crate) fn new(maximum: usize) -> Self {
        Self {
            maximum,
            state: Mutex::new(ConnectionState::default()),
        }
    }

    #[must_use]
    pub(crate) fn try_reserve(&self, device_id: &str) -> bool {
        let now = Instant::now();
        let mut state = self.lock();
        state.pending.retain(|_, expires_at| *expires_at > now);
        if state.pending.contains_key(device_id) {
            return true;
        }
        if state.active.contains_key(device_id) {
            state
                .pending
                .insert(device_id.to_owned(), now + CONNECTION_RESERVATION_TTL);
            return true;
        }
        let pending_without_active = state
            .pending
            .keys()
            .filter(|device_id| !state.active.contains_key(*device_id))
            .count();
        if state.active.len().saturating_add(pending_without_active) >= self.maximum {
            return false;
        }
        // shortcut: failed handshakes hold a slot for 30s; revisit if the broker timeout changes.
        state
            .pending
            .insert(device_id.to_owned(), now + CONNECTION_RESERVATION_TTL);
        true
    }

    fn connected(&self, device_id: &str, generation: &str) {
        let mut state = self.lock();
        state.pending.remove(device_id);
        state
            .active
            .insert(device_id.to_owned(), generation.to_owned());
    }

    fn disconnected(&self, device_id: &str, generation: &str) {
        let mut state = self.lock();
        if state
            .active
            .get(device_id)
            .is_some_and(|current| current == generation)
        {
            state.active.remove(device_id);
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, ConnectionState> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// Keeps online state in RAM and `devices.last_seen_at` at connect/disconnect.
#[derive(Clone)]
pub struct PresenceHooks {
    pool: SqlitePool,
    presence: Arc<Presence>,
    connections: Arc<ConnectionLimiter>,
}

impl PresenceHooks {
    /// Hooks share the registry used by the rest of the server.
    #[must_use]
    pub(crate) fn new(
        pool: SqlitePool,
        presence: Arc<Presence>,
        connections: Arc<ConnectionLimiter>,
    ) -> Self {
        Self {
            pool,
            presence,
            connections,
        }
    }

    async fn record_last_seen(&self, device_id: &str) {
        // Last-seen persistence is advisory and must not affect broker readiness.
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
                let generation = session.id.to_string();
                self.connections.connected(device_id, &generation);
                if self.presence.connect(device_id, &generation) {
                    Metrics::global().mqtt_connections_active.inc();
                }
                self.record_last_seen(device_id).await;
            }
            Parameter::ClientDisconnected(session, _reason) => {
                let device_id: &str = &session.id.client_id;
                let generation = session.id.to_string();
                self.connections.disconnected(device_id, &generation);
                if self.presence.disconnect(device_id, &generation) {
                    Metrics::global().mqtt_connections_active.dec();
                }
                self.record_last_seen(device_id).await;
            }
            _ => {}
        }
        (true, None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn limiter_counts_pending_and_active_connections_without_stale_disconnects() {
        let limiter = ConnectionLimiter::new(1);
        assert!(limiter.try_reserve("device-a"));
        assert!(!limiter.try_reserve("device-b"));

        limiter.connected("device-a", "session-1");
        assert!(
            limiter.try_reserve("device-a"),
            "duplicate id reuses its slot"
        );
        limiter.disconnected("device-a", "session-1");
        assert!(!limiter.try_reserve("device-b"));

        limiter.connected("device-a", "session-2");
        assert!(!limiter.try_reserve("device-b"));
        limiter.disconnected("device-a", "session-2");
        assert!(limiter.try_reserve("device-b"));
    }

    #[test]
    fn failed_reconnect_reservation_does_not_double_count_an_active_device() {
        let limiter = ConnectionLimiter::new(2);
        limiter.connected("device-a", "session-a");

        assert!(limiter.try_reserve("device-a"));
        assert!(limiter.try_reserve("device-b"));
        limiter.connected("device-b", "session-b");
        assert!(
            limiter.lock().pending.contains_key("device-a"),
            "leaving the reconnect reservation pending models an abandoned handshake"
        );
        assert!(!limiter.try_reserve("device-c"));
    }
}
