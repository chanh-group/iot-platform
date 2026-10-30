//! Maps RMQTT session lifecycle into the presence registry (`02` §2.10, §3.4).

use std::collections::HashMap;
use std::fmt::Display;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use rmqtt::hook::{Handler, HookResult, Parameter, ReturnType};
use sqlx::SqlitePool;
use tokio_util::sync::CancellationToken;

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
    readiness: Arc<AtomicBool>,
    readiness_transition: Arc<Mutex<()>>,
    listener_stopped: AtomicBool,
    failure: CancellationToken,
}

#[derive(Debug, thiserror::Error)]
#[error("MQTT connection limiter state is poisoned")]
pub(crate) struct ConnectionLimiterError;

impl ConnectionLimiter {
    #[must_use]
    pub(crate) fn new(
        maximum: usize,
        readiness: Arc<AtomicBool>,
        readiness_transition: Arc<Mutex<()>>,
        failure: CancellationToken,
    ) -> Self {
        Self {
            maximum,
            state: Mutex::new(ConnectionState::default()),
            readiness,
            readiness_transition,
            listener_stopped: AtomicBool::new(false),
            failure,
        }
    }

    pub(crate) fn try_reserve(&self, device_id: &str) -> Result<bool, ConnectionLimiterError> {
        let now = Instant::now();
        let mut state = self.lock()?;
        state.pending.retain(|_, expires_at| *expires_at > now);
        if state.pending.contains_key(device_id) {
            return Ok(true);
        }
        if state.active.contains_key(device_id) {
            state
                .pending
                .insert(device_id.to_owned(), now + CONNECTION_RESERVATION_TTL);
            return Ok(true);
        }
        let pending_without_active = state
            .pending
            .keys()
            .filter(|device_id| !state.active.contains_key(*device_id))
            .count();
        if state.active.len().saturating_add(pending_without_active) >= self.maximum {
            return Ok(false);
        }
        // shortcut: failed handshakes hold a slot for 30s; revisit if the broker timeout changes.
        state
            .pending
            .insert(device_id.to_owned(), now + CONNECTION_RESERVATION_TTL);
        Ok(true)
    }

    fn connected(&self, device_id: &str, generation: &str) -> Result<(), ConnectionLimiterError> {
        let mut state = self.lock()?;
        state.pending.remove(device_id);
        state
            .active
            .insert(device_id.to_owned(), generation.to_owned());
        Ok(())
    }

    fn disconnected(
        &self,
        device_id: &str,
        generation: &str,
    ) -> Result<(), ConnectionLimiterError> {
        let mut state = self.lock()?;
        if state
            .active
            .get(device_id)
            .is_some_and(|current| current == generation)
        {
            state.active.remove(device_id);
        }
        Ok(())
    }

    fn lock(&self) -> Result<MutexGuard<'_, ConnectionState>, ConnectionLimiterError> {
        self.state.lock().map_err(|_| {
            self.report_poisoned_state("MQTT connection limiter mutex poisoned");
            ConnectionLimiterError
        })
    }

    pub(crate) fn report_poisoned_state(&self, error: impl Display) {
        let _transition = self.readiness_transition.lock();
        self.readiness.store(false, Ordering::Release);
        if !self.failure.is_cancelled() {
            tracing::error!(%error, "MQTT presence state is inconsistent; stopping listener");
            self.failure.cancel();
        }
    }

    pub(crate) fn publish_ready(&self) {
        let transition = self.readiness_transition.lock();
        let transition_ok = transition.is_ok();
        let ready = transition_ok
            && !self.failure.is_cancelled()
            && !self.listener_stopped.load(Ordering::Acquire);
        self.readiness.store(ready, Ordering::Release);
        if !transition_ok {
            self.failure.cancel();
        }
    }

    pub(crate) fn listener_stopped(&self) {
        let _transition = self.readiness_transition.lock();
        self.listener_stopped.store(true, Ordering::Release);
        self.readiness.store(false, Ordering::Release);
    }

    pub(crate) fn failure(&self) -> &CancellationToken {
        &self.failure
    }

    #[cfg(test)]
    pub(crate) fn poison_state_for_test(&self) {
        let join = std::thread::scope(|scope| {
            scope
                .spawn(|| {
                    let _guard = self.state.lock().unwrap();
                    panic!("poison connection limiter mutex for test");
                })
                .join()
        });
        assert!(join.is_err());
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
                if let Err(error) = self.connections.connected(device_id, &generation) {
                    self.connections.report_poisoned_state(error);
                    return (true, None);
                }
                match self.presence.connect(device_id, &generation) {
                    Ok(true) => Metrics::global().mqtt_connections_active.inc(),
                    Ok(false) => {}
                    Err(error) => {
                        self.connections.report_poisoned_state(error);
                        return (true, None);
                    }
                }
                self.record_last_seen(device_id).await;
            }
            Parameter::ClientDisconnected(session, _reason) => {
                let device_id: &str = &session.id.client_id;
                let generation = session.id.to_string();
                if let Err(error) = self.connections.disconnected(device_id, &generation) {
                    self.connections.report_poisoned_state(error);
                    return (true, None);
                }
                match self.presence.disconnect(device_id, &generation) {
                    Ok(true) => Metrics::global().mqtt_connections_active.dec(),
                    Ok(false) => {}
                    Err(error) => {
                        self.connections.report_poisoned_state(error);
                        return (true, None);
                    }
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
        let limiter = ConnectionLimiter::new(
            1,
            Arc::new(AtomicBool::new(true)),
            Arc::new(Mutex::new(())),
            CancellationToken::new(),
        );
        assert!(limiter.try_reserve("device-a").unwrap());
        assert!(!limiter.try_reserve("device-b").unwrap());

        limiter.connected("device-a", "session-1").unwrap();
        assert!(
            limiter.try_reserve("device-a").unwrap(),
            "duplicate id reuses its slot"
        );
        limiter.disconnected("device-a", "session-1").unwrap();
        assert!(!limiter.try_reserve("device-b").unwrap());

        limiter.connected("device-a", "session-2").unwrap();
        assert!(!limiter.try_reserve("device-b").unwrap());
        limiter.disconnected("device-a", "session-2").unwrap();
        assert!(limiter.try_reserve("device-b").unwrap());
    }

    #[test]
    fn failed_reconnect_reservation_does_not_double_count_an_active_device() {
        let limiter = ConnectionLimiter::new(
            2,
            Arc::new(AtomicBool::new(true)),
            Arc::new(Mutex::new(())),
            CancellationToken::new(),
        );
        limiter.connected("device-a", "session-a").unwrap();

        assert!(limiter.try_reserve("device-a").unwrap());
        assert!(limiter.try_reserve("device-b").unwrap());
        limiter.connected("device-b", "session-b").unwrap();
        assert!(
            limiter.lock().unwrap().pending.contains_key("device-a"),
            "leaving the reconnect reservation pending models an abandoned handshake"
        );
        assert!(!limiter.try_reserve("device-c").unwrap());
    }

    #[test]
    fn poisoned_limiter_fails_closed_and_signals_shutdown() {
        let readiness = Arc::new(AtomicBool::new(true));
        let failure = CancellationToken::new();
        let limiter = Arc::new(ConnectionLimiter::new(
            1,
            Arc::clone(&readiness),
            Arc::new(Mutex::new(())),
            failure.clone(),
        ));
        limiter.poison_state_for_test();

        assert!(limiter.try_reserve("device-a").is_err());
        assert!(!readiness.load(Ordering::Acquire));
        assert!(failure.is_cancelled());
    }

    #[test]
    fn concurrent_startup_and_presence_failure_leave_readiness_down() {
        let readiness = Arc::new(AtomicBool::new(false));
        let limiter = Arc::new(ConnectionLimiter::new(
            1,
            Arc::clone(&readiness),
            Arc::new(Mutex::new(())),
            CancellationToken::new(),
        ));
        let barrier = Arc::new(std::sync::Barrier::new(3));

        let startup_limiter = Arc::clone(&limiter);
        let startup_barrier = Arc::clone(&barrier);
        let startup = std::thread::spawn(move || {
            startup_barrier.wait();
            startup_limiter.publish_ready();
        });

        let failure_limiter = Arc::clone(&limiter);
        let failure_barrier = Arc::clone(&barrier);
        let failure = std::thread::spawn(move || {
            failure_barrier.wait();
            failure_limiter.report_poisoned_state("test presence poison");
        });

        barrier.wait();
        startup.join().unwrap();
        failure.join().unwrap();

        assert!(!readiness.load(Ordering::Acquire));
        assert!(limiter.failure().is_cancelled());
    }

    #[tokio::test]
    async fn listener_exit_before_startup_publish_keeps_readiness_down() {
        let readiness = Arc::new(AtomicBool::new(false));
        let limiter = Arc::new(ConnectionLimiter::new(
            1,
            Arc::clone(&readiness),
            Arc::new(Mutex::new(())),
            CancellationToken::new(),
        ));
        let task_limiter = Arc::clone(&limiter);

        tokio::spawn(async move {
            task_limiter.listener_stopped();
        })
        .await
        .unwrap();
        limiter.publish_ready();

        assert!(!readiness.load(Ordering::Acquire));
        assert!(!limiter.failure().is_cancelled());
    }
}
