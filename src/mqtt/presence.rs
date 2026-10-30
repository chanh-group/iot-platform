//! Maps RMQTT session lifecycle into the presence registry (`02` §2.10, §3.4).

use std::collections::{HashMap, HashSet};
use std::fmt::Display;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use rmqtt::hook::{Handler, HookResult, Parameter, ReturnType};
use sqlx::SqlitePool;
use tokio::sync::{oneshot, watch};
use tokio_util::sync::CancellationToken;

use crate::core::auth;
use crate::core::presence::Presence;
use crate::observability::metrics::Metrics;
use crate::storage::sqlite::devices;

use super::server::DeviceKickQueue;

const CONNECTION_RESERVATION_TTL: Duration = Duration::from_secs(30);

#[derive(Default)]
struct ConnectionState {
    active: HashMap<String, String>,
    pending: HashMap<String, PendingConnections>,
    blocked: HashMap<String, usize>,
    // Retains the deny fence only while an authenticated or pending connection exists.
    disabled: HashSet<String>,
}

struct PendingConnections {
    expires_at: Instant,
    count: usize,
}

/// Bounds authenticated connections, including handshakes not yet in presence.
pub(crate) struct ConnectionLimiter {
    maximum: usize,
    state: Mutex<ConnectionState>,
    readiness: Arc<AtomicBool>,
    readiness_transition: Arc<Mutex<()>>,
    state_changed: watch::Sender<u64>,
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
        let (state_changed, _) = watch::channel(0);
        Self {
            maximum,
            state: Mutex::new(ConnectionState::default()),
            readiness,
            readiness_transition,
            state_changed,
            listener_stopped: AtomicBool::new(false),
            failure,
        }
    }

    pub(crate) fn admission(self: &Arc<Self>) -> DeviceAdmission {
        DeviceAdmission(Arc::clone(self))
    }

    pub(crate) fn try_reserve(&self, device_id: &str) -> Result<bool, ConnectionLimiterError> {
        let now = Instant::now();
        let mut state = self.lock()?;
        state.pending.retain(|_, pending| pending.expires_at > now);
        if state.blocked.contains_key(device_id) {
            return Ok(false);
        }
        if let Some(pending) = state.pending.get_mut(device_id) {
            pending.count = pending.count.saturating_add(1);
            pending.expires_at = now + CONNECTION_RESERVATION_TTL;
            return Ok(true);
        }
        if state.active.contains_key(device_id) {
            state.pending.insert(
                device_id.to_owned(),
                PendingConnections {
                    expires_at: now + CONNECTION_RESERVATION_TTL,
                    count: 1,
                },
            );
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
        state.pending.insert(
            device_id.to_owned(),
            PendingConnections {
                expires_at: now + CONNECTION_RESERVATION_TTL,
                count: 1,
            },
        );
        Ok(true)
    }

    pub(crate) fn cancel_reservation(&self, device_id: &str) -> Result<(), ConnectionLimiterError> {
        let mut state = self.lock()?;
        release_pending(&mut state, device_id);
        self.state_changed
            .send_modify(|version| *version = version.wrapping_add(1));
        Ok(())
    }

    pub(crate) fn set_device_enabled(
        &self,
        device_id: &str,
        enabled: bool,
    ) -> Result<(), ConnectionLimiterError> {
        let mut state = self.lock()?;
        if enabled {
            state.disabled.remove(device_id);
        } else if state.active.contains_key(device_id) || state.pending.contains_key(device_id) {
            state.disabled.insert(device_id.to_owned());
        } else {
            state.disabled.remove(device_id);
        }
        self.state_changed
            .send_modify(|version| *version = version.wrapping_add(1));
        Ok(())
    }

    pub(crate) fn device_can_exchange(
        &self,
        device_id: &str,
    ) -> Result<bool, ConnectionLimiterError> {
        let state = self.lock()?;
        Ok(!state.disabled.contains(device_id) && !state.blocked.contains_key(device_id))
    }

    pub(crate) fn connected(
        &self,
        device_id: &str,
        generation: &str,
    ) -> Result<(), ConnectionLimiterError> {
        let mut state = self.lock()?;
        release_pending(&mut state, device_id);
        state
            .active
            .insert(device_id.to_owned(), generation.to_owned());
        self.state_changed
            .send_modify(|version| *version = version.wrapping_add(1));
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
        if !state.active.contains_key(device_id) && !state.pending.contains_key(device_id) {
            state.disabled.remove(device_id);
        }
        self.state_changed
            .send_modify(|version| *version = version.wrapping_add(1));
        Ok(())
    }

    fn lock(&self) -> Result<MutexGuard<'_, ConnectionState>, ConnectionLimiterError> {
        self.state.lock().map_err(|_| {
            self.report_failure("MQTT connection limiter mutex poisoned");
            ConnectionLimiterError
        })
    }

    pub(crate) fn report_failure(&self, error: impl Display) {
        let _transition = self.readiness_transition.lock();
        self.readiness.store(false, Ordering::Release);
        if !self.failure.is_cancelled() {
            tracing::error!(%error, "MQTT is unhealthy; stopping listener");
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

    #[cfg(test)]
    pub(crate) fn device_is_blocked_for_test(&self, device_id: &str) -> bool {
        self.state
            .lock()
            .is_ok_and(|state| state.blocked.contains_key(device_id))
    }
}

fn release_pending(state: &mut ConnectionState, device_id: &str) {
    if let Some(pending) = state.pending.get_mut(device_id) {
        if pending.count > 1 {
            pending.count -= 1;
        } else {
            state.pending.remove(device_id);
        }
    }
}

#[derive(Clone)]
pub(crate) struct DeviceAdmission(Arc<ConnectionLimiter>);

pub(crate) struct DeviceAdmissionGuard {
    limiter: Arc<ConnectionLimiter>,
    device_id: String,
}

impl DeviceAdmission {
    pub(crate) fn block(
        &self,
        device_id: &str,
    ) -> Result<DeviceAdmissionGuard, ConnectionLimiterError> {
        let mut state = self.0.lock()?;
        let count = state.blocked.entry(device_id.to_owned()).or_default();
        *count = count.saturating_add(1);
        Ok(DeviceAdmissionGuard {
            limiter: Arc::clone(&self.0),
            device_id: device_id.to_owned(),
        })
    }

    pub(crate) fn set_device_enabled(
        &self,
        device_id: &str,
        enabled: bool,
    ) -> Result<(), ConnectionLimiterError> {
        self.0.set_device_enabled(device_id, enabled)
    }
}

impl DeviceAdmissionGuard {
    pub(crate) fn set_device_enabled(&self, enabled: bool) -> Result<(), ConnectionLimiterError> {
        self.limiter.set_device_enabled(&self.device_id, enabled)
    }

    pub(crate) async fn wait_for_pending(&self) -> Result<(), ConnectionLimiterError> {
        let mut changed = self.limiter.state_changed.subscribe();
        loop {
            let _ = *changed.borrow_and_update();
            let expires_at = {
                let mut state = self.limiter.lock()?;
                let now = Instant::now();
                state.pending.retain(|_, pending| pending.expires_at > now);
                state
                    .pending
                    .get(&self.device_id)
                    .map(|pending| pending.expires_at)
            };
            let Some(expires_at) = expires_at else {
                return Ok(());
            };
            tokio::select! {
                result = changed.changed() => {
                    if result.is_err() {
                        return Err(ConnectionLimiterError);
                    }
                }
                _ = tokio::time::sleep_until(tokio::time::Instant::from_std(expires_at)) => {}
            }
        }
    }
}

impl Drop for DeviceAdmissionGuard {
    fn drop(&mut self) {
        match self.limiter.state.lock() {
            Ok(mut state) => {
                if let Some(count) = state.blocked.get_mut(&self.device_id) {
                    if *count > 1 {
                        *count -= 1;
                    } else {
                        state.blocked.remove(&self.device_id);
                    }
                }
                self.limiter
                    .state_changed
                    .send_modify(|version| *version = version.wrapping_add(1));
            }
            Err(_) => self
                .limiter
                .report_failure("MQTT connection limiter state is poisoned"),
        }
    }
}

/// Keeps online state in RAM and `devices.last_seen_at` at connect/disconnect.
#[derive(Clone)]
pub struct PresenceHooks {
    pool: SqlitePool,
    presence: Arc<Presence>,
    connections: Arc<ConnectionLimiter>,
    kicks: DeviceKickQueue,
}

impl PresenceHooks {
    /// Hooks share the registry used by the rest of the server.
    #[must_use]
    pub(crate) fn new(
        pool: SqlitePool,
        presence: Arc<Presence>,
        connections: Arc<ConnectionLimiter>,
        kicks: DeviceKickQueue,
    ) -> Self {
        Self {
            pool,
            presence,
            connections,
            kicks,
        }
    }

    async fn record_last_seen(&self, device_id: &str) {
        // Last-seen persistence is advisory and must not affect broker readiness.
        if let Err(error) = devices::update_last_seen(&self.pool, device_id, auth::now_ms()).await {
            tracing::warn!(%error, %device_id, "cannot update devices.last_seen_at");
        }
    }

    fn enqueue_kick(&self, device_id: &str) -> Result<(), ()> {
        let admission = self
            .connections
            .admission()
            .block(device_id)
            .map_err(|_| ())?;
        let permit = self.kicks.try_reserve()?;
        let (committed, decision) = oneshot::channel();
        let completion = permit.enqueue(device_id.to_owned(), admission, decision);
        let _ = committed.send(true);
        drop(completion);
        Ok(())
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
                    self.connections.report_failure(error);
                    return (true, None);
                }
                match devices::active_credential(&self.pool, device_id).await {
                    Ok(Some(credential)) if credential.device_enabled => {
                        if let Err(error) = self.connections.set_device_enabled(device_id, true) {
                            self.connections.report_failure(error);
                            return (true, None);
                        }
                    }
                    Ok(_) => {
                        if let Err(error) = self.connections.set_device_enabled(device_id, false) {
                            self.connections.report_failure(error);
                            return (true, None);
                        }
                        if self.enqueue_kick(device_id).is_err() {
                            self.connections
                                .report_failure("cannot queue kick for a disabled device session");
                        }
                        return (true, None);
                    }
                    Err(error) => {
                        if let Err(state_error) =
                            self.connections.set_device_enabled(device_id, false)
                        {
                            self.connections.report_failure(state_error);
                        }
                        if self.enqueue_kick(device_id).is_err() {
                            tracing::error!("cannot queue kick for an unverified device session");
                        }
                        self.connections.report_failure(error);
                        return (true, None);
                    }
                }
                match self.presence.connect(device_id, &generation) {
                    Ok(true) => Metrics::global().mqtt_connections_active.inc(),
                    Ok(false) => {}
                    Err(error) => {
                        self.connections.report_failure(error);
                        return (true, None);
                    }
                }
                self.record_last_seen(device_id).await;
            }
            Parameter::ClientDisconnected(session, _reason) => {
                let device_id: &str = &session.id.client_id;
                let generation = session.id.to_string();
                if let Err(error) = self.connections.disconnected(device_id, &generation) {
                    self.connections.report_failure(error);
                    return (true, None);
                }
                match self.presence.disconnect(device_id, &generation) {
                    Ok(true) => Metrics::global().mqtt_connections_active.dec(),
                    Ok(false) => {}
                    Err(error) => {
                        self.connections.report_failure(error);
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

    #[tokio::test]
    async fn device_disable_gate_waits_for_each_allowed_handshake() {
        let limiter = Arc::new(ConnectionLimiter::new(
            2,
            Arc::new(AtomicBool::new(true)),
            Arc::new(Mutex::new(())),
            CancellationToken::new(),
        ));
        assert!(limiter.try_reserve("device").unwrap());
        assert!(limiter.try_reserve("device").unwrap());

        let guard = limiter.admission().block("device").unwrap();
        let waiting = tokio::spawn(async move {
            guard.wait_for_pending().await.unwrap();
            guard
        });
        tokio::task::yield_now().await;
        assert!(!waiting.is_finished());
        assert!(!limiter.try_reserve("device").unwrap());

        limiter.connected("device", "session-1").unwrap();
        tokio::task::yield_now().await;
        assert!(!waiting.is_finished());
        limiter.connected("device", "session-2").unwrap();
        let guard = waiting.await.unwrap();
        assert!(limiter.lock().unwrap().pending.is_empty());

        // The disable transaction/kick owns this guard; only after that work
        // completes can a later authentication reserve another handshake.
        drop(guard);
        assert!(limiter.try_reserve("device").unwrap());
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
            failure_limiter.report_failure("test presence poison");
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
