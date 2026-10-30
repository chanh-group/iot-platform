//! Axum router, middleware, handlers, and static assets.

pub mod api_error;
pub mod assets;
pub mod auth;
pub mod devices;
pub mod events;
pub mod metrics;
pub mod middleware;
pub mod router;
pub mod telemetry;

use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::Duration;

use sqlx::SqlitePool;
use tokio::sync::Semaphore;

use crate::config::LoadedConfig;
use crate::core::events::EventBus;
use crate::core::presence::Presence;
use crate::mqtt::server::PresenceFailure;
use middleware::LoginRateLimiter;

/// Shared handler state: database pool, config, login limiter, and task readiness.
#[derive(Clone)]
pub struct AppState {
    /// SQLite connection pool.
    pub pool: SqlitePool,
    /// Loaded and validated configuration.
    pub config: Arc<LoadedConfig>,
    /// Shared failed-login window.
    pub login_limiter: Arc<LoginRateLimiter>,
    /// Cleared during shutdown so `/readyz` reports not-ready.
    pub ready: Arc<AtomicBool>,
    /// Cleared if the MQTT listener stops.
    pub mqtt_ready: Arc<AtomicBool>,
    /// Cleared while telemetry commits are failing and retried.
    pub telemetry_ready: Arc<AtomicBool>,
    /// Best-effort event stream shared with the telemetry worker.
    pub events: EventBus,
    /// Registry shared with the MQTT listener.
    pub presence: Arc<Presence>,
    /// Signals a poisoned presence registry to the MQTT supervisor.
    pub presence_failure: PresenceFailure,
    /// Limits active WebSocket connections before upgrade.
    pub ws_connections: Arc<Semaphore>,
}

impl AppState {
    /// Builds state with a fresh limiter and readiness set to `true`.
    pub fn new(
        pool: SqlitePool,
        config: Arc<LoadedConfig>,
        mqtt_ready: Arc<AtomicBool>,
        telemetry_ready: Arc<AtomicBool>,
        events: EventBus,
        presence: Arc<Presence>,
        presence_failure: PresenceFailure,
    ) -> Self {
        let max_ws_connections =
            usize::try_from(config.config.http.max_ws_connections).unwrap_or(usize::MAX);
        let limiter = LoginRateLimiter::new(
            config.config.auth.login_max_attempts,
            Duration::from_millis(config.config.auth.login_window_ms),
        );
        Self {
            pool,
            config,
            login_limiter: Arc::new(limiter),
            ready: Arc::new(AtomicBool::new(true)),
            mqtt_ready,
            telemetry_ready,
            events,
            presence,
            presence_failure,
            ws_connections: Arc::new(Semaphore::new(max_ws_connections)),
        }
    }
}
