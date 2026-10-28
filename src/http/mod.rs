//! Axum router, middleware, handlers, and static assets.

pub mod api_error;
pub mod assets;
pub mod auth;
pub mod middleware;
pub mod router;

use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::Duration;

use sqlx::SqlitePool;

use crate::config::LoadedConfig;
use middleware::LoginRateLimiter;

/// Shared handler state: database pool, config, login limiter, readiness flag.
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
}

impl AppState {
    /// Builds state with a fresh limiter and readiness set to `true`.
    pub fn new(pool: SqlitePool, config: Arc<LoadedConfig>) -> Self {
        let limiter = LoginRateLimiter::new(
            config.config.auth.login_max_attempts,
            Duration::from_millis(config.config.auth.login_window_ms),
        );
        Self {
            pool,
            config,
            login_limiter: Arc::new(limiter),
            ready: Arc::new(AtomicBool::new(true)),
        }
    }
}
