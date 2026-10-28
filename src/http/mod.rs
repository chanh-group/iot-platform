//! Axum router, middleware, handlers, and static assets.

pub mod assets;
pub mod auth;
pub mod error;
pub mod middleware;
pub mod router;

use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::Duration;

use sqlx::SqlitePool;

use crate::config::LoadedConfig;
use middleware::LoginRateLimiter;

#[derive(Clone)]
pub struct AppState {
    pub pool: SqlitePool,
    pub config: Arc<LoadedConfig>,
    pub login_limiter: Arc<LoginRateLimiter>,
    pub ready: Arc<AtomicBool>,
}

impl AppState {
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
