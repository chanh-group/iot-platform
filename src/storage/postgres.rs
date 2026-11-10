//! PostgreSQL pool and migrations.

pub mod admin;
pub mod attributes;
pub mod commands;
pub mod devices;
pub mod sessions;
pub mod system;
pub mod telemetry;

use std::time::Duration;

use anyhow::{Context, anyhow};
use sqlx::PgPool;
use sqlx::migrate::{MigrateError, Migrator};
use sqlx::postgres::PgPoolOptions;
use tokio::time::sleep;

use crate::config::LoadedConfig;

static MIGRATOR: Migrator = sqlx::migrate!("./migrations/postgres");

const CONNECT_ATTEMPTS: u8 = 5;

pub async fn connect(loaded: &LoadedConfig) -> anyhow::Result<PgPool> {
    let options = crate::config::database::postgres_connect_options(&loaded.database_url)
        .map_err(anyhow::Error::msg)?;
    let max_connections = u32::try_from(loaded.config.database.max_connections)
        .context("database.max_connections is outside the PostgreSQL pool range")?;
    let acquire_timeout = Duration::from_millis(loaded.config.database.connect_timeout_ms);
    let mut attempt = 1;
    let mut backoff = Duration::from_millis(100);
    loop {
        let result = PgPoolOptions::new()
            .max_connections(max_connections)
            .acquire_timeout(acquire_timeout)
            .connect_with(options.clone())
            .await;
        match result {
            Ok(pool) => return Ok(pool),
            Err(_) if attempt == CONNECT_ATTEMPTS => {
                return Err(anyhow!(
                    "PostgreSQL connection failed after bounded retries"
                ));
            }
            Err(_) => {
                attempt += 1;
                sleep(backoff).await;
                backoff = (backoff * 2).min(Duration::from_millis(800));
            }
        }
    }
}

pub async fn migrate(pool: &PgPool) -> Result<(), MigrateError> {
    MIGRATOR.run(pool).await
}
