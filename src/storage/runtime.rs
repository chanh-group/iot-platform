//! Backend-selected pools, migrations, and repository adapters.

use std::sync::Arc;

use sqlx::{PgPool, SqlitePool};

use crate::config::{DatabaseBackend, LoadedConfig};
use crate::core::attributes::AttributesRepository;
use crate::core::auth::{AdminRepository, SessionCleanupRepository};
use crate::core::command::CommandRepository;
use crate::core::device::DeviceRepository;
use crate::core::storage::StorageRepository;
use crate::core::telemetry::{
    TelemetryReadRepository, TelemetryRepository, TelemetryRetentionRepository,
};

#[derive(Clone)]
pub struct StorageRepositories {
    pub admin: Arc<dyn AdminRepository>,
    pub commands: Arc<dyn CommandRepository>,
    pub devices: Arc<dyn DeviceRepository>,
    pub attributes: Arc<dyn AttributesRepository>,
    pub storage: Arc<dyn StorageRepository>,
    pub telemetry: Arc<dyn TelemetryRepository>,
    pub telemetry_read: Arc<dyn TelemetryReadRepository>,
    pub telemetry_retention: Arc<dyn TelemetryRetentionRepository>,
    pub sessions: Arc<dyn SessionCleanupRepository>,
}

enum Pool {
    Sqlite(SqlitePool),
    Postgres(PgPool),
}

pub struct StorageRuntime {
    pool: Pool,
    pub repositories: StorageRepositories,
}

impl StorageRuntime {
    pub async fn connect(loaded: &LoadedConfig) -> anyhow::Result<Self> {
        let (pool, repositories) = match loaded.database_backend {
            DatabaseBackend::Sqlite => {
                let pool = crate::storage::sqlite::pool::connect(loaded).await?;
                let telemetry = Arc::new(
                    crate::storage::sqlite::telemetry::SqliteTelemetryRepository::new(pool.clone()),
                );
                let repositories = StorageRepositories {
                    admin: Arc::new(crate::storage::sqlite::admin::SqliteAdminRepository::new(
                        pool.clone(),
                    )),
                    commands: Arc::new(
                        crate::storage::sqlite::commands::SqliteCommandRepository::new(
                            pool.clone(),
                        ),
                    ),
                    devices: Arc::new(
                        crate::storage::sqlite::devices::SqliteDeviceRepository::new(pool.clone()),
                    ),
                    attributes: Arc::new(
                        crate::storage::sqlite::attributes::SqliteAttributesRepository::new(
                            pool.clone(),
                        ),
                    ),
                    storage: Arc::new(crate::storage::sqlite::system::SqliteSystemRepository::new(
                        pool.clone(),
                    )),
                    telemetry: Arc::clone(&telemetry) as Arc<dyn TelemetryRepository>,
                    telemetry_read: Arc::clone(&telemetry) as Arc<dyn TelemetryReadRepository>,
                    telemetry_retention: telemetry as Arc<dyn TelemetryRetentionRepository>,
                    sessions: Arc::new(
                        crate::storage::sqlite::sessions::SqliteSessionCleanupRepository::new(
                            pool.clone(),
                        ),
                    ),
                };
                (Pool::Sqlite(pool), repositories)
            }
            DatabaseBackend::Postgres => {
                let pool = crate::storage::postgres::connect(loaded).await?;
                let telemetry = Arc::new(
                    crate::storage::postgres::telemetry::PostgresTelemetryRepository::new(
                        pool.clone(),
                    ),
                );
                let repositories = StorageRepositories {
                    admin: Arc::new(
                        crate::storage::postgres::admin::PostgresAdminRepository::new(pool.clone()),
                    ),
                    commands: Arc::new(
                        crate::storage::postgres::commands::PostgresCommandRepository::new(
                            pool.clone(),
                        ),
                    ),
                    devices: Arc::new(
                        crate::storage::postgres::devices::PostgresDeviceRepository::new(
                            pool.clone(),
                        ),
                    ),
                    attributes: Arc::new(
                        crate::storage::postgres::attributes::PostgresAttributesRepository::new(
                            pool.clone(),
                        ),
                    ),
                    storage: Arc::new(
                        crate::storage::postgres::system::PostgresSystemRepository::new(
                            pool.clone(),
                        ),
                    ),
                    telemetry: Arc::clone(&telemetry) as Arc<dyn TelemetryRepository>,
                    telemetry_read: Arc::clone(&telemetry) as Arc<dyn TelemetryReadRepository>,
                    telemetry_retention: telemetry as Arc<dyn TelemetryRetentionRepository>,
                    sessions: Arc::new(
                        crate::storage::postgres::sessions::PostgresSessionCleanupRepository::new(
                            pool.clone(),
                        ),
                    ),
                };
                (Pool::Postgres(pool), repositories)
            }
        };
        Ok(Self { pool, repositories })
    }

    pub async fn migrate(&self) -> anyhow::Result<()> {
        match &self.pool {
            Pool::Sqlite(pool) => crate::storage::sqlite::migrate::run(pool).await?,
            Pool::Postgres(pool) => crate::storage::postgres::migrate(pool).await?,
        }
        Ok(())
    }

    pub async fn close(self) {
        match self.pool {
            Pool::Sqlite(pool) => pool.close().await,
            Pool::Postgres(pool) => pool.close().await,
        }
    }
}
