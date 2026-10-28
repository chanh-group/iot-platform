//! Embedded SQLx migrations (`02` §3.1).

use sqlx::SqlitePool;
use sqlx::migrate::{MigrateError, Migrator};

static MIGRATOR: Migrator = sqlx::migrate!("./migrations/sqlite");

/// Runs before any listener. Idempotent: SQLx records applied versions in
/// `_sqlx_migrations`.
pub async fn run(pool: &SqlitePool) -> Result<(), MigrateError> {
    MIGRATOR.run(pool).await
}
