//! SQLite pool with contract pragmas (`02` §3.1).

use std::time::Duration;

use sqlx::SqlitePool;
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous};

use crate::config::LoadedConfig;

/// Pool connections run these pragmas on every new connection:
/// WAL, `synchronous=FULL`, `foreign_keys=ON`, configured `busy_timeout`,
/// `wal_autocheckpoint=1000`.
pub async fn connect(loaded: &LoadedConfig) -> Result<SqlitePool, sqlx::Error> {
    let database = &loaded.config.database;
    let options: SqliteConnectOptions = loaded.database_url.parse()?;
    let options = options
        .create_if_missing(true)
        .journal_mode(SqliteJournalMode::Wal)
        // FULL is mandatory: a `stored` ACK is sent only after commit and must
        // survive power loss (02 §3.1); NORMAL needs a new ADR.
        .synchronous(SqliteSynchronous::Full)
        .foreign_keys(true)
        .busy_timeout(Duration::from_millis(database.busy_timeout_ms))
        .pragma("wal_autocheckpoint", "1000");

    // shortcut: SQLite has no separate connect timeout; use the configured
    // value as the pool acquire timeout until a real connect deadline exists.
    SqlitePoolOptions::new()
        .max_connections(database.max_connections as u32)
        .acquire_timeout(Duration::from_millis(database.connect_timeout_ms))
        .connect_with(options)
        .await
}
