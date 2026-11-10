//! Bounded telemetry retention job (`02` §3.4).

use std::sync::Arc;
use std::time::Duration;

use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::config::runtime::RuntimeConfigStore;
use crate::core::auth::{self, SessionCleanupRepository};
use crate::core::telemetry::TelemetryRetentionRepository;
use crate::observability::metrics::Metrics;

const RETENTION_INTERVAL: Duration = Duration::from_secs(60 * 60);
const BATCH_SIZE: i64 = 5_000;
const BATCH_SIZE_U64: u64 = 5_000;
const INTER_BATCH_DELAY: Duration = Duration::from_millis(10);

pub(crate) fn spawn(
    telemetry: Arc<dyn TelemetryRetentionRepository>,
    sessions: Arc<dyn SessionCleanupRepository>,
    config: RuntimeConfigStore,
    cancel: CancellationToken,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(RETENTION_INTERVAL);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = cancel.cancelled() => break,
                _ = ticker.tick() => {
                    let result = tokio::select! {
                        _ = cancel.cancelled() => break,
                        result = run_once(telemetry.as_ref(), &config) => result,
                    };
                    if let Err(error) = result {
                        Metrics::global().telemetry_retention_errors_total.inc();
                        tracing::error!(%error, "telemetry retention batch failed");
                    }
                    let result = run_session_cleanup_once(
                        sessions.as_ref(),
                        auth::now_ms(),
                        &cancel,
                    )
                    .await;
                    if let Err(error) = result {
                        Metrics::global().session_cleanup_errors_total.inc();
                        tracing::error!(%error, "session cleanup batch failed");
                    }
                }
            }
        }
    })
}

/// Runs one retention sweep using a single hot-config snapshot.
pub(crate) async fn run_once(
    telemetry: &dyn TelemetryRetentionRepository,
    config: &RuntimeConfigStore,
) -> anyhow::Result<u64> {
    let settings = config.load();
    let days = i64::try_from(settings.config.telemetry.retention_days).map_err(|_| {
        anyhow::anyhow!("telemetry.retention_days does not fit signed milliseconds")
    })?;
    let cutoff = auth::now_ms().saturating_sub(days.saturating_mul(86_400_000));
    let mut deleted = 0;
    loop {
        let batch = telemetry.delete_before(cutoff, BATCH_SIZE).await?;
        deleted += batch;
        if batch < BATCH_SIZE_U64 {
            return Ok(deleted);
        }
        tokio::time::sleep(INTER_BATCH_DELAY).await;
    }
}

async fn run_session_cleanup_once(
    sessions: &dyn SessionCleanupRepository,
    now: i64,
    cancel: &CancellationToken,
) -> anyhow::Result<u64> {
    let mut deleted = 0;
    loop {
        let batch = tokio::select! {
            _ = cancel.cancelled() => return Ok(deleted),
            result = sessions.delete_expired_sessions(now) => result?,
        };
        deleted += batch;
        if batch < BATCH_SIZE_U64 {
            return Ok(deleted);
        }
        tokio::select! {
            _ = cancel.cancelled() => return Ok(deleted),
            _ = tokio::time::sleep(INTER_BATCH_DELAY) => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use sqlx::sqlite::SqlitePoolOptions;
    use tempfile::TempDir;

    use super::*;

    #[tokio::test]
    async fn next_retention_sweep_uses_the_updated_retention_days() {
        let dir = TempDir::new().unwrap();
        let config_path = dir.path().join("conf.toml");
        std::fs::write(
            &config_path,
            "config_version = 1\n\
             [server]\n\
             data_dir = \"./data\"\n\
             [database]\n\
             url = \"sqlite://./data/test.db\"\n",
        )
        .unwrap();
        let loaded = crate::config::load(&config_path).unwrap();
        let pool = sqlx::sqlite::SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .unwrap();
        let repository =
            crate::storage::sqlite::telemetry::SqliteTelemetryRepository::new(pool.clone());
        sqlx::query(
            "CREATE TABLE telemetry_events (
                internal_id INTEGER PRIMARY KEY,
                received_at INTEGER NOT NULL
             )",
        )
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query("CREATE INDEX telemetry_received_idx ON telemetry_events(received_at)")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query(
            "WITH digits(n) AS (VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9))
             INSERT INTO telemetry_events (received_at)
             SELECT ? FROM digits a CROSS JOIN digits b CROSS JOIN digits c CROSS JOIN digits d
             LIMIT 5001",
        )
        .bind(auth::now_ms() - 2 * 86_400_000)
        .execute(&pool)
        .await
        .unwrap();

        assert_eq!(
            run_once(&repository, &loaded.runtime_config).await.unwrap(),
            0
        );
        loaded
            .runtime_config
            .update(0, &serde_json::json!({"telemetry":{"retention_days":1}}))
            .await
            .unwrap();
        assert_eq!(
            run_once(&repository, &loaded.runtime_config).await.unwrap(),
            5_001
        );
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM telemetry_events")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(count, 0);
    }

    #[tokio::test]
    async fn session_cleanup_removes_expired_and_old_revoked_sessions_in_bounded_batches() {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .unwrap();
        let repository =
            crate::storage::sqlite::sessions::SqliteSessionCleanupRepository::new(pool.clone());
        crate::storage::sqlite::migrate::run(&pool).await.unwrap();
        sqlx::query(
            "INSERT INTO users (id, username, password_hash, enabled, created_at, updated_at)
             VALUES ('user', 'admin', 'hash', 1, 0, 0)",
        )
        .execute(&pool)
        .await
        .unwrap();
        let now = 604_801_100_i64;
        sqlx::query(
            "WITH RECURSIVE seq(n) AS (
                 SELECT 1 UNION ALL SELECT n + 1 FROM seq WHERE n < 5001
             )
             INSERT INTO sessions (id, user_id, token_hash, created_at, last_seen_at, expires_at)
             SELECT 'expired-' || n, 'user', CAST(printf('%032d', n) AS BLOB), 1, 1, 100
             FROM seq",
        )
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO sessions
                (id, user_id, token_hash, created_at, last_seen_at, expires_at, revoked_at)
             VALUES ('old-revoked', 'user', CAST(printf('%032d', 6000) AS BLOB), 1, 1, ?, 99),
                    ('live', 'user', CAST(printf('%032d', 7000) AS BLOB), 1, 1, ?, NULL)",
        )
        .bind(now + 100)
        .bind(now + 100)
        .execute(&pool)
        .await
        .unwrap();

        let deleted = run_session_cleanup_once(&repository, now, &CancellationToken::new())
            .await
            .unwrap();
        let remaining: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM sessions")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(deleted, 5_002);
        assert_eq!(remaining, 1);
    }
}
