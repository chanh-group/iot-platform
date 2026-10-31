//! Bounded telemetry retention job (`02` §3.4).

use std::time::Duration;

use sqlx::SqlitePool;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::config::runtime::RuntimeConfigStore;
use crate::core::auth;
use crate::observability::metrics::Metrics;
use crate::storage::sqlite::telemetry;

const RETENTION_INTERVAL: Duration = Duration::from_secs(60 * 60);
const BATCH_SIZE: i64 = 5_000;
const BATCH_SIZE_U64: u64 = 5_000;
const INTER_BATCH_DELAY: Duration = Duration::from_millis(10);

pub(crate) fn spawn(
    pool: SqlitePool,
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
                    if let Err(error) = run_once(&pool, &config).await {
                        Metrics::global().telemetry_retention_errors_total.inc();
                        tracing::error!(%error, "telemetry retention batch failed");
                    }
                }
            }
        }
    })
}

/// Runs one retention sweep using a single hot-config snapshot.
pub(crate) async fn run_once(
    pool: &SqlitePool,
    config: &RuntimeConfigStore,
) -> Result<u64, sqlx::Error> {
    let settings = config.load();
    let days = i64::try_from(settings.config.telemetry.retention_days)
        .map_err(|error| sqlx::Error::Protocol(error.to_string()))?;
    let cutoff = auth::now_ms().saturating_sub(days.saturating_mul(86_400_000));
    let mut deleted = 0;
    loop {
        let batch = telemetry::delete_before(pool, cutoff, BATCH_SIZE).await?;
        deleted += batch;
        if batch < BATCH_SIZE_U64 {
            return Ok(deleted);
        }
        tokio::time::sleep(INTER_BATCH_DELAY).await;
    }
}

#[cfg(test)]
mod tests {
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

        assert_eq!(run_once(&pool, &loaded.runtime_config).await.unwrap(), 0);
        loaded
            .runtime_config
            .update(0, &serde_json::json!({"telemetry":{"retention_days":1}}))
            .await
            .unwrap();
        assert_eq!(
            run_once(&pool, &loaded.runtime_config).await.unwrap(),
            5_001
        );
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM telemetry_events")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(count, 0);
    }
}
