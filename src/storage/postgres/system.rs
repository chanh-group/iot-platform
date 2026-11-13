//! PostgreSQL health, dashboard summaries, and config audit operations.

use serde_json::json;
use sqlx::PgPool;

use crate::core::storage::{ConfigAudit, OverviewCounts, StorageRepository};
use crate::observability::metrics::Metrics;

pub struct PostgresSystemRepository {
    pool: PgPool,
}

impl PostgresSystemRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait::async_trait]
impl StorageRepository for PostgresSystemRepository {
    async fn health_check(&self) -> anyhow::Result<()> {
        let _: i64 = sqlx::query_scalar("SELECT 1::BIGINT")
            .fetch_one(&self.pool)
            .await?;
        Ok(())
    }

    async fn overview(&self, from: i64, now: i64) -> anyhow::Result<OverviewCounts> {
        let (devices_total, devices_disabled): (i64, i64) =
            sqlx::query_as("SELECT COUNT(*), COUNT(*) FILTER (WHERE enabled = FALSE) FROM devices")
                .fetch_one(&self.pool)
                .await?;
        let (telemetry_events_24h, last_received_at): (i64, Option<i64>) = sqlx::query_as(
            "SELECT
                 (SELECT COUNT(*) FROM telemetry_events WHERE received_at >= $1 AND received_at < $2),
                 (SELECT MAX(received_at) FROM telemetry_events)",
        )
        .bind(from)
        .bind(now)
        .fetch_one(&self.pool)
        .await?;
        let (
            commands_pending,
            commands_dispatched,
            commands_failed_24h,
            commands_expired_24h,
        ): (i64, i64, i64, i64) = sqlx::query_as(
            "SELECT
                 COUNT(*) FILTER (WHERE status = 'pending'),
                 COUNT(*) FILTER (WHERE status = 'dispatched'),
                 COUNT(*) FILTER (WHERE status = 'failed' AND completed_at >= $1 AND completed_at < $2),
                 COUNT(*) FILTER (WHERE status = 'expired' AND completed_at >= $1 AND completed_at < $2)
             FROM commands",
        )
        .bind(from)
        .bind(now)
        .fetch_one(&self.pool)
        .await?;
        Ok(OverviewCounts {
            devices_total,
            devices_disabled,
            telemetry_events_24h,
            last_received_at,
            commands_pending,
            commands_dispatched,
            commands_failed_24h,
            commands_expired_24h,
        })
    }

    async fn pending_commands(&self) -> anyhow::Result<i64> {
        Ok(sqlx::query_scalar(
            "SELECT COUNT(*) FROM commands WHERE status IN ('pending', 'dispatched')",
        )
        .fetch_one(&self.pool)
        .await?)
    }

    async fn audit_config_update(&self, event: ConfigAudit<'_>) -> anyhow::Result<()> {
        let mut tx = self.pool.begin().await?;
        sqlx::query(
            "INSERT INTO audit_events
             (occurred_at, actor_type, actor_id, action, target_type, target_id,
              request_id, remote_ip, details_json)
             VALUES ($1, 'admin', $2, 'config.update', 'config', 'runtime', $3, $4, $5)",
        )
        .bind(event.occurred_at)
        .bind(event.actor_id)
        .bind(event.request_id)
        .bind(event.remote_ip)
        .bind(json!({
            "old_revision": event.old_revision,
            "new_revision": event.new_revision,
            "paths": event.changed_fields,
        }))
        .execute(&mut *tx)
        .await
        .inspect_err(|_| Metrics::global().audit_write_errors_total.inc())?;
        tx.commit()
            .await
            .inspect_err(|_| Metrics::global().audit_write_errors_total.inc())?;
        Ok(())
    }
}
