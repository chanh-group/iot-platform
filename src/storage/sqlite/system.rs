//! SQLite overview, health, and post-commit config audit operations.

use serde_json::json;
use sqlx::SqlitePool;

use crate::core::storage::{ConfigAudit, OverviewCounts, StorageRepository};
use crate::observability::metrics::Metrics;

use super::audit::{self, AuditEvent};

pub struct SqliteSystemRepository {
    pool: SqlitePool,
}

impl SqliteSystemRepository {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }
}

#[async_trait::async_trait]
impl StorageRepository for SqliteSystemRepository {
    async fn health_check(&self) -> anyhow::Result<()> {
        let _: i64 = sqlx::query_scalar("SELECT 1").fetch_one(&self.pool).await?;
        Ok(())
    }

    async fn overview(&self, from: i64, now: i64) -> anyhow::Result<OverviewCounts> {
        let (devices_total, devices_disabled): (i64, i64) = sqlx::query_as(
            "SELECT COUNT(*), COALESCE(SUM(CASE WHEN enabled = 0 THEN 1 ELSE 0 END), 0)
             FROM devices",
        )
        .fetch_one(&self.pool)
        .await?;
        let (telemetry_events_24h, last_received_at): (i64, Option<i64>) = sqlx::query_as(
            "SELECT
                 (SELECT COUNT(*) FROM telemetry_events WHERE received_at >= ? AND received_at < ?),
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
                     COALESCE(SUM(CASE WHEN status = 'pending' THEN 1 ELSE 0 END), 0),
                     COALESCE(SUM(CASE WHEN status = 'dispatched' THEN 1 ELSE 0 END), 0),
                     COALESCE(SUM(CASE WHEN status = 'failed'
                                       AND completed_at >= ? AND completed_at < ? THEN 1 ELSE 0 END), 0),
                     COALESCE(SUM(CASE WHEN status = 'expired'
                                       AND completed_at >= ? AND completed_at < ? THEN 1 ELSE 0 END), 0)
                 FROM commands",
            )
            .bind(from)
            .bind(now)
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
        let details = json!({
            "old_revision": event.old_revision,
            "new_revision": event.new_revision,
            "paths": event.changed_fields,
        })
        .to_string();
        let mut tx = self.pool.begin().await.inspect_err(|_| {
            Metrics::global().audit_write_errors_total.inc();
        })?;
        audit::insert(
            &mut tx,
            event.occurred_at,
            &AuditEvent {
                actor_type: "admin",
                actor_id: Some(event.actor_id),
                action: "config.update",
                target_type: Some("config"),
                target_id: Some("runtime"),
                request_id: Some(event.request_id),
                remote_ip: Some(event.remote_ip),
                details_json: Some(&details),
            },
        )
        .await?;
        tx.commit().await.inspect_err(|_| {
            Metrics::global().audit_write_errors_total.inc();
        })?;
        Ok(())
    }
}
