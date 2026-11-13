//! PostgreSQL telemetry writes, committed-prefix checkpoint, reads, and retention.

use serde_json::Value;
use sqlx::{PgPool, Postgres, QueryBuilder};

use crate::core::telemetry::{
    LatestTelemetryValue, TelemetryHistoryRange, TelemetryInsertResult, TelemetryReadRepository,
    TelemetryRepository, TelemetryRetentionRepository, TelemetryRow, ValidatedTelemetry,
};

pub struct PostgresTelemetryRepository {
    pool: PgPool,
}

impl PostgresTelemetryRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait::async_trait]
impl TelemetryRepository for PostgresTelemetryRepository {
    async fn insert_batch(
        &self,
        events: &[(String, ValidatedTelemetry)],
    ) -> anyhow::Result<Vec<TelemetryInsertResult>> {
        if events.is_empty() {
            return Ok(Vec::new());
        }
        let mut tx = self.pool.begin().await?;
        sqlx::query("SELECT last_id FROM telemetry_id_allocator WHERE id = 1 FOR UPDATE")
            .fetch_one(&mut *tx)
            .await?;

        let mut outcomes = Vec::with_capacity(events.len());
        for (device_id, event) in events {
            let internal_id: i64 = sqlx::query_scalar(
                "UPDATE telemetry_id_allocator SET last_id = last_id + 1
                 WHERE id = 1 RETURNING last_id",
            )
            .fetch_one(&mut *tx)
            .await?;
            let values = Value::Object(event.values.clone());
            let inserted: Option<i64> = sqlx::query_scalar(
                "INSERT INTO telemetry_events
                 (internal_id, device_id, event_id, ts, received_at, values_json, payload_sha256)
                 VALUES ($1, $2, $3, $4, $5, $6, $7)
                 ON CONFLICT (device_id, event_id) DO NOTHING
                 RETURNING internal_id",
            )
            .bind(internal_id)
            .bind(device_id)
            .bind(&event.event_id)
            .bind(event.ts)
            .bind(event.received_at)
            .bind(values)
            .bind(event.payload_sha256.as_slice())
            .fetch_optional(&mut *tx)
            .await?;
            if inserted.is_some() {
                outcomes.push(TelemetryInsertResult::Stored);
            } else {
                let old_hash: Vec<u8> = sqlx::query_scalar(
                    "SELECT payload_sha256 FROM telemetry_events
                     WHERE device_id = $1 AND event_id = $2",
                )
                .bind(device_id)
                .bind(&event.event_id)
                .fetch_one(&mut *tx)
                .await?;
                outcomes.push(if old_hash.as_slice() == event.payload_sha256 {
                    TelemetryInsertResult::Duplicate
                } else {
                    TelemetryInsertResult::Conflict
                });
            }
        }
        #[cfg(debug_assertions)]
        crate::test_hooks::pause("IOT_TEST_PAUSE_BEFORE_COMMIT").await;
        tx.commit().await?;
        Ok(outcomes)
    }
}

#[async_trait::async_trait]
impl TelemetryReadRepository for PostgresTelemetryRepository {
    async fn device_exists(&self, device_id: &str) -> anyhow::Result<bool> {
        Ok(
            sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM devices WHERE id = $1)")
                .bind(device_id)
                .fetch_one(&self.pool)
                .await?,
        )
    }

    async fn committed_through(&self) -> anyhow::Result<i64> {
        Ok(
            sqlx::query_scalar("SELECT last_id FROM telemetry_id_allocator WHERE id = 1")
                .fetch_one(&self.pool)
                .await?,
        )
    }

    async fn history(
        &self,
        device_id: &str,
        range: TelemetryHistoryRange,
        after: Option<(i64, i64)>,
        keys: &[String],
        limit: i64,
    ) -> anyhow::Result<Vec<TelemetryRow>> {
        let mut query = QueryBuilder::<Postgres>::new(
            "SELECT e.internal_id, e.event_id, e.ts, e.received_at, e.values_json
             FROM telemetry_events e WHERE e.device_id = ",
        );
        query.push_bind(device_id);
        if range.commit_order {
            query
                .push(" AND e.internal_id > ")
                .push_bind(range.from)
                .push(" AND e.internal_id <= ")
                .push_bind(range.to);
        } else {
            query
                .push(" AND e.ts >= ")
                .push_bind(range.from)
                .push(" AND e.ts < ")
                .push_bind(range.to);
        }
        if let Some((timestamp, internal_id)) = after {
            if range.commit_order {
                query.push(" AND e.internal_id > ").push_bind(internal_id);
            } else {
                query
                    .push(" AND (e.ts > ")
                    .push_bind(timestamp)
                    .push(" OR (e.ts = ")
                    .push_bind(timestamp)
                    .push(" AND e.internal_id > ")
                    .push_bind(internal_id)
                    .push("))");
            }
        }
        if !keys.is_empty() {
            query
                .push(" AND e.values_json ?| ")
                .push_bind(keys.to_vec());
        }
        let order = if range.commit_order {
            " ORDER BY e.internal_id LIMIT "
        } else {
            " ORDER BY e.ts, e.internal_id LIMIT "
        };
        let rows = query
            .push(order)
            .push_bind(limit)
            .build_query_as::<(i64, String, i64, i64, Value)>()
            .fetch_all(&self.pool)
            .await?;
        rows.into_iter()
            .map(|(internal_id, event_id, ts, received_at, values)| {
                Ok(TelemetryRow {
                    internal_id,
                    event_id,
                    ts,
                    received_at,
                    values_json: serde_json::to_string(&values)?,
                })
            })
            .collect()
    }

    async fn latest(
        &self,
        device_id: &str,
        keys: &[String],
    ) -> anyhow::Result<Vec<LatestTelemetryValue>> {
        let mut query = QueryBuilder::<Postgres>::new(
            "WITH ranked AS (
                 SELECT e.internal_id, v.key, e.event_id, e.ts, e.received_at, e.values_json,
                        ROW_NUMBER() OVER (
                            PARTITION BY v.key
                            ORDER BY e.ts DESC, e.received_at DESC, e.internal_id DESC
                        ) AS rank
                 FROM telemetry_events e
                 CROSS JOIN LATERAL jsonb_object_keys(e.values_json) AS v(key)
                 WHERE e.device_id = ",
        );
        query.push_bind(device_id);
        if !keys.is_empty() {
            query
                .push(" AND v.key = ANY(")
                .push_bind(keys.to_vec())
                .push(")");
        }
        let rows = query
            .push(
                ")
                 SELECT internal_id, key, event_id, ts, received_at, values_json
                 FROM ranked WHERE rank = 1 ORDER BY key",
            )
            .build_query_as::<(i64, String, String, i64, i64, Value)>()
            .fetch_all(&self.pool)
            .await?;
        rows.into_iter()
            .map(|(internal_id, key, event_id, ts, received_at, values)| {
                Ok(LatestTelemetryValue {
                    internal_id,
                    key,
                    event_id,
                    ts,
                    received_at,
                    values_json: serde_json::to_string(&values)?,
                })
            })
            .collect()
    }
}

#[async_trait::async_trait]
impl TelemetryRetentionRepository for PostgresTelemetryRepository {
    async fn delete_before(&self, received_before: i64, limit: i64) -> anyhow::Result<u64> {
        let result = sqlx::query(
            "WITH expired AS (
                 SELECT internal_id FROM telemetry_events
                 WHERE received_at < $1
                 ORDER BY received_at, internal_id
                 LIMIT $2
                 FOR UPDATE SKIP LOCKED
             )
             DELETE FROM telemetry_events e USING expired x
             WHERE e.internal_id = x.internal_id",
        )
        .bind(received_before)
        .bind(limit.clamp(1, 5_000))
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected())
    }
}
