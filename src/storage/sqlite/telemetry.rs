//! Telemetry inserts and event-id deduplication (`02` §2.6, §3.3).

use serde_json::Value;
use sqlx::{SqliteConnection, SqlitePool};

use crate::core::telemetry::{
    TelemetryInsertResult as InsertResult, TelemetryRepository, ValidatedTelemetry,
};

/// SQLite-backed implementation of the telemetry persistence port.
pub struct SqliteTelemetryRepository {
    pool: SqlitePool,
}

impl SqliteTelemetryRepository {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }
}

#[async_trait::async_trait]
impl TelemetryRepository for SqliteTelemetryRepository {
    async fn insert_batch(
        &self,
        events: &[(String, ValidatedTelemetry)],
    ) -> anyhow::Result<Vec<InsertResult>> {
        Ok(insert_batch(&self.pool, events).await?)
    }
}

/// Inserts one batch atomically; identical retries deduplicate, changed bytes conflict.
pub async fn insert_batch(
    pool: &SqlitePool,
    events: &[(String, ValidatedTelemetry)],
) -> Result<Vec<InsertResult>, sqlx::Error> {
    let mut conn = pool.acquire().await?;
    sqlx::query("BEGIN IMMEDIATE").execute(&mut *conn).await?;
    let result = insert_batch_in_transaction(&mut conn, events).await;
    match result {
        Ok(outcomes) => match sqlx::query("COMMIT").execute(&mut *conn).await {
            Ok(_) => Ok(outcomes),
            Err(error) => {
                let _ = sqlx::query("ROLLBACK").execute(&mut *conn).await;
                Err(error)
            }
        },
        Err(error) => {
            let _ = sqlx::query("ROLLBACK").execute(&mut *conn).await;
            Err(error)
        }
    }
}

async fn insert_batch_in_transaction(
    conn: &mut SqliteConnection,
    events: &[(String, ValidatedTelemetry)],
) -> Result<Vec<InsertResult>, sqlx::Error> {
    let mut outcomes = Vec::with_capacity(events.len());
    for (device_id, event) in events {
        let values = serde_json::to_string(&Value::Object(event.values.clone()))
            .map_err(|error| sqlx::Error::Protocol(error.to_string()))?;
        let inserted = sqlx::query(
            "INSERT OR IGNORE INTO telemetry_events
             (device_id, event_id, ts, received_at, values_json, payload_sha256)
             VALUES (?, ?, ?, ?, ?, ?)",
        )
        .bind(device_id)
        .bind(&event.event_id)
        .bind(event.ts)
        .bind(event.received_at)
        .bind(values)
        .bind(event.payload_sha256.as_slice())
        .execute(&mut *conn)
        .await?
        .rows_affected();

        if inserted == 1 {
            outcomes.push(InsertResult::Stored);
        } else {
            let old_hash: Vec<u8> = sqlx::query_scalar(
                "SELECT payload_sha256 FROM telemetry_events WHERE device_id = ? AND event_id = ?",
            )
            .bind(device_id)
            .bind(&event.event_id)
            .fetch_one(&mut *conn)
            .await?;
            outcomes.push(if old_hash.as_slice() == event.payload_sha256 {
                InsertResult::Duplicate
            } else {
                InsertResult::Conflict
            });
        }
    }
    Ok(outcomes)
}
