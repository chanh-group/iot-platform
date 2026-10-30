//! Read-only telemetry queries used by the HTTP API.

use serde_json::{Map, Value};
use sqlx::{QueryBuilder, Sqlite, SqlitePool};

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct TelemetryRow {
    pub internal_id: i64,
    pub event_id: String,
    pub ts: i64,
    pub received_at: i64,
    pub values_json: String,
}

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct LatestValueRow {
    pub internal_id: i64,
    pub key: String,
    pub event_id: String,
    pub ts: i64,
    pub received_at: i64,
    pub values_json: String,
}

pub async fn device_exists(pool: &SqlitePool, device_id: &str) -> Result<bool, sqlx::Error> {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM devices WHERE id = ?)")
        .bind(device_id)
        .fetch_one(pool)
        .await
}

pub async fn history(
    pool: &SqlitePool,
    device_id: &str,
    from: i64,
    to: i64,
    after: Option<(i64, i64)>,
    keys: &[String],
    limit: i64,
) -> Result<Vec<TelemetryRow>, sqlx::Error> {
    let mut query = QueryBuilder::<Sqlite>::new(
        "SELECT e.internal_id, e.event_id, e.ts, e.received_at, e.values_json
         FROM telemetry_events e
         WHERE e.device_id = ",
    );
    query
        .push_bind(device_id)
        .push(" AND e.ts >= ")
        .push_bind(from)
        .push(" AND e.ts < ")
        .push_bind(to);
    if let Some((ts, internal_id)) = after {
        query
            .push(" AND (e.ts > ")
            .push_bind(ts)
            .push(" OR (e.ts = ")
            .push_bind(ts)
            .push(" AND e.internal_id > ")
            .push_bind(internal_id)
            .push("))");
    }
    if !keys.is_empty() {
        query.push(" AND EXISTS (SELECT 1 FROM json_each(e.values_json) AS v WHERE v.key IN (");
        let mut separated = query.separated(", ");
        for key in keys {
            separated.push_bind(key);
        }
        separated.push_unseparated("))");
    }
    query
        .push(" ORDER BY e.ts, e.internal_id LIMIT ")
        .push_bind(limit)
        .build_query_as::<TelemetryRow>()
        .fetch_all(pool)
        .await
}

pub async fn latest(
    pool: &SqlitePool,
    device_id: &str,
    keys: &[String],
) -> Result<Vec<LatestValueRow>, sqlx::Error> {
    let mut query = QueryBuilder::<Sqlite>::new(
        "WITH ranked AS (
             SELECT e.internal_id, v.key, e.event_id, e.ts, e.received_at, e.values_json,
                    ROW_NUMBER() OVER (
                        PARTITION BY v.key
                        ORDER BY e.ts DESC, e.received_at DESC, e.internal_id DESC
                    ) AS rank
             FROM telemetry_events e, json_each(e.values_json) AS v
             WHERE e.device_id = ",
    );
    query.push_bind(device_id);
    if !keys.is_empty() {
        query.push(" AND v.key IN (");
        let mut separated = query.separated(", ");
        for key in keys {
            separated.push_bind(key);
        }
        separated.push_unseparated(")");
    }
    query.push(
        ")
         SELECT internal_id, key, event_id, ts, received_at, values_json
         FROM ranked WHERE rank = 1
         ORDER BY ts, received_at, internal_id, key",
    );
    query
        .build_query_as::<LatestValueRow>()
        .fetch_all(pool)
        .await
}

pub fn filtered_values(
    values_json: &str,
    keys: &[String],
) -> Result<Map<String, Value>, serde_json::Error> {
    let mut values: Map<String, Value> = serde_json::from_str(values_json)?;
    if !keys.is_empty() {
        values.retain(|key, _| keys.iter().any(|filter| filter == key));
    }
    Ok(values)
}
