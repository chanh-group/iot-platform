//! Read-only telemetry queries used by the HTTP API.

use sqlx::{QueryBuilder, Sqlite, SqlitePool};

use crate::core::telemetry::{LatestTelemetryValue, TelemetryHistoryRange, TelemetryRow};

#[derive(Debug, Clone, sqlx::FromRow)]
struct TelemetryDbRow {
    pub internal_id: i64,
    pub event_id: String,
    pub ts: i64,
    pub received_at: i64,
    pub values_json: String,
}

#[derive(Debug, Clone, sqlx::FromRow)]
struct LatestValueDbRow {
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

pub async fn committed_through(pool: &SqlitePool) -> Result<i64, sqlx::Error> {
    sqlx::query_scalar(
        "SELECT MAX(
            COALESCE((SELECT MAX(internal_id) FROM telemetry_events), 0),
            COALESCE((SELECT seq FROM sqlite_sequence WHERE name = 'telemetry_events'), 0)
         )",
    )
    .fetch_one(pool)
    .await
}

pub async fn history(
    pool: &SqlitePool,
    device_id: &str,
    range: TelemetryHistoryRange,
    after: Option<(i64, i64)>,
    keys: &[String],
    limit: i64,
) -> Result<Vec<TelemetryRow>, sqlx::Error> {
    let mut query = QueryBuilder::<Sqlite>::new(
        "SELECT e.internal_id, e.event_id, e.ts, e.received_at, e.values_json
         FROM telemetry_events e
         WHERE e.device_id = ",
    );
    query.push_bind(device_id);
    let order_column = if range.commit_order {
        query
            .push(" AND e.internal_id > ")
            .push_bind(range.from)
            .push(" AND e.internal_id <= ")
            .push_bind(range.to);
        "e.internal_id"
    } else {
        query
            .push(" AND e.ts >= ")
            .push_bind(range.from)
            .push(" AND e.ts < ")
            .push_bind(range.to);
        "e.ts"
    };
    if let Some((timestamp, internal_id)) = after {
        if range.commit_order {
            query.push(" AND e.internal_id > ").push_bind(internal_id);
        } else {
            query
                .push(" AND (")
                .push(order_column)
                .push(" > ")
                .push_bind(timestamp)
                .push(" OR (")
                .push(order_column)
                .push(" = ")
                .push_bind(timestamp)
                .push(" AND e.internal_id > ")
                .push_bind(internal_id)
                .push("))");
        }
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
        .push(" ORDER BY ")
        .push(order_column)
        .push(if range.commit_order {
            " LIMIT "
        } else {
            ", e.internal_id LIMIT "
        })
        .push_bind(limit)
        .build_query_as::<TelemetryDbRow>()
        .fetch_all(pool)
        .await
        .map(|rows| {
            rows.into_iter()
                .map(|row| TelemetryRow {
                    internal_id: row.internal_id,
                    event_id: row.event_id,
                    ts: row.ts,
                    received_at: row.received_at,
                    values_json: row.values_json,
                })
                .collect()
        })
}

pub async fn latest(
    pool: &SqlitePool,
    device_id: &str,
    keys: &[String],
) -> Result<Vec<LatestTelemetryValue>, sqlx::Error> {
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
         ORDER BY key",
    );
    query
        .build_query_as::<LatestValueDbRow>()
        .fetch_all(pool)
        .await
        .map(|rows| {
            rows.into_iter()
                .map(|row| LatestTelemetryValue {
                    internal_id: row.internal_id,
                    key: row.key,
                    event_id: row.event_id,
                    ts: row.ts,
                    received_at: row.received_at,
                    values_json: row.values_json,
                })
                .collect()
        })
}
