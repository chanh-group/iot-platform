//! Latest-value attribute upserts and their bounded writer queue.

use sqlx::{SqliteConnection, SqlitePool};
use tokio::sync::mpsc;

use crate::core::attributes::ValidatedAttributes;
use crate::observability::metrics::Metrics;

struct Work {
    device_id: String,
    attributes: ValidatedAttributes,
}

/// Non-blocking handle for the bounded attribute writer queue.
#[derive(Clone)]
pub(crate) struct AttributeHandle(mpsc::Sender<Work>);

/// Why an attribute update could not be queued.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SubmitError {
    Full,
    Closed,
}

impl AttributeHandle {
    pub(crate) fn try_submit(
        &self,
        device_id: String,
        attributes: ValidatedAttributes,
    ) -> Result<(), SubmitError> {
        self.0
            .try_send(Work {
                device_id,
                attributes,
            })
            .map_err(|error| match error {
                mpsc::error::TrySendError::Full(_) => SubmitError::Full,
                mpsc::error::TrySendError::Closed(_) => SubmitError::Closed,
            })
    }
}

/// Starts the bounded queue and its single SQLite writer.
pub(crate) fn spawn_worker(
    pool: SqlitePool,
    capacity: usize,
) -> (AttributeHandle, tokio::task::JoinHandle<()>) {
    let (sender, mut receiver) = mpsc::channel::<Work>(capacity);
    let task = tokio::spawn(async move {
        while let Some(work) = receiver.recv().await {
            if let Err(error) = upsert(&pool, &work.device_id, &work.attributes).await {
                Metrics::global().attribute_rejected_total.inc();
                tracing::warn!(%error, device_id = %work.device_id, "attribute update dropped");
            }
        }
    });
    (AttributeHandle(sender), task)
}

/// Upserts every value in one short transaction, with no attribute history.
pub async fn upsert(
    pool: &SqlitePool,
    device_id: &str,
    attributes: &ValidatedAttributes,
) -> Result<(), sqlx::Error> {
    let values = attributes
        .values
        .iter()
        .map(|(key, value)| {
            serde_json::to_string(value)
                .map(|value| (key, value))
                .map_err(|error| sqlx::Error::Protocol(error.to_string()))
        })
        .collect::<Result<Vec<_>, _>>()?;

    let mut conn = pool.acquire().await?;
    sqlx::query("BEGIN IMMEDIATE").execute(&mut *conn).await?;
    let result = upsert_in_transaction(&mut conn, device_id, attributes, &values).await;
    match result {
        Ok(()) => match sqlx::query("COMMIT").execute(&mut *conn).await {
            Ok(_) => Ok(()),
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

const FIRST_PAGE: &str = "SELECT key, value_json, device_ts, updated_at FROM device_attributes
    WHERE device_id = ? ORDER BY key LIMIT ?";
const NEXT_PAGE: &str = "SELECT key, value_json, device_ts, updated_at FROM device_attributes
    WHERE device_id = ? AND key > ? ORDER BY key LIMIT ?";

/// Returns the latest stored attributes for one device, ordered by key.
pub async fn latest(
    pool: &SqlitePool,
    device_id: &str,
    after: Option<&str>,
    limit: i64,
) -> Result<Vec<(String, String, Option<i64>, i64)>, sqlx::Error> {
    let query = sqlx::query_as(if after.is_some() {
        NEXT_PAGE
    } else {
        FIRST_PAGE
    })
    .bind(device_id);
    let query = if let Some(key) = after {
        query.bind(key)
    } else {
        query
    };
    query.bind(limit).fetch_all(pool).await
}

async fn upsert_in_transaction(
    conn: &mut SqliteConnection,
    device_id: &str,
    attributes: &ValidatedAttributes,
    values: &[(&String, String)],
) -> Result<(), sqlx::Error> {
    for (key, value_json) in values {
        sqlx::query(
            "INSERT INTO device_attributes (device_id, key, value_json, device_ts, updated_at)
             VALUES (?, ?, ?, ?, ?)
             ON CONFLICT(device_id, key) DO UPDATE SET
                 value_json = excluded.value_json,
                 device_ts = excluded.device_ts,
                 updated_at = excluded.updated_at",
        )
        .bind(device_id)
        .bind(key)
        .bind(value_json)
        .bind(attributes.device_ts)
        .bind(attributes.updated_at)
        .execute(&mut *conn)
        .await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use serde_json::Value;
    use sqlx::SqlitePool;

    #[tokio::test]
    async fn cursor_query_seeks_by_device_and_key() {
        let pool = SqlitePool::connect("sqlite::memory:").await.unwrap();
        sqlx::query("CREATE TABLE device_attributes (device_id TEXT NOT NULL, key TEXT NOT NULL, value_json TEXT, device_ts INTEGER, updated_at INTEGER, PRIMARY KEY (device_id, key))")
            .execute(&pool).await.unwrap();
        let plan: Vec<(i64, i64, i64, String)> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
            "EXPLAIN QUERY PLAN {}",
            super::NEXT_PAGE
        )))
        .bind("device")
        .bind("key")
        .bind(51_i64)
        .fetch_all(&pool)
        .await
        .unwrap();
        assert!(
            plan.iter()
                .any(|row| row.3.contains("device_id=? AND key>?")),
            "{plan:?}"
        );
    }

    fn json_is_serializable(value: &Value) -> bool {
        serde_json::to_string(value).is_ok()
    }

    #[test]
    fn sqlite_json_values_are_serializable() {
        assert!(json_is_serializable(&Value::Bool(true)));
    }
}
