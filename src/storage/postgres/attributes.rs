//! PostgreSQL latest-value device attributes.

use sqlx::PgPool;

use crate::core::attributes::{AttributeRow, AttributesRepository, ValidatedAttributes};

pub struct PostgresAttributesRepository {
    pool: PgPool,
}

impl PostgresAttributesRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait::async_trait]
impl AttributesRepository for PostgresAttributesRepository {
    async fn upsert(
        &self,
        device_id: &str,
        attributes: &ValidatedAttributes,
    ) -> anyhow::Result<()> {
        let mut tx = self.pool.begin().await?;
        for (key, value) in &attributes.values {
            sqlx::query(
                "INSERT INTO device_attributes (device_id, key, value_json, device_ts, updated_at)
                 VALUES ($1, $2, $3, $4, $5)
                 ON CONFLICT (device_id, key) DO UPDATE SET
                     value_json = EXCLUDED.value_json,
                     device_ts = EXCLUDED.device_ts,
                     updated_at = EXCLUDED.updated_at",
            )
            .bind(device_id)
            .bind(key)
            .bind(value)
            .bind(attributes.device_ts)
            .bind(attributes.updated_at)
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        Ok(())
    }

    async fn latest(
        &self,
        device_id: &str,
        after: Option<&str>,
        limit: i64,
    ) -> anyhow::Result<Vec<AttributeRow>> {
        let query = if after.is_some() {
            "SELECT key, value_json, device_ts, updated_at FROM device_attributes
             WHERE device_id = $1 AND key > $2 ORDER BY key LIMIT $3"
        } else {
            "SELECT key, value_json, device_ts, updated_at FROM device_attributes
             WHERE device_id = $1 ORDER BY key LIMIT $2"
        };
        let mut query = sqlx::query_as::<_, (String, serde_json::Value, Option<i64>, i64)>(query)
            .bind(device_id);
        if let Some(key) = after {
            query = query.bind(key);
        }
        let rows = query.bind(limit).fetch_all(&self.pool).await?;
        rows.into_iter()
            .map(|(key, value, device_ts, updated_at)| {
                Ok(AttributeRow {
                    key,
                    value_json: serde_json::to_string(&value)?,
                    device_ts,
                    updated_at,
                })
            })
            .collect()
    }
}
