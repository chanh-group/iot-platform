//! PostgreSQL device, credential, and audit transactions.

use serde_json::{Value, json};
use sqlx::{PgPool, Postgres, QueryBuilder};

use crate::core::device::{
    ActiveCredential, DeviceAudit, DeviceCredentialRotation, DeviceKickState, DevicePatch,
    DeviceRepository, DeviceRepositoryError, DeviceRow, NewDevice,
};
use crate::observability::metrics::Metrics;

#[derive(sqlx::FromRow)]
struct DeviceDbRow {
    id: String,
    name: String,
    enabled: bool,
    metadata_json: Value,
    created_at: i64,
    updated_at: i64,
    last_seen_at: Option<i64>,
    credential_id: Option<String>,
    credential_created_at: Option<i64>,
}

impl TryFrom<DeviceDbRow> for DeviceRow {
    type Error = serde_json::Error;

    fn try_from(row: DeviceDbRow) -> Result<Self, Self::Error> {
        Ok(Self {
            id: row.id,
            name: row.name,
            enabled: row.enabled,
            metadata_json: serde_json::to_string(&row.metadata_json)?,
            created_at: row.created_at,
            updated_at: row.updated_at,
            last_seen_at: row.last_seen_at,
            credential_id: row.credential_id,
            credential_created_at: row.credential_created_at,
        })
    }
}

pub struct PostgresDeviceRepository {
    pool: PgPool,
}

impl PostgresDeviceRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait::async_trait]
impl DeviceRepository for PostgresDeviceRepository {
    async fn get(&self, id: &str) -> Result<Option<DeviceRow>, DeviceRepositoryError> {
        let row = QueryBuilder::<Postgres>::new(DEVICE_SELECT)
            .push(" WHERE d.id = ")
            .push_bind(id)
            .build_query_as::<DeviceDbRow>()
            .fetch_optional(&self.pool)
            .await
            .map_err(storage)?;
        row.map(DeviceRow::try_from).transpose().map_err(storage)
    }

    async fn list(
        &self,
        q: Option<&str>,
        enabled: Option<bool>,
        after: Option<(i64, &str)>,
        limit: i64,
    ) -> Result<Vec<DeviceRow>, DeviceRepositoryError> {
        let mut query = QueryBuilder::<Postgres>::new(DEVICE_SELECT);
        query.push(" WHERE TRUE");
        if let Some(q) = q {
            query
                .push(" AND (position(lower(")
                .push_bind(q)
                .push(" COLLATE \"C\") in lower(d.name COLLATE \"C\")) > 0")
                .push(" OR position(lower(")
                .push_bind(q)
                .push(" COLLATE \"C\") in lower(d.id COLLATE \"C\")) > 0)");
        }
        if let Some(enabled) = enabled {
            query.push(" AND d.enabled = ").push_bind(enabled);
        }
        if let Some((created_at, id)) = after {
            query
                .push(" AND (d.created_at, d.id) < (")
                .push_bind(created_at)
                .push(", ")
                .push_bind(id)
                .push(")");
        }
        let rows = query
            .push(" ORDER BY d.created_at DESC, d.id DESC LIMIT ")
            .push_bind(limit)
            .build_query_as::<DeviceDbRow>()
            .fetch_all(&self.pool)
            .await
            .map_err(storage)?;
        rows.into_iter()
            .map(DeviceRow::try_from)
            .collect::<Result<Vec<_>, _>>()
            .map_err(storage)
    }

    async fn active_credential(
        &self,
        device_id: &str,
    ) -> Result<Option<ActiveCredential>, DeviceRepositoryError> {
        let row = sqlx::query_as::<_, (String, Vec<u8>, bool)>(
            "SELECT c.id, c.token_hash, d.enabled
             FROM devices d
             JOIN device_credentials c ON c.device_id = d.id AND c.status = 'active'
             WHERE d.id = $1",
        )
        .bind(device_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(storage)?;
        Ok(row.map(
            |(credential_id, token_hash, device_enabled)| ActiveCredential {
                credential_id,
                token_hash,
                device_enabled,
            },
        ))
    }

    async fn update_last_seen(
        &self,
        device_id: &str,
        now: i64,
    ) -> Result<(), DeviceRepositoryError> {
        sqlx::query("UPDATE devices SET last_seen_at = $1 WHERE id = $2")
            .bind(now)
            .bind(device_id)
            .execute(&self.pool)
            .await
            .map_err(storage)?;
        Ok(())
    }

    async fn kick_state(
        &self,
        device_id: &str,
    ) -> Result<Option<DeviceKickState>, DeviceRepositoryError> {
        let row = sqlx::query_as::<_, (bool, Option<String>)>(
            "SELECT d.enabled, c.id
             FROM devices d
             LEFT JOIN device_credentials c ON c.device_id = d.id AND c.status = 'active'
             WHERE d.id = $1",
        )
        .bind(device_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(storage)?;
        Ok(row.map(|(enabled, active_credential_id)| DeviceKickState {
            enabled,
            active_credential_id,
        }))
    }

    async fn create(
        &self,
        device: &NewDevice<'_>,
        audit_context: &DeviceAudit<'_>,
    ) -> Result<(), DeviceRepositoryError> {
        let metadata: Value = serde_json::from_str(device.metadata_json).map_err(storage)?;
        let mut tx = self.pool.begin().await.map_err(storage)?;
        if let Err(error) = sqlx::query(
            "INSERT INTO devices (id, name, metadata_json, enabled, created_at, updated_at)
             VALUES ($1, $2, $3, $4, $5, $5)",
        )
        .bind(device.id)
        .bind(device.name)
        .bind(metadata)
        .bind(device.enabled)
        .bind(device.created_at)
        .execute(&mut *tx)
        .await
        {
            return Err(if is_name_conflict(&error) {
                DeviceRepositoryError::NameConflict
            } else {
                storage(error)
            });
        }
        sqlx::query(
            "INSERT INTO device_credentials (id, device_id, token_hash, status, created_at)
             VALUES ($1, $2, $3, 'active', $4)",
        )
        .bind(device.credential_id)
        .bind(device.id)
        .bind(device.token_hash)
        .bind(device.created_at)
        .execute(&mut *tx)
        .await
        .map_err(storage)?;
        insert_audit(
            &mut tx,
            DeviceAuditRow {
                occurred_at: device.created_at,
                audit: audit_context,
                action: "device.create",
                target_id: device.id,
                details: json!({"name": device.name, "enabled": device.enabled}),
            },
        )
        .await
        .map_err(storage)?;
        tx.commit().await.map_err(storage)
    }

    async fn patch(
        &self,
        device_id: &str,
        patch: DevicePatch,
        now: i64,
        audit_context: &DeviceAudit<'_>,
        after_commit: Box<dyn FnOnce() + Send>,
    ) -> Result<DeviceRow, DeviceRepositoryError> {
        let mut tx = self.pool.begin().await.map_err(storage)?;
        let old = QueryBuilder::<Postgres>::new(DEVICE_SELECT)
            .push(" WHERE d.id = ")
            .push_bind(device_id)
            .push(" FOR UPDATE OF d")
            .build_query_as::<DeviceDbRow>()
            .fetch_optional(&mut *tx)
            .await
            .map_err(storage)?
            .ok_or(DeviceRepositoryError::NotFound)?;
        let old = DeviceRow::try_from(old).map_err(storage)?;
        let metadata: Option<Value> = patch
            .metadata_json
            .as_deref()
            .map(serde_json::from_str)
            .transpose()
            .map_err(storage)?;
        if let Err(error) = sqlx::query(
            "UPDATE devices SET
                 name = COALESCE($2, name),
                 metadata_json = COALESCE($3, metadata_json),
                 enabled = COALESCE($4, enabled),
                 updated_at = $5
             WHERE id = $1",
        )
        .bind(device_id)
        .bind(patch.name.as_deref())
        .bind(metadata)
        .bind(patch.enabled)
        .bind(now)
        .execute(&mut *tx)
        .await
        {
            return Err(if is_name_conflict(&error) {
                DeviceRepositoryError::NameConflict
            } else {
                storage(error)
            });
        }

        let enabled_change = patch.enabled.filter(|enabled| *enabled != old.enabled);
        let action = match enabled_change {
            Some(true) => "device.enable",
            Some(false) => "device.disable",
            None => "device.update",
        };
        let mut details = json!({
            "name": patch.name.as_deref().is_some_and(|name| name != old.name),
            "metadata": patch.metadata_json.as_deref().is_some_and(|value| value != old.metadata_json),
        });
        if let Some(enabled) = enabled_change {
            details["enabled_from"] = json!(old.enabled);
            details["enabled_to"] = json!(enabled);
        }
        insert_audit(
            &mut tx,
            DeviceAuditRow {
                occurred_at: now,
                audit: audit_context,
                action,
                target_id: device_id,
                details,
            },
        )
        .await
        .map_err(storage)?;
        tx.commit().await.map_err(storage)?;
        after_commit();
        self.get(device_id)
            .await?
            .ok_or_else(|| storage(anyhow::anyhow!("device disappeared after update")))
    }

    async fn rotate_credential(
        &self,
        request: DeviceCredentialRotation<'_>,
        after_commit: Box<dyn FnOnce() + Send>,
    ) -> Result<(), DeviceRepositoryError> {
        let DeviceCredentialRotation {
            device_id,
            expected_credential_id,
            new_credential_id,
            token_hash,
            now,
            audit: audit_context,
        } = request;
        let mut tx = self.pool.begin().await.map_err(storage)?;
        let credential_id: Option<String> = sqlx::query_scalar(
            "SELECT c.id FROM devices d
             LEFT JOIN device_credentials c ON c.device_id = d.id AND c.status = 'active'
             WHERE d.id = $1 FOR UPDATE OF d",
        )
        .bind(device_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(storage)?
        .ok_or(DeviceRepositoryError::NotFound)?;
        if credential_id.as_deref() != Some(expected_credential_id) {
            return Err(DeviceRepositoryError::CredentialChanged);
        }
        let revoked = sqlx::query(
            "UPDATE device_credentials SET status = 'revoked', revoked_at = $1
             WHERE id = $2 AND device_id = $3 AND status = 'active'",
        )
        .bind(now)
        .bind(expected_credential_id)
        .bind(device_id)
        .execute(&mut *tx)
        .await
        .map_err(storage)?
        .rows_affected();
        if revoked != 1 {
            return Err(DeviceRepositoryError::CredentialChanged);
        }
        sqlx::query(
            "INSERT INTO device_credentials
             (id, device_id, token_hash, status, created_at, rotated_from)
             VALUES ($1, $2, $3, 'active', $4, $5)",
        )
        .bind(new_credential_id)
        .bind(device_id)
        .bind(token_hash)
        .bind(now)
        .bind(expected_credential_id)
        .execute(&mut *tx)
        .await
        .map_err(storage)?;
        insert_audit(
            &mut tx,
            DeviceAuditRow {
                occurred_at: now,
                audit: &audit_context,
                action: "credential.rotate",
                target_id: device_id,
                details: json!({
                    "old_credential_id": expected_credential_id,
                    "new_credential_id": new_credential_id,
                }),
            },
        )
        .await
        .map_err(storage)?;
        tx.commit().await.map_err(storage)?;
        after_commit();
        Ok(())
    }
}

const DEVICE_SELECT: &str = "SELECT d.id, d.name, d.enabled, d.metadata_json, d.created_at, d.updated_at, d.last_seen_at, c.id AS credential_id, c.created_at AS credential_created_at FROM devices d LEFT JOIN device_credentials c ON c.device_id = d.id AND c.status = 'active'";

fn storage(error: impl Into<anyhow::Error>) -> DeviceRepositoryError {
    DeviceRepositoryError::Storage(error.into())
}

fn is_name_conflict(error: &sqlx::Error) -> bool {
    error
        .as_database_error()
        .and_then(|error| error.constraint())
        == Some("devices_name_ascii_uq")
}

struct DeviceAuditRow<'a> {
    occurred_at: i64,
    audit: &'a DeviceAudit<'a>,
    action: &'a str,
    target_id: &'a str,
    details: Value,
}

async fn insert_audit(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    event: DeviceAuditRow<'_>,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO audit_events
         (occurred_at, actor_type, actor_id, action, target_type, target_id,
          request_id, remote_ip, details_json)
         VALUES ($1, 'admin', $2, $3, 'device', $4, $5, $6, $7)",
    )
    .bind(event.occurred_at)
    .bind(event.audit.actor_id)
    .bind(event.action)
    .bind(event.target_id)
    .bind(event.audit.request_id)
    .bind(event.audit.remote_ip)
    .bind(event.details)
    .execute(&mut **tx)
    .await
    .inspect_err(|_| Metrics::global().audit_write_errors_total.inc())?;
    Ok(())
}
