//! Device credential lookup for MQTT auth (`02` §2.2) and presence writes.

use serde_json::json;
use sqlx::{QueryBuilder, Sqlite, SqliteConnection, SqlitePool};

use crate::core::device::{
    ActiveCredential, DeviceCredentialRotation, DeviceKickState, DeviceRepository, DeviceRow,
};
use crate::core::device::{DeviceAudit, DevicePatch, DeviceRepositoryError, NewDevice};

use super::audit::{self, AuditEvent};
#[derive(sqlx::FromRow)]
struct DeviceRowDb {
    pub id: String,
    pub name: String,
    pub enabled: bool,
    pub metadata_json: String,
    pub created_at: i64,
    pub updated_at: i64,
    pub last_seen_at: Option<i64>,
    pub credential_id: Option<String>,
    pub credential_created_at: Option<i64>,
}

impl From<DeviceRowDb> for DeviceRow {
    fn from(row: DeviceRowDb) -> Self {
        Self {
            id: row.id,
            name: row.name,
            enabled: row.enabled,
            metadata_json: row.metadata_json,
            created_at: row.created_at,
            updated_at: row.updated_at,
            last_seen_at: row.last_seen_at,
            credential_id: row.credential_id,
            credential_created_at: row.credential_created_at,
        }
    }
}

#[derive(Clone)]
pub struct SqliteDeviceRepository {
    pool: SqlitePool,
}

impl SqliteDeviceRepository {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }
}

#[async_trait::async_trait]
impl DeviceRepository for SqliteDeviceRepository {
    async fn get(&self, id: &str) -> Result<Option<DeviceRow>, DeviceRepositoryError> {
        get(&self.pool, id)
            .await
            .map_err(|error| DeviceRepositoryError::Storage(error.into()))
    }

    async fn list(
        &self,
        q: Option<&str>,
        enabled: Option<bool>,
        after: Option<(i64, &str)>,
        limit: i64,
    ) -> Result<Vec<DeviceRow>, DeviceRepositoryError> {
        list(&self.pool, q, enabled, after, limit)
            .await
            .map_err(|error| DeviceRepositoryError::Storage(error.into()))
    }

    async fn active_credential(
        &self,
        device_id: &str,
    ) -> Result<Option<ActiveCredential>, DeviceRepositoryError> {
        active_credential(&self.pool, device_id)
            .await
            .map_err(|error| DeviceRepositoryError::Storage(error.into()))
    }

    async fn update_last_seen(
        &self,
        device_id: &str,
        now: i64,
    ) -> Result<(), DeviceRepositoryError> {
        update_last_seen(&self.pool, device_id, now)
            .await
            .map_err(|error| DeviceRepositoryError::Storage(error.into()))
    }

    async fn kick_state(
        &self,
        device_id: &str,
    ) -> Result<Option<DeviceKickState>, DeviceRepositoryError> {
        let row = sqlx::query_as::<_, (bool, Option<String>)>(
            "SELECT d.enabled, c.id
             FROM devices d
             LEFT JOIN device_credentials c ON c.device_id = d.id AND c.status = 'active'
             WHERE d.id = ?",
        )
        .bind(device_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|error| DeviceRepositoryError::Storage(error.into()))?;
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
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|error| DeviceRepositoryError::Storage(error.into()))?;
        if let Err(error) = insert_with_credential(&mut tx, device).await {
            return Err(if is_device_name_conflict(&error) {
                DeviceRepositoryError::NameConflict
            } else {
                DeviceRepositoryError::Storage(error.into())
            });
        }
        let details = json!({"name": device.name, "enabled": device.enabled}).to_string();
        audit::insert(
            &mut tx,
            device.created_at,
            &AuditEvent {
                actor_type: "admin",
                actor_id: Some(audit_context.actor_id),
                action: "device.create",
                target_type: Some("device"),
                target_id: Some(device.id),
                request_id: Some(audit_context.request_id),
                remote_ip: Some(audit_context.remote_ip),
                details_json: Some(&details),
            },
        )
        .await
        .map_err(|error| DeviceRepositoryError::Storage(error.into()))?;
        tx.commit()
            .await
            .map_err(|error| DeviceRepositoryError::Storage(error.into()))?;
        Ok(())
    }

    async fn patch(
        &self,
        device_id: &str,
        patch: DevicePatch,
        now: i64,
        audit_context: &DeviceAudit<'_>,
        after_commit: Box<dyn FnOnce() + Send>,
    ) -> Result<DeviceRow, DeviceRepositoryError> {
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|error| DeviceRepositoryError::Storage(error.into()))?;
        let old_device = get_on_connection(&mut tx, device_id)
            .await
            .map_err(|error| DeviceRepositoryError::Storage(error.into()))?
            .ok_or(DeviceRepositoryError::NotFound)?;
        if let Err(error) = update(
            &mut tx,
            device_id,
            patch.name.as_deref(),
            patch.metadata_json.as_deref(),
            patch.enabled,
            now,
        )
        .await
        {
            return Err(if is_device_name_conflict(&error) {
                DeviceRepositoryError::NameConflict
            } else {
                DeviceRepositoryError::Storage(error.into())
            });
        }
        let enabled_change = patch
            .enabled
            .filter(|enabled| *enabled != old_device.enabled);
        let action = match enabled_change {
            Some(true) => "device.enable",
            Some(false) => "device.disable",
            None => "device.update",
        };
        let mut details = json!({
            "name": patch.name.as_deref().is_some_and(|name| name != old_device.name),
            "metadata": patch.metadata_json.as_deref().is_some_and(|value| value != old_device.metadata_json),
        });
        if let Some(enabled) = enabled_change {
            details["enabled_from"] = json!(old_device.enabled);
            details["enabled_to"] = json!(enabled);
        }
        let details = details.to_string();
        audit::insert(
            &mut tx,
            now,
            &AuditEvent {
                actor_type: "admin",
                actor_id: Some(audit_context.actor_id),
                action,
                target_type: Some("device"),
                target_id: Some(device_id),
                request_id: Some(audit_context.request_id),
                remote_ip: Some(audit_context.remote_ip),
                details_json: Some(&details),
            },
        )
        .await
        .map_err(|error| DeviceRepositoryError::Storage(error.into()))?;
        tx.commit()
            .await
            .map_err(|error| DeviceRepositoryError::Storage(error.into()))?;
        after_commit();
        get(&self.pool, device_id)
            .await
            .map_err(|error| DeviceRepositoryError::Storage(error.into()))?
            .ok_or_else(|| {
                DeviceRepositoryError::Storage(anyhow::anyhow!("device disappeared after update"))
            })
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
        let mut tx = self
            .pool
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(|error| DeviceRepositoryError::Storage(error.into()))?;
        let device = get_on_connection(&mut tx, device_id)
            .await
            .map_err(|error| DeviceRepositoryError::Storage(error.into()))?
            .ok_or(DeviceRepositoryError::NotFound)?;
        if device.credential_id.as_deref() != Some(expected_credential_id) {
            return Err(DeviceRepositoryError::CredentialChanged);
        }
        if !rotate_credential(
            &mut tx,
            device_id,
            expected_credential_id,
            new_credential_id,
            token_hash,
            now,
        )
        .await
        .map_err(|error| DeviceRepositoryError::Storage(error.into()))?
        {
            return Err(DeviceRepositoryError::CredentialChanged);
        }
        let details = json!({
            "old_credential_id": expected_credential_id,
            "new_credential_id": new_credential_id,
        })
        .to_string();
        audit::insert(
            &mut tx,
            now,
            &AuditEvent {
                actor_type: "admin",
                actor_id: Some(audit_context.actor_id),
                action: "credential.rotate",
                target_type: Some("device"),
                target_id: Some(device_id),
                request_id: Some(audit_context.request_id),
                remote_ip: Some(audit_context.remote_ip),
                details_json: Some(&details),
            },
        )
        .await
        .map_err(|error| DeviceRepositoryError::Storage(error.into()))?;
        tx.commit()
            .await
            .map_err(|error| DeviceRepositoryError::Storage(error.into()))?;
        after_commit();
        Ok(())
    }
}

const DEVICE_SELECT: &str = "SELECT d.id, d.name, d.enabled, d.metadata_json, d.created_at, d.updated_at, d.last_seen_at, c.id AS credential_id, c.created_at AS credential_created_at FROM devices d LEFT JOIN device_credentials c ON c.device_id = d.id AND c.status = 'active'";

/// Returns a device and its active credential summary.
pub async fn get(pool: &SqlitePool, id: &str) -> Result<Option<DeviceRow>, sqlx::Error> {
    get_on_connection(&mut *pool.acquire().await?, id).await
}

pub async fn get_on_connection(
    connection: &mut SqliteConnection,
    id: &str,
) -> Result<Option<DeviceRow>, sqlx::Error> {
    let mut query = QueryBuilder::<Sqlite>::new(DEVICE_SELECT);
    query.push(" WHERE d.id = ").push_bind(id);
    query
        .build_query_as::<DeviceRowDb>()
        .fetch_optional(connection)
        .await
        .map(|row| row.map(DeviceRow::from))
}

pub async fn insert_with_credential(
    connection: &mut SqliteConnection,
    device: &NewDevice<'_>,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO devices (id, name, metadata_json, enabled, created_at, updated_at)
         VALUES (?, ?, ?, ?, ?, ?)",
    )
    .bind(device.id)
    .bind(device.name)
    .bind(device.metadata_json)
    .bind(device.enabled)
    .bind(device.created_at)
    .bind(device.created_at)
    .execute(&mut *connection)
    .await?;
    sqlx::query(
        "INSERT INTO device_credentials (id, device_id, token_hash, status, created_at)
         VALUES (?, ?, ?, 'active', ?)",
    )
    .bind(device.credential_id)
    .bind(device.id)
    .bind(device.token_hash)
    .bind(device.created_at)
    .execute(&mut *connection)
    .await?;
    Ok(())
}

pub async fn update(
    connection: &mut SqliteConnection,
    id: &str,
    name: Option<&str>,
    metadata_json: Option<&str>,
    enabled: Option<bool>,
    updated_at: i64,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "UPDATE devices
         SET name = COALESCE(?, name),
             metadata_json = COALESCE(?, metadata_json),
             enabled = COALESCE(?, enabled),
             updated_at = ?
         WHERE id = ?",
    )
    .bind(name)
    .bind(metadata_json)
    .bind(enabled)
    .bind(updated_at)
    .bind(id)
    .execute(&mut *connection)
    .await?;
    Ok(())
}

/// Revokes the active credential and inserts its replacement in the caller's transaction.
pub async fn rotate_credential(
    connection: &mut SqliteConnection,
    device_id: &str,
    old_credential_id: &str,
    new_credential_id: &str,
    token_hash: &[u8],
    now: i64,
) -> Result<bool, sqlx::Error> {
    let revoked = sqlx::query(
        "UPDATE device_credentials
         SET status = 'revoked', revoked_at = ?
         WHERE id = ? AND device_id = ? AND status = 'active'",
    )
    .bind(now)
    .bind(old_credential_id)
    .bind(device_id)
    .execute(&mut *connection)
    .await?
    .rows_affected();
    if revoked != 1 {
        return Ok(false);
    }
    sqlx::query(
        "INSERT INTO device_credentials
         (id, device_id, token_hash, status, created_at, rotated_from)
         VALUES (?, ?, ?, 'active', ?, ?)",
    )
    .bind(new_credential_id)
    .bind(device_id)
    .bind(token_hash)
    .bind(now)
    .bind(old_credential_id)
    .execute(&mut *connection)
    .await?;
    Ok(true)
}

/// Lists devices using a stable descending keyset.
pub async fn list(
    pool: &SqlitePool,
    q: Option<&str>,
    enabled: Option<bool>,
    after: Option<(i64, &str)>,
    limit: i64,
) -> Result<Vec<DeviceRow>, sqlx::Error> {
    let mut query = QueryBuilder::<Sqlite>::new(DEVICE_SELECT);
    query.push(" WHERE 1 = 1");
    if let Some(q) = q {
        query
            .push(" AND (instr(lower(d.name), lower(")
            .push_bind(q)
            .push(")) > 0 OR instr(lower(d.id), lower(")
            .push_bind(q)
            .push(")) > 0)");
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
    query
        .push(" ORDER BY d.created_at DESC, d.id DESC LIMIT ")
        .push_bind(limit);
    query
        .build_query_as::<DeviceRowDb>()
        .fetch_all(pool)
        .await
        .map(|rows| rows.into_iter().map(DeviceRow::from).collect())
}

/// The single active credential of a device plus its enabled flag.
#[derive(sqlx::FromRow)]
struct ActiveCredentialRow {
    credential_id: String,
    token_hash: Vec<u8>,
    device_enabled: bool,
}

/// Looks up the active credential for `device_id`; disabled devices still resolve.
pub async fn active_credential(
    pool: &SqlitePool,
    device_id: &str,
) -> Result<Option<ActiveCredential>, sqlx::Error> {
    sqlx::query_as::<_, ActiveCredentialRow>(
        "SELECT c.id AS credential_id, c.token_hash, d.enabled AS device_enabled
         FROM devices d
         JOIN device_credentials c ON c.device_id = d.id AND c.status = 'active'
         WHERE d.id = ?",
    )
    .bind(device_id)
    .fetch_optional(pool)
    .await
    .map(|credential| {
        credential.map(|row| ActiveCredential {
            credential_id: row.credential_id,
            token_hash: row.token_hash,
            device_enabled: row.device_enabled,
        })
    })
}

/// Records a connect/disconnect time; `updated_at` stays an admin-mutation field.
pub async fn update_last_seen(
    pool: &SqlitePool,
    device_id: &str,
    now: i64,
) -> Result<(), sqlx::Error> {
    sqlx::query("UPDATE devices SET last_seen_at = ? WHERE id = ?")
        .bind(now)
        .bind(device_id)
        .execute(pool)
        .await?;
    Ok(())
}

fn is_device_name_conflict(error: &sqlx::Error) -> bool {
    error
        .as_database_error()
        .is_some_and(|error| error.message().contains("devices.name"))
}
