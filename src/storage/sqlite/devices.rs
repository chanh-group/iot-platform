//! Device credential lookup for MQTT auth (`02` §2.2) and presence writes.

use sqlx::{QueryBuilder, Sqlite, SqliteConnection, SqlitePool};

/// Device and active credential summary, without credential secrets.
#[derive(sqlx::FromRow)]
pub struct DeviceRow {
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

pub struct NewDevice<'a> {
    pub id: &'a str,
    pub name: &'a str,
    pub metadata_json: &'a str,
    pub enabled: bool,
    pub created_at: i64,
    pub credential_id: &'a str,
    pub token_hash: &'a [u8],
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
    query.build_query_as().fetch_optional(connection).await
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
    query.build_query_as().fetch_all(pool).await
}

/// The single active credential of a device plus its enabled flag.
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct ActiveCredential {
    /// SHA-256 of the device token; compared in constant time by the caller.
    pub token_hash: Vec<u8>,
    pub device_enabled: bool,
}

/// Looks up the active credential for `device_id`; disabled devices still resolve.
pub async fn active_credential(
    pool: &SqlitePool,
    device_id: &str,
) -> Result<Option<ActiveCredential>, sqlx::Error> {
    sqlx::query_as::<_, ActiveCredential>(
        "SELECT c.token_hash, d.enabled AS device_enabled
         FROM devices d
         JOIN device_credentials c ON c.device_id = d.id AND c.status = 'active'
         WHERE d.id = ?",
    )
    .bind(device_id)
    .fetch_optional(pool)
    .await
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
