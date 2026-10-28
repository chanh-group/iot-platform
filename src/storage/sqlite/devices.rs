//! Device credential lookup for MQTT auth (`02` §2.2) and presence writes.

use sqlx::SqlitePool;

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
