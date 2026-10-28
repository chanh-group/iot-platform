//! Admin sessions (`02` §1.3, §3.4). Only the SHA-256 of the cookie token is stored.

use sqlx::{SqliteConnection, SqlitePool};
use uuid::Uuid;

const LAST_SEEN_INTERVAL_MS: i64 = 5 * 60 * 1000;
const REVOKED_RETENTION_MS: i64 = 7 * 24 * 60 * 60 * 1000;

/// Live session joined with the owning user's username.
#[derive(Debug, Clone)]
pub struct Session {
    pub id: String,
    pub user_id: String,
    pub username: String,
    pub expires_at: i64,
    last_seen_at: i64,
}

#[derive(sqlx::FromRow)]
struct SessionRow {
    id: String,
    user_id: String,
    username: String,
    expires_at: i64,
    last_seen_at: i64,
}

impl From<SessionRow> for Session {
    fn from(row: SessionRow) -> Self {
        Self {
            id: row.id,
            user_id: row.user_id,
            username: row.username,
            expires_at: row.expires_at,
            last_seen_at: row.last_seen_at,
        }
    }
}

/// Inserts a session. The caller owns the transaction so login can audit atomically.
pub async fn create(
    conn: &mut SqliteConnection,
    user_id: &str,
    token_hash: &[u8],
    created_at: i64,
    expires_at: i64,
    remote_ip: Option<&str>,
    user_agent: Option<&str>,
) -> Result<String, sqlx::Error> {
    let id = Uuid::now_v7().to_string();
    sqlx::query(
        "INSERT INTO sessions (
            id, user_id, token_hash, created_at, last_seen_at, expires_at, remote_ip, user_agent
         ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(&id)
    .bind(user_id)
    .bind(token_hash)
    .bind(created_at)
    .bind(created_at)
    .bind(expires_at)
    .bind(remote_ip)
    .bind(user_agent)
    .execute(&mut *conn)
    .await?;
    Ok(id)
}

/// Returns a live session for the token hash and refreshes `last_seen_at`
/// at most once per five minutes.
pub async fn verify(
    pool: &SqlitePool,
    token_hash: &[u8],
    now: i64,
) -> Result<Option<Session>, sqlx::Error> {
    let row = sqlx::query_as::<_, SessionRow>(
        "SELECT s.id, s.user_id, u.username, s.expires_at, s.last_seen_at
         FROM sessions s
         JOIN users u ON u.id = s.user_id
         WHERE s.token_hash = ?
           AND s.revoked_at IS NULL
           AND s.expires_at > ?
           AND u.enabled = 1",
    )
    .bind(token_hash)
    .bind(now)
    .fetch_optional(pool)
    .await?;

    let Some(row) = row else {
        return Ok(None);
    };
    let session = Session::from(row);

    if now.saturating_sub(session.last_seen_at) >= LAST_SEEN_INTERVAL_MS {
        sqlx::query(
            "UPDATE sessions SET last_seen_at = ?
             WHERE id = ? AND last_seen_at <= ?",
        )
        .bind(now)
        .bind(&session.id)
        .bind(now.saturating_sub(LAST_SEEN_INTERVAL_MS))
        .execute(pool)
        .await?;
    }

    Ok(Some(session))
}

/// Revokes a session. The caller owns the transaction so logout can audit atomically.
pub async fn revoke(
    conn: &mut SqliteConnection,
    session_id: &str,
    now: i64,
) -> Result<(), sqlx::Error> {
    sqlx::query("UPDATE sessions SET revoked_at = ? WHERE id = ? AND revoked_at IS NULL")
        .bind(now)
        .bind(session_id)
        .execute(&mut *conn)
        .await?;
    Ok(())
}

/// Deletes expired sessions and revoked sessions older than seven days.
pub async fn delete_expired(pool: &SqlitePool, now: i64) -> Result<u64, sqlx::Error> {
    let cutoff = now.saturating_sub(REVOKED_RETENTION_MS);
    let result = sqlx::query(
        "DELETE FROM sessions
         WHERE expires_at <= ? OR (revoked_at IS NOT NULL AND revoked_at <= ?)",
    )
    .bind(now)
    .bind(cutoff)
    .execute(pool)
    .await?;
    Ok(result.rows_affected())
}
