//! Bounded cleanup of expired and old revoked PostgreSQL sessions.

use sqlx::PgPool;

use crate::core::auth::SessionCleanupRepository;

const BATCH_SIZE: i64 = 5_000;
const REVOKED_RETENTION_MS: i64 = 7 * 24 * 60 * 60 * 1_000;

pub struct PostgresSessionCleanupRepository {
    pool: PgPool,
}

impl PostgresSessionCleanupRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait::async_trait]
impl SessionCleanupRepository for PostgresSessionCleanupRepository {
    async fn delete_expired_sessions(&self, now: i64) -> anyhow::Result<u64> {
        let result = sqlx::query(
            "WITH expired AS (
                 SELECT id FROM sessions
                 WHERE expires_at <= $1 OR (revoked_at IS NOT NULL AND revoked_at <= $2)
                 ORDER BY expires_at, id
                 LIMIT $3
                 FOR UPDATE SKIP LOCKED
             )
             DELETE FROM sessions s USING expired e WHERE s.id = e.id",
        )
        .bind(now)
        .bind(now.saturating_sub(REVOKED_RETENTION_MS))
        .bind(BATCH_SIZE)
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected())
    }
}
