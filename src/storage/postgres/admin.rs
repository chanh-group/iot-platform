//! PostgreSQL admin accounts, sessions, and atomic auth audit.

use serde_json::json;
use sqlx::PgPool;
use uuid::Uuid;

use crate::core::auth::{
    AdminCreateError, AdminRepository, AdminSession, AdminUser, NewAdminSession,
};
use crate::observability::metrics::Metrics;

pub struct PostgresAdminRepository {
    pool: PgPool,
}

impl PostgresAdminRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait::async_trait]
impl AdminRepository for PostgresAdminRepository {
    async fn create_admin(
        &self,
        username: &str,
        password_hash: &str,
        now: i64,
    ) -> Result<AdminUser, AdminCreateError> {
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|error| AdminCreateError::Storage(error.into()))?;
        sqlx::query("SELECT pg_advisory_xact_lock(712940016)")
            .fetch_one(&mut *tx)
            .await
            .map_err(|error| AdminCreateError::Storage(error.into()))?;
        let exists: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM users)")
            .fetch_one(&mut *tx)
            .await
            .map_err(|error| AdminCreateError::Storage(error.into()))?;
        if exists {
            return Err(AdminCreateError::AlreadyExists);
        }

        let id = Uuid::now_v7().to_string();
        sqlx::query(
            "INSERT INTO users (id, username, password_hash, enabled, created_at, updated_at)
             VALUES ($1, $2, $3, TRUE, $4, $4)",
        )
        .bind(&id)
        .bind(username)
        .bind(password_hash)
        .bind(now)
        .execute(&mut *tx)
        .await
        .map_err(|error| AdminCreateError::Storage(error.into()))?;
        insert_audit(
            &mut tx,
            AuditRow {
                occurred_at: now,
                actor_type: "system",
                actor_id: None,
                action: "admin.create",
                target_type: Some("user"),
                target_id: Some(&id),
                request_id: None,
                remote_ip: None,
                details: Some(json!({ "username": username })),
            },
        )
        .await
        .map_err(|error| AdminCreateError::Storage(error.into()))?;
        tx.commit()
            .await
            .map_err(|error| AdminCreateError::Storage(error.into()))?;

        Ok(AdminUser {
            id,
            username: username.to_owned(),
            password_hash: password_hash.to_owned(),
            enabled: true,
        })
    }

    async fn find_user(&self, username: &str) -> anyhow::Result<Option<AdminUser>> {
        let row = sqlx::query_as::<_, (String, String, String, bool)>(
            "SELECT id, username, password_hash, enabled FROM users
             WHERE lower(username COLLATE \"C\") = lower($1 COLLATE \"C\")",
        )
        .bind(username)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(|(id, username, password_hash, enabled)| AdminUser {
            id,
            username,
            password_hash,
            enabled,
        }))
    }

    async fn create_session_and_audit(&self, request: NewAdminSession<'_>) -> anyhow::Result<()> {
        let NewAdminSession {
            user,
            token_hash,
            now,
            expires_at,
            remote_ip,
            user_agent,
            request_id,
        } = request;
        let mut tx = self.pool.begin().await?;
        let session_id = Uuid::now_v7().to_string();
        sqlx::query(
            "INSERT INTO sessions
             (id, user_id, token_hash, created_at, last_seen_at, expires_at, remote_ip, user_agent)
             VALUES ($1, $2, $3, $4, $4, $5, $6, $7)",
        )
        .bind(&session_id)
        .bind(&user.id)
        .bind(token_hash)
        .bind(now)
        .bind(expires_at)
        .bind(remote_ip)
        .bind(user_agent)
        .execute(&mut *tx)
        .await?;
        insert_audit(
            &mut tx,
            AuditRow {
                occurred_at: now,
                actor_type: "admin",
                actor_id: Some(&user.id),
                action: "auth.login",
                target_type: Some("user"),
                target_id: Some(&user.id),
                request_id: Some(request_id),
                remote_ip: Some(remote_ip),
                details: Some(json!({ "username": user.username })),
            },
        )
        .await?;
        tx.commit().await?;
        Ok(())
    }

    async fn verify_session(
        &self,
        token_hash: &[u8],
        now: i64,
    ) -> anyhow::Result<Option<AdminSession>> {
        let row = sqlx::query_as::<_, (String, String, String, i64)>(
            "SELECT s.id, s.user_id, u.username, s.expires_at
             FROM sessions s
             JOIN users u ON u.id = s.user_id
             WHERE s.token_hash = $1 AND s.revoked_at IS NULL
               AND s.expires_at > $2 AND u.enabled = TRUE",
        )
        .bind(token_hash)
        .bind(now)
        .fetch_optional(&self.pool)
        .await?;
        let Some((id, user_id, username, expires_at)) = row else {
            return Ok(None);
        };
        sqlx::query(
            "UPDATE sessions SET last_seen_at = $1
             WHERE id = $2 AND last_seen_at <= $3",
        )
        .bind(now)
        .bind(&id)
        .bind(now.saturating_sub(5 * 60 * 1000))
        .execute(&self.pool)
        .await?;
        Ok(Some(AdminSession {
            id,
            user_id,
            username,
            expires_at,
        }))
    }

    async fn session_is_live(&self, session_id: &str, now: i64) -> anyhow::Result<bool> {
        Ok(sqlx::query_scalar(
            "SELECT EXISTS(
                 SELECT 1 FROM sessions
                 WHERE id = $1 AND revoked_at IS NULL AND expires_at > $2
             )",
        )
        .bind(session_id)
        .bind(now)
        .fetch_one(&self.pool)
        .await?)
    }

    async fn revoke_session_and_audit(
        &self,
        session: &AdminSession,
        now: i64,
        request_id: &str,
        remote_ip: &str,
    ) -> anyhow::Result<()> {
        let mut tx = self.pool.begin().await?;
        sqlx::query("UPDATE sessions SET revoked_at = $1 WHERE id = $2 AND revoked_at IS NULL")
            .bind(now)
            .bind(&session.id)
            .execute(&mut *tx)
            .await?;
        insert_audit(
            &mut tx,
            AuditRow {
                occurred_at: now,
                actor_type: "admin",
                actor_id: Some(&session.user_id),
                action: "auth.logout",
                target_type: Some("session"),
                target_id: Some(&session.id),
                request_id: Some(request_id),
                remote_ip: Some(remote_ip),
                details: None,
            },
        )
        .await?;
        tx.commit().await?;
        Ok(())
    }
}

struct AuditRow<'a> {
    occurred_at: i64,
    actor_type: &'a str,
    actor_id: Option<&'a str>,
    action: &'a str,
    target_type: Option<&'a str>,
    target_id: Option<&'a str>,
    request_id: Option<&'a str>,
    remote_ip: Option<&'a str>,
    details: Option<serde_json::Value>,
}

async fn insert_audit(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    event: AuditRow<'_>,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO audit_events
         (occurred_at, actor_type, actor_id, action, target_type, target_id,
          request_id, remote_ip, details_json)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)",
    )
    .bind(event.occurred_at)
    .bind(event.actor_type)
    .bind(event.actor_id)
    .bind(event.action)
    .bind(event.target_type)
    .bind(event.target_id)
    .bind(event.request_id)
    .bind(event.remote_ip)
    .bind(event.details)
    .execute(&mut **tx)
    .await
    .inspect_err(|_| Metrics::global().audit_write_errors_total.inc())?;
    Ok(())
}
