//! SQLite admin/session repository; audit rows share mutation transactions.

use serde_json::json;
use sqlx::SqlitePool;

use crate::core::auth::{AdminCreateError, AdminRepository, AdminSession, AdminUser};

use super::{audit, sessions, users};

pub struct SqliteAdminRepository {
    pool: SqlitePool,
}

impl SqliteAdminRepository {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }
}

#[async_trait::async_trait]
impl AdminRepository for SqliteAdminRepository {
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
        let user = match users::create_admin(&mut tx, username, password_hash, now).await {
            Ok(user) => user,
            Err(users::CreateAdminError::AlreadyExists) => {
                return Err(AdminCreateError::AlreadyExists);
            }
            Err(error) => return Err(AdminCreateError::Storage(error.into())),
        };
        tx.commit()
            .await
            .map_err(|error| AdminCreateError::Storage(error.into()))?;
        Ok(AdminUser {
            id: user.id,
            username: user.username,
            password_hash: user.password_hash,
            enabled: user.enabled,
        })
    }

    async fn find_user(&self, username: &str) -> anyhow::Result<Option<AdminUser>> {
        Ok(users::find_by_username(&self.pool, username)
            .await?
            .map(|user| AdminUser {
                id: user.id,
                username: user.username,
                password_hash: user.password_hash,
                enabled: user.enabled,
            }))
    }

    async fn create_session_and_audit(
        &self,
        request: crate::core::auth::NewAdminSession<'_>,
    ) -> anyhow::Result<()> {
        let crate::core::auth::NewAdminSession {
            user,
            token_hash,
            now,
            expires_at,
            remote_ip,
            user_agent,
            request_id,
        } = request;
        let mut tx = self.pool.begin().await?;
        sessions::create(
            &mut tx,
            &user.id,
            token_hash,
            now,
            expires_at,
            Some(remote_ip),
            user_agent,
        )
        .await?;
        let details = json!({ "username": user.username }).to_string();
        audit::insert(
            &mut tx,
            now,
            &audit::AuditEvent {
                actor_type: "admin",
                actor_id: Some(&user.id),
                action: "auth.login",
                target_type: Some("user"),
                target_id: Some(&user.id),
                request_id: Some(request_id),
                remote_ip: Some(remote_ip),
                details_json: Some(&details),
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
        Ok(sessions::verify(&self.pool, token_hash, now)
            .await?
            .map(|session| AdminSession {
                id: session.id,
                user_id: session.user_id,
                username: session.username,
                expires_at: session.expires_at,
            }))
    }

    async fn session_is_live(&self, session_id: &str, now: i64) -> anyhow::Result<bool> {
        Ok(sqlx::query_scalar(
            "SELECT EXISTS(
                 SELECT 1 FROM sessions
                 WHERE id = ? AND revoked_at IS NULL AND expires_at > ?
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
        sessions::revoke(&mut tx, &session.id, now).await?;
        audit::insert(
            &mut tx,
            now,
            &audit::AuditEvent {
                actor_type: "admin",
                actor_id: Some(&session.user_id),
                action: "auth.logout",
                target_type: Some("session"),
                target_id: Some(&session.id),
                request_id: Some(request_id),
                remote_ip: Some(remote_ip),
                details_json: None,
            },
        )
        .await?;
        tx.commit().await?;
        Ok(())
    }
}
