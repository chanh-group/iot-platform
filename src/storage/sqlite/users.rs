//! Admin users (`02` §3.2, §6). MVP has exactly one admin.

use serde_json::json;
use sqlx::{SqliteConnection, SqlitePool};
use thiserror::Error;
use uuid::Uuid;

use crate::storage::sqlite::audit::{self, AuditEvent};

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct User {
    pub id: String,
    pub username: String,
    pub password_hash: String,
    pub enabled: bool,
}

#[derive(Debug, Error)]
pub enum CreateAdminError {
    #[error("an admin user already exists")]
    AlreadyExists,
    #[error(transparent)]
    Database(#[from] sqlx::Error),
}

/// Inserts the user and `admin.create` audit row. The caller owns the transaction.
pub async fn create_admin(
    conn: &mut SqliteConnection,
    username: &str,
    password_hash: &str,
    now: i64,
) -> Result<User, CreateAdminError> {
    let existing: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM users")
        .fetch_one(&mut *conn)
        .await?;
    if existing > 0 {
        return Err(CreateAdminError::AlreadyExists);
    }

    let id = Uuid::now_v7().to_string();
    sqlx::query(
        "INSERT INTO users (id, username, password_hash, enabled, created_at, updated_at)
         VALUES (?, ?, ?, 1, ?, ?)",
    )
    .bind(&id)
    .bind(username)
    .bind(password_hash)
    .bind(now)
    .bind(now)
    .execute(&mut *conn)
    .await?;

    let details = json!({ "username": username }).to_string();
    audit::insert(
        conn,
        now,
        &AuditEvent {
            actor_type: "system",
            actor_id: None,
            action: "admin.create",
            target_type: Some("user"),
            target_id: Some(&id),
            request_id: None,
            remote_ip: None,
            details_json: Some(&details),
        },
    )
    .await?;

    Ok(User {
        id,
        username: username.to_owned(),
        password_hash: password_hash.to_owned(),
        enabled: true,
    })
}

pub async fn find_by_username(
    pool: &SqlitePool,
    username: &str,
) -> Result<Option<User>, sqlx::Error> {
    sqlx::query_as::<_, User>(
        "SELECT id, username, password_hash, enabled FROM users WHERE username = ?",
    )
    .bind(username)
    .fetch_optional(pool)
    .await
}
