//! Audit rows (`02` §3.6). Callers must write these inside the mutation transaction.

use sqlx::SqliteConnection;

pub struct AuditEvent<'a> {
    pub actor_type: &'a str,
    pub actor_id: Option<&'a str>,
    pub action: &'a str,
    pub target_type: Option<&'a str>,
    pub target_id: Option<&'a str>,
    pub request_id: Option<&'a str>,
    pub remote_ip: Option<&'a str>,
    pub details_json: Option<&'a str>,
}

pub async fn insert(
    conn: &mut SqliteConnection,
    occurred_at: i64,
    event: &AuditEvent<'_>,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO audit_events (
            occurred_at, actor_type, actor_id, action, target_type, target_id,
            request_id, remote_ip, details_json
         ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(occurred_at)
    .bind(event.actor_type)
    .bind(event.actor_id)
    .bind(event.action)
    .bind(event.target_type)
    .bind(event.target_id)
    .bind(event.request_id)
    .bind(event.remote_ip)
    .bind(event.details_json)
    .execute(&mut *conn)
    .await?;
    Ok(())
}
