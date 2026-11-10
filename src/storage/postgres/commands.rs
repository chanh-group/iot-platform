//! PostgreSQL command lifecycle and ACK transactions.

use std::collections::HashSet;

use serde_json::{Value, json};
use sqlx::{PgPool, Postgres, QueryBuilder};
use uuid::Uuid;

use crate::core::auth;
use crate::core::command::{
    self, AckApplyOutcome, Command, CommandAck, CommandCursor, CommandRepository,
    CommandRepositoryError, CommandState, CommandStatus, CreateOutcome, NewCommand,
};
use crate::observability::metrics::Metrics;

#[derive(sqlx::FromRow)]
struct CommandRow {
    id: String,
    device_id: String,
    name: String,
    params_json: Value,
    status: String,
    created_at: i64,
    expires_at: i64,
    dispatched_at: Option<i64>,
    completed_at: Option<i64>,
    result_json: Option<Value>,
    error: Option<String>,
    attempts: i64,
    request_sha256: Option<Vec<u8>>,
}

impl CommandRow {
    fn into_command(self) -> Result<Command, CommandRepositoryError> {
        let status = CommandStatus::parse(&self.status).ok_or_else(|| {
            tracing::error!(status = %self.status, "invalid command status in PostgreSQL");
            CommandRepositoryError::StorageUnavailable
        })?;
        Ok(Command {
            id: self.id,
            device_id: self.device_id,
            name: self.name,
            params: self.params_json,
            status,
            created_at: self.created_at,
            expires_at: self.expires_at,
            dispatched_at: self.dispatched_at,
            completed_at: self.completed_at,
            result: self.result_json,
            error: self.error,
            attempts: self.attempts,
        })
    }
}

#[derive(sqlx::FromRow)]
struct CommandStateRow {
    id: String,
    device_id: String,
    status: String,
    completed_at: Option<i64>,
    error: Option<String>,
}

impl CommandStateRow {
    fn into_state(self) -> Result<CommandState, CommandRepositoryError> {
        let status = CommandStatus::parse(&self.status).ok_or_else(|| {
            tracing::error!(status = %self.status, "invalid command status in PostgreSQL");
            CommandRepositoryError::StorageUnavailable
        })?;
        Ok(CommandState {
            id: self.id,
            device_id: self.device_id,
            status,
            completed_at: self.completed_at,
            error: self.error,
        })
    }
}

pub struct PostgresCommandRepository {
    pool: PgPool,
}

impl PostgresCommandRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait::async_trait]
impl CommandRepository for PostgresCommandRepository {
    async fn create(
        &self,
        command: NewCommand<'_>,
    ) -> Result<CreateOutcome, CommandRepositoryError> {
        let NewCommand {
            device_id,
            request,
            request_sha256,
            default_ttl_ms,
            max_ttl_ms,
            max_pending_per_device,
            audit: audit_context,
        } = command;
        let mut tx = self.pool.begin().await.map_err(storage_error)?;
        let idempotency_key = command::validate_idempotency_key(&request)
            .map_err(CommandRepositoryError::Validation)?;
        let enabled: Option<bool> =
            sqlx::query_scalar("SELECT enabled FROM devices WHERE id = $1 FOR UPDATE")
                .bind(device_id)
                .fetch_optional(&mut *tx)
                .await
                .map_err(storage_error)?;
        if let Some(key) = idempotency_key {
            let row = QueryBuilder::<Postgres>::new(COMMAND_SELECT)
                .push(" WHERE device_id = ")
                .push_bind(device_id)
                .push(" AND idempotency_key = ")
                .push_bind(key)
                .build_query_as::<CommandRow>()
                .fetch_optional(&mut *tx)
                .await
                .map_err(storage_error)?;
            if let Some(row) = row {
                if row.request_sha256.as_deref() != Some(request_sha256.as_slice()) {
                    return Err(CommandRepositoryError::IdempotencyConflict);
                }
                let command = row.into_command()?;
                tx.commit().await.map_err(storage_error)?;
                return Ok(CreateOutcome {
                    command,
                    replayed: true,
                });
            }
        }

        let command = command::validate_create_value(request, default_ttl_ms, max_ttl_ms)
            .map_err(CommandRepositoryError::Validation)?;
        let Some(enabled) = enabled else {
            return Err(CommandRepositoryError::DeviceNotFound);
        };
        if !enabled {
            return Err(CommandRepositoryError::DeviceDisabled);
        }
        let active_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM commands
             WHERE device_id = $1 AND status IN ('pending', 'dispatched')",
        )
        .bind(device_id)
        .fetch_one(&mut *tx)
        .await
        .map_err(storage_error)?;
        if active_count >= i64::try_from(max_pending_per_device).unwrap_or(i64::MAX) {
            return Err(CommandRepositoryError::QueueFull {
                limit: max_pending_per_device,
            });
        }

        let created_at = auth::now_ms();
        let Some(expires_at) = i64::try_from(command.ttl_ms)
            .ok()
            .and_then(|ttl| created_at.checked_add(ttl))
        else {
            return Err(CommandRepositoryError::StorageUnavailable);
        };
        let id = Uuid::now_v7().to_string();
        let params: Value = serde_json::from_str(&command.params_json).map_err(storage_error)?;
        let request_hash = command
            .idempotency_key
            .as_ref()
            .map(|_| request_sha256.as_slice());
        sqlx::query(
            "INSERT INTO commands
             (id, device_id, name, params_json, status, created_at, expires_at,
              next_attempt_at, idempotency_key, request_sha256)
             VALUES ($1, $2, $3, $4, 'pending', $5, $6, $5, $7, $8)",
        )
        .bind(&id)
        .bind(device_id)
        .bind(&command.name)
        .bind(params)
        .bind(created_at)
        .bind(expires_at)
        .bind(&command.idempotency_key)
        .bind(request_hash)
        .execute(&mut *tx)
        .await
        .map_err(storage_error)?;
        insert_audit(
            &mut tx,
            CommandAuditRow {
                occurred_at: created_at,
                actor_id: audit_context.actor_id,
                action: "command.create",
                target_id: &id,
                request_id: audit_context.request_id,
                remote_ip: audit_context.remote_ip,
                details: json!({"name": command.name, "expires_at": expires_at}),
            },
        )
        .await
        .map_err(storage_error)?;
        tx.commit().await.map_err(storage_error)?;
        Ok(CreateOutcome {
            command: Command {
                id,
                device_id: device_id.to_owned(),
                name: command.name,
                params: command.params,
                status: CommandStatus::Pending,
                created_at,
                expires_at,
                dispatched_at: None,
                completed_at: None,
                result: None,
                error: None,
                attempts: 0,
            },
            replayed: false,
        })
    }

    async fn get(&self, id: &str) -> Result<Option<Command>, CommandRepositoryError> {
        sqlx::query_as::<_, CommandRow>("SELECT id, device_id, name, params_json, status, created_at, expires_at, dispatched_at, completed_at, result_json, error, attempts, request_sha256 FROM commands WHERE id = $1")
            .bind(id)
            .fetch_optional(&self.pool)
            .await
            .map_err(storage_error)?
            .map(CommandRow::into_command)
            .transpose()
    }

    async fn list(
        &self,
        device_id: &str,
        status: Option<CommandStatus>,
        after: Option<CommandCursor>,
        limit: i64,
    ) -> Result<Vec<Command>, CommandRepositoryError> {
        let exists: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM devices WHERE id = $1)")
            .bind(device_id)
            .fetch_one(&self.pool)
            .await
            .map_err(storage_error)?;
        if !exists {
            return Err(CommandRepositoryError::DeviceNotFound);
        }
        let mut query = QueryBuilder::<Postgres>::new(COMMAND_SELECT);
        query.push(" WHERE device_id = ").push_bind(device_id);
        if let Some(status) = status {
            query.push(" AND status = ").push_bind(status.as_str());
        }
        if let Some(after) = after {
            query
                .push(" AND (created_at, id) < (")
                .push_bind(after.created_at)
                .push(", ")
                .push_bind(after.id)
                .push(")");
        }
        let rows = query
            .push(" ORDER BY created_at DESC, id DESC LIMIT ")
            .push_bind(limit)
            .build_query_as::<CommandRow>()
            .fetch_all(&self.pool)
            .await
            .map_err(storage_error)?;
        rows.into_iter().map(CommandRow::into_command).collect()
    }

    async fn due(&self, now: i64, limit: i64) -> Result<Vec<Command>, CommandRepositoryError> {
        fetch_due(&self.pool, None, now, limit).await
    }

    async fn due_at(
        &self,
        scheduled_at: i64,
        now: i64,
        limit: i64,
    ) -> Result<Vec<Command>, CommandRepositoryError> {
        fetch_due(&self.pool, Some(scheduled_at), now, limit).await
    }

    async fn can_dispatch(
        &self,
        command_id: &str,
        now: i64,
    ) -> Result<bool, CommandRepositoryError> {
        sqlx::query_scalar(
            "SELECT EXISTS(
                 SELECT 1 FROM commands c JOIN devices d ON d.id = c.device_id
                 WHERE c.id = $1 AND c.status IN ('pending', 'dispatched')
                   AND c.expires_at > $2 AND d.enabled = TRUE
             )",
        )
        .bind(command_id)
        .bind(now)
        .fetch_one(&self.pool)
        .await
        .map_err(storage_error)
    }

    async fn defer_offline(
        &self,
        command_id: &str,
        now: i64,
        next_attempt_at: i64,
    ) -> Result<(), CommandRepositoryError> {
        sqlx::query(
            "UPDATE commands SET next_attempt_at = $1
             WHERE id = $2 AND status IN ('pending', 'dispatched') AND expires_at > $3",
        )
        .bind(next_attempt_at)
        .bind(command_id)
        .bind(now)
        .execute(&self.pool)
        .await
        .map_err(storage_error)?;
        Ok(())
    }

    async fn wake_device(&self, device_id: &str, now: i64) -> Result<u64, CommandRepositoryError> {
        let result = sqlx::query(
            "UPDATE commands SET next_attempt_at = $1
             WHERE device_id = $2 AND status IN ('pending', 'dispatched')
               AND expires_at > $1 AND next_attempt_at > $1
               AND EXISTS (SELECT 1 FROM devices WHERE id = $2 AND enabled = TRUE)",
        )
        .bind(now)
        .bind(device_id)
        .execute(&self.pool)
        .await
        .map_err(storage_error)?;
        Ok(result.rows_affected())
    }

    async fn wake_all(&self, now: i64, limit: i64) -> Result<u64, CommandRepositoryError> {
        let result = sqlx::query(
            "WITH due AS (
                 SELECT c.id FROM commands c JOIN devices d ON d.id = c.device_id
                 WHERE d.enabled = TRUE AND c.status IN ('pending', 'dispatched')
                   AND c.expires_at > $1 AND c.next_attempt_at > $1
                 ORDER BY c.next_attempt_at, c.created_at, c.id
                 LIMIT $2 FOR UPDATE OF c SKIP LOCKED
             )
             UPDATE commands c SET next_attempt_at = $1 FROM due WHERE c.id = due.id",
        )
        .bind(now)
        .bind(limit.clamp(1, 100))
        .execute(&self.pool)
        .await
        .map_err(storage_error)?;
        Ok(result.rows_affected())
    }

    async fn record_dispatch_attempt(
        &self,
        command_id: &str,
        attempted_at: i64,
        finished_at: i64,
        next_attempt_at: i64,
        published: bool,
    ) -> Result<Option<CommandState>, CommandRepositoryError> {
        sqlx::query_as::<_, CommandStateRow>(
            "UPDATE commands
             SET status = CASE WHEN expires_at > $1 AND $2 THEN 'dispatched' ELSE status END,
                 dispatched_at = CASE WHEN expires_at > $1 AND $2
                                      THEN COALESCE(dispatched_at, $1) ELSE dispatched_at END,
                 attempts = attempts + 1, last_attempt_at = $3, next_attempt_at = $4
             WHERE id = $5 AND status IN ('pending', 'dispatched')
             RETURNING id, device_id, status, completed_at, error",
        )
        .bind(finished_at)
        .bind(published)
        .bind(attempted_at)
        .bind(next_attempt_at)
        .bind(command_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(storage_error)?
        .map(CommandStateRow::into_state)
        .transpose()
    }

    async fn expire_due(
        &self,
        now: i64,
        limit: i64,
        pending_ack_ids: &[String],
    ) -> Result<Vec<CommandState>, CommandRepositoryError> {
        let limit = limit.clamp(1, 100);
        let scan_limit =
            limit.saturating_add(i64::try_from(pending_ack_ids.len()).unwrap_or(i64::MAX));
        let mut tx = self.pool.begin().await.map_err(storage_error)?;
        let candidates: Vec<String> = sqlx::query_scalar(
            "SELECT id FROM commands
             WHERE status IN ('pending', 'dispatched') AND expires_at <= $1
             ORDER BY expires_at, id LIMIT $2 FOR UPDATE SKIP LOCKED",
        )
        .bind(now)
        .bind(scan_limit)
        .fetch_all(&mut *tx)
        .await
        .map_err(storage_error)?;
        let pending_ack_ids: HashSet<&str> = pending_ack_ids.iter().map(String::as_str).collect();
        let ids: Vec<String> = candidates
            .into_iter()
            .filter(|id| !pending_ack_ids.contains(id.as_str()))
            .take(usize::try_from(limit).unwrap_or(100))
            .collect();
        if ids.is_empty() {
            tx.commit().await.map_err(storage_error)?;
            return Ok(Vec::new());
        }
        let expired = sqlx::query_as::<_, CommandStateRow>(
            "UPDATE commands SET status = 'expired', completed_at = expires_at
             WHERE id = ANY($1) AND status IN ('pending', 'dispatched') AND expires_at <= $2
             RETURNING id, device_id, status, completed_at, error",
        )
        .bind(ids)
        .bind(now)
        .fetch_all(&mut *tx)
        .await
        .map_err(storage_error)?;
        tx.commit().await.map_err(storage_error)?;
        expired
            .into_iter()
            .map(CommandStateRow::into_state)
            .collect()
    }

    async fn apply_ack(
        &self,
        device_id: &str,
        ack: &CommandAck,
        received_at: i64,
    ) -> Result<AckApplyOutcome, CommandRepositoryError> {
        let mut tx = self.pool.begin().await.map_err(storage_error)?;
        let row = sqlx::query_as::<_, (String, String, i64)>(
            "SELECT device_id, status, expires_at FROM commands WHERE id = $1 FOR UPDATE",
        )
        .bind(&ack.command_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(storage_error)?;
        let Some((stored_device_id, status, expires_at)) = row else {
            tx.commit().await.map_err(storage_error)?;
            return Ok(AckApplyOutcome::UnknownCommand);
        };
        if stored_device_id != device_id {
            tx.commit().await.map_err(storage_error)?;
            return Ok(AckApplyOutcome::WrongDevice);
        }
        if status == "succeeded" || status == "failed" {
            tx.commit().await.map_err(storage_error)?;
            return Ok(AckApplyOutcome::Duplicate);
        }
        if status == "expired" && received_at < expires_at {
            tx.commit().await.map_err(storage_error)?;
            return Ok(AckApplyOutcome::Duplicate);
        }
        let late_ack = serde_json::to_value(ack).map_err(storage_error)?;
        let result = ack.result.clone();
        if received_at >= expires_at {
            let update = if status == "expired" {
                sqlx::query(
                    "UPDATE commands SET late_ack_at = $1, late_ack_json = $2
                     WHERE id = $3 AND device_id = $4 AND status = 'expired'
                       AND late_ack_at IS NULL",
                )
                .bind(received_at)
                .bind(late_ack)
                .bind(&ack.command_id)
                .bind(device_id)
                .execute(&mut *tx)
                .await
            } else {
                sqlx::query(
                    "UPDATE commands SET status = 'expired', completed_at = expires_at,
                         late_ack_at = $1, late_ack_json = $2
                     WHERE id = $3 AND device_id = $4
                       AND status IN ('pending', 'dispatched') AND expires_at <= $1",
                )
                .bind(received_at)
                .bind(late_ack)
                .bind(&ack.command_id)
                .bind(device_id)
                .execute(&mut *tx)
                .await
            }
            .map_err(storage_error)?;
            tx.commit().await.map_err(storage_error)?;
            return Ok(if update.rows_affected() == 0 {
                AckApplyOutcome::Duplicate
            } else if status == "expired" {
                AckApplyOutcome::LateRecorded
            } else {
                AckApplyOutcome::ExpiredAndLateRecorded
            });
        }
        let update = sqlx::query(
            "UPDATE commands SET status = $1, completed_at = $2, result_json = $3, error = $4
             WHERE id = $5 AND device_id = $6
               AND status IN ('pending', 'dispatched') AND expires_at > $2",
        )
        .bind(ack.status.as_str())
        .bind(received_at)
        .bind(result)
        .bind(&ack.error)
        .bind(&ack.command_id)
        .bind(device_id)
        .execute(&mut *tx)
        .await
        .map_err(storage_error)?;
        tx.commit().await.map_err(storage_error)?;
        Ok(if update.rows_affected() == 1 {
            AckApplyOutcome::Applied
        } else {
            AckApplyOutcome::Duplicate
        })
    }
}

const COMMAND_SELECT: &str = "SELECT c.id, c.device_id, c.name, c.params_json, c.status, c.created_at, c.expires_at, c.dispatched_at, c.completed_at, c.result_json, c.error, c.attempts, c.request_sha256 FROM commands c";

async fn fetch_due(
    pool: &PgPool,
    scheduled_at: Option<i64>,
    now: i64,
    limit: i64,
) -> Result<Vec<Command>, CommandRepositoryError> {
    let mut query = QueryBuilder::<Postgres>::new(COMMAND_SELECT);
    query.push(
        " JOIN devices d ON d.id = c.device_id
         WHERE d.enabled = TRUE AND c.status IN ('pending', 'dispatched') AND ",
    );
    if let Some(scheduled_at) = scheduled_at {
        query.push("c.next_attempt_at = ").push_bind(scheduled_at);
    } else {
        query.push("c.next_attempt_at <= ").push_bind(now);
    }
    query
        .push(" AND c.expires_at > ")
        .push_bind(now)
        .push(if scheduled_at.is_some() {
            " ORDER BY c.created_at, c.id LIMIT "
        } else {
            " ORDER BY c.next_attempt_at, c.created_at, c.id LIMIT "
        })
        .push_bind(limit.clamp(1, 100));
    let rows = query
        .build_query_as::<CommandRow>()
        .fetch_all(pool)
        .await
        .map_err(storage_error)?;
    rows.into_iter().map(CommandRow::into_command).collect()
}

struct CommandAuditRow<'a> {
    occurred_at: i64,
    actor_id: &'a str,
    action: &'a str,
    target_id: &'a str,
    request_id: &'a str,
    remote_ip: &'a str,
    details: Value,
}

async fn insert_audit(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    event: CommandAuditRow<'_>,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO audit_events
         (occurred_at, actor_type, actor_id, action, target_type, target_id,
          request_id, remote_ip, details_json)
         VALUES ($1, 'admin', $2, $3, 'command', $4, $5, $6, $7)",
    )
    .bind(event.occurred_at)
    .bind(event.actor_id)
    .bind(event.action)
    .bind(event.target_id)
    .bind(event.request_id)
    .bind(event.remote_ip)
    .bind(event.details)
    .execute(&mut **tx)
    .await
    .inspect_err(|_| Metrics::global().audit_write_errors_total.inc())?;
    Ok(())
}

fn storage_error(error: impl std::fmt::Display) -> CommandRepositoryError {
    tracing::warn!(%error, "command storage unavailable");
    CommandRepositoryError::StorageUnavailable
}
