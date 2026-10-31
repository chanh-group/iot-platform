//! SQLite command repository (`02` §3.3–§3.6).

use std::collections::HashSet;

use sqlx::{FromRow, QueryBuilder, Sqlite, SqlitePool};
use uuid::Uuid;

use crate::core::auth;
use crate::core::command::{
    self, AckApplyOutcome, Command, CommandAck, CommandCursor, CommandRepository,
    CommandRepositoryError, CommandState, CommandStatus, CreateOutcome, NewCommand,
};
use crate::storage::sqlite::audit::{self, AuditEvent};

const COMMAND_SELECT: &str =
    "SELECT id, device_id, name, params_json, status, created_at, expires_at,
            dispatched_at, completed_at, result_json, error, attempts, request_sha256
     FROM commands";

#[derive(FromRow)]
struct CommandRow {
    id: String,
    device_id: String,
    name: String,
    params_json: String,
    status: String,
    created_at: i64,
    expires_at: i64,
    dispatched_at: Option<i64>,
    completed_at: Option<i64>,
    result_json: Option<String>,
    error: Option<String>,
    attempts: i64,
    request_sha256: Option<Vec<u8>>,
}

impl CommandRow {
    fn into_command(self) -> Result<Command, CommandRepositoryError> {
        let params = serde_json::from_str(&self.params_json).map_err(storage_error)?;
        let result = self
            .result_json
            .as_deref()
            .map(serde_json::from_str)
            .transpose()
            .map_err(storage_error)?;
        let status = CommandStatus::parse(&self.status).ok_or_else(|| {
            tracing::error!(status = %self.status, "invalid command status in SQLite");
            CommandRepositoryError::StorageUnavailable
        })?;
        Ok(Command {
            id: self.id,
            device_id: self.device_id,
            name: self.name,
            params,
            status,
            created_at: self.created_at,
            expires_at: self.expires_at,
            dispatched_at: self.dispatched_at,
            completed_at: self.completed_at,
            result,
            error: self.error,
            attempts: self.attempts,
        })
    }
}

#[derive(FromRow)]
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
            tracing::error!(status = %self.status, "invalid command status in SQLite");
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

#[derive(Clone)]
pub struct SqliteCommandRepository {
    pool: SqlitePool,
}

impl SqliteCommandRepository {
    #[must_use]
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }
}

#[async_trait::async_trait]
impl CommandRepository for SqliteCommandRepository {
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
        let mut tx = self
            .pool
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(storage_error)?;

        let idempotency_key = match command::validate_idempotency_key(&request) {
            Ok(key) => key,
            Err(errors) => {
                tx.rollback().await.map_err(storage_error)?;
                return Err(CommandRepositoryError::Validation(errors));
            }
        };
        if let Some(idempotency_key) = idempotency_key {
            let mut query = QueryBuilder::<Sqlite>::new(COMMAND_SELECT);
            query
                .push(" WHERE device_id = ")
                .push_bind(device_id)
                .push(" AND idempotency_key = ")
                .push_bind(idempotency_key);
            if let Some(row) = query
                .build_query_as::<CommandRow>()
                .fetch_optional(&mut *tx)
                .await
                .map_err(storage_error)?
            {
                if row.request_sha256.as_deref() != Some(request_sha256.as_slice()) {
                    tx.rollback().await.map_err(storage_error)?;
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

        let command = match command::validate_create_value(request, default_ttl_ms, max_ttl_ms) {
            Ok(command) => command,
            Err(errors) => {
                tx.rollback().await.map_err(storage_error)?;
                return Err(CommandRepositoryError::Validation(errors));
            }
        };

        let enabled = sqlx::query_scalar::<_, bool>("SELECT enabled FROM devices WHERE id = ?")
            .bind(device_id)
            .fetch_optional(&mut *tx)
            .await
            .map_err(storage_error)?;
        let Some(enabled) = enabled else {
            tx.rollback().await.map_err(storage_error)?;
            return Err(CommandRepositoryError::DeviceNotFound);
        };
        if !enabled {
            tx.rollback().await.map_err(storage_error)?;
            return Err(CommandRepositoryError::DeviceDisabled);
        }

        let active_count = sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM commands
             WHERE device_id = ? AND status IN ('pending', 'dispatched')",
        )
        .bind(device_id)
        .fetch_one(&mut *tx)
        .await
        .map_err(storage_error)?;
        if active_count >= i64::try_from(max_pending_per_device).unwrap_or(i64::MAX) {
            tx.rollback().await.map_err(storage_error)?;
            return Err(CommandRepositoryError::QueueFull {
                limit: max_pending_per_device,
            });
        }

        let created_at = auth::now_ms();
        let Some(expires_at) = i64::try_from(command.ttl_ms)
            .ok()
            .and_then(|ttl_ms| created_at.checked_add(ttl_ms))
        else {
            tx.rollback().await.map_err(storage_error)?;
            return Err(CommandRepositoryError::StorageUnavailable);
        };
        let id = Uuid::now_v7().to_string();
        let stored_hash = command
            .idempotency_key
            .as_ref()
            .map(|_| request_sha256.as_slice());
        sqlx::query(
            "INSERT INTO commands (
                id, device_id, name, params_json, status, created_at, expires_at,
                next_attempt_at, idempotency_key, request_sha256
             ) VALUES (?, ?, ?, ?, 'pending', ?, ?, ?, ?, ?)",
        )
        .bind(&id)
        .bind(device_id)
        .bind(&command.name)
        .bind(&command.params_json)
        .bind(created_at)
        .bind(expires_at)
        .bind(created_at)
        .bind(&command.idempotency_key)
        .bind(stored_hash)
        .execute(&mut *tx)
        .await
        .map_err(storage_error)?;

        let details = serde_json::json!({
            "name": command.name,
            "expires_at": expires_at,
        })
        .to_string();
        if let Err(error) = audit::insert(
            &mut tx,
            created_at,
            &AuditEvent {
                actor_type: "admin",
                actor_id: Some(audit_context.actor_id),
                action: "command.create",
                target_type: Some("command"),
                target_id: Some(&id),
                request_id: Some(audit_context.request_id),
                remote_ip: Some(audit_context.remote_ip),
                details_json: Some(&details),
            },
        )
        .await
        {
            if let Err(rollback_error) = tx.rollback().await {
                tracing::error!(%rollback_error, "command audit rollback failed");
            }
            return Err(storage_error(error));
        }
        tx.commit().await.map_err(storage_error)?;

        Ok(CreateOutcome {
            command: Command {
                id,
                device_id: device_id.to_owned(),
                name: command.name.clone(),
                params: command.params.clone(),
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
        let mut query = QueryBuilder::<Sqlite>::new(COMMAND_SELECT);
        query.push(" WHERE id = ").push_bind(id);
        query
            .build_query_as::<CommandRow>()
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
        let exists =
            sqlx::query_scalar::<_, bool>("SELECT EXISTS(SELECT 1 FROM devices WHERE id = ?)")
                .bind(device_id)
                .fetch_one(&self.pool)
                .await
                .map_err(storage_error)?;
        if !exists {
            return Err(CommandRepositoryError::DeviceNotFound);
        }

        let mut query = QueryBuilder::<Sqlite>::new(COMMAND_SELECT);
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
        query
            .push(" ORDER BY created_at DESC, id DESC LIMIT ")
            .push_bind(limit);
        let rows = query
            .build_query_as::<CommandRow>()
            .fetch_all(&self.pool)
            .await
            .map_err(storage_error)?;
        rows.into_iter().map(CommandRow::into_command).collect()
    }

    async fn due(&self, now: i64, limit: i64) -> Result<Vec<Command>, CommandRepositoryError> {
        let rows = sqlx::query_as::<_, CommandRow>(
            "SELECT c.id, c.device_id, c.name, c.params_json, c.status, c.created_at,
                    c.expires_at, c.dispatched_at, c.completed_at, c.result_json, c.error,
                    c.attempts, c.request_sha256
             FROM commands AS c INDEXED BY commands_due_idx
             JOIN devices d ON d.id = c.device_id
             WHERE d.enabled = 1 AND c.status IN ('pending', 'dispatched')
               AND c.next_attempt_at <= ? AND c.expires_at > ?
             ORDER BY c.next_attempt_at, c.created_at, c.id
             LIMIT ?",
        )
        .bind(now)
        .bind(now)
        .bind(limit.clamp(1, 100))
        .fetch_all(&self.pool)
        .await
        .map_err(storage_error)?;
        rows.into_iter().map(CommandRow::into_command).collect()
    }

    async fn due_at(
        &self,
        scheduled_at: i64,
        now: i64,
        limit: i64,
    ) -> Result<Vec<Command>, CommandRepositoryError> {
        let rows = sqlx::query_as::<_, CommandRow>(
            "SELECT c.id, c.device_id, c.name, c.params_json, c.status, c.created_at,
                    c.expires_at, c.dispatched_at, c.completed_at, c.result_json, c.error,
                    c.attempts, c.request_sha256
             FROM commands AS c INDEXED BY commands_due_idx
             JOIN devices d ON d.id = c.device_id
             WHERE d.enabled = 1 AND c.status IN ('pending', 'dispatched')
               AND c.next_attempt_at = ? AND c.expires_at > ?
             ORDER BY c.created_at, c.id
             LIMIT ?",
        )
        .bind(scheduled_at)
        .bind(now)
        .bind(limit.clamp(1, 100))
        .fetch_all(&self.pool)
        .await
        .map_err(storage_error)?;
        rows.into_iter().map(CommandRow::into_command).collect()
    }

    async fn can_dispatch(
        &self,
        command_id: &str,
        now: i64,
    ) -> Result<bool, CommandRepositoryError> {
        sqlx::query_scalar(
            "SELECT EXISTS(
                SELECT 1 FROM commands c
                JOIN devices d ON d.id = c.device_id
                WHERE c.id = ? AND c.status IN ('pending', 'dispatched')
                  AND c.expires_at > ? AND d.enabled = 1
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
            "UPDATE commands SET next_attempt_at = ?
             WHERE id = ? AND status IN ('pending', 'dispatched') AND expires_at > ?",
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
        let eligible: bool = sqlx::query_scalar(
            "SELECT EXISTS(
                SELECT 1 FROM commands c JOIN devices d ON d.id = c.device_id
                WHERE c.device_id = ? AND c.status IN ('pending', 'dispatched')
                  AND c.expires_at > ? AND c.next_attempt_at > ? AND d.enabled = 1
            )",
        )
        .bind(device_id)
        .bind(now)
        .bind(now)
        .fetch_one(&self.pool)
        .await
        .map_err(storage_error)?;
        if !eligible {
            return Ok(0);
        }

        let result = sqlx::query(
            "UPDATE commands SET next_attempt_at = ?
             WHERE device_id = ? AND status IN ('pending', 'dispatched')
               AND expires_at > ? AND next_attempt_at > ?
               AND EXISTS (
                   SELECT 1 FROM devices WHERE id = ? AND enabled = 1
               )",
        )
        .bind(now)
        .bind(device_id)
        .bind(now)
        .bind(now)
        .bind(device_id)
        .execute(&self.pool)
        .await
        .map_err(storage_error)?;
        Ok(result.rows_affected())
    }

    async fn wake_all(&self, now: i64, limit: i64) -> Result<u64, CommandRepositoryError> {
        let result = sqlx::query(
            "UPDATE commands SET next_attempt_at = ?
             WHERE id IN (
                 SELECT c.id FROM commands c INDEXED BY commands_due_idx
                 JOIN devices d ON d.id = c.device_id
                 WHERE d.enabled = 1 AND c.status IN ('pending', 'dispatched')
                   AND c.expires_at > ? AND c.next_attempt_at > ?
                 ORDER BY c.next_attempt_at, c.created_at, c.id
                 LIMIT ?
             )",
        )
        .bind(now)
        .bind(now)
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
             SET status = CASE
                     WHEN expires_at > ? AND ? THEN 'dispatched'
                     ELSE status
                 END,
                 dispatched_at = CASE
                     WHEN expires_at > ? AND ? THEN COALESCE(dispatched_at, ?)
                     ELSE dispatched_at
                 END,
                 attempts = attempts + 1,
                 last_attempt_at = ?,
                 next_attempt_at = ?
             WHERE id = ? AND status IN ('pending', 'dispatched')
             RETURNING id, device_id, status, completed_at, error
            ",
        )
        .bind(finished_at)
        .bind(published)
        .bind(finished_at)
        .bind(published)
        .bind(finished_at)
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
        let candidates = sqlx::query_scalar::<_, String>(
            "SELECT id FROM commands INDEXED BY commands_status_expiry_idx
             WHERE status IN ('pending', 'dispatched') AND expires_at <= ?
             ORDER BY expires_at, id LIMIT ?",
        )
        .bind(now)
        .bind(scan_limit)
        .fetch_all(&self.pool)
        .await
        .map_err(storage_error)?;
        let pending_ack_ids: HashSet<&str> = pending_ack_ids.iter().map(String::as_str).collect();
        let ids: Vec<String> = candidates
            .into_iter()
            .filter(|id| !pending_ack_ids.contains(id.as_str()))
            .take(usize::try_from(limit).unwrap_or(100))
            .collect();
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let mut query = QueryBuilder::<Sqlite>::new(
            "UPDATE commands SET status = 'expired', completed_at = expires_at WHERE id IN (",
        );
        let mut ids_list = query.separated(", ");
        for id in &ids {
            ids_list.push_bind(id);
        }
        ids_list.push_unseparated(") AND status IN ('pending', 'dispatched') AND expires_at <= ");
        query
            .push_bind(now)
            .push(" RETURNING id, device_id, status, completed_at, error");
        let expired = query
            .build_query_as::<CommandStateRow>()
            .fetch_all(&self.pool)
            .await
            .map_err(storage_error)?;
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
        let mut tx = self
            .pool
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(storage_error)?;
        let row = sqlx::query_as::<_, (String, String, i64)>(
            "SELECT device_id, status, expires_at FROM commands WHERE id = ?",
        )
        .bind(&ack.command_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(storage_error)?;
        let Some((stored_device_id, status, expires_at)) = row else {
            tx.rollback().await.map_err(storage_error)?;
            return Ok(AckApplyOutcome::UnknownCommand);
        };
        if stored_device_id != device_id {
            tx.rollback().await.map_err(storage_error)?;
            return Ok(AckApplyOutcome::WrongDevice);
        }

        if status == "succeeded" || status == "failed" {
            tx.rollback().await.map_err(storage_error)?;
            return Ok(AckApplyOutcome::Duplicate);
        }
        let late_ack_json = serde_json::to_string(ack).map_err(storage_error)?;
        let result_json = ack
            .result
            .as_ref()
            .map(serde_json::to_string)
            .transpose()
            .map_err(storage_error)?;

        if status == "expired" && received_at < expires_at {
            tx.rollback().await.map_err(storage_error)?;
            return Ok(AckApplyOutcome::Duplicate);
        }

        if received_at >= expires_at {
            let result = if status != "expired" {
                sqlx::query(
                    "UPDATE commands
                     SET status = 'expired', completed_at = expires_at,
                         late_ack_at = ?, late_ack_json = ?
                     WHERE id = ? AND device_id = ?
                       AND status IN ('pending', 'dispatched') AND expires_at <= ?",
                )
                .bind(received_at)
                .bind(&late_ack_json)
                .bind(&ack.command_id)
                .bind(device_id)
                .bind(received_at)
                .execute(&mut *tx)
                .await
                .map_err(storage_error)?
            } else {
                sqlx::query(
                    "UPDATE commands SET late_ack_at = ?, late_ack_json = ?
                     WHERE id = ? AND device_id = ? AND status = 'expired'
                       AND late_ack_at IS NULL",
                )
                .bind(received_at)
                .bind(&late_ack_json)
                .bind(&ack.command_id)
                .bind(device_id)
                .execute(&mut *tx)
                .await
                .map_err(storage_error)?
            };
            tx.commit().await.map_err(storage_error)?;
            return Ok(if result.rows_affected() == 1 && status != "expired" {
                AckApplyOutcome::ExpiredAndLateRecorded
            } else if result.rows_affected() == 1 {
                AckApplyOutcome::LateRecorded
            } else {
                AckApplyOutcome::Duplicate
            });
        }

        let result = sqlx::query(
            "UPDATE commands
             SET status = ?, completed_at = ?, result_json = ?, error = ?
             WHERE id = ? AND device_id = ?
               AND status IN ('pending', 'dispatched') AND expires_at > ?",
        )
        .bind(ack.status.as_str())
        .bind(received_at)
        .bind(result_json)
        .bind(&ack.error)
        .bind(&ack.command_id)
        .bind(device_id)
        .bind(received_at)
        .execute(&mut *tx)
        .await
        .map_err(storage_error)?;
        tx.commit().await.map_err(storage_error)?;
        Ok(if result.rows_affected() == 1 {
            AckApplyOutcome::Applied
        } else {
            AckApplyOutcome::Duplicate
        })
    }
}

fn storage_error(error: impl std::fmt::Display) -> CommandRepositoryError {
    tracing::warn!(%error, "command storage unavailable");
    CommandRepositoryError::StorageUnavailable
}
