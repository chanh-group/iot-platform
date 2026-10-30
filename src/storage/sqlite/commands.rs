//! SQLite command repository (`02` §3.3–§3.6).

use sqlx::{FromRow, QueryBuilder, Sqlite, SqlitePool};
use uuid::Uuid;

use crate::core::auth;
use crate::core::command::{
    self, Command, CommandCursor, CommandRepository, CommandRepositoryError, CommandStatus,
    CreateOutcome, NewCommand,
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
}

fn storage_error(error: impl std::fmt::Display) -> CommandRepositoryError {
    tracing::warn!(%error, "command storage unavailable");
    CommandRepositoryError::StorageUnavailable
}
