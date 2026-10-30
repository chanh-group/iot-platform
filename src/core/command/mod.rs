//! Command domain and persistence port.

mod create;
mod state;

pub use create::{
    CreateCommandRequest, ValidatedCommand, validate_create, validate_create_value,
    validate_idempotency_key,
};
pub use state::CommandStatus;

use serde::Serialize;
use serde_json::Value;
use thiserror::Error;

/// A command as returned by the REST API.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Command {
    pub id: String,
    pub device_id: String,
    pub name: String,
    pub params: Value,
    pub status: CommandStatus,
    pub created_at: i64,
    pub expires_at: i64,
    pub dispatched_at: Option<i64>,
    pub completed_at: Option<i64>,
    pub result: Option<Value>,
    pub error: Option<String>,
    pub attempts: i64,
}

/// Validated create request plus request/audit context for an atomic insert.
pub struct NewCommand<'a> {
    pub device_id: &'a str,
    pub request: Value,
    pub request_sha256: [u8; 32],
    pub default_ttl_ms: u64,
    pub max_ttl_ms: u64,
    pub max_pending_per_device: u64,
    pub audit: CommandAudit<'a>,
}

/// Server-controlled audit fields for command creation.
pub struct CommandAudit<'a> {
    pub actor_id: &'a str,
    pub request_id: &'a str,
    pub remote_ip: &'a str,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandCursor {
    pub created_at: i64,
    pub id: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CreateOutcome {
    pub command: Command,
    pub replayed: bool,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum CommandRepositoryError {
    #[error("invalid command")]
    Validation(Vec<crate::core::domain_error::FieldError>),
    #[error("device not found")]
    DeviceNotFound,
    #[error("device is disabled")]
    DeviceDisabled,
    #[error("command queue is full")]
    QueueFull { limit: u64 },
    #[error("idempotency key conflicts with a different request")]
    IdempotencyConflict,
    #[error("command storage unavailable")]
    StorageUnavailable,
}

#[async_trait::async_trait]
pub trait CommandRepository: Send + Sync {
    async fn create(
        &self,
        command: NewCommand<'_>,
    ) -> Result<CreateOutcome, CommandRepositoryError>;

    async fn get(&self, id: &str) -> Result<Option<Command>, CommandRepositoryError>;

    async fn list(
        &self,
        device_id: &str,
        status: Option<CommandStatus>,
        after: Option<CommandCursor>,
        limit: i64,
    ) -> Result<Vec<Command>, CommandRepositoryError>;
}
