//! Domain errors. No axum/sqlx/rmqtt types here; adapters map their errors in.

use serde::Serialize;
use thiserror::Error;

/// Field-level validation failure, rendered as a `details` array entry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct FieldError {
    pub field: String,
    pub reason: String,
}

/// Failures that map onto the HTTP error envelope; adapters convert their errors.
#[derive(Debug, Error)]
pub enum DomainError {
    #[error("{message}")]
    Validation {
        message: String,
        details: Vec<FieldError>,
    },
    #[error("{0}")]
    NotFound(String),
    #[error("{message}")]
    Conflict {
        /// Contract error code, e.g. `device_name_conflict`.
        code: &'static str,
        message: String,
    },
    #[error("unauthenticated")]
    Unauthorized,
    #[error("{0}")]
    Unavailable(String),
    #[error(transparent)]
    Internal(#[from] anyhow::Error),
}

impl DomainError {
    /// True when the cause must stay out of responses and be logged instead.
    #[must_use]
    pub fn is_internal(&self) -> bool {
        matches!(self, Self::Internal(_))
    }
}
