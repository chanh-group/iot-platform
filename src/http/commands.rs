//! Admin command creation and read-only command REST endpoints.

use std::net::SocketAddr;

use axum::Json;
use axum::extract::rejection::QueryRejection;
use axum::extract::{ConnectInfo, Path, Query, State};
use axum::http::{HeaderName, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

use crate::core::command::{
    self, CommandAudit, CommandCursor, CommandRepositoryError, CommandStatus, NewCommand,
};
use crate::core::domain_error::DomainError;
use crate::observability::request_id::RequestId;

use super::AppState;
use super::api_error::ApiError;
use super::auth::AuthSession;
use super::middleware::ApiJsonBytes;

#[derive(Debug, Deserialize)]
pub struct ListQuery {
    limit: Option<i64>,
    cursor: Option<String>,
    status: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Cursor {
    v: u8,
    device_id: String,
    status: Option<CommandStatus>,
    created_at: i64,
    id: String,
}

/// Creates and audits a command before returning its durable ID.
pub async fn create(
    State(state): State<AppState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    AuthSession(session): AuthSession,
    RequestId(request_id): RequestId,
    Path(device_id): Path<String>,
    ApiJsonBytes(payload, body): ApiJsonBytes<Value>,
) -> Result<Response, ApiError> {
    let request_sha256: [u8; 32] = Sha256::digest(&body).into();
    let remote_ip = peer.ip().to_string();
    let outcome = state
        .command_repository
        .create(NewCommand {
            device_id: &device_id,
            request: payload,
            request_sha256,
            default_ttl_ms: state.config.config.commands.default_ttl_ms,
            max_ttl_ms: state.config.config.commands.max_ttl_ms,
            max_pending_per_device: state.config.config.commands.max_pending_per_device,
            audit: CommandAudit {
                actor_id: &session.user_id,
                request_id: &request_id,
                remote_ip: &remote_ip,
            },
        })
        .await
        .map_err(|error| repository_error(error, &request_id))?;

    let command = outcome.command;
    let mut response = (
        StatusCode::ACCEPTED,
        Json(json!({
            "command_id": command.id,
            "device_id": command.device_id,
            "status": command.status.as_str(),
            "expires_at": command.expires_at,
        })),
    )
        .into_response();
    let location = format!("/api/v1/commands/{}", command.id);
    response.headers_mut().insert(
        header::LOCATION,
        HeaderValue::from_bytes(location.as_bytes())
            .map_err(|error| internal_error(error, &request_id))?,
    );
    if outcome.replayed {
        response.headers_mut().insert(
            HeaderName::from_static("idempotent-replayed"),
            HeaderValue::from_static("true"),
        );
    }
    Ok(response)
}

/// Returns one command without its internal idempotency or retry fields.
pub async fn get(
    State(state): State<AppState>,
    AuthSession(_session): AuthSession,
    RequestId(request_id): RequestId,
    Path(id): Path<String>,
) -> Result<Json<command::Command>, ApiError> {
    state
        .command_repository
        .get(&id)
        .await
        .map_err(|error| repository_error(error, &request_id))?
        .map(Json)
        .ok_or_else(|| {
            ApiError::new(
                StatusCode::NOT_FOUND,
                "command_not_found",
                "command not found".to_owned(),
                request_id,
            )
        })
}

/// Lists one device's commands by stable descending `(created_at, id)` order.
pub async fn list(
    State(state): State<AppState>,
    AuthSession(_session): AuthSession,
    RequestId(request_id): RequestId,
    Path(device_id): Path<String>,
    query: Result<Query<ListQuery>, QueryRejection>,
) -> Result<Json<Value>, ApiError> {
    let Query(query) = query.map_err(|_| validation_error(&request_id, "invalid query"))?;
    let limit = query.limit.unwrap_or(50);
    if !(1..=200).contains(&limit) {
        return Err(validation_error(&request_id, "limit must be 1..=200"));
    }
    let status = match query.status.as_deref() {
        Some(status) => Some(
            CommandStatus::parse(status)
                .ok_or_else(|| validation_error(&request_id, "invalid command status"))?,
        ),
        None => None,
    };
    let after = query
        .cursor
        .as_deref()
        .map(|encoded| decode_cursor(encoded, &device_id, status, &request_id))
        .transpose()?;
    let mut rows = state
        .command_repository
        .list(&device_id, status, after, limit + 1)
        .await
        .map_err(|error| repository_error(error, &request_id))?;
    let has_more = rows.len() > limit as usize;
    rows.truncate(limit as usize);
    let next_cursor = if has_more {
        rows.last()
            .map(|row| {
                encode_cursor(
                    &Cursor {
                        v: 1,
                        device_id,
                        status,
                        created_at: row.created_at,
                        id: row.id.clone(),
                    },
                    &request_id,
                )
            })
            .transpose()?
    } else {
        None
    };
    Ok(Json(json!({"items": rows, "next_cursor": next_cursor})))
}

fn encode_cursor(cursor: &Cursor, request_id: &str) -> Result<String, ApiError> {
    serde_json::to_vec(cursor)
        .map(|bytes| URL_SAFE_NO_PAD.encode(bytes))
        .map_err(|error| internal_error(error, request_id))
}

fn decode_cursor(
    encoded: &str,
    device_id: &str,
    status: Option<CommandStatus>,
    request_id: &str,
) -> Result<CommandCursor, ApiError> {
    let invalid = || {
        ApiError::new(
            StatusCode::BAD_REQUEST,
            "invalid_cursor",
            "invalid command cursor".to_owned(),
            request_id.to_owned(),
        )
    };
    if encoded.len() > 4_096 {
        return Err(invalid());
    }
    let bytes = URL_SAFE_NO_PAD.decode(encoded).map_err(|_| invalid())?;
    let cursor: Cursor = serde_json::from_slice(&bytes).map_err(|_| invalid())?;
    if cursor.v != 1
        || cursor.device_id != device_id
        || cursor.status != status
        || cursor.created_at <= 0
        || !crate::core::auth::is_device_id(&cursor.id)
    {
        return Err(invalid());
    }
    Ok(CommandCursor {
        created_at: cursor.created_at,
        id: cursor.id,
    })
}

fn repository_error(error: CommandRepositoryError, request_id: &str) -> ApiError {
    match error {
        CommandRepositoryError::Validation(details) => ApiError::from_domain(
            DomainError::Validation {
                message: "invalid command".to_owned(),
                details,
            },
            request_id.to_owned(),
        ),
        CommandRepositoryError::DeviceNotFound => ApiError::new(
            StatusCode::NOT_FOUND,
            "device_not_found",
            "device not found".to_owned(),
            request_id.to_owned(),
        ),
        CommandRepositoryError::DeviceDisabled => ApiError::new(
            StatusCode::CONFLICT,
            "device_disabled",
            "device is disabled".to_owned(),
            request_id.to_owned(),
        ),
        CommandRepositoryError::QueueFull { limit } => ApiError::new(
            StatusCode::CONFLICT,
            "command_queue_full",
            "device command queue is full".to_owned(),
            request_id.to_owned(),
        )
        .with_details(json!({"limit": limit})),
        CommandRepositoryError::IdempotencyConflict => ApiError::new(
            StatusCode::CONFLICT,
            "idempotency_conflict",
            "idempotency key was used with a different request".to_owned(),
            request_id.to_owned(),
        ),
        CommandRepositoryError::StorageUnavailable => ApiError::from_domain(
            DomainError::Unavailable("storage unavailable".to_owned()),
            request_id.to_owned(),
        ),
    }
}

fn validation_error(request_id: &str, message: &str) -> ApiError {
    ApiError::new(
        StatusCode::BAD_REQUEST,
        "validation_error",
        message.to_owned(),
        request_id.to_owned(),
    )
}

fn internal_error(error: impl Into<anyhow::Error>, request_id: &str) -> ApiError {
    ApiError::from_domain(DomainError::Internal(error.into()), request_id.to_owned())
}
