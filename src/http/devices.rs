//! Device administration HTTP handlers.

use std::net::SocketAddr;

use axum::Json;
use axum::extract::{ConnectInfo, Path, Query, State, rejection::QueryRejection};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::sync::oneshot;
use uuid::Uuid;

use crate::core::auth;
use crate::core::device;
use crate::core::domain_error::{DomainError, FieldError};
use crate::observability::request_id::RequestId;
use crate::storage::sqlite::audit::{self, AuditEvent};
use crate::storage::sqlite::devices::{self, DeviceRow};

use super::AppState;
use super::api_error::ApiError;
use super::auth::AuthSession;
use super::middleware::ApiJson;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CreateDeviceRequest {
    name: String,
    #[serde(default = "empty_metadata")]
    metadata: Value,
    #[serde(default = "enabled_by_default")]
    enabled: bool,
}

fn empty_metadata() -> Value {
    json!({})
}

const fn enabled_by_default() -> bool {
    true
}

#[derive(Deserialize)]
pub struct ListQuery {
    limit: Option<i64>,
    cursor: Option<String>,
    q: Option<String>,
    enabled: Option<bool>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Cursor {
    v: u8,
    created_at: i64,
    id: String,
    q: Option<String>,
    enabled: Option<bool>,
}

/// Creates a device and its first credential atomically.
pub async fn create(
    State(state): State<AppState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    AuthSession(session): AuthSession,
    RequestId(request_id): RequestId,
    ApiJson(payload): ApiJson<CreateDeviceRequest>,
) -> Result<impl IntoResponse, ApiError> {
    let mut errors = Vec::new();
    let name = device::validate_name(&payload.name)
        .map_err(|field| errors.push(field))
        .ok();
    let metadata_json = device::validate_metadata(&payload.metadata)
        .map_err(|field| errors.push(field))
        .ok();
    if !errors.is_empty() {
        return Err(validation_error(errors, &request_id));
    }
    let name = name.ok_or_else(|| internal_error("validated device name missing", &request_id))?;
    let metadata_json = metadata_json
        .ok_or_else(|| internal_error("validated device metadata missing", &request_id))?;

    let id = Uuid::now_v7().to_string();
    let credential_id = Uuid::now_v7().to_string();
    let token = format!(
        "iot_{}",
        auth::generate_session_token().map_err(|error| {
            ApiError::from_domain(DomainError::Internal(error.into()), request_id.clone())
        })?
    );
    let token_hash = auth::hash_token(&token);
    let now = auth::now_ms();
    let remote_ip = peer.ip().to_string();
    let mut tx = state
        .pool
        .begin()
        .await
        .map_err(|error| storage_error(error, &request_id))?;
    if let Err(db_error) = devices::insert_with_credential(
        &mut tx,
        &devices::NewDevice {
            id: &id,
            name: &name,
            metadata_json: &metadata_json,
            enabled: payload.enabled,
            created_at: now,
            credential_id: &credential_id,
            token_hash: &token_hash,
        },
    )
    .await
    {
        if is_device_name_conflict(&db_error) {
            return Err(error(
                StatusCode::CONFLICT,
                "device_name_conflict",
                "device name already exists",
                &request_id,
            ));
        }
        return Err(storage_error(db_error, &request_id));
    }
    let details = json!({"name": name, "enabled": payload.enabled}).to_string();
    audit::insert(
        &mut tx,
        now,
        &AuditEvent {
            actor_type: "admin",
            actor_id: Some(&session.user_id),
            action: "device.create",
            target_type: Some("device"),
            target_id: Some(&id),
            request_id: Some(&request_id),
            remote_ip: Some(&remote_ip),
            details_json: Some(&details),
        },
    )
    .await
    .map_err(|error| storage_error(error, &request_id))?;
    tx.commit()
        .await
        .map_err(|error| storage_error(error, &request_id))?;

    let metadata = payload.metadata;
    let created = json!({
        "id": id,
        "name": name,
        "enabled": payload.enabled,
        "online": false,
        "metadata": metadata,
        "created_at": now,
        "updated_at": now,
        "last_seen_at": null,
        "credential": {"id": credential_id, "created_at": now},
    });
    Ok((
        StatusCode::CREATED,
        Json(json!({
            "device": created,
            "credential": {"id": credential_id, "token": token, "created_at": now},
        })),
    )
        .into_response())
}

/// Partially updates a device; disabling commits before its session is kicked.
pub async fn patch(
    State(state): State<AppState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    AuthSession(session): AuthSession,
    RequestId(request_id): RequestId,
    Path(id): Path<String>,
    ApiJson(body): ApiJson<Value>,
) -> Result<Json<Value>, ApiError> {
    let patch =
        device::validate_patch(&body).map_err(|errors| validation_error(errors, &request_id))?;
    let kick_reservation = if patch.enabled == Some(false) {
        let permit = state.device_kicks.reserve().await.map_err(|_| {
            state
                .mqtt_failure
                .report_failure("device kick worker is unavailable");
            error(
                StatusCode::SERVICE_UNAVAILABLE,
                "service_unavailable",
                "device kick unavailable",
                &request_id,
            )
        })?;
        let admission = state.device_admission.block(&id).map_err(|failure| {
            state.mqtt_failure.report_failure(failure);
            error(
                StatusCode::SERVICE_UNAVAILABLE,
                "service_unavailable",
                "device admission unavailable",
                &request_id,
            )
        })?;
        admission.wait_for_pending().await.map_err(|failure| {
            state.mqtt_failure.report_failure(failure);
            error(
                StatusCode::SERVICE_UNAVAILABLE,
                "service_unavailable",
                "device admission unavailable",
                &request_id,
            )
        })?;
        Some((permit, admission))
    } else {
        None
    };
    let now = auth::now_ms();
    let remote_ip = peer.ip().to_string();
    let mut tx = state
        .pool
        .begin()
        .await
        .map_err(|error| storage_error(error, &request_id))?;
    let old_device = devices::get_on_connection(&mut tx, &id)
        .await
        .map_err(|error| storage_error(error, &request_id))?
        .ok_or_else(|| {
            error(
                StatusCode::NOT_FOUND,
                "device_not_found",
                "device not found",
                &request_id,
            )
        })?;
    if let Err(db_error) = devices::update(
        &mut tx,
        &id,
        patch.name.as_deref(),
        patch.metadata_json.as_deref(),
        patch.enabled,
        now,
    )
    .await
    {
        if is_device_name_conflict(&db_error) {
            return Err(error(
                StatusCode::CONFLICT,
                "device_name_conflict",
                "device name already exists",
                &request_id,
            ));
        }
        return Err(storage_error(db_error, &request_id));
    }
    let enabled_change = patch
        .enabled
        .filter(|enabled| *enabled != old_device.enabled);
    let action = match enabled_change {
        Some(true) => "device.enable",
        Some(false) => "device.disable",
        None => "device.update",
    };
    let mut details = json!({
        "name": patch.name.is_some(),
        "metadata": patch.metadata_json.is_some(),
    });
    if let Some(enabled) = enabled_change {
        details["enabled_from"] = json!(old_device.enabled);
        details["enabled_to"] = json!(enabled);
    }
    let details = details.to_string();
    audit::insert(
        &mut tx,
        now,
        &AuditEvent {
            actor_type: "admin",
            actor_id: Some(&session.user_id),
            action,
            target_type: Some("device"),
            target_id: Some(&id),
            request_id: Some(&request_id),
            remote_ip: Some(&remote_ip),
            details_json: Some(&details),
        },
    )
    .await
    .map_err(|error| storage_error(error, &request_id))?;
    if let Some((_, admission)) = &kick_reservation {
        admission.set_device_enabled(false).map_err(|failure| {
            state.mqtt_failure.report_failure(failure);
            error(
                StatusCode::SERVICE_UNAVAILABLE,
                "service_unavailable",
                "device admission unavailable",
                &request_id,
            )
        })?;
    }
    let (commit_sender, kick_completion) = if let Some((permit, admission)) = kick_reservation {
        let (commit_sender, committed) = oneshot::channel();
        let completion = permit.enqueue(id.clone(), admission, committed);
        (Some(commit_sender), Some(completion))
    } else {
        (None, None)
    };
    if let Err(error) = tx.commit().await {
        drop(commit_sender);
        return Err(storage_error(error, &request_id));
    }
    if let Some(commit_sender) = commit_sender {
        let _ = commit_sender.send(true);
    }
    if patch.enabled == Some(true) {
        state
            .device_admission
            .set_device_enabled(&id, true)
            .map_err(|failure| {
                state.mqtt_failure.report_failure(failure);
                error(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "service_unavailable",
                    "device admission unavailable",
                    &request_id,
                )
            })?;
    }
    if let Some(kick_completion) = kick_completion {
        match kick_completion.await {
            Ok(Ok(())) => {}
            Ok(Err(())) | Err(_) => {
                return Err(error(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "device_kick_failed",
                    "device session could not be closed",
                    &request_id,
                ));
            }
        }
    }
    let row = devices::get(&state.pool, &id)
        .await
        .map_err(|error| storage_error(error, &request_id))?
        .ok_or_else(|| internal_error("device disappeared after update", &request_id))?;
    Ok(Json(render(&state, row, &request_id)?))
}

/// Lists device summaries in descending creation order.
pub async fn list(
    State(state): State<AppState>,
    AuthSession(_session): AuthSession,
    RequestId(request_id): RequestId,
    query: Result<Query<ListQuery>, QueryRejection>,
) -> Result<Json<Value>, ApiError> {
    let Query(query) = query.map_err(|_| {
        error(
            StatusCode::BAD_REQUEST,
            "validation_error",
            "invalid query",
            &request_id,
        )
    })?;
    let limit = query.limit.unwrap_or(50);
    if !(1..=200).contains(&limit)
        || query
            .q
            .as_ref()
            .is_some_and(|q| !(2..=64).contains(&q.chars().count()))
    {
        return Err(error(
            StatusCode::BAD_REQUEST,
            "validation_error",
            "invalid query",
            &request_id,
        ));
    }
    let after = query
        .cursor
        .as_deref()
        .map(|encoded| {
            let invalid = || {
                error(
                    StatusCode::BAD_REQUEST,
                    "invalid_cursor",
                    "invalid cursor",
                    &request_id,
                )
            };
            if encoded.len() > 4096 {
                return Err(invalid());
            }
            let bytes = URL_SAFE_NO_PAD.decode(encoded).map_err(|_| invalid())?;
            let cursor: Cursor = serde_json::from_slice(&bytes).map_err(|_| invalid())?;
            if cursor.v != 1
                || cursor.q != query.q
                || cursor.enabled != query.enabled
                || !auth::is_device_id(&cursor.id)
                || cursor.created_at < 0
            {
                return Err(invalid());
            }
            Ok(cursor)
        })
        .transpose()?;
    let mut rows = devices::list(
        &state.pool,
        query.q.as_deref(),
        query.enabled,
        after.as_ref().map(|c| (c.created_at, c.id.as_str())),
        limit + 1,
    )
    .await
    .map_err(|e| storage_error(e, &request_id))?;
    let more = rows.len() > limit as usize;
    rows.truncate(limit as usize);
    let next_cursor = if more {
        rows.last()
            .map(|row| {
                serde_json::to_vec(&Cursor {
                    v: 1,
                    created_at: row.created_at,
                    id: row.id.clone(),
                    q: query.q.clone(),
                    enabled: query.enabled,
                })
                .map(|bytes| URL_SAFE_NO_PAD.encode(bytes))
                .map_err(|e| {
                    ApiError::from_domain(DomainError::Internal(e.into()), request_id.clone())
                })
            })
            .transpose()?
    } else {
        None
    };
    let items = rows
        .into_iter()
        .map(|row| render(&state, row, &request_id))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Json(json!({"items": items, "next_cursor": next_cursor})))
}

/// Returns a device summary without its credential secret.
pub async fn get(
    State(state): State<AppState>,
    AuthSession(_session): AuthSession,
    RequestId(request_id): RequestId,
    Path(id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    let row = devices::get(&state.pool, &id)
        .await
        .map_err(|e| storage_error(e, &request_id))?
        .ok_or_else(|| {
            error(
                StatusCode::NOT_FOUND,
                "device_not_found",
                "device not found",
                &request_id,
            )
        })?;
    Ok(Json(render(&state, row, &request_id)?))
}

fn render(state: &AppState, row: DeviceRow, request_id: &str) -> Result<Value, ApiError> {
    let online = state.presence.is_online(&row.id).map_err(|failure| {
        state.mqtt_failure.report_failure(failure);
        error(
            StatusCode::SERVICE_UNAVAILABLE,
            "service_unavailable",
            "device presence unavailable",
            request_id,
        )
    })?;
    let (Some(credential_id), Some(credential_created_at)) =
        (row.credential_id, row.credential_created_at)
    else {
        return Err(error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal_error",
            "internal server error",
            request_id,
        ));
    };
    let metadata: Value = serde_json::from_str(&row.metadata_json).map_err(|e| {
        ApiError::from_domain(DomainError::Internal(e.into()), request_id.to_owned())
    })?;
    Ok(
        json!({"id": row.id, "name": row.name, "enabled": row.enabled, "online": online, "metadata": metadata, "created_at": row.created_at, "updated_at": row.updated_at, "last_seen_at": row.last_seen_at, "credential": {"id": credential_id, "created_at": credential_created_at}}),
    )
}

fn storage_error(error: sqlx::Error, request_id: &str) -> ApiError {
    tracing::warn!(%error, "device storage unavailable");
    ApiError::from_domain(
        DomainError::Unavailable("storage unavailable".to_owned()),
        request_id.to_owned(),
    )
}

fn error(status: StatusCode, code: &'static str, message: &str, request_id: &str) -> ApiError {
    ApiError::new(status, code, message.to_owned(), request_id.to_owned())
}

fn validation_error(fields: Vec<FieldError>, request_id: &str) -> ApiError {
    ApiError::from_domain(
        DomainError::Validation {
            message: "invalid device fields".to_owned(),
            details: fields,
        },
        request_id.to_owned(),
    )
}

fn internal_error(error: impl std::fmt::Display, request_id: &str) -> ApiError {
    ApiError::from_domain(
        DomainError::Internal(anyhow::anyhow!("{error}")),
        request_id.to_owned(),
    )
}

fn is_device_name_conflict(error: &sqlx::Error) -> bool {
    error
        .as_database_error()
        .is_some_and(|error| error.message().contains("devices.name"))
}
