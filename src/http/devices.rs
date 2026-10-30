//! Read-only device administration queries.

use axum::Json;
use axum::extract::{Path, Query, State, rejection::QueryRejection};
use axum::http::StatusCode;
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::core::auth;
use crate::core::domain_error::DomainError;
use crate::observability::request_id::RequestId;
use crate::storage::sqlite::devices::{self, DeviceRow};

use super::AppState;
use super::api_error::ApiError;
use super::auth::AuthSession;

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
        state.presence_failure.report(failure);
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
