//! Runtime config GET/PATCH (`02` §4.6).

use std::net::SocketAddr;

use axum::Json;
use axum::extract::{ConnectInfo, State};
use axum::http::StatusCode;
use serde_json::{Value, json};

use crate::config;
use crate::config::runtime::{RuntimeConfigError, RuntimeSnapshot};
use crate::observability::metrics::Metrics;
use crate::observability::request_id::RequestId;
use crate::storage::sqlite::audit;

use super::AppState;
use super::api_error::ApiError;
use super::auth::AuthSession;
use super::middleware::ApiJson;

/// Returns one consistent effective config snapshot with secret fields redacted.
pub async fn get(
    State(state): State<AppState>,
    AuthSession(_session): AuthSession,
    RequestId(request_id): RequestId,
) -> Result<Json<Value>, ApiError> {
    render(&state.config.runtime_config.load(), &request_id)
}

/// Persists and publishes one serialized hot-config update.
pub async fn patch(
    State(state): State<AppState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    AuthSession(session): AuthSession,
    RequestId(request_id): RequestId,
    ApiJson(body): ApiJson<Value>,
) -> Result<Json<Value>, ApiError> {
    let Some(object) = body.as_object() else {
        return Err(validation_error("body", "expected an object", &request_id));
    };
    if object.len() != 2
        || !object.contains_key("expected_revision")
        || !object.contains_key("settings")
    {
        return Err(validation_error(
            "body",
            "expected only expected_revision and settings",
            &request_id,
        ));
    }
    let Some(expected_revision) = object.get("expected_revision").and_then(Value::as_u64) else {
        return Err(validation_error(
            "expected_revision",
            "expected a non-negative integer",
            &request_id,
        ));
    };
    let settings = &object["settings"];
    let runtime_config = state.config.runtime_config.clone();
    let audit_state = state.clone();
    let actor_id = session.user_id.clone();
    let audit_request_id = request_id.clone();

    let update = runtime_config
        .update_and_then(
            expected_revision,
            settings,
            crate::observability::logging::prepare_reload,
            move |snapshot, changed_fields| async move {
                if !changed_fields.is_empty() {
                    audit_config_update(
                        &audit_state,
                        &actor_id,
                        &peer,
                        &audit_request_id,
                        expected_revision,
                        &snapshot,
                        &changed_fields,
                    )
                    .await;
                    audit_state
                        .events
                        .publish_config_revision(snapshot.revision);
                }
            },
        )
        .await
        .map_err(|error| patch_error(error, &request_id))?;

    if let Some(error) = &update.durability_error {
        tracing::error!(stage = error.stage.name(), %error.source, "runtime config rename committed but directory durability could not be confirmed");
        return Err(post_rename_write_error(
            update.snapshot.revision,
            &request_id,
        ));
    }

    render(&update.snapshot, &request_id)
}

fn render(snapshot: &RuntimeSnapshot, request_id: &str) -> Result<Json<Value>, ApiError> {
    let config = config::redact(&snapshot.config).map_err(|error| {
        tracing::error!(%error, "could not serialize runtime config response");
        internal_error(request_id)
    })?;
    Ok(Json(json!({
        "revision": snapshot.revision,
        "config": config,
        "restart_required": []
    })))
}

async fn audit_config_update(
    state: &AppState,
    actor_id: &str,
    peer: &SocketAddr,
    request_id: &str,
    old_revision: u64,
    snapshot: &RuntimeSnapshot,
    changed_fields: &[String],
) {
    let details = json!({
        "old_revision": old_revision,
        "new_revision": snapshot.revision,
        "paths": changed_fields,
    })
    .to_string();
    let remote_ip = peer.ip().to_string();
    let event = audit::AuditEvent {
        actor_type: "admin",
        actor_id: Some(actor_id),
        action: "config.update",
        target_type: Some("config"),
        target_id: Some("runtime"),
        request_id: Some(request_id),
        remote_ip: Some(&remote_ip),
        details_json: Some(&details),
    };
    let result = async {
        let mut transaction = state.pool.begin().await?;
        audit::insert(&mut transaction, crate::core::auth::now_ms(), &event).await?;
        transaction.commit().await
    }
    .await;
    if let Err(error) = result {
        Metrics::global().audit_write_errors_total.inc();
        tracing::error!(%error, "config update audit write failed after runtime snapshot publication");
    }
}

fn patch_error(error: RuntimeConfigError, request_id: &str) -> ApiError {
    match error {
        RuntimeConfigError::Validation(fields) => ApiError::new(
            StatusCode::BAD_REQUEST,
            "validation_error",
            "invalid runtime config fields".to_owned(),
            request_id.to_owned(),
        )
        .with_details(json!({
            "fields": fields.into_iter().map(|field| {
                let (field, reason) = field.split_once(": ").unwrap_or((&field, "invalid value"));
                json!({"field": field, "reason": reason})
            }).collect::<Vec<_>>()
        })),
        RuntimeConfigError::RestartRequired(fields) => ApiError::new(
            StatusCode::UNPROCESSABLE_ENTITY,
            "restart_required",
            "one or more settings require restart".to_owned(),
            request_id.to_owned(),
        )
        .with_details(json!({"restart_required": fields})),
        RuntimeConfigError::RevisionConflict(current_revision) => ApiError::new(
            StatusCode::CONFLICT,
            "revision_conflict",
            "runtime config revision has changed".to_owned(),
            request_id.to_owned(),
        )
        .with_details(json!({"current_revision": current_revision})),
        RuntimeConfigError::RevisionExhausted => write_error("revision", false, request_id),
        RuntimeConfigError::Serialization(message) => {
            tracing::error!(%message, "could not serialize runtime config update");
            write_error("serialize", false, request_id)
        }
        RuntimeConfigError::Apply(message) => {
            tracing::error!(%message, "could not prepare runtime log filter");
            write_error("filter_prepare", false, request_id)
        }
        RuntimeConfigError::Write(error) => {
            tracing::warn!(stage = error.stage.name(), %error.source, "runtime config write failed before rename");
            write_error(error.stage.name(), false, request_id)
        }
    }
}

fn write_error(stage: &str, applied: bool, request_id: &str) -> ApiError {
    ApiError::new(
        StatusCode::INTERNAL_SERVER_ERROR,
        "config_write_failed",
        "runtime config could not be saved".to_owned(),
        request_id.to_owned(),
    )
    .with_details(json!({"applied": applied, "stage": stage}))
}

fn post_rename_write_error(revision: u64, request_id: &str) -> ApiError {
    ApiError::new(
        StatusCode::INTERNAL_SERVER_ERROR,
        "config_write_failed",
        "runtime config durability could not be confirmed".to_owned(),
        request_id.to_owned(),
    )
    .with_details(json!({
        "applied": true,
        "revision": revision,
        "durability": "uncertain"
    }))
}

fn validation_error(field: &str, reason: &str, request_id: &str) -> ApiError {
    ApiError::new(
        StatusCode::BAD_REQUEST,
        "validation_error",
        "invalid runtime config request".to_owned(),
        request_id.to_owned(),
    )
    .with_details(json!({"fields":[{"field":field,"reason":reason}]}))
}

fn internal_error(request_id: &str) -> ApiError {
    ApiError::new(
        StatusCode::INTERNAL_SERVER_ERROR,
        "internal_error",
        "internal server error".to_owned(),
        request_id.to_owned(),
    )
}

#[cfg(test)]
mod tests {
    use axum::body::to_bytes;
    use axum::response::IntoResponse;

    use super::*;

    #[tokio::test]
    async fn post_rename_error_identifies_applied_revision_and_uncertain_durability() {
        let response = post_rename_write_error(7, "req-12345678").into_response();
        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
        let body: Value =
            serde_json::from_slice(&to_bytes(response.into_body(), 4096).await.unwrap()).unwrap();
        assert_eq!(body["code"], "config_write_failed");
        assert_eq!(body["details"]["applied"], true);
        assert_eq!(body["details"]["revision"], 7);
        assert_eq!(body["details"]["durability"], "uncertain");
    }
}
