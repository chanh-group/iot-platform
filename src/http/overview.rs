//! Dashboard summary queries (`02` §4.7).

use std::time::Duration;

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::Json;
use serde_json::{Value, json};

use crate::core::auth;
use crate::core::domain_error::DomainError;
use crate::observability::metrics::Metrics;
use crate::observability::request_id::RequestId;

use super::AppState;
use super::api_error::ApiError;
use super::auth::AuthSession;

const DAY_MS: i64 = 86_400_000;

pub async fn get(
    State(state): State<AppState>,
    AuthSession(_session): AuthSession,
    RequestId(request_id): RequestId,
) -> Result<Json<Value>, ApiError> {
    let now = auth::now_ms();
    let from = now.saturating_sub(DAY_MS);
    let database_ok = tokio::time::timeout(
        Duration::from_secs(1),
        state.storage_repository.health_check(),
    )
    .await
    .is_ok_and(|result| result.is_ok());
    let device_online = state.presence.online_count().map_err(|error| {
        state.mqtt_failure.report_failure(error);
        ApiError::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "service_unavailable",
            "device presence unavailable".to_owned(),
            request_id.clone(),
        )
    })?;

    let counts = state
        .storage_repository
        .overview(from, now)
        .await
        .map_err(|error| storage_error(error, &request_id))?;

    let config = &state.config.config;
    Ok(Json(json!({
        "devices": {
            "total": counts.devices_total,
            "online": device_online,
            "disabled": counts.devices_disabled,
        },
        "telemetry": {
            "events_24h": counts.telemetry_events_24h,
            "last_received_at": counts.last_received_at,
        },
        "commands": {
            "pending": counts.commands_pending,
            "dispatched": counts.commands_dispatched,
            "failed_24h": counts.commands_failed_24h,
            "expired_24h": counts.commands_expired_24h,
        },
        "ingest": {
            "queue_depth": Metrics::global().telemetry_queue_depth.get(),
            "queue_capacity": config.telemetry.queue_capacity,
            "rejected_24h": Metrics::global().ingest_rejected_24h(),
        },
        "storage_ok": database_ok
            && state.telemetry_ready.load(std::sync::atomic::Ordering::Acquire)
            && state.command_ready.load(std::sync::atomic::Ordering::Acquire),
    })))
}

fn storage_error(_error: impl std::fmt::Display, request_id: &str) -> ApiError {
    tracing::warn!("overview storage unavailable");
    ApiError::from_domain(
        DomainError::Unavailable("storage unavailable".to_owned()),
        request_id.to_owned(),
    )
}
