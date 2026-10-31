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
        sqlx::query_scalar::<_, i64>("SELECT 1").fetch_one(&state.pool),
    )
    .await
    .is_ok_and(|result| result.is_ok());

    let (device_total, device_disabled): (i64, i64) = sqlx::query_as(
        "SELECT COUNT(*), COALESCE(SUM(CASE WHEN enabled = 0 THEN 1 ELSE 0 END), 0)
         FROM devices",
    )
    .fetch_one(&state.pool)
    .await
    .map_err(|error| storage_error(error, &request_id))?;
    let device_online = state.presence.online_count().map_err(|error| {
        state.mqtt_failure.report_failure(error);
        ApiError::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "service_unavailable",
            "device presence unavailable".to_owned(),
            request_id.clone(),
        )
    })?;

    let (telemetry_events_24h, last_received_at): (i64, Option<i64>) = sqlx::query_as(
        "SELECT
             (SELECT COUNT(*) FROM telemetry_events WHERE received_at >= ? AND received_at < ?),
             (SELECT MAX(received_at) FROM telemetry_events)",
    )
    .bind(from)
    .bind(now)
    .fetch_one(&state.pool)
    .await
    .map_err(|error| storage_error(error, &request_id))?;

    let (pending, dispatched, failed_24h, expired_24h): (i64, i64, i64, i64) =
        sqlx::query_as(
            "SELECT
                 COALESCE(SUM(CASE WHEN status = 'pending' THEN 1 ELSE 0 END), 0),
                 COALESCE(SUM(CASE WHEN status = 'dispatched' THEN 1 ELSE 0 END), 0),
                 COALESCE(SUM(CASE WHEN status = 'failed'
                                   AND completed_at >= ? AND completed_at < ? THEN 1 ELSE 0 END), 0),
                 COALESCE(SUM(CASE WHEN status = 'expired'
                                   AND completed_at >= ? AND completed_at < ? THEN 1 ELSE 0 END), 0)
             FROM commands",
        )
        .bind(from)
        .bind(now)
        .bind(from)
        .bind(now)
        .fetch_one(&state.pool)
        .await
        .map_err(|error| storage_error(error, &request_id))?;

    let config = &state.config.config;
    Ok(Json(json!({
        "devices": {
            "total": device_total,
            "online": device_online,
            "disabled": device_disabled,
        },
        "telemetry": {
            "events_24h": telemetry_events_24h,
            "last_received_at": last_received_at,
        },
        "commands": {
            "pending": pending,
            "dispatched": dispatched,
            "failed_24h": failed_24h,
            "expired_24h": expired_24h,
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

fn storage_error(error: sqlx::Error, request_id: &str) -> ApiError {
    tracing::warn!(%error, "overview storage unavailable");
    ApiError::from_domain(
        DomainError::Unavailable("storage unavailable".to_owned()),
        request_id.to_owned(),
    )
}
