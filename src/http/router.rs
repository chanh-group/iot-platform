//! Axum router shell: API, health, SPA fallback (`02` §4.9).

use std::sync::atomic::Ordering;
use std::time::Duration;

use axum::extract::{DefaultBodyLimit, State};
use axum::http::{Method, StatusCode, Uri, header};
use axum::middleware::from_fn;
use axum::response::{Html, IntoResponse, Response};
use axum::routing;
use axum::{Json, Router};
use serde_json::{Value, json};
use tower_http::timeout::TimeoutLayer;
use tower_http::trace::TraceLayer;

use crate::observability::request_id::{self, RequestId};

use super::AppState;
use super::api_error::ApiError;
use super::assets;
use super::auth;
use super::middleware;
use super::{commands, devices, events, metrics, telemetry};

/// Builds the app: API routes, health checks, SPA fallback, and layers.
pub fn build(state: AppState) -> Router {
    let max_body_bytes = state.config.config.http.max_body_bytes;
    let request_timeout_ms = state.config.config.http.request_timeout_ms;
    let dashboard = state.config.config.http.dashboard;

    let mut router = Router::new()
        .route("/healthz", routing::get(healthz))
        .route("/readyz", routing::get(readyz))
        .route("/api/v1/auth/login", routing::post(auth::login))
        .route("/api/v1/auth/logout", routing::post(auth::logout))
        .route("/api/v1/auth/me", routing::get(auth::me))
        .route(
            "/api/v1/devices",
            routing::get(devices::list).post(devices::create),
        )
        .route(
            "/api/v1/devices/{id}",
            routing::get(devices::get).patch(devices::patch),
        )
        .route(
            "/api/v1/devices/{id}/credentials/rotate",
            routing::post(devices::rotate),
        )
        .route(
            "/api/v1/devices/{id}/telemetry",
            routing::get(telemetry::history),
        )
        .route(
            "/api/v1/devices/{id}/telemetry/latest",
            routing::get(telemetry::latest),
        )
        .route(
            "/api/v1/devices/{id}/attributes",
            routing::get(telemetry::attributes),
        )
        .route(
            "/api/v1/devices/{id}/commands",
            routing::get(commands::list).post(commands::create),
        )
        .route("/api/v1/commands/{id}", routing::get(commands::get))
        .route("/api/v1/events", routing::get(events::upgrade))
        .route("/metrics", routing::get(metrics::export))
        .fallback(fallback)
        .method_not_allowed_fallback(method_not_allowed);

    if dashboard {
        router = router.route("/", routing::get(index));
    }

    router
        .layer(DefaultBodyLimit::max(
            usize::try_from(max_body_bytes).unwrap_or(usize::MAX),
        ))
        .layer(from_fn(middleware::csrf))
        .layer(TimeoutLayer::with_status_code(
            StatusCode::REQUEST_TIMEOUT,
            Duration::from_millis(request_timeout_ms),
        ))
        .layer(TraceLayer::new_for_http())
        .layer(from_fn(request_id::middleware))
        .with_state(state)
}

async fn healthz() -> Json<Value> {
    Json(json!({ "status": "ok" }))
}

async fn readyz(State(state): State<AppState>) -> (StatusCode, Json<Value>) {
    let ready = state.ready.load(Ordering::Acquire);
    let database_ok = ready
        && tokio::time::timeout(
            Duration::from_secs(1),
            sqlx::query_scalar::<_, i64>("SELECT 1").fetch_one(&state.pool),
        )
        .await
        .is_ok_and(|result| result.is_ok());

    let mqtt_ok = state.mqtt_ready.load(Ordering::Acquire);
    let telemetry_ok = state.telemetry_ready.load(Ordering::Acquire);
    let commands_ok = state.command_ready.load(Ordering::Acquire);

    if database_ok && mqtt_ok && telemetry_ok && commands_ok {
        (
            StatusCode::OK,
            Json(
                json!({ "status": "ready", "checks": { "database": "ok", "mqtt": "ok", "ingest": "ok", "commands": "ok" } }),
            ),
        )
    } else {
        tracing::warn!("readiness check failed");
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({
                "status": "not_ready",
                "checks": {
                    "database": if database_ok { "ok" } else { "error" },
                    "mqtt": if mqtt_ok { "ok" } else { "error" },
                    "ingest": if telemetry_ok { "ok" } else { "degraded" },
                    "commands": if commands_ok { "ok" } else { "error" }
                }
            })),
        )
    }
}

async fn index() -> Response {
    let mut response = Html(assets::INDEX_HTML).into_response();
    response.headers_mut().insert(
        header::CACHE_CONTROL,
        header::HeaderValue::from_static("no-store"),
    );
    response
}

/// Non-API GETs are the SPA; everything else gets the JSON error envelope.
async fn fallback(
    State(state): State<AppState>,
    RequestId(request_id): RequestId,
    method: Method,
    uri: Uri,
) -> Response {
    let path = uri.path();
    if state.config.config.http.dashboard
        && matches!(method, Method::GET | Method::HEAD)
        && !path.starts_with("/api")
        && !path.starts_with("/assets")
    {
        return index().await;
    }
    not_found(RequestId(request_id)).await.into_response()
}

async fn not_found(RequestId(request_id): RequestId) -> ApiError {
    ApiError::new(
        StatusCode::NOT_FOUND,
        "not_found",
        "not found".to_owned(),
        request_id,
    )
}

async fn method_not_allowed(RequestId(request_id): RequestId) -> ApiError {
    ApiError::new(
        StatusCode::METHOD_NOT_ALLOWED,
        "method_not_allowed",
        "method not allowed".to_owned(),
        request_id,
    )
}
