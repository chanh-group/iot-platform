//! Axum router shell: API, health, SPA fallback (`02` §4.9).

use std::time::Duration;

use axum::extract::{DefaultBodyLimit, State};
use axum::http::{Method, StatusCode, Uri, header};
use axum::middleware::from_fn;
use axum::response::{Html, IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde_json::{Value, json};
use tower_http::timeout::TimeoutLayer;
use tower_http::trace::TraceLayer;

use crate::observability::request_id::{self, RequestId};

use super::AppState;
use super::assets;
use super::auth;
use super::error::ApiError;
use super::middleware;

pub fn build(state: AppState) -> Router {
    let max_body_bytes = state.config.config.http.max_body_bytes;
    let request_timeout_ms = state.config.config.http.request_timeout_ms;
    let dashboard = state.config.config.http.dashboard;

    let mut router = Router::new()
        .route("/healthz", get(healthz))
        .route("/readyz", get(readyz))
        .route("/api/v1/auth/login", post(auth::login))
        .route("/api/v1/auth/logout", post(auth::logout))
        .route("/api/v1/auth/me", get(auth::me))
        .fallback(fallback)
        .method_not_allowed_fallback(method_not_allowed);

    if dashboard {
        router = router.route("/", get(index));
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
    // shortcut: phase 1 has no MQTT/ingest tasks, so only the database is checked.
    let database_ok = tokio::time::timeout(
        Duration::from_secs(1),
        sqlx::query_scalar::<_, i64>("SELECT 1").fetch_one(&state.pool),
    )
    .await
    .is_ok_and(|result| result.is_ok());

    if database_ok {
        (
            StatusCode::OK,
            Json(json!({ "status": "ready", "checks": { "database": "ok" } })),
        )
    } else {
        tracing::warn!("readiness check failed");
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({ "status": "not_ready", "checks": { "database": "error" } })),
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
    ApiError::new(StatusCode::NOT_FOUND, "not_found", "not found", request_id)
}

async fn method_not_allowed(RequestId(request_id): RequestId) -> ApiError {
    ApiError::new(
        StatusCode::METHOD_NOT_ALLOWED,
        "method_not_allowed",
        "method not allowed",
        request_id,
    )
}
