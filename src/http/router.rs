//! Axum router shell for the Phase 1 auth surface (`02` §4).

use std::time::Duration;

use axum::Router;
use axum::extract::DefaultBodyLimit;
use axum::http::StatusCode;
use axum::middleware::from_fn;
use axum::routing::{get, post};
use tower_http::timeout::TimeoutLayer;

use crate::observability::request_id::{self, RequestId};

use super::AppState;
use super::auth;
use super::error::ApiError;
use super::middleware;

pub fn build(state: AppState) -> Router {
    let max_body_bytes = state.config.config.http.max_body_bytes;
    let request_timeout_ms = state.config.config.http.request_timeout_ms;

    Router::new()
        .route("/api/v1/auth/login", post(auth::login))
        .route("/api/v1/auth/logout", post(auth::logout))
        .route("/api/v1/auth/me", get(auth::me))
        .fallback(not_found)
        .method_not_allowed_fallback(method_not_allowed)
        .layer(DefaultBodyLimit::max(
            usize::try_from(max_body_bytes).unwrap_or(usize::MAX),
        ))
        .layer(from_fn(middleware::csrf))
        .layer(TimeoutLayer::with_status_code(
            StatusCode::REQUEST_TIMEOUT,
            Duration::from_millis(request_timeout_ms),
        ))
        .layer(from_fn(request_id::middleware))
        .with_state(state)
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
