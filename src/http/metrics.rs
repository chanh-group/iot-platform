//! Bearer-authenticated Prometheus endpoint.

use axum::extract::State;
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use subtle::ConstantTimeEq;

use crate::core::domain_error::DomainError;
use crate::observability::metrics::Metrics;
use crate::observability::request_id::RequestId;

use super::AppState;
use super::api_error::ApiError;

pub async fn export(
    State(state): State<AppState>,
    RequestId(request_id): RequestId,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    if !state.config.config.http.metrics {
        return Err(ApiError::new(
            StatusCode::NOT_FOUND,
            "not_found",
            "not found".to_owned(),
            request_id,
        ));
    }

    let expected = state.config.metrics_token.as_deref().unwrap_or("");
    let valid = headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .is_some_and(|token| token.as_bytes().ct_eq(expected.as_bytes()).unwrap_u8() == 1);
    if !valid || expected.is_empty() {
        return Err(ApiError::new(
            StatusCode::UNAUTHORIZED,
            "unauthenticated",
            "unauthenticated".to_owned(),
            request_id,
        ));
    }

    let pending: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM commands WHERE status IN ('pending', 'dispatched')",
    )
    .fetch_one(&state.pool)
    .await
    .map_err(|error| {
        tracing::warn!(%error, "command metrics storage unavailable");
        ApiError::from_domain(
            DomainError::Unavailable("storage unavailable".to_owned()),
            request_id.clone(),
        )
    })?;
    Metrics::global()
        .commands_pending
        .set(u64::try_from(pending).unwrap_or(0));

    let mut response = Metrics::global().render_prometheus().into_response();
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("text/plain; version=0.0.4; charset=utf-8"),
    );
    Ok(response)
}
