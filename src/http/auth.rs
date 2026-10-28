//! Admin login/logout/me and session cookie (`02` §1.3, §4.2).

use std::net::SocketAddr;

use axum::Json;
use axum::extract::{ConnectInfo, FromRequestParts, State};
use axum::http::request::Parts;
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum_extra::extract::cookie::CookieJar;
use serde::Deserialize;
use serde_json::{Value, json};

use crate::core::auth;
use crate::core::domain_error::DomainError;
use crate::observability::request_id::RequestId;
use crate::storage::sqlite::audit::{self, AuditEvent};
use crate::storage::sqlite::sessions::{self, Session};
use crate::storage::sqlite::users;

use super::AppState;
use super::api_error::ApiError;
use super::middleware::ApiJson;

const SESSION_COOKIE: &str = "iot_session";

/// Body of `POST /api/v1/auth/login`.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LoginRequest {
    pub username: String,
    pub password: String,
}

/// Extracted session for routes that require an authenticated admin.
pub struct AuthSession(pub Session);

impl FromRequestParts<AppState> for AuthSession {
    type Rejection = ApiError;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &AppState,
    ) -> Result<Self, Self::Rejection> {
        let request_id = parts
            .extensions
            .get::<RequestId>()
            .map_or_else(String::new, |id| id.0.clone());
        let jar = CookieJar::from_request_parts(parts, state)
            .await
            .map_err(|never| match never {})?;
        let Some(token) = jar
            .get(SESSION_COOKIE)
            .map(|cookie| cookie.value().to_owned())
        else {
            return Err(ApiError::from_domain(DomainError::Unauthorized, request_id));
        };

        let token_hash = auth::hash_token(&token);
        let session = sessions::verify(&state.pool, &token_hash, auth::now_ms())
            .await
            .map_err(|error| internal(error, &request_id))?;
        session
            .map(AuthSession)
            .ok_or_else(|| ApiError::from_domain(DomainError::Unauthorized, request_id))
    }
}

/// Verifies credentials, records the audit row, and sets the session cookie.
pub async fn login(
    State(state): State<AppState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    RequestId(request_id): RequestId,
    headers: HeaderMap,
    ApiJson(payload): ApiJson<LoginRequest>,
) -> Result<Response, ApiError> {
    let ip = peer.ip();
    if let Some(retry_after) = state.login_limiter.check(ip, &payload.username).await {
        let mut response = ApiError::new(
            StatusCode::TOO_MANY_REQUESTS,
            "rate_limited",
            "too many login attempts".to_owned(),
            request_id,
        )
        .into_response();
        response
            .headers_mut()
            .insert(header::RETRY_AFTER, HeaderValue::from(retry_after));
        return Ok(response);
    }

    let user = users::find_by_username(&state.pool, &payload.username)
        .await
        .map_err(|error| internal(error, &request_id))?;
    let Some(user) = user else {
        state
            .login_limiter
            .record_failure(ip, &payload.username)
            .await;
        return Err(invalid_credentials(request_id));
    };
    if !user.enabled {
        state
            .login_limiter
            .record_failure(ip, &payload.username)
            .await;
        return Err(invalid_credentials(request_id));
    }

    let password = payload.password;
    let password_hash = user.password_hash.clone();
    let valid =
        tokio::task::spawn_blocking(move || auth::verify_password(&password_hash, &password))
            .await
            .map_err(|error| internal(error, &request_id))?;
    if !valid {
        state
            .login_limiter
            .record_failure(ip, &payload.username)
            .await;
        return Err(invalid_credentials(request_id));
    }
    state.login_limiter.clear(ip, &payload.username).await;

    let token = auth::generate_session_token().map_err(|error| internal(error, &request_id))?;
    let token_hash = auth::hash_token(&token);
    let now = auth::now_ms();
    let ttl_ms = state.config.config.auth.session_ttl_ms;
    let expires_at = now.saturating_add(i64::try_from(ttl_ms).unwrap_or(i64::MAX));
    let remote_ip = peer.ip().to_string();
    let user_agent = headers
        .get(header::USER_AGENT)
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);

    let mut tx = state
        .pool
        .begin()
        .await
        .map_err(|error| internal(error, &request_id))?;
    sessions::create(
        &mut tx,
        &user.id,
        &token_hash,
        now,
        expires_at,
        Some(&remote_ip),
        user_agent.as_deref(),
    )
    .await
    .map_err(|error| internal(error, &request_id))?;
    let details = json!({ "username": user.username }).to_string();
    audit::insert(
        &mut tx,
        now,
        &AuditEvent {
            actor_type: "admin",
            actor_id: Some(&user.id),
            action: "auth.login",
            target_type: Some("user"),
            target_id: Some(&user.id),
            request_id: Some(&request_id),
            remote_ip: Some(&remote_ip),
            details_json: Some(&details),
        },
    )
    .await
    .map_err(|error| internal(error, &request_id))?;
    tx.commit()
        .await
        .map_err(|error| internal(error, &request_id))?;

    let cookie = session_cookie(&token, ttl_ms, state.config.config.http.cookie_secure);
    let mut response = Json(json!({
        "user": { "id": user.id, "username": user.username },
        "expires_at": expires_at,
    }))
    .into_response();
    response.headers_mut().insert(
        header::SET_COOKIE,
        HeaderValue::from_str(&cookie).map_err(|error| internal(error, &request_id))?,
    );
    Ok(response)
}

/// Revokes the current session and clears the cookie.
pub async fn logout(
    State(state): State<AppState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    RequestId(request_id): RequestId,
    AuthSession(session): AuthSession,
) -> Result<Response, ApiError> {
    let now = auth::now_ms();
    let remote_ip = peer.ip().to_string();
    let mut tx = state
        .pool
        .begin()
        .await
        .map_err(|error| internal(error, &request_id))?;
    sessions::revoke(&mut tx, &session.id, now)
        .await
        .map_err(|error| internal(error, &request_id))?;
    audit::insert(
        &mut tx,
        now,
        &AuditEvent {
            actor_type: "admin",
            actor_id: Some(&session.user_id),
            action: "auth.logout",
            target_type: Some("session"),
            target_id: Some(&session.id),
            request_id: Some(&request_id),
            remote_ip: Some(&remote_ip),
            details_json: None,
        },
    )
    .await
    .map_err(|error| internal(error, &request_id))?;
    tx.commit()
        .await
        .map_err(|error| internal(error, &request_id))?;

    let mut response = StatusCode::NO_CONTENT.into_response();
    response.headers_mut().insert(
        header::SET_COOKIE,
        HeaderValue::from_str(&clear_cookie(state.config.config.http.cookie_secure))
            .map_err(|error| internal(error, &request_id))?,
    );
    Ok(response)
}

/// Returns the current admin user and session expiry.
pub async fn me(AuthSession(session): AuthSession) -> Json<Value> {
    Json(json!({
        "id": session.user_id,
        "username": session.username,
        "expires_at": session.expires_at,
    }))
}

fn invalid_credentials(request_id: String) -> ApiError {
    ApiError::new(
        StatusCode::UNAUTHORIZED,
        "invalid_credentials",
        "invalid credentials".to_owned(),
        request_id,
    )
}

fn internal(error: impl Into<anyhow::Error>, request_id: &str) -> ApiError {
    ApiError::from_domain(DomainError::Internal(error.into()), request_id.to_owned())
}

fn session_cookie(token: &str, ttl_ms: u64, secure: bool) -> String {
    let secure = if secure { "; Secure" } else { "" };
    format!(
        "{SESSION_COOKIE}={token}; Path=/; HttpOnly; SameSite=Strict; Max-Age={}{secure}",
        ttl_ms / 1000
    )
}

fn clear_cookie(secure: bool) -> String {
    let secure = if secure { "; Secure" } else { "" };
    format!("{SESSION_COOKIE}=; Path=/; HttpOnly; SameSite=Strict; Max-Age=0{secure}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn session_cookie_has_contract_flags() {
        let cookie = session_cookie("token", 86_400_000, false);
        assert!(cookie.starts_with("iot_session=token;"));
        assert!(cookie.contains("HttpOnly"));
        assert!(cookie.contains("SameSite=Strict"));
        assert!(cookie.contains("Path=/"));
        assert!(cookie.contains("Max-Age=86400"));
        assert!(!cookie.contains("Secure"));
    }

    #[test]
    fn secure_cookie_is_opt_in() {
        assert!(session_cookie("token", 300_000, true).contains("; Secure"));
        assert!(clear_cookie(true).contains("; Secure"));
        assert!(!clear_cookie(false).contains("; Secure"));
    }
}
