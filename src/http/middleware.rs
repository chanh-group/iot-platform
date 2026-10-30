//! Request hardening (`02` §1.3, §4.1): CSRF, JSON body mapping, login rate limit.

use std::collections::{HashMap, VecDeque};
use std::net::IpAddr;
use std::time::{Duration, Instant};

use axum::extract::{FromRequest, Request};
use axum::http::{HeaderMap, Method, StatusCode, header};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use serde::de::DeserializeOwned;
use tokio::sync::Mutex;

use crate::observability::request_id::RequestId;

use super::api_error::ApiError;

const CSRF_HEADER: &str = "x-iot-csrf";
const CLEANUP_THRESHOLD: usize = 1024;

/// Why a mutating request failed the CSRF gate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CsrfError {
    UnsupportedMediaType,
    Forbidden,
}

/// Pure check so the rules are testable without a server.
pub fn check_csrf(method: &Method, headers: &HeaderMap) -> Result<(), CsrfError> {
    if !matches!(*method, Method::POST | Method::PATCH | Method::DELETE) {
        return Ok(());
    }
    if !is_json(headers) {
        return Err(CsrfError::UnsupportedMediaType);
    }
    if headers
        .get(CSRF_HEADER)
        .and_then(|value| value.to_str().ok())
        != Some("1")
    {
        return Err(CsrfError::Forbidden);
    }
    if let Some(origin) = headers.get(header::ORIGIN) {
        let origin = origin.to_str().map_err(|_| CsrfError::Forbidden)?;
        let origin_host = origin
            .strip_prefix("http://")
            .or_else(|| origin.strip_prefix("https://"))
            .ok_or(CsrfError::Forbidden)?;
        let host = headers
            .get(header::HOST)
            .and_then(|value| value.to_str().ok())
            .ok_or(CsrfError::Forbidden)?;
        if !origin_host.eq_ignore_ascii_case(host) {
            return Err(CsrfError::Forbidden);
        }
    }
    Ok(())
}

fn is_json(headers: &HeaderMap) -> bool {
    headers
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split(';').next())
        .is_some_and(|value| value.trim().eq_ignore_ascii_case("application/json"))
}

/// Rejects mutating requests that fail the CSRF gate.
pub async fn csrf(request: Request, next: Next) -> Response {
    let request_id = request_id(&request);
    match check_csrf(request.method(), request.headers()) {
        Ok(()) => next.run(request).await,
        Err(CsrfError::UnsupportedMediaType) => ApiError::new(
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "unsupported_media_type",
            "content type must be application/json".to_owned(),
            request_id,
        )
        .into_response(),
        Err(CsrfError::Forbidden) => ApiError::new(
            StatusCode::FORBIDDEN,
            "csrf_failed",
            "csrf check failed".to_owned(),
            request_id,
        )
        .into_response(),
    }
}

fn request_id(request: &Request) -> String {
    request
        .extensions()
        .get::<RequestId>()
        .map_or_else(String::new, |id| id.0.clone())
}

/// JSON extractor that maps rejections to the contract error envelope.
pub struct ApiJson<T>(pub T);

impl<T, S> FromRequest<S> for ApiJson<T>
where
    T: DeserializeOwned,
    S: Send + Sync,
{
    type Rejection = ApiError;

    async fn from_request(request: Request, state: &S) -> Result<Self, Self::Rejection> {
        let request_id = request_id(&request);
        match axum::Json::<T>::from_request(request, state).await {
            Ok(axum::Json(value)) => Ok(Self(value)),
            Err(rejection) => {
                let (status, code, message) = match rejection.status() {
                    StatusCode::PAYLOAD_TOO_LARGE => (
                        StatusCode::PAYLOAD_TOO_LARGE,
                        "payload_too_large",
                        "request body too large",
                    ),
                    StatusCode::UNSUPPORTED_MEDIA_TYPE => (
                        StatusCode::UNSUPPORTED_MEDIA_TYPE,
                        "unsupported_media_type",
                        "content type must be application/json",
                    ),
                    StatusCode::UNPROCESSABLE_ENTITY => (
                        StatusCode::BAD_REQUEST,
                        "validation_error",
                        "invalid request body",
                    ),
                    _ => (StatusCode::BAD_REQUEST, "invalid_json", "invalid JSON body"),
                };
                Err(ApiError::new(status, code, message.to_owned(), request_id))
            }
        }
    }
}

/// JSON extractor that also preserves the original bytes for idempotency hashes.
pub struct ApiJsonBytes<T>(pub T, pub axum::body::Bytes);

impl<T, S> FromRequest<S> for ApiJsonBytes<T>
where
    T: DeserializeOwned,
    axum::body::Bytes: FromRequest<S>,
    S: Send + Sync,
{
    type Rejection = ApiError;

    async fn from_request(request: Request, state: &S) -> Result<Self, Self::Rejection> {
        let request_id = request_id(&request);
        let bytes = axum::body::Bytes::from_request(request, state)
            .await
            .map_err(|rejection| {
                let status = rejection.into_response().status();
                if status == StatusCode::PAYLOAD_TOO_LARGE {
                    ApiError::new(
                        StatusCode::PAYLOAD_TOO_LARGE,
                        "payload_too_large",
                        "request body too large".to_owned(),
                        request_id.clone(),
                    )
                } else {
                    ApiError::new(
                        StatusCode::BAD_REQUEST,
                        "invalid_json",
                        "invalid JSON body".to_owned(),
                        request_id.clone(),
                    )
                }
            })?;
        let value = serde_json::from_slice(&bytes).map_err(|error| {
            let code = match error.classify() {
                serde_json::error::Category::Data => "validation_error",
                serde_json::error::Category::Syntax
                | serde_json::error::Category::Eof
                | serde_json::error::Category::Io => "invalid_json",
            };
            ApiError::new(
                StatusCode::BAD_REQUEST,
                code,
                if code == "invalid_json" {
                    "invalid JSON body".to_owned()
                } else {
                    "invalid request body".to_owned()
                },
                request_id,
            )
        })?;
        Ok(Self(value, bytes))
    }
}

struct Inner {
    failures: HashMap<(IpAddr, String), VecDeque<Instant>>,
}

/// In-memory failed-login window keyed by IP + lowercase username.
pub struct LoginRateLimiter {
    max_attempts: usize,
    window: Duration,
    // Async mutex: no poisoning, so a panicked request cannot wedge login.
    inner: Mutex<Inner>,
}

impl LoginRateLimiter {
    /// Builds a limiter with a fixed failure budget per window.
    pub fn new(max_attempts: u64, window: Duration) -> Self {
        Self {
            max_attempts: usize::try_from(max_attempts).unwrap_or(usize::MAX).max(1),
            window,
            inner: Mutex::new(Inner {
                failures: HashMap::new(),
            }),
        }
    }

    /// Returns `Retry-After` seconds when this key is over the limit.
    #[must_use]
    pub async fn check(&self, ip: IpAddr, username: &str) -> Option<u64> {
        let now = Instant::now();
        let key = rate_key(ip, username);
        let mut inner = self.inner.lock().await;
        let times = inner.failures.get_mut(&key)?;
        prune(times, now, self.window);
        if times.len() < self.max_attempts {
            return None;
        }
        let oldest = times.front()?;
        let remaining = self
            .window
            .saturating_sub(now.saturating_duration_since(*oldest));
        let seconds = remaining.as_secs();
        Some(
            if remaining.subsec_nanos() > 0 {
                seconds + 1
            } else {
                seconds
            }
            .max(1),
        )
    }

    /// Records one failed attempt and prunes expired entries.
    pub async fn record_failure(&self, ip: IpAddr, username: &str) {
        let now = Instant::now();
        let key = rate_key(ip, username);
        let mut inner = self.inner.lock().await;
        if inner.failures.len() >= CLEANUP_THRESHOLD {
            inner.failures.retain(|_, times| {
                prune(times, now, self.window);
                !times.is_empty()
            });
        }
        let times = inner.failures.entry(key).or_default();
        prune(times, now, self.window);
        while times.len() >= self.max_attempts {
            times.pop_front();
        }
        times.push_back(now);
    }

    /// Forgets the failures for this key after a successful login.
    pub async fn clear(&self, ip: IpAddr, username: &str) {
        let key = rate_key(ip, username);
        self.inner.lock().await.failures.remove(&key);
    }
}

fn rate_key(ip: IpAddr, username: &str) -> (IpAddr, String) {
    // Login accepts arbitrary strings; cap the retained key so a flood of long
    // usernames cannot grow the limiter map without bound within the window.
    let username = username
        .chars()
        .take(64)
        .map(|character| character.to_ascii_lowercase())
        .collect();
    (ip, username)
}

fn prune(times: &mut VecDeque<Instant>, now: Instant, window: Duration) {
    while times
        .front()
        .is_some_and(|oldest| now.saturating_duration_since(*oldest) >= window)
    {
        times.pop_front();
    }
}

#[cfg(test)]
mod tests {
    use std::net::Ipv4Addr;

    use axum::http::HeaderValue;

    use super::*;

    fn headers(content_type: &str, csrf: Option<&str>, origin: Option<&str>) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(
            header::CONTENT_TYPE,
            HeaderValue::from_str(content_type).unwrap(),
        );
        headers.insert(header::HOST, HeaderValue::from_static("127.0.0.1:8080"));
        if let Some(csrf) = csrf {
            headers.insert(CSRF_HEADER, HeaderValue::from_str(csrf).unwrap());
        }
        if let Some(origin) = origin {
            headers.insert(header::ORIGIN, HeaderValue::from_str(origin).unwrap());
        }
        headers
    }

    #[test]
    fn get_needs_no_csrf() {
        assert!(check_csrf(&Method::GET, &HeaderMap::new()).is_ok());
    }

    #[test]
    fn mutation_requires_json_csrf_and_matching_origin() {
        assert!(matches!(
            check_csrf(&Method::POST, &headers("text/plain", Some("1"), None)),
            Err(CsrfError::UnsupportedMediaType)
        ));
        assert!(matches!(
            check_csrf(&Method::POST, &headers("application/json", None, None)),
            Err(CsrfError::Forbidden)
        ));
        assert!(matches!(
            check_csrf(
                &Method::POST,
                &headers("application/json", Some("1"), Some("http://evil.example"))
            ),
            Err(CsrfError::Forbidden)
        ));
        assert!(
            check_csrf(
                &Method::POST,
                &headers(
                    "application/json; charset=utf-8",
                    Some("1"),
                    Some("http://127.0.0.1:8080")
                )
            )
            .is_ok()
        );
        assert!(
            check_csrf(
                &Method::POST,
                &headers(
                    "application/json",
                    Some("1"),
                    Some("https://127.0.0.1:8080")
                )
            )
            .is_ok()
        );
    }

    #[tokio::test]
    async fn rate_limit_blocks_after_max_failures_until_cleared() {
        let limiter = LoginRateLimiter::new(2, Duration::from_secs(60));
        let ip = IpAddr::V4(Ipv4Addr::LOCALHOST);
        assert_eq!(limiter.check(ip, "Admin").await, None);
        limiter.record_failure(ip, "Admin").await;
        limiter.record_failure(ip, "admin").await;
        assert!(limiter.check(ip, "ADMIN").await.is_some());
        limiter.clear(ip, "admin").await;
        assert_eq!(limiter.check(ip, "admin").await, None);
    }

    #[test]
    fn rate_key_is_lowercase_and_capped_at_64_chars() {
        let ip = IpAddr::V4(Ipv4Addr::LOCALHOST);
        assert_eq!(rate_key(ip, "AdMiN"), rate_key(ip, "admin"));
        let long = "a".repeat(100);
        assert_eq!(
            rate_key(ip, &long),
            rate_key(ip, &format!("{}b", "a".repeat(64)))
        );
    }

    #[tokio::test]
    async fn rate_limit_prunes_old_failures() {
        let limiter = LoginRateLimiter::new(1, Duration::ZERO);
        let ip = IpAddr::V4(Ipv4Addr::LOCALHOST);
        limiter.record_failure(ip, "admin").await;
        assert_eq!(limiter.check(ip, "admin").await, None);
    }
}
