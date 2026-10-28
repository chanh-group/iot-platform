//! `X-Request-ID` accept/generate/echo (`02` §1.2).

use axum::extract::{FromRequestParts, Request};
use axum::http::{HeaderName, HeaderValue, request::Parts};
use axum::middleware::Next;
use axum::response::Response;
use tracing::Instrument;
use uuid::Uuid;

const HEADER: &str = "x-request-id";

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RequestId(pub String);

fn valid(value: &str) -> bool {
    (8..=64).contains(&value.len())
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'))
}

fn normalize(incoming: Option<&HeaderValue>) -> String {
    incoming
        .and_then(|value| value.to_str().ok())
        .filter(|value| valid(value))
        .map_or_else(|| Uuid::now_v7().to_string(), str::to_owned)
}

fn attach(mut response: Response, id: &str) -> Response {
    if let Ok(value) = HeaderValue::from_str(id) {
        response
            .headers_mut()
            .insert(HeaderName::from_static(HEADER), value);
    }
    response
}

pub async fn middleware(mut request: Request, next: Next) -> Response {
    let id = normalize(request.headers().get(HEADER));
    request.extensions_mut().insert(RequestId(id.clone()));
    let span = tracing::info_span!(
        "request",
        request_id = %id,
        method = %request.method(),
        path = %request.uri().path(),
    );
    let response = next.run(request).instrument(span).await;
    attach(response, &id)
}

impl<S> FromRequestParts<S> for RequestId
where
    S: Send + Sync,
{
    type Rejection = std::convert::Infallible;

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        // Middleware luôn gắn id; fallback chỉ để dùng handler độc lập trong test.
        Ok(parts
            .extensions
            .get::<RequestId>()
            .cloned()
            .unwrap_or_else(|| RequestId(Uuid::now_v7().to_string())))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keeps_valid_incoming_id() {
        let value = HeaderValue::from_static("req-12345678");
        assert_eq!(normalize(Some(&value)), "req-12345678");
    }

    #[test]
    fn replaces_missing_or_invalid_id_with_uuid_v7() {
        let short = HeaderValue::from_static("short");
        let space = HeaderValue::from_static("bad space");
        for incoming in [None, Some(&short), Some(&space)] {
            let id = normalize(incoming);
            assert_ne!(id, "short");
            assert_ne!(id, "bad space");
            assert!(valid(&id));
            let parsed = Uuid::parse_str(&id).unwrap();
            assert_eq!(parsed.get_version_num(), 7);
        }
    }

    #[test]
    fn response_always_carries_the_id_header() {
        let response = attach(Response::new(axum::body::Body::empty()), "req-12345678");
        assert_eq!(response.headers().get(HEADER).unwrap(), "req-12345678");
    }
}
