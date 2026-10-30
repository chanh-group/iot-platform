//! Domain error to HTTP envelope (`02` §1.2, §4.10).

use axum::Json;
use axum::http::{HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use serde::Serialize;
use serde_json::Value;

use crate::core::domain_error::DomainError;

const JSON_UTF8: &str = "application/json; charset=utf-8";

/// HTTP error body shared by every endpoint (`code`, `message`, `request_id`).
#[derive(Debug)]
pub struct ApiError {
    status: StatusCode,
    code: &'static str,
    message: String,
    details: Option<Value>,
    request_id: String,
}

impl ApiError {
    /// Builds an error envelope without `details`.
    pub fn new(
        status: StatusCode,
        code: &'static str,
        message: String,
        request_id: String,
    ) -> Self {
        Self {
            status,
            code,
            message,
            details: None,
            request_id,
        }
    }

    /// Adds contract-defined details to an error response.
    pub fn with_details(mut self, details: Value) -> Self {
        self.details = Some(details);
        self
    }

    /// Maps a domain error onto its contract status and code.
    pub fn from_domain(error: DomainError, request_id: String) -> Self {
        match error {
            DomainError::Validation { message, details } => Self {
                status: StatusCode::BAD_REQUEST,
                code: "validation_error",
                message,
                details: Some(serde_json::json!(details)),
                request_id,
            },
            DomainError::NotFound(message) => Self {
                status: StatusCode::NOT_FOUND,
                code: "not_found",
                message,
                details: None,
                request_id,
            },
            DomainError::Conflict { code, message } => Self {
                status: StatusCode::CONFLICT,
                code,
                message,
                details: None,
                request_id,
            },
            DomainError::Unauthorized => Self {
                status: StatusCode::UNAUTHORIZED,
                code: "unauthenticated",
                message: "unauthenticated".to_owned(),
                details: None,
                request_id,
            },
            DomainError::Unavailable(_) => Self {
                status: StatusCode::SERVICE_UNAVAILABLE,
                code: "storage_unavailable",
                message: "storage unavailable".to_owned(),
                details: None,
                request_id,
            },
            // The originating layer logs the detail; keep DB URLs/secrets out of responses.
            DomainError::Internal(_) => Self {
                status: StatusCode::INTERNAL_SERVER_ERROR,
                code: "internal_error",
                message: "internal server error".to_owned(),
                details: None,
                request_id,
            },
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        #[derive(Serialize)]
        struct Envelope {
            code: &'static str,
            message: String,
            request_id: String,
            #[serde(skip_serializing_if = "Option::is_none")]
            details: Option<Value>,
        }

        let mut response = (
            self.status,
            Json(Envelope {
                code: self.code,
                message: self.message,
                request_id: self.request_id,
                details: self.details,
            }),
        )
            .into_response();
        response
            .headers_mut()
            .insert(header::CONTENT_TYPE, HeaderValue::from_static(JSON_UTF8));
        response
    }
}

#[cfg(test)]
mod tests {
    use axum::body::to_bytes;
    use axum::http::StatusCode;
    use serde_json::{Value, json};

    use super::*;
    use crate::core::domain_error::FieldError;

    async fn json_body(response: Response) -> Value {
        let bytes = to_bytes(response.into_body(), 4096).await.unwrap();
        serde_json::from_slice(&bytes).unwrap()
    }

    #[tokio::test]
    async fn validation_envelope_has_code_message_request_id_and_details() {
        let error = DomainError::Validation {
            message: "invalid fields".to_owned(),
            details: vec![FieldError {
                field: "name".to_owned(),
                reason: "must not be empty".to_owned(),
            }],
        };
        let response = ApiError::from_domain(error, "req-12345678".to_owned()).into_response();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);

        let body = json_body(response).await;
        assert_eq!(body["code"], "validation_error");
        assert_eq!(body["message"], "invalid fields");
        assert_eq!(body["request_id"], "req-12345678");
        assert_eq!(body["details"][0]["field"], "name");
        assert_eq!(body["details"][0]["reason"], "must not be empty");
    }

    #[tokio::test]
    async fn internal_error_does_not_leak_detail() {
        let error = DomainError::Internal(anyhow::anyhow!(
            "SELECT * FROM users; sqlite:///secret/path"
        ));
        let response = ApiError::from_domain(error, "req-12345678".to_owned()).into_response();
        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);

        let body = json_body(response).await;
        assert_eq!(body["code"], "internal_error");
        assert_eq!(body["message"], "internal server error");
        assert_eq!(
            body,
            json!({
                "code": "internal_error",
                "message": "internal server error",
                "request_id": "req-12345678",
            })
        );
    }

    #[tokio::test]
    async fn unavailable_hides_cause() {
        let error = DomainError::Unavailable("database is locked".to_owned());
        let response = ApiError::from_domain(error, "req-12345678".to_owned()).into_response();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);

        let body = json_body(response).await;
        assert_eq!(body["code"], "storage_unavailable");
        assert_eq!(body["message"], "storage unavailable");
    }
}
