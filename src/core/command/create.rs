use serde::Deserialize;
use serde_json::Value;

use crate::core::domain_error::FieldError;

const MAX_PARAMS_BYTES: usize = 16_384;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CreateCommandRequest {
    pub name: String,
    #[serde(default = "empty_params")]
    pub params: Value,
    pub ttl_ms: Option<u64>,
    pub idempotency_key: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ValidatedCommand {
    pub name: String,
    pub params: Value,
    pub params_json: String,
    pub ttl_ms: u64,
    pub idempotency_key: Option<String>,
}

fn empty_params() -> Value {
    serde_json::json!({})
}

pub fn validate_create(
    request: CreateCommandRequest,
    default_ttl_ms: u64,
    max_ttl_ms: u64,
) -> Result<ValidatedCommand, Vec<FieldError>> {
    let mut errors = Vec::new();
    if !valid_name(&request.name) {
        errors.push(FieldError {
            field: "name".to_owned(),
            reason: "must match ^[a-z][a-z0-9_.-]{0,63}$".to_owned(),
        });
    }

    let params_json = serde_json::to_string(&request.params).ok();
    if !request.params.is_object() {
        errors.push(FieldError {
            field: "params".to_owned(),
            reason: "must be an object".to_owned(),
        });
    } else if params_json
        .as_ref()
        .is_none_or(|encoded| encoded.len() > MAX_PARAMS_BYTES)
    {
        errors.push(FieldError {
            field: "params".to_owned(),
            reason: "must not exceed 16384 bytes".to_owned(),
        });
    }

    let ttl_ms = request.ttl_ms.unwrap_or(default_ttl_ms);
    if !(1_000..=max_ttl_ms).contains(&ttl_ms) {
        errors.push(FieldError {
            field: "ttl_ms".to_owned(),
            reason: format!("must be 1000..={max_ttl_ms}"),
        });
    }

    if let Some(key) = request.idempotency_key.as_deref()
        && let Some(error) = idempotency_key_error(key)
    {
        errors.push(error);
    }

    if !errors.is_empty() {
        return Err(errors);
    }
    let params_json = params_json.unwrap_or_else(|| "{}".to_owned());
    Ok(ValidatedCommand {
        name: request.name,
        params: request.params,
        params_json,
        ttl_ms,
        idempotency_key: request.idempotency_key,
    })
}

pub fn validate_create_value(
    request: Value,
    default_ttl_ms: u64,
    max_ttl_ms: u64,
) -> Result<ValidatedCommand, Vec<FieldError>> {
    let request: CreateCommandRequest = serde_json::from_value(request).map_err(|_| {
        vec![FieldError {
            field: "body".to_owned(),
            reason: "invalid command request".to_owned(),
        }]
    })?;
    validate_create(request, default_ttl_ms, max_ttl_ms)
}

pub fn validate_idempotency_key(request: &Value) -> Result<Option<&str>, Vec<FieldError>> {
    let Some(value) = request
        .as_object()
        .and_then(|object| object.get("idempotency_key"))
    else {
        return Ok(None);
    };
    match value {
        Value::Null => Ok(None),
        Value::String(key) if idempotency_key_error(key).is_none() => Ok(Some(key)),
        Value::String(_)
        | Value::Bool(_)
        | Value::Number(_)
        | Value::Array(_)
        | Value::Object(_) => Err(vec![FieldError {
            field: "idempotency_key".to_owned(),
            reason: "must be 1..128 printable ASCII characters".to_owned(),
        }]),
    }
}

fn idempotency_key_error(key: &str) -> Option<FieldError> {
    (!(1..=128).contains(&key.len()) || !key.bytes().all(|byte| (0x20..=0x7e).contains(&byte)))
        .then(|| FieldError {
            field: "idempotency_key".to_owned(),
            reason: "must be 1..128 printable ASCII characters".to_owned(),
        })
}

fn valid_name(name: &str) -> bool {
    let mut bytes = name.bytes();
    bytes.next().is_some_and(|first| first.is_ascii_lowercase())
        && name.len() <= 64
        && bytes.all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'_' | b'.' | b'-')
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request(json: &str) -> CreateCommandRequest {
        serde_json::from_str(json).unwrap()
    }

    #[test]
    fn defaults_params_and_ttl_and_accepts_contract_values() {
        let command = validate_create(request(r#"{"name":"reboot"}"#), 60_000, 86_400_000).unwrap();
        assert_eq!(command.params, serde_json::json!({}));
        assert_eq!(command.ttl_ms, 60_000);

        let command = validate_create(
            request(
                r#"{"name":"relay.toggle-1","params":{"channel":1},"ttl_ms":1000,"idempotency_key":"retry 1"}"#,
            ),
            60_000,
            86_400_000,
        )
        .unwrap();
        assert_eq!(command.ttl_ms, 1_000);
        assert_eq!(command.idempotency_key.as_deref(), Some("retry 1"));
    }

    #[test]
    fn rejects_invalid_name_params_ttl_and_idempotency_key() {
        for json in [
            r#"{"name":"Reboot"}"#,
            r#"{"name":"1reboot"}"#,
            r#"{"name":"reboot","params":null}"#,
            r#"{"name":"reboot","ttl_ms":999}"#,
            r#"{"name":"reboot","ttl_ms":86400001}"#,
            r#"{"name":"reboot","idempotency_key":"bad\tkey"}"#,
            &format!(
                r#"{{"name":"reboot","idempotency_key":"{}"}}"#,
                "x".repeat(129)
            ),
            r#"{"name":"reboot","unknown":true}"#,
        ] {
            if json.contains("unknown") {
                assert!(serde_json::from_str::<CreateCommandRequest>(json).is_err());
                continue;
            }
            let request = request(json);
            assert!(
                validate_create(request, 60_000, 86_400_000).is_err(),
                "{json}"
            );
        }

        let oversized = CreateCommandRequest {
            name: "reboot".to_owned(),
            params: serde_json::json!({"value": "x".repeat(MAX_PARAMS_BYTES)}),
            ttl_ms: None,
            idempotency_key: None,
        };
        let errors = validate_create(oversized, 60_000, 86_400_000).unwrap_err();
        assert!(errors.iter().any(|error| error.field == "params"));
    }

    #[test]
    fn extracts_only_a_valid_key_before_full_request_validation() {
        let request = serde_json::json!({
            "idempotency_key": "stable-key",
            "name": 123,
            "extra": true,
        });
        assert_eq!(validate_idempotency_key(&request), Ok(Some("stable-key")));
        assert!(validate_create_value(request, 60_000, 86_400_000).is_err());

        let invalid_key = serde_json::json!({"idempotency_key": "bad\tkey"});
        assert!(validate_idempotency_key(&invalid_key).is_err());
    }
}
