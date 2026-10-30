//! Device mutation validation (`02` §4.3).

use serde_json::Value;

use super::domain_error::FieldError;

pub const MAX_METADATA_BYTES: usize = 8_192;
const MAX_METADATA_DEPTH: usize = 8;

#[derive(Debug, PartialEq)]
pub struct DevicePatch {
    pub name: Option<String>,
    pub metadata_json: Option<String>,
    pub enabled: Option<bool>,
}

pub fn validate_name(value: &str) -> Result<String, FieldError> {
    let name = value.trim();
    if !(1..=64).contains(&name.chars().count()) || name.chars().any(char::is_control) {
        return Err(FieldError {
            field: "name".to_owned(),
            reason: "must be 1..64 characters without control characters".to_owned(),
        });
    }
    Ok(name.to_owned())
}

pub fn validate_metadata(value: &Value) -> Result<String, FieldError> {
    if !value.is_object() {
        return Err(FieldError {
            field: "metadata".to_owned(),
            reason: "must be an object".to_owned(),
        });
    }
    if json_depth(value) > MAX_METADATA_DEPTH {
        return Err(FieldError {
            field: "metadata".to_owned(),
            reason: "must not exceed depth 8".to_owned(),
        });
    }
    let encoded = serde_json::to_string(value).map_err(|_| FieldError {
        field: "metadata".to_owned(),
        reason: "must be valid JSON".to_owned(),
    })?;
    if encoded.len() > MAX_METADATA_BYTES {
        return Err(FieldError {
            field: "metadata".to_owned(),
            reason: "must not exceed 8192 bytes".to_owned(),
        });
    }
    Ok(encoded)
}

pub fn validate_patch(body: &Value) -> Result<DevicePatch, Vec<FieldError>> {
    let Some(object) = body.as_object() else {
        return Err(vec![FieldError {
            field: "body".to_owned(),
            reason: "must be an object".to_owned(),
        }]);
    };
    let mut errors = Vec::new();
    for key in object.keys() {
        if !matches!(key.as_str(), "name" | "metadata" | "enabled") {
            errors.push(FieldError {
                field: key.clone(),
                reason: "unknown field".to_owned(),
            });
        }
    }
    if object.is_empty() {
        errors.push(FieldError {
            field: "body".to_owned(),
            reason: "at least one field is required".to_owned(),
        });
    }

    let name = object.get("name").map(|value| {
        value
            .as_str()
            .ok_or_else(|| FieldError {
                field: "name".to_owned(),
                reason: "must be a string".to_owned(),
            })
            .and_then(validate_name)
    });
    let metadata_json = object.get("metadata").map(validate_metadata);
    let enabled = object.get("enabled").map(|value| {
        value.as_bool().ok_or_else(|| FieldError {
            field: "enabled".to_owned(),
            reason: "must be a boolean".to_owned(),
        })
    });

    let name = collect_field(name, &mut errors);
    let metadata_json = collect_field(metadata_json, &mut errors);
    let enabled = collect_field(enabled, &mut errors);
    if errors.is_empty() {
        Ok(DevicePatch {
            name,
            metadata_json,
            enabled,
        })
    } else {
        Err(errors)
    }
}

fn collect_field<T>(
    field: Option<Result<T, FieldError>>,
    errors: &mut Vec<FieldError>,
) -> Option<T> {
    field.and_then(|result| match result {
        Ok(value) => Some(value),
        Err(error) => {
            errors.push(error);
            None
        }
    })
}

fn json_depth(value: &Value) -> usize {
    match value {
        Value::Array(items) => items.iter().map(json_depth).max().unwrap_or(0) + 1,
        Value::Object(items) => items.values().map(json_depth).max().unwrap_or(0) + 1,
        _ => 0,
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn validates_trimmed_names_metadata_limits_and_partial_fields() {
        assert_eq!(validate_name("  sensor-1  ").unwrap(), "sensor-1");
        assert_eq!(validate_name("x").unwrap(), "x");
        assert_eq!(validate_name(&"x".repeat(64)).unwrap().len(), 64);
        assert!(validate_name("\n").is_err());
        assert!(validate_name(&"x".repeat(65)).is_err());
        assert!(validate_metadata(&json!({"x": [1]})).is_ok());
        assert!(validate_metadata(&json!({"x": "x".repeat(8_184)})).is_ok());
        assert!(validate_metadata(&json!({"x": "x".repeat(8_185)})).is_err());
        assert!(validate_metadata(&json!({"x": "x".repeat(MAX_METADATA_BYTES)})).is_err());
        assert!(
            validate_metadata(&json!({"a":{"b":{"c":{"d":{"e":{"f":{"g":{"h":0}}}}}}}})).is_ok()
        );
        assert!(
            validate_metadata(&json!({"a":{"b":{"c":{"d":{"e":{"f":{"g":{"h":{"i":0}}}}}}}}}))
                .is_err()
        );
        assert_eq!(
            validate_patch(&json!({"metadata": {}, "enabled": false})).unwrap(),
            DevicePatch {
                name: None,
                metadata_json: Some("{}".to_owned()),
                enabled: Some(false),
            }
        );
        assert!(validate_patch(&json!({})).is_err());
        assert!(validate_patch(&json!({"name": null})).is_err());
        assert!(validate_patch(&json!({"unknown": true})).is_err());
    }
}
