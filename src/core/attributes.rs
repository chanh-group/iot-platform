//! Validated latest-value device attributes (`02` §2.7).

use serde::Deserialize;
use serde_json::{Map, Value};

use crate::core::telemetry::{TelemetryPayload, valid_values};

/// Attribute updates ready for persistence.
#[derive(Debug, Clone, PartialEq)]
pub struct ValidatedAttributes {
    /// Optional device timestamp from the payload.
    pub device_ts: Option<i64>,
    /// Server timestamp for this update.
    pub updated_at: i64,
    /// Values to upsert by key.
    pub values: Map<String, Value>,
}

/// A payload validation failure. Attributes are dropped without an application ACK.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AttributesError {
    PayloadTooLarge,
    InvalidPayload,
    InvalidTimestamp,
}

/// Validates an attributes object using the telemetry payload/value limits.
pub fn validate(
    raw: &[u8],
    updated_at: i64,
    limits: TelemetryPayload,
) -> Result<ValidatedAttributes, AttributesError> {
    if raw.len() > limits.max_payload_bytes {
        return Err(AttributesError::PayloadTooLarge);
    }
    let mut deserializer = serde_json::Deserializer::from_slice(raw);
    let value =
        Value::deserialize(&mut deserializer).map_err(|_| AttributesError::InvalidPayload)?;
    deserializer
        .end()
        .map_err(|_| AttributesError::InvalidPayload)?;
    let Value::Object(mut object) = value else {
        return Err(AttributesError::InvalidPayload);
    };

    let device_ts = match object.remove("ts") {
        None => None,
        Some(Value::Number(number)) => {
            let ts = number.as_i64().ok_or(AttributesError::InvalidTimestamp)?;
            if ts <= 0
                || ts < updated_at.saturating_sub(limits.max_past_skew_ms)
                || ts > updated_at.saturating_add(limits.max_future_skew_ms)
            {
                return Err(AttributesError::InvalidTimestamp);
            }
            Some(ts)
        }
        Some(_) => return Err(AttributesError::InvalidTimestamp),
    };

    let values = object
        .remove("values")
        .and_then(|value| match value {
            Value::Object(values) => Some(values),
            _ => None,
        })
        .filter(|values| !values.is_empty() && values.len() <= limits.max_keys)
        .ok_or(AttributesError::InvalidPayload)?;
    object.remove("device_id");
    if !object.is_empty() || !valid_values(&values, limits.max_string_bytes) {
        return Err(AttributesError::InvalidPayload);
    }

    Ok(ValidatedAttributes {
        device_ts,
        updated_at,
        values,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn limits() -> TelemetryPayload {
        TelemetryPayload {
            max_payload_bytes: 1024,
            max_keys: 2,
            max_string_bytes: 8,
            max_past_skew_ms: 100,
            max_future_skew_ms: 100,
        }
    }

    #[test]
    fn accepts_latest_values_and_ignores_payload_device_id() {
        let attrs = validate(
            br#"{"ts":1000,"device_id":"spoofed","values":{"firmware":"1.4.2","active":true}}"#,
            1_000,
            limits(),
        )
        .unwrap();

        assert_eq!(attrs.device_ts, Some(1_000));
        assert_eq!(attrs.updated_at, 1_000);
        assert_eq!(attrs.values["firmware"], json!("1.4.2"));
    }

    #[test]
    fn rejects_invalid_values_timestamps_and_trailing_data() {
        assert_eq!(
            validate(br#"{"values":{"x":null}}"#, 1_000, limits()),
            Err(AttributesError::InvalidPayload)
        );
        assert_eq!(
            validate(br#"{"ts":1200,"values":{"x":1}}"#, 1_000, limits()),
            Err(AttributesError::InvalidTimestamp)
        );
        assert_eq!(
            validate(br#"{"values":{"x":1}} false"#, 1_000, limits()),
            Err(AttributesError::InvalidPayload)
        );
    }
}
