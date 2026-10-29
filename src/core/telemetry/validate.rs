use serde::Deserialize;
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};

use super::payload::{TelemetryPayload, ValidatedTelemetry};

/// A telemetry payload validation failure, mapped to the contract ACK code.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TelemetryError {
    PayloadTooLarge,
    InvalidPayload,
    MissingEventId,
    InvalidEventId,
    InvalidTimestamp,
}

/// Returns an event id only when it meets the wire contract, for rejected ACKs.
#[must_use]
pub fn event_id_from_raw(raw: &[u8]) -> Option<String> {
    let value: Value = serde_json::from_slice(raw).ok()?;
    let event_id = value.as_object()?.get("event_id")?.as_str()?;
    (1..=128).contains(&event_id.len()).then_some(())?;
    event_id
        .bytes()
        .all(|b| b.is_ascii_alphanumeric() || b"._:-".contains(&b))
        .then(|| event_id.to_owned())
}

/// Parses and validates a telemetry JSON object and hashes its original bytes.
pub fn validate(
    raw: &[u8],
    received_at: i64,
    limits: TelemetryPayload,
) -> Result<ValidatedTelemetry, TelemetryError> {
    if raw.len() > limits.max_payload_bytes {
        return Err(TelemetryError::PayloadTooLarge);
    }
    let mut deserializer = serde_json::Deserializer::from_slice(raw);
    let value =
        Value::deserialize(&mut deserializer).map_err(|_| TelemetryError::InvalidPayload)?;
    deserializer
        .end()
        .map_err(|_| TelemetryError::InvalidPayload)?;
    let Value::Object(mut object) = value else {
        return Err(TelemetryError::InvalidPayload);
    };

    let event_id = match object.remove("event_id") {
        None => return Err(TelemetryError::MissingEventId),
        Some(Value::String(event_id)) if event_id.is_empty() => {
            return Err(TelemetryError::MissingEventId);
        }
        Some(Value::String(event_id)) => event_id,
        Some(_) => return Err(TelemetryError::InvalidEventId),
    };
    if !(1..=128).contains(&event_id.len())
        || !event_id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"._:-".contains(&b))
    {
        return Err(TelemetryError::InvalidEventId);
    }

    let ts = match object.remove("ts") {
        None => received_at,
        Some(Value::Number(number)) => number.as_i64().ok_or(TelemetryError::InvalidTimestamp)?,
        Some(_) => return Err(TelemetryError::InvalidTimestamp),
    };
    if ts <= 0
        || ts < received_at.saturating_sub(limits.max_past_skew_ms)
        || ts > received_at.saturating_add(limits.max_future_skew_ms)
    {
        return Err(TelemetryError::InvalidTimestamp);
    }

    let values = object
        .remove("values")
        .and_then(|value| match value {
            Value::Object(values) => Some(values),
            _ => None,
        })
        .filter(|values| !values.is_empty() && values.len() <= limits.max_keys)
        .ok_or(TelemetryError::InvalidPayload)?;
    object.remove("device_id");
    if !object.is_empty() || !valid_values(&values, limits.max_string_bytes) {
        return Err(TelemetryError::InvalidPayload);
    }

    Ok(ValidatedTelemetry {
        event_id,
        ts,
        received_at,
        values,
        payload_sha256: Sha256::digest(raw).into(),
        raw_bytes: raw.len(),
    })
}

fn valid_values(values: &Map<String, Value>, max_string_bytes: usize) -> bool {
    values.iter().all(|(key, value)| {
        (1..=64).contains(&key.len())
            && !key.starts_with('$')
            && key
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"_.:-".contains(&b))
            && match value {
                Value::Bool(_) => true,
                Value::Number(number) => {
                    number.as_i64().is_some()
                        || number.as_u64().is_some()
                        || number.as_f64().is_some_and(f64::is_finite)
                }
                Value::String(value) => value.len() <= max_string_bytes,
                Value::Null | Value::Array(_) | Value::Object(_) => false,
            }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

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
    fn validates_default_timestamp_and_hashes_exact_raw_bytes() {
        let raw = br#"{"event_id":"evt-1","device_id":"spoofed","values":{"x":1}}"#;
        let telemetry = validate(raw, 1_000, limits()).unwrap();
        assert_eq!(telemetry.ts, 1_000);
        assert_eq!(telemetry.received_at, 1_000);
        assert_eq!(telemetry.payload_sha256, Sha256::digest(raw).as_slice());
    }

    #[test]
    fn rejects_trailing_data_invalid_values_and_timestamp_skew() {
        assert_eq!(
            validate(
                br#"{"event_id":"x","values":{"x":1}} false"#,
                1000,
                limits()
            ),
            Err(TelemetryError::InvalidPayload)
        );
        assert_eq!(
            validate(br#"{"event_id":"x","values":{"x":null}}"#, 1000, limits()),
            Err(TelemetryError::InvalidPayload)
        );
        assert_eq!(
            validate(
                br#"{"event_id":"x","ts":1200,"values":{"x":1}}"#,
                1000,
                limits()
            ),
            Err(TelemetryError::InvalidTimestamp)
        );
    }

    #[test]
    fn reports_event_id_and_size_validation_codes() {
        assert_eq!(
            validate(br#"{"values":{"x":1}}"#, 1_000, limits()),
            Err(TelemetryError::MissingEventId)
        );
        assert_eq!(
            validate(
                br#"{"event_id":"bad/id","values":{"x":1}}"#,
                1_000,
                limits()
            ),
            Err(TelemetryError::InvalidEventId)
        );
        assert_eq!(
            validate(br#"{"event_id":"x","values":{"$key":1}}"#, 1_000, limits()),
            Err(TelemetryError::InvalidPayload)
        );
        assert_eq!(
            validate(&[b' '; 1025], 1_000, limits()),
            Err(TelemetryError::PayloadTooLarge)
        );
    }
}
