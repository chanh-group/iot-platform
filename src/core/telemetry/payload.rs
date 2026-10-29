use serde_json::{Map, Value};

/// Validated device telemetry, with the server timestamp and raw-byte digest.
#[derive(Debug, Clone, PartialEq)]
pub struct ValidatedTelemetry {
    /// Device-provided id used to deduplicate retries.
    pub event_id: String,
    /// Event timestamp in milliseconds since the Unix epoch.
    pub ts: i64,
    /// Server receive timestamp in milliseconds since the Unix epoch.
    pub received_at: i64,
    /// Validated telemetry values.
    pub values: Map<String, Value>,
    /// SHA-256 digest of the original payload bytes.
    pub payload_sha256: [u8; 32],
    /// Length of the original payload in bytes.
    pub raw_bytes: usize,
}

/// Limits taken from `[telemetry]` configuration.
#[derive(Debug, Clone, Copy)]
pub struct TelemetryPayload {
    /// Maximum accepted raw payload size in bytes.
    pub max_payload_bytes: usize,
    /// Maximum number of telemetry keys.
    pub max_keys: usize,
    /// Maximum string value length in bytes.
    pub max_string_bytes: usize,
    /// Maximum allowed timestamp age in milliseconds.
    pub max_past_skew_ms: i64,
    /// Maximum allowed timestamp lead in milliseconds.
    pub max_future_skew_ms: i64,
}

impl TryFrom<&crate::config::schema::TelemetryConfig> for TelemetryPayload {
    type Error = &'static str;

    fn try_from(config: &crate::config::schema::TelemetryConfig) -> Result<Self, Self::Error> {
        Ok(Self {
            max_payload_bytes: usize::try_from(config.max_payload_bytes)
                .map_err(|_| "telemetry.max_payload_bytes does not fit usize")?,
            max_keys: usize::try_from(config.max_keys)
                .map_err(|_| "telemetry.max_keys does not fit usize")?,
            max_string_bytes: usize::try_from(config.max_string_bytes)
                .map_err(|_| "telemetry.max_string_bytes does not fit usize")?,
            max_past_skew_ms: i64::try_from(config.max_past_skew_ms)
                .map_err(|_| "telemetry.max_past_skew_ms does not fit i64")?,
            max_future_skew_ms: i64::try_from(config.max_future_skew_ms)
                .map_err(|_| "telemetry.max_future_skew_ms does not fit i64")?,
        })
    }
}
