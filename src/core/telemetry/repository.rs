//! Storage port for atomic telemetry batch insertion.

use async_trait::async_trait;
use serde_json::{Map, Value};

use super::ValidatedTelemetry;

/// Result of inserting a telemetry event, including retry resolution.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TelemetryInsertResult {
    Stored,
    Duplicate,
    Conflict,
}

#[derive(Debug, Clone, Copy)]
pub struct TelemetryHistoryRange {
    pub from: i64,
    pub to: i64,
    pub commit_order: bool,
}

#[derive(Debug, Clone)]
pub struct TelemetryRow {
    pub internal_id: i64,
    pub event_id: String,
    pub ts: i64,
    pub received_at: i64,
    pub values_json: String,
}

#[derive(Debug, Clone)]
pub struct LatestTelemetryValue {
    pub internal_id: i64,
    pub key: String,
    pub event_id: String,
    pub ts: i64,
    pub received_at: i64,
    pub values_json: String,
}

/// Persists telemetry batches atomically.
#[async_trait]
pub trait TelemetryRepository: Send + Sync {
    async fn insert_batch(
        &self,
        events: &[(String, ValidatedTelemetry)],
    ) -> anyhow::Result<Vec<TelemetryInsertResult>>;
}

#[async_trait]
pub trait TelemetryReadRepository: Send + Sync {
    async fn device_exists(&self, device_id: &str) -> anyhow::Result<bool>;

    async fn committed_through(&self) -> anyhow::Result<i64>;

    async fn history(
        &self,
        device_id: &str,
        range: TelemetryHistoryRange,
        after: Option<(i64, i64)>,
        keys: &[String],
        limit: i64,
    ) -> anyhow::Result<Vec<TelemetryRow>>;

    async fn latest(
        &self,
        device_id: &str,
        keys: &[String],
    ) -> anyhow::Result<Vec<LatestTelemetryValue>>;
}

#[async_trait]
pub trait TelemetryRetentionRepository: Send + Sync {
    async fn delete_before(&self, received_before: i64, limit: i64) -> anyhow::Result<u64>;
}

pub fn filtered_values(
    values_json: &str,
    keys: &[String],
) -> Result<Map<String, Value>, serde_json::Error> {
    let mut values: Map<String, Value> = serde_json::from_str(values_json)?;
    if !keys.is_empty() {
        values.retain(|key, _| keys.iter().any(|filter| filter == key));
    }
    Ok(values)
}
