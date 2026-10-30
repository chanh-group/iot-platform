//! Storage port for atomic telemetry batch insertion.

use async_trait::async_trait;

use super::ValidatedTelemetry;

/// Result of inserting a telemetry event, including retry resolution.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TelemetryInsertResult {
    Stored,
    Duplicate,
    Conflict,
}

/// Persists telemetry batches atomically.
#[async_trait]
pub trait TelemetryRepository: Send + Sync {
    async fn insert_batch(
        &self,
        events: &[(String, ValidatedTelemetry)],
    ) -> anyhow::Result<Vec<TelemetryInsertResult>>;
}
