//! Metric registry skeleton (`02` §4.7). `/metrics` is exposed in P2-13.

use std::sync::atomic::{AtomicU64, Ordering};

/// Monotonic counter.
#[derive(Debug, Default)]
pub struct Counter(AtomicU64);

impl Counter {
    /// Adds one.
    pub fn inc(&self) {
        self.add(1);
    }

    /// Adds `value`.
    pub fn add(&self, value: u64) {
        self.0.fetch_add(value, Ordering::Relaxed);
    }

    /// Returns the current value.
    #[must_use]
    pub fn get(&self) -> u64 {
        self.0.load(Ordering::Relaxed)
    }
}

/// Last-value gauge.
#[derive(Debug, Default)]
pub struct Gauge(AtomicU64);

impl Gauge {
    /// Replaces the current value.
    pub fn set(&self, value: u64) {
        self.0.store(value, Ordering::Relaxed);
    }

    /// Returns the current value.
    #[must_use]
    pub fn get(&self) -> u64 {
        self.0.load(Ordering::Relaxed)
    }
}

/// Process-wide metrics named after `02` §4.7.
#[derive(Debug, Default)]
pub struct Metrics {
    pub mqtt_connections_active: Gauge,
    pub ingest_accepted_total: Counter,
    // shortcut: reason label arrives with the `/metrics` exporter in P2-13.
    pub ingest_rejected_total: Counter,
    pub telemetry_queue_depth: Gauge,
    // Histogram `iot_telemetry_commit_seconds` as count + micros until P2-13.
    pub telemetry_commit_count: Counter,
    pub telemetry_commit_micros_total: Counter,
    pub telemetry_commit_errors_total: Counter,
    pub commands_pending: Gauge,
    pub commands_expired_total: Counter,
    pub ws_clients: Gauge,
    pub ws_dropped_total: Counter,
}

impl Metrics {
    /// Empty registry; counters start at zero.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counters_and_gauges_update() {
        let metrics = Metrics::new();
        metrics.ingest_accepted_total.inc();
        metrics.ingest_accepted_total.add(2);
        assert_eq!(metrics.ingest_accepted_total.get(), 3);

        metrics.telemetry_queue_depth.set(7);
        assert_eq!(metrics.telemetry_queue_depth.get(), 7);
    }
}
