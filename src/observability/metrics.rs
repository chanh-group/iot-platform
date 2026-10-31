//! Process-wide metrics registry and Prometheus exposition (`02` §4.7).

use std::sync::OnceLock;
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

    /// Increments an active-resource gauge.
    pub fn inc(&self) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }

    /// Decrements an active-resource gauge.
    pub fn dec(&self) {
        self.0.fetch_sub(1, Ordering::Relaxed);
    }

    /// Returns the current value.
    #[must_use]
    pub fn get(&self) -> u64 {
        self.0.load(Ordering::Relaxed)
    }
}

#[derive(Debug, Clone, Copy)]
pub enum IngestRejectReason {
    PayloadTooLarge,
    InvalidPayload,
    MissingEventId,
    InvalidEventId,
    InvalidTimestamp,
    QueueFull,
}

impl IngestRejectReason {
    const ALL: [Self; 6] = [
        Self::PayloadTooLarge,
        Self::InvalidPayload,
        Self::MissingEventId,
        Self::InvalidEventId,
        Self::InvalidTimestamp,
        Self::QueueFull,
    ];

    const fn label(self) -> &'static str {
        match self {
            Self::PayloadTooLarge => "payload_too_large",
            Self::InvalidPayload => "invalid_payload",
            Self::MissingEventId => "missing_event_id",
            Self::InvalidEventId => "invalid_event_id",
            Self::InvalidTimestamp => "invalid_timestamp",
            Self::QueueFull => "queue_full",
        }
    }

    const fn index(self) -> usize {
        match self {
            Self::PayloadTooLarge => 0,
            Self::InvalidPayload => 1,
            Self::MissingEventId => 2,
            Self::InvalidEventId => 3,
            Self::InvalidTimestamp => 4,
            Self::QueueFull => 5,
        }
    }
}

const COMMIT_BUCKETS_MICROS: [u64; 11] = [
    5_000, 10_000, 25_000, 50_000, 100_000, 250_000, 500_000, 1_000_000, 2_500_000, 5_000_000,
    10_000_000,
];

/// Process-wide metrics named after `02` §4.7.
#[derive(Debug, Default)]
pub struct Metrics {
    pub mqtt_connections_active: Gauge,
    pub mqtt_kick_failures_total: Counter,
    pub ingest_accepted_total: Counter,
    pub ingest_rejected_total: Counter,
    pub ingest_rejected_reasons: [Counter; 6],
    pub telemetry_ack_dropped_total: Counter,
    pub telemetry_queue_depth: Gauge,
    pub telemetry_commit_count: Counter,
    pub telemetry_commit_micros_total: Counter,
    pub telemetry_commit_buckets: [Counter; 11],
    pub telemetry_commit_errors_total: Counter,
    pub attribute_rejected_total: Counter,
    pub retained_cap_rejected_total: Counter,
    pub commands_pending: Gauge,
    pub commands_expired_total: Counter,
    pub command_ack_dropped_total: Counter,
    pub command_ack_invalid_total: Counter,
    pub command_ack_storage_errors_total: Counter,
    pub command_publish_errors_total: Counter,
    pub command_storage_errors_total: Counter,
    pub command_dispatch_wake_dropped_total: Counter,
    pub ws_clients: Gauge,
    pub ws_dropped_total: Counter,
}

impl Metrics {
    /// Empty registry; counters start at zero.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Process-wide registry shared by broker hooks and the exporter.
    pub fn global() -> &'static Self {
        static METRICS: OnceLock<Metrics> = OnceLock::new();
        METRICS.get_or_init(Self::new)
    }

    pub fn reject_ingest(&self, reason: IngestRejectReason) {
        self.ingest_rejected_total.inc();
        self.ingest_rejected_reasons[reason.index()].inc();
    }

    pub fn observe_telemetry_commit(&self, micros: u64) {
        self.telemetry_commit_count.inc();
        self.telemetry_commit_micros_total.add(micros);
        for (bucket, bound) in self
            .telemetry_commit_buckets
            .iter()
            .zip(COMMIT_BUCKETS_MICROS)
        {
            if micros <= bound {
                bucket.inc();
                break;
            }
        }
    }

    /// Renders the bounded registry as Prometheus text without device labels.
    #[must_use]
    pub fn render_prometheus(&self) -> String {
        let mut output = String::new();
        gauge(
            &mut output,
            "iot_mqtt_connections_active",
            self.mqtt_connections_active.get(),
        );
        counter(
            &mut output,
            "iot_ingest_accepted_total",
            self.ingest_accepted_total.get(),
        );
        counter(
            &mut output,
            "iot_mqtt_kick_failures_total",
            self.mqtt_kick_failures_total.get(),
        );
        counter(
            &mut output,
            "iot_retained_cap_rejected_total",
            self.retained_cap_rejected_total.get(),
        );
        output.push_str(
            "# HELP iot_ingest_rejected_total Rejected telemetry events by reason.\n# TYPE iot_ingest_rejected_total counter\n",
        );
        for reason in IngestRejectReason::ALL {
            let count = self.ingest_rejected_reasons[reason.index()].get();
            output.push_str(&format!(
                "iot_ingest_rejected_total{{reason=\"{}\"}} {count}\n",
                reason.label()
            ));
        }
        gauge(
            &mut output,
            "iot_telemetry_queue_depth",
            self.telemetry_queue_depth.get(),
        );
        output.push_str(
            "# HELP iot_telemetry_commit_seconds Duration of telemetry batch commits.\n# TYPE iot_telemetry_commit_seconds histogram\n",
        );
        let mut cumulative = 0;
        for (bucket, micros) in self
            .telemetry_commit_buckets
            .iter()
            .zip(COMMIT_BUCKETS_MICROS)
        {
            cumulative += bucket.get();
            output.push_str(&format!(
                "iot_telemetry_commit_seconds_bucket{{le=\"{}\"}} {cumulative}\n",
                micros as f64 / 1_000_000.0
            ));
        }
        output.push_str(&format!(
            "iot_telemetry_commit_seconds_bucket{{le=\"+Inf\"}} {}\niot_telemetry_commit_seconds_sum {}\niot_telemetry_commit_seconds_count {}\n",
            self.telemetry_commit_count.get(),
            self.telemetry_commit_micros_total.get() as f64 / 1_000_000.0,
            self.telemetry_commit_count.get()
        ));
        counter(
            &mut output,
            "iot_telemetry_commit_errors_total",
            self.telemetry_commit_errors_total.get(),
        );
        gauge(
            &mut output,
            "iot_commands_pending",
            self.commands_pending.get(),
        );
        counter(
            &mut output,
            "iot_commands_expired_total",
            self.commands_expired_total.get(),
        );
        counter(
            &mut output,
            "iot_command_ack_dropped_total",
            self.command_ack_dropped_total.get(),
        );
        counter(
            &mut output,
            "iot_command_ack_invalid_total",
            self.command_ack_invalid_total.get(),
        );
        counter(
            &mut output,
            "iot_command_ack_storage_errors_total",
            self.command_ack_storage_errors_total.get(),
        );
        counter(
            &mut output,
            "iot_command_publish_errors_total",
            self.command_publish_errors_total.get(),
        );
        counter(
            &mut output,
            "iot_command_storage_errors_total",
            self.command_storage_errors_total.get(),
        );
        counter(
            &mut output,
            "iot_command_dispatch_wake_dropped_total",
            self.command_dispatch_wake_dropped_total.get(),
        );
        gauge(&mut output, "iot_ws_clients", self.ws_clients.get());
        counter(
            &mut output,
            "iot_ws_dropped_total",
            self.ws_dropped_total.get(),
        );
        output
    }
}

fn gauge(output: &mut String, name: &str, value: u64) {
    output.push_str(&format!("# TYPE {name} gauge\n{name} {value}\n"));
}

fn counter(output: &mut String, name: &str, value: u64) {
    output.push_str(&format!("# TYPE {name} counter\n{name} {value}\n"));
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

    #[test]
    fn exposition_has_reason_labels_and_commit_histogram() {
        let metrics = Metrics::new();
        metrics.reject_ingest(IngestRejectReason::QueueFull);
        metrics.observe_telemetry_commit(20_000);
        metrics.mqtt_kick_failures_total.inc();
        let exposition = metrics.render_prometheus();
        assert!(exposition.contains("reason=\"queue_full\"} 1"));
        assert!(exposition.contains("iot_mqtt_kick_failures_total 1"));
        assert!(exposition.contains("iot_telemetry_commit_seconds_bucket{le=\"0.025\"} 1"));
        assert!(exposition.contains("iot_telemetry_commit_seconds_count 1"));
        assert!(!exposition.contains("device_id"));
    }
}
