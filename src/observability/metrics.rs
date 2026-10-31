//! Process-wide metrics registry and Prometheus exposition (`02` §4.7).

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};

const REJECTION_WINDOW: Duration = Duration::from_secs(24 * 60 * 60);
const REJECTION_BUCKET: Duration = Duration::from_secs(1);

#[derive(Debug, Default)]
struct RollingCounter(Mutex<VecDeque<(Instant, u64)>>);

impl RollingCounter {
    fn add(&self) {
        let Ok(mut buckets) = self.0.lock() else {
            return;
        };
        Self::add_at(&mut buckets, Instant::now());
    }

    fn add_at(buckets: &mut VecDeque<(Instant, u64)>, now: Instant) {
        Self::prune(buckets, now);
        if let Some((started, count)) = buckets.back_mut()
            && now.saturating_duration_since(*started) < REJECTION_BUCKET
        {
            *count = count.saturating_add(1);
        } else {
            buckets.push_back((now, 1));
        }
    }

    fn prune(buckets: &mut VecDeque<(Instant, u64)>, now: Instant) {
        let cutoff = now
            .checked_sub(REJECTION_WINDOW)
            .unwrap_or_else(|| buckets.front().map_or(now, |(started, _)| *started));
        while buckets.front().is_some_and(|(started, _)| {
            started
                .checked_add(REJECTION_BUCKET)
                .is_some_and(|end| end <= cutoff)
        }) {
            buckets.pop_front();
        }
    }

    fn count(&self) -> u64 {
        let Ok(mut buckets) = self.0.lock() else {
            return 0;
        };
        Self::count_at(&mut buckets, Instant::now())
    }

    fn count_at(buckets: &mut VecDeque<(Instant, u64)>, now: Instant) -> u64 {
        Self::prune(buckets, now);
        buckets
            .iter()
            .fold(0_u64, |total, (_, count)| total.saturating_add(*count))
    }
}

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
    ingest_rejected_rolling: RollingCounter,
    pub telemetry_ack_dropped_total: Counter,
    pub telemetry_queue_depth: Gauge,
    pub telemetry_commit_count: Counter,
    pub telemetry_commit_micros_total: Counter,
    pub telemetry_commit_buckets: [Counter; 11],
    pub telemetry_commit_errors_total: Counter,
    pub telemetry_retention_errors_total: Counter,
    pub audit_write_errors_total: Counter,
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
        self.ingest_rejected_rolling.add();
    }

    /// Returns the rolling process-local rejection count with one-second resolution.
    #[must_use]
    pub fn ingest_rejected_24h(&self) -> u64 {
        self.ingest_rejected_rolling.count()
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
        counter(
            &mut output,
            "iot_telemetry_retention_errors_total",
            self.telemetry_retention_errors_total.get(),
        );
        counter(
            &mut output,
            "iot_audit_write_errors_total",
            self.audit_write_errors_total.get(),
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

    #[test]
    fn rolling_rejection_count_expires_outside_24_hours() {
        let mut buckets = VecDeque::new();
        let start = Instant::now();
        RollingCounter::add_at(&mut buckets, start);
        RollingCounter::add_at(&mut buckets, start + Duration::from_millis(500));
        assert_eq!(
            RollingCounter::count_at(&mut buckets, start + Duration::from_secs(2)),
            2
        );

        let before_expiry = start + REJECTION_WINDOW + Duration::from_millis(500);
        assert_eq!(RollingCounter::count_at(&mut buckets, before_expiry), 2);

        let after_expiry = start + REJECTION_WINDOW + Duration::from_secs(2);
        assert_eq!(RollingCounter::count_at(&mut buckets, after_expiry), 0);
        assert!(before_expiry < after_expiry);
    }
}
