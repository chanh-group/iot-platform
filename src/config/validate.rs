//! Range and cross-field validation (`02` §5.2–§5.3). Collects every error.

use crate::config::schema::Config;

fn range(errors: &mut Vec<String>, path: &str, value: u64, min: u64, max: u64) {
    if !(min..=max).contains(&value) {
        errors.push(format!("{path}: must be in {min}..={max} (got {value})"));
    }
}

pub fn validate(config: &Config) -> Vec<String> {
    let mut errors = Vec::new();

    if config.config_version != 1 {
        errors.push(format!(
            "config_version: must be 1 (got {})",
            config.config_version
        ));
    }

    let name_len = config.server.name.chars().count();
    if !(1..=64).contains(&name_len) {
        errors.push(format!(
            "server.name: must be 1..=64 characters (got {name_len})"
        ));
    }
    range(
        &mut errors,
        "server.shutdown_timeout_ms",
        config.server.shutdown_timeout_ms,
        1_000,
        60_000,
    );

    range(
        &mut errors,
        "http.max_body_bytes",
        config.http.max_body_bytes,
        1_024,
        10_485_760,
    );
    range(
        &mut errors,
        "http.request_timeout_ms",
        config.http.request_timeout_ms,
        1_000,
        120_000,
    );
    range(
        &mut errors,
        "http.max_ws_connections",
        config.http.max_ws_connections,
        1,
        10_000,
    );
    range(
        &mut errors,
        "http.ws_queue_capacity",
        config.http.ws_queue_capacity,
        16,
        4_096,
    );

    if config.mqtt.allow_anonymous {
        errors.push("mqtt.allow_anonymous: must be false".to_owned());
    }
    range(
        &mut errors,
        "mqtt.max_connections",
        config.mqtt.max_connections,
        1,
        100_000,
    );
    range(
        &mut errors,
        "mqtt.max_packet_bytes",
        config.mqtt.max_packet_bytes,
        1_024,
        16_777_216,
    );
    range(
        &mut errors,
        "mqtt.max_inflight_per_client",
        config.mqtt.max_inflight_per_client,
        1,
        65_535,
    );
    range(
        &mut errors,
        "mqtt.max_queued_messages_per_client",
        config.mqtt.max_queued_messages_per_client,
        1,
        65_535,
    );
    range(
        &mut errors,
        "mqtt.max_subscriptions_per_client",
        config.mqtt.max_subscriptions_per_client,
        1,
        1_024,
    );
    range(
        &mut errors,
        "mqtt.max_session_expiry_ms",
        config.mqtt.max_session_expiry_ms,
        60_000,
        3_600_000,
    );
    range(
        &mut errors,
        "mqtt.max_client_id_len",
        config.mqtt.max_client_id_len,
        36,
        256,
    );
    range(
        &mut errors,
        "mqtt.max_topic_levels",
        config.mqtt.max_topic_levels,
        1,
        32,
    );
    range(
        &mut errors,
        "mqtt.max_retained_messages",
        config.mqtt.max_retained_messages,
        0,
        10_000,
    );
    range(
        &mut errors,
        "mqtt.max_retained_bytes",
        config.mqtt.max_retained_bytes,
        0,
        16_777_216,
    );

    range(
        &mut errors,
        "database.max_connections",
        config.database.max_connections,
        1,
        64,
    );
    range(
        &mut errors,
        "database.connect_timeout_ms",
        config.database.connect_timeout_ms,
        100,
        60_000,
    );
    range(
        &mut errors,
        "database.busy_timeout_ms",
        config.database.busy_timeout_ms,
        100,
        60_000,
    );

    range(
        &mut errors,
        "telemetry.queue_capacity",
        config.telemetry.queue_capacity,
        16,
        65_536,
    );
    range(
        &mut errors,
        "telemetry.batch_size",
        config.telemetry.batch_size,
        1,
        2_000,
    );
    range(
        &mut errors,
        "telemetry.max_batch_bytes",
        config.telemetry.max_batch_bytes,
        65_536,
        67_108_864,
    );
    range(
        &mut errors,
        "telemetry.flush_interval_ms",
        config.telemetry.flush_interval_ms,
        10,
        60_000,
    );
    range(
        &mut errors,
        "telemetry.retention_days",
        config.telemetry.retention_days,
        1,
        3_650,
    );
    range(
        &mut errors,
        "telemetry.max_future_skew_ms",
        config.telemetry.max_future_skew_ms,
        0,
        86_400_000,
    );
    range(
        &mut errors,
        "telemetry.max_past_skew_ms",
        config.telemetry.max_past_skew_ms,
        0,
        31_536_000_000,
    );
    range(
        &mut errors,
        "telemetry.max_payload_bytes",
        config.telemetry.max_payload_bytes,
        256,
        1_048_576,
    );
    range(
        &mut errors,
        "telemetry.max_keys",
        config.telemetry.max_keys,
        1,
        1_024,
    );
    range(
        &mut errors,
        "telemetry.max_string_bytes",
        config.telemetry.max_string_bytes,
        1,
        65_536,
    );

    range(
        &mut errors,
        "commands.default_ttl_ms",
        config.commands.default_ttl_ms,
        1_000,
        config.commands.max_ttl_ms,
    );
    range(
        &mut errors,
        "commands.max_ttl_ms",
        config.commands.max_ttl_ms,
        60_000,
        604_800_000,
    );
    range(
        &mut errors,
        "commands.max_pending_per_device",
        config.commands.max_pending_per_device,
        1,
        10_000,
    );
    range(
        &mut errors,
        "commands.retry_initial_ms",
        config.commands.retry_initial_ms,
        100,
        60_000,
    );
    range(
        &mut errors,
        "commands.retry_max_ms",
        config.commands.retry_max_ms,
        1_000,
        300_000,
    );
    range(
        &mut errors,
        "commands.dispatch_interval_ms",
        config.commands.dispatch_interval_ms,
        100,
        10_000,
    );

    range(
        &mut errors,
        "auth.session_ttl_ms",
        config.auth.session_ttl_ms,
        300_000,
        604_800_000,
    );
    range(
        &mut errors,
        "auth.login_max_attempts",
        config.auth.login_max_attempts,
        1,
        100,
    );
    range(
        &mut errors,
        "auth.login_window_ms",
        config.auth.login_window_ms,
        1_000,
        3_600_000,
    );
    range(
        &mut errors,
        "auth.password_min_length",
        config.auth.password_min_length,
        8,
        128,
    );

    if config.telemetry.batch_size > config.telemetry.queue_capacity {
        errors.push("telemetry.batch_size: must be <= telemetry.queue_capacity".to_owned());
    }
    if config.commands.default_ttl_ms > config.commands.max_ttl_ms {
        errors.push("commands.default_ttl_ms: must be <= commands.max_ttl_ms".to_owned());
    }
    if config.commands.retry_initial_ms > config.commands.retry_max_ms {
        errors.push("commands.retry_initial_ms: must be <= commands.retry_max_ms".to_owned());
    }
    if config.telemetry.max_payload_bytes > config.mqtt.max_packet_bytes {
        errors.push("telemetry.max_payload_bytes: must be <= mqtt.max_packet_bytes".to_owned());
    }
    if config.http.listen == config.mqtt.listen {
        errors.push("http.listen: must differ from mqtt.listen".to_owned());
    }

    errors
}
