//! Typed `conf.toml` schema (`02` §5.1–§5.2) with contract defaults.

use std::net::{Ipv4Addr, SocketAddr};
use std::path::PathBuf;

use serde::{Deserialize, Serialize};

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub config_version: u32,
    #[serde(default)]
    pub server: ServerConfig,
    #[serde(default)]
    pub http: HttpConfig,
    #[serde(default)]
    pub mqtt: MqttConfig,
    #[serde(default)]
    pub database: DatabaseConfig,
    #[serde(default)]
    pub telemetry: TelemetryConfig,
    #[serde(default)]
    pub commands: CommandsConfig,
    #[serde(default)]
    pub auth: AuthConfig,
    #[serde(default)]
    pub runtime: RuntimeConfig,
    #[serde(default)]
    pub logging: LoggingConfig,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct ServerConfig {
    pub name: String,
    pub data_dir: PathBuf,
    pub shutdown_timeout_ms: u64,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            name: "iot-server".to_owned(),
            data_dir: PathBuf::from("./data"),
            shutdown_timeout_ms: 10_000,
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct HttpConfig {
    pub listen: SocketAddr,
    pub dashboard: bool,
    pub max_body_bytes: u64,
    pub cookie_secure: bool,
    pub request_timeout_ms: u64,
    pub max_ws_connections: u64,
    pub ws_queue_capacity: u64,
    pub metrics: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metrics_bearer_token_env: Option<String>,
}

impl Default for HttpConfig {
    fn default() -> Self {
        Self {
            listen: SocketAddr::from((Ipv4Addr::LOCALHOST, 8080)),
            dashboard: true,
            max_body_bytes: 262_144,
            cookie_secure: false,
            request_timeout_ms: 30_000,
            max_ws_connections: 100,
            ws_queue_capacity: 256,
            metrics: false,
            metrics_bearer_token_env: None,
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct MqttConfig {
    pub listen: SocketAddr,
    pub allow_anonymous: bool,
    pub max_connections: u64,
    pub max_packet_bytes: u64,
    pub max_inflight_per_client: u64,
    pub max_queued_messages_per_client: u64,
    pub max_subscriptions_per_client: u64,
    pub max_session_expiry_ms: u64,
    pub max_client_id_len: u64,
    pub max_topic_levels: u64,
    pub max_retained_messages: u64,
    pub max_retained_bytes: u64,
}

impl Default for MqttConfig {
    fn default() -> Self {
        Self {
            listen: SocketAddr::from((Ipv4Addr::LOCALHOST, 1883)),
            allow_anonymous: false,
            max_connections: 1_000,
            max_packet_bytes: 262_144,
            max_inflight_per_client: 32,
            max_queued_messages_per_client: 256,
            max_subscriptions_per_client: 8,
            max_session_expiry_ms: 3_600_000,
            max_client_id_len: 64,
            max_topic_levels: 16,
            max_retained_messages: 10_000,
            max_retained_bytes: 16_777_216,
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct DatabaseConfig {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub url_env: Option<String>,
    pub max_connections: u64,
    pub connect_timeout_ms: u64,
    pub busy_timeout_ms: u64,
}

impl Default for DatabaseConfig {
    fn default() -> Self {
        Self {
            url: None,
            url_env: None,
            max_connections: 5,
            connect_timeout_ms: 5_000,
            busy_timeout_ms: 5_000,
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct TelemetryConfig {
    pub queue_capacity: u64,
    pub batch_size: u64,
    pub max_batch_bytes: u64,
    pub flush_interval_ms: u64,
    pub retention_days: u64,
    pub max_future_skew_ms: u64,
    pub max_past_skew_ms: u64,
    pub max_payload_bytes: u64,
    pub max_keys: u64,
    pub max_string_bytes: u64,
}

impl Default for TelemetryConfig {
    fn default() -> Self {
        Self {
            queue_capacity: 4_096,
            batch_size: 500,
            max_batch_bytes: 8_388_608,
            flush_interval_ms: 1_000,
            retention_days: 30,
            max_future_skew_ms: 300_000,
            max_past_skew_ms: 31_536_000_000,
            max_payload_bytes: 65_536,
            max_keys: 128,
            max_string_bytes: 4_096,
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct CommandsConfig {
    pub default_ttl_ms: u64,
    pub max_ttl_ms: u64,
    pub max_pending_per_device: u64,
    pub retry_initial_ms: u64,
    pub retry_max_ms: u64,
    pub dispatch_interval_ms: u64,
}

impl Default for CommandsConfig {
    fn default() -> Self {
        Self {
            default_ttl_ms: 60_000,
            max_ttl_ms: 86_400_000,
            max_pending_per_device: 100,
            retry_initial_ms: 1_000,
            retry_max_ms: 30_000,
            dispatch_interval_ms: 500,
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct AuthConfig {
    pub session_ttl_ms: u64,
    pub login_max_attempts: u64,
    pub login_window_ms: u64,
    pub password_min_length: u64,
}

impl Default for AuthConfig {
    fn default() -> Self {
        Self {
            session_ttl_ms: 86_400_000,
            login_max_attempts: 5,
            login_window_ms: 300_000,
            password_min_length: 12,
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct RuntimeConfig {
    pub settings_path: PathBuf,
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            settings_path: PathBuf::from("./data/runtime.toml"),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct LoggingConfig {
    pub level: LogLevel,
    pub format: LogFormat,
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            level: LogLevel::Info,
            format: LogFormat::Json,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum LogLevel {
    Trace,
    Debug,
    Info,
    Warn,
    Error,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum LogFormat {
    Json,
    Text,
}
