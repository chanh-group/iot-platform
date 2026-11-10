//! Axum router, middleware, handlers, and static assets.

pub mod api_error;
pub mod assets;
pub mod auth;
pub mod commands;
pub mod config;
pub mod devices;
pub mod events;
pub mod metrics;
pub mod middleware;
pub mod overview;
pub mod router;
pub mod telemetry;

use std::collections::HashMap;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Weak};
use std::time::Duration;

use tokio::sync::{Mutex, OwnedMutexGuard, Semaphore};

use crate::config::LoadedConfig;
use crate::core::attributes::AttributesRepository;
use crate::core::auth::AdminRepository;
use crate::core::command::CommandRepository;
use crate::core::device::DeviceRepository;
use crate::core::events::EventBus;
use crate::core::presence::Presence;
use crate::core::storage::StorageRepository;
use crate::core::telemetry::TelemetryReadRepository;
use crate::mqtt::presence::DeviceAdmission;
use crate::mqtt::server::{DeviceKickQueue, MqttControl, MqttFailure};
use middleware::LoginRateLimiter;

/// Health signals for long-running services that back the HTTP readiness check.
pub struct ServiceReadiness {
    /// Whether the MQTT listener is serving.
    pub mqtt: Arc<AtomicBool>,
    /// Whether telemetry commits are healthy.
    pub telemetry: Arc<AtomicBool>,
    /// Whether the command dispatcher task is alive.
    pub commands: Arc<AtomicBool>,
}

pub struct AppRepositories {
    pub commands: Arc<dyn CommandRepository>,
    pub admin: Arc<dyn AdminRepository>,
    pub devices: Arc<dyn DeviceRepository>,
    pub attributes: Arc<dyn AttributesRepository>,
    pub telemetry: Arc<dyn TelemetryReadRepository>,
    pub storage: Arc<dyn StorageRepository>,
}

/// Shared handler state: database pool, config, login limiter, and task readiness.
#[derive(Clone)]
pub struct AppState {
    /// Loaded and validated configuration.
    pub config: Arc<LoadedConfig>,
    /// Shared failed-login window.
    pub login_limiter: Arc<LoginRateLimiter>,
    /// Cleared during shutdown so `/readyz` reports not-ready.
    pub ready: Arc<AtomicBool>,
    /// Cleared if the MQTT listener stops.
    pub mqtt_ready: Arc<AtomicBool>,
    /// Cleared while telemetry commits are failing and retried.
    pub telemetry_ready: Arc<AtomicBool>,
    /// Cleared if the SQL-backed command dispatcher stops.
    pub command_ready: Arc<AtomicBool>,
    /// Best-effort event stream shared with the telemetry worker.
    pub events: EventBus,
    /// Blocks new MQTT authentication while a committed disable is kicked.
    pub(crate) device_admission: DeviceAdmission,
    /// Bounded, server-owned device-kick worker.
    pub(crate) device_kicks: DeviceKickQueue,
    /// Command domain persistence port.
    pub(crate) command_repository: Arc<dyn CommandRepository>,
    /// Admin account, session, and login/logout audit port.
    pub(crate) admin_repository: Arc<dyn AdminRepository>,
    /// Device reads and MQTT credential/presence state port.
    pub(crate) device_repository: Arc<dyn DeviceRepository>,
    /// Latest-value attribute persistence and pagination port.
    pub(crate) attributes_repository: Arc<dyn AttributesRepository>,
    /// Database health, summary reads, and best-effort config audit.
    pub(crate) storage_repository: Arc<dyn StorageRepository>,
    /// Telemetry ingest, query, and retention port.
    pub(crate) telemetry_repository: Arc<dyn TelemetryReadRepository>,
    /// Per-device locks; expired entries are pruned on each acquisition.
    credential_rotations: Arc<Mutex<HashMap<String, Weak<Mutex<()>>>>>,
    /// Registry shared with the MQTT listener.
    pub presence: Arc<Presence>,
    /// Signals a fatal MQTT failure to the supervisor.
    pub mqtt_failure: MqttFailure,
    /// Limits active WebSocket connections before upgrade.
    pub ws_connections: Arc<Semaphore>,
    /// Limits concurrent HTTP requests before handlers start.
    pub http_requests: Arc<Semaphore>,
}

impl AppState {
    /// Builds state with a fresh limiter and readiness set to `true`.
    pub fn new(
        repositories: AppRepositories,
        config: Arc<LoadedConfig>,
        readiness: ServiceReadiness,
        events: EventBus,
        presence: Arc<Presence>,
        mqtt_control: MqttControl,
    ) -> Self {
        let max_ws_connections =
            usize::try_from(config.config.http.max_ws_connections).unwrap_or(usize::MAX);
        let max_concurrent_requests =
            usize::try_from(config.config.http.max_concurrent_requests).unwrap_or(usize::MAX);
        let limiter = LoginRateLimiter::new(
            config.config.auth.login_max_attempts,
            Duration::from_millis(config.config.auth.login_window_ms),
        );
        Self {
            config,
            login_limiter: Arc::new(limiter),
            ready: Arc::new(AtomicBool::new(true)),
            mqtt_ready: readiness.mqtt,
            telemetry_ready: readiness.telemetry,
            command_ready: readiness.commands,
            events,
            device_admission: mqtt_control.device_admission,
            device_kicks: mqtt_control.device_kicks,
            command_repository: repositories.commands,
            admin_repository: repositories.admin,
            device_repository: repositories.devices,
            attributes_repository: repositories.attributes,
            telemetry_repository: repositories.telemetry,
            storage_repository: repositories.storage,
            credential_rotations: Arc::new(Mutex::new(HashMap::new())),
            presence,
            mqtt_failure: mqtt_control.failure,
            ws_connections: Arc::new(Semaphore::new(max_ws_connections)),
            http_requests: Arc::new(Semaphore::new(max_concurrent_requests)),
        }
    }

    /// Serializes rotations for one device without retaining locks for inactive devices.
    pub(crate) async fn lock_credential_rotation(&self, device_id: &str) -> OwnedMutexGuard<()> {
        let lock = {
            let mut rotations = self.credential_rotations.lock().await;
            rotations.retain(|_, lock| lock.strong_count() > 0);
            if let Some(lock) = rotations.get(device_id).and_then(Weak::upgrade) {
                lock
            } else {
                let lock = Arc::new(Mutex::new(()));
                rotations.insert(device_id.to_owned(), Arc::downgrade(&lock));
                lock
            }
        };
        lock.lock_owned().await
    }
}
