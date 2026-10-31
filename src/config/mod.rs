//! Typed configuration: schema, load, validate, paths, runtime overlay, redaction.

use std::fmt;
use std::path::PathBuf;

pub mod config_error;
pub mod load;
pub mod paths;
pub mod redact;
pub mod runtime;
pub mod schema;
pub mod validate;

pub use config_error::ConfigError;
pub use load::{load, load_with_env};
pub use redact::redact;
pub use schema::{Config, LogFormat, LogLevel};
pub use validate::validate;

/// Effective configuration after path/env resolution and runtime overlay.
pub struct LoadedConfig {
    /// Parsed, overlaid, and validated configuration.
    pub config: Config,
    /// Absolute, lexically normalized data dir.
    pub data_dir: PathBuf,
    /// Absolute `sqlite://` URL after `url_env` resolution.
    pub database_url: String,
    /// Absolute path of `runtime.toml`.
    pub runtime_settings_path: PathBuf,
    /// Secret from `metrics_bearer_token_env`; never serialize or log.
    pub metrics_token: Option<String>,
    /// Applied `runtime.toml` revision; 0 when the file is absent.
    pub revision: u64,
    /// Shared hot-config snapshot store.
    pub runtime_config: runtime::RuntimeConfigStore,
}

impl fmt::Debug for LoadedConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Never print config/DB URL/token: those fields may hold secrets.
        f.debug_struct("LoadedConfig")
            .field("data_dir", &self.data_dir)
            .field("runtime_settings_path", &self.runtime_settings_path)
            .field("revision", &self.revision)
            .finish_non_exhaustive()
    }
}
