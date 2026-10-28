//! Typed configuration: schema, load, validate, paths, runtime overlay, redaction.

use std::path::PathBuf;

use serde_json::Value;
use thiserror::Error;

pub mod load;
pub mod paths;
pub mod runtime;
pub mod schema;
pub mod validate;

pub use load::{load, load_with_env};
pub use schema::{Config, LogFormat, LogLevel};
pub use validate::validate;

const REDACTED: &str = "<redacted>";

/// Effective configuration after path/env resolution and runtime overlay.
pub struct LoadedConfig {
    pub config: Config,
    /// Absolute, lexically normalized data dir.
    pub data_dir: PathBuf,
    /// Absolute `sqlite://` URL after `url_env` resolution.
    pub database_url: String,
    /// Absolute path of `runtime.toml`.
    pub runtime_settings_path: PathBuf,
    /// Secret from `metrics_bearer_token_env`; never serialize or log.
    pub metrics_token: Option<String>,
    pub revision: u64,
}

impl std::fmt::Debug for LoadedConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Không in config/DB URL/token: các field đó có thể chứa secret.
        f.debug_struct("LoadedConfig")
            .field("data_dir", &self.data_dir)
            .field("runtime_settings_path", &self.runtime_settings_path)
            .field("revision", &self.revision)
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("cannot read {path}: {source}")]
    Read {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("cannot parse {path}: {source}")]
    Parse {
        path: PathBuf,
        #[source]
        source: toml::de::Error,
    },
    #[error("cannot resolve config directory {path}: {source}")]
    Dir {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("invalid configuration: {}", .0.join("; "))]
    Invalid(Vec<String>),
}

/// Serialize for `GET /config` with DB URL and secret env names masked.
pub fn redact(config: &Config) -> Result<Value, serde_json::Error> {
    let mut value = serde_json::to_value(config)?;

    if let Some(database) = value.get_mut("database").and_then(Value::as_object_mut) {
        for key in ["url", "url_env"] {
            if database.contains_key(key) {
                database.insert(key.to_owned(), Value::String(REDACTED.to_owned()));
            }
        }
    }
    if let Some(http) = value.get_mut("http").and_then(Value::as_object_mut)
        && http.contains_key("metrics_bearer_token_env")
    {
        http.insert(
            "metrics_bearer_token_env".to_owned(),
            Value::String(REDACTED.to_owned()),
        );
    }

    Ok(value)
}
