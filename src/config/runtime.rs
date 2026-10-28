//! `runtime.toml` overlay: hot fields only, revision defaults to 0.

use std::fs;
use std::io;
use std::path::Path;

use serde::Deserialize;

use crate::config::schema::{Config, LogLevel};

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RuntimeFile {
    revision: u64,
    #[serde(default)]
    logging: RuntimeLogging,
    #[serde(default)]
    telemetry: RuntimeTelemetry,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields, default)]
struct RuntimeLogging {
    level: Option<LogLevel>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields, default)]
struct RuntimeTelemetry {
    retention_days: Option<u64>,
    batch_size: Option<u64>,
    flush_interval_ms: Option<u64>,
}

impl RuntimeFile {
    fn apply(&self, config: &mut Config) {
        if let Some(level) = self.logging.level {
            config.logging.level = level;
        }
        if let Some(value) = self.telemetry.retention_days {
            config.telemetry.retention_days = value;
        }
        if let Some(value) = self.telemetry.batch_size {
            config.telemetry.batch_size = value;
        }
        if let Some(value) = self.telemetry.flush_interval_ms {
            config.telemetry.flush_interval_ms = value;
        }
    }
}

/// Missing file is not an error (revision 0). Parse errors fail startup.
pub(crate) fn apply_file(
    path: &Path,
    config: &mut Config,
) -> Result<u64, crate::config::ConfigError> {
    let text = match fs::read_to_string(path) {
        Ok(text) => text,
        Err(source) if source.kind() == io::ErrorKind::NotFound => return Ok(0),
        Err(source) => {
            return Err(crate::config::ConfigError::Read {
                path: path.to_path_buf(),
                source,
            });
        }
    };
    let overlay: RuntimeFile =
        toml::from_str(&text).map_err(|source| crate::config::ConfigError::Parse {
            path: path.to_path_buf(),
            source,
        })?;
    overlay.apply(config);
    Ok(overlay.revision)
}
