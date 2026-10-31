//! Load `conf.toml`, resolve paths/env, merge `runtime.toml`, validate.

use std::fs;
use std::path::Path;

use crate::config::schema::Config;
use crate::config::{ConfigError, LoadedConfig, paths, runtime, validate};

/// Reads `conf.toml`, resolves paths and env, applies `runtime.toml`, validates.
pub fn load(path: &Path) -> Result<LoadedConfig, ConfigError> {
    load_with_env(path, |name| std::env::var(name).ok())
}

/// Same as [`load`]; env lookup is injectable so tests never mutate process env.
pub fn load_with_env(
    path: &Path,
    env: impl Fn(&str) -> Option<String>,
) -> Result<LoadedConfig, ConfigError> {
    let text = fs::read_to_string(path).map_err(|source| ConfigError::Read {
        path: path.to_path_buf(),
        source,
    })?;
    let mut config: Config = toml::from_str(&text).map_err(|source| ConfigError::Parse {
        path: path.to_path_buf(),
        source,
    })?;

    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let base = fs::canonicalize(parent).map_err(|source| ConfigError::Dir {
        path: parent.to_path_buf(),
        source,
    })?;

    let mut errors = Vec::new();
    let data_dir = paths::resolve(&base, &config.server.data_dir);

    let runtime_path = paths::resolve(&base, &config.runtime.settings_path);
    let runtime_inside_data_dir = paths::is_within(&runtime_path, &data_dir);
    if !runtime_inside_data_dir {
        errors.push(format!(
            "runtime.settings_path: {} is outside data_dir {}",
            runtime_path.display(),
            data_dir.display()
        ));
    }

    let database_url = match (&config.database.url, &config.database.url_env) {
        (Some(url), None) => match paths::resolve_sqlite_url(url, &base, &data_dir) {
            Ok(url) => Some(url),
            Err(error) => {
                errors.push(error);
                None
            }
        },
        (None, Some(name)) => match env(name) {
            Some(url) => match paths::resolve_sqlite_url(&url, &base, &data_dir) {
                Ok(url) => Some(url),
                Err(error) => {
                    errors.push(error);
                    None
                }
            },
            None => {
                errors.push(format!(
                    "database.url_env: environment variable {name} is not set"
                ));
                None
            }
        },
        (Some(_), Some(_)) => {
            errors.push("database: exactly one of url or url_env is required".to_owned());
            None
        }
        (None, None) => {
            errors.push("database: exactly one of url or url_env is required".to_owned());
            None
        }
    };

    let mut metrics_token = None;
    if config.http.metrics {
        match config.http.metrics_bearer_token_env.as_deref() {
            None | Some("") => errors.push(
                "http.metrics_bearer_token_env: required when http.metrics = true".to_owned(),
            ),
            Some(name) => match env(name) {
                None => errors.push(format!(
                    "http.metrics_bearer_token_env: environment variable {name} is not set"
                )),
                Some(token) if token.len() < 32 => errors.push(format!(
                    "http.metrics_bearer_token_env: token from {name} must be at least 32 bytes"
                )),
                Some(token) => metrics_token = Some(token),
            },
        }
    }

    let (revision, overlay) = if runtime_inside_data_dir {
        runtime::apply_file(&runtime_path, &mut config)?
    } else {
        (0, runtime::RuntimeFile::default())
    };

    errors.extend(validate::validate(&config));
    if !errors.is_empty() {
        return Err(ConfigError::Invalid(errors));
    }

    let database_url = database_url.ok_or_else(|| {
        ConfigError::Invalid(vec!["database: url could not be resolved".to_owned()])
    })?;

    let runtime_config =
        runtime::RuntimeConfigStore::new(runtime_path.clone(), config.clone(), revision, overlay);
    Ok(LoadedConfig {
        config,
        data_dir,
        database_url,
        runtime_settings_path: runtime_path,
        metrics_token,
        revision,
        runtime_config,
    })
}
