//! Errors surfaced while loading and validating `conf.toml`.

use std::io;
use std::path::PathBuf;

use thiserror::Error;

/// Failure to read, parse, or validate configuration.
#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("cannot read {path}: {source}")]
    Read {
        path: PathBuf,
        #[source]
        source: io::Error,
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
        source: io::Error,
    },
    #[error("invalid configuration: {}", .0.join("; "))]
    Invalid(Vec<String>),
}
