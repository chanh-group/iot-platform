//! Tracing setup and lifecycle events (`02` §5.3–§5.4).

use std::net::SocketAddr;
use std::path::Path;
use std::sync::OnceLock;

use tracing_subscriber::EnvFilter;
use tracing_subscriber::prelude::*;
use tracing_subscriber::reload;

use crate::config::LoadedConfig;
use crate::config::schema::{LogFormat, LogLevel};

type FilterHandle = reload::Handle<EnvFilter, tracing_subscriber::Registry>;

static FILTER_HANDLE: OnceLock<FilterHandle> = OnceLock::new();

/// Installs the global subscriber from `[logging]`. `try_init` also installs `LogTracer`.
pub fn init(loaded: &LoadedConfig) -> anyhow::Result<()> {
    let filter = filter_for_level(loaded.runtime_config.load().config.logging.level)?;
    let (filter_layer, handle) = reload::Layer::new(filter);
    let subscriber = tracing_subscriber::registry().with(filter_layer);
    match loaded.config.logging.format {
        LogFormat::Json => subscriber
            .with(
                tracing_subscriber::fmt::layer()
                    .json()
                    .with_target(false)
                    .with_ansi(false),
            )
            .try_init()
            .map_err(anyhow::Error::msg)?,
        LogFormat::Text => subscriber
            .with(
                tracing_subscriber::fmt::layer()
                    .with_target(false)
                    .with_ansi(false),
            )
            .try_init()
            .map_err(anyhow::Error::msg)?,
    }
    FILTER_HANDLE
        .set(handle)
        .map_err(|_| anyhow::anyhow!("logging filter already initialized"))?;
    Ok(())
}

/// Parses the new filter before file commit and returns its infallible apply step.
pub fn prepare_reload(level: LogLevel) -> Result<Box<dyn FnOnce() + Send>, String> {
    let filter = filter_for_level(level).map_err(|error| error.to_string())?;
    let prepared: Box<dyn FnOnce() + Send> = if let Some(handle) = FILTER_HANDLE.get() {
        Box::new(move || {
            // The process-global subscriber is installed once and outlives runtime config.
            handle
                .reload(filter)
                .expect("the process-global tracing subscriber remains installed");
        })
    } else {
        Box::new(|| {})
    };
    Ok(prepared)
}

/// Startup fields only; secrets stay in `LoadedConfig` and are never logged.
pub fn startup(config_path: &Path, loaded: &LoadedConfig, http_listen: SocketAddr) {
    tracing::info!(
        config = %config_path.display(),
        revision = loaded.runtime_config.load().revision,
        http_listen = %http_listen,
        "server starting"
    );
}

/// Emits the final lifecycle event; call after the listeners have drained.
pub fn shutdown(reason: &str) {
    tracing::info!(reason, "server stopped");
}

fn level_name(level: LogLevel) -> &'static str {
    match level {
        LogLevel::Trace => "trace",
        LogLevel::Debug => "debug",
        LogLevel::Info => "info",
        LogLevel::Warn => "warn",
        LogLevel::Error => "error",
    }
}

fn filter_for_level(level: LogLevel) -> Result<EnvFilter, tracing_subscriber::filter::ParseError> {
    EnvFilter::try_new(level_name(level))
}

#[cfg(test)]
mod tests {
    use std::io::{self, Write};
    use std::sync::{Arc, Mutex};

    use tracing_subscriber::fmt::MakeWriter;

    use super::*;
    use crate::config;

    #[derive(Clone, Default)]
    struct Buffer(Arc<Mutex<Vec<u8>>>);

    impl Write for Buffer {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(bytes);
            Ok(bytes.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    impl<'a> MakeWriter<'a> for Buffer {
        type Writer = Self;

        fn make_writer(&'a self) -> Self::Writer {
            self.clone()
        }
    }

    #[test]
    fn startup_log_keeps_safe_fields_and_drops_secrets() {
        let buffer = Buffer::default();
        let subscriber = tracing_subscriber::fmt()
            .json()
            .with_writer(buffer.clone())
            .with_max_level(tracing::Level::INFO)
            .finish();

        let dir = tempfile::TempDir::new().unwrap();
        let config_path = dir.path().join("conf.toml");
        std::fs::write(
            &config_path,
            "config_version = 1\n\
             [server]\n\
             data_dir = \"./data\"\n\
             [database]\n\
             url = \"sqlite://./data/iot.db\"\n",
        )
        .unwrap();
        let mut loaded = config::load_with_env(&config_path, |_| None).unwrap();
        loaded.database_url = "sqlite:///private/iot.db".to_owned();
        loaded.metrics_token = Some("super-secret-token".to_owned());

        tracing::subscriber::with_default(subscriber, || {
            startup(&config_path, &loaded, "127.0.0.1:8080".parse().unwrap());
        });

        let output = String::from_utf8(buffer.0.lock().unwrap().clone()).unwrap();
        assert!(output.contains("\"message\":\"server starting\""));
        assert!(output.contains("\"revision\":0"));
        assert!(output.contains("127.0.0.1:8080"));
        assert!(!output.contains("super-secret-token"));
        assert!(!output.contains("private/iot.db"));
    }

    #[test]
    fn level_names_match_filter_directives() {
        assert_eq!(level_name(LogLevel::Trace), "trace");
        assert_eq!(level_name(LogLevel::Debug), "debug");
        assert_eq!(level_name(LogLevel::Info), "info");
        assert_eq!(level_name(LogLevel::Warn), "warn");
        assert_eq!(level_name(LogLevel::Error), "error");
    }

    #[test]
    fn reload_changes_the_filter_for_subsequent_events() {
        let buffer = Buffer::default();
        let (filter, handle) = reload::Layer::new(filter_for_level(LogLevel::Info).unwrap());
        let subscriber = tracing_subscriber::registry().with(filter).with(
            tracing_subscriber::fmt::layer()
                .json()
                .with_writer(buffer.clone()),
        );

        tracing::subscriber::with_default(subscriber, || {
            tracing::debug!("hidden before reload");
            handle
                .reload(filter_for_level(LogLevel::Debug).unwrap())
                .unwrap();
            tracing::debug!("visible after reload");
        });

        let output = String::from_utf8(buffer.0.lock().unwrap().clone()).unwrap();
        assert!(!output.contains("hidden before reload"));
        assert!(output.contains("visible after reload"));
    }
}
