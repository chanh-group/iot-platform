//! Tracing setup and lifecycle events (`02` §5.3–§5.4).

use std::net::SocketAddr;
use std::path::Path;

use tracing_subscriber::EnvFilter;

use crate::config::LoadedConfig;
use crate::config::schema::{LogFormat, LogLevel};

/// Installs the global subscriber from `[logging]`. `try_init` also installs `LogTracer`.
pub fn init(loaded: &LoadedConfig) -> anyhow::Result<()> {
    let filter = EnvFilter::try_new(level_name(loaded.config.logging.level))?;
    let builder = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .with_ansi(false);
    match loaded.config.logging.format {
        LogFormat::Json => builder.json().try_init().map_err(anyhow::Error::msg)?,
        LogFormat::Text => builder.try_init().map_err(anyhow::Error::msg)?,
    }
    Ok(())
}

/// Startup fields only; secrets stay in `LoadedConfig` and are never logged.
pub fn startup(config_path: &Path, loaded: &LoadedConfig, http_listen: SocketAddr) {
    tracing::info!(
        config = %config_path.display(),
        revision = loaded.revision,
        http_listen = %http_listen,
        "server starting"
    );
}

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

#[cfg(test)]
mod tests {
    use std::io::{self, Write};
    use std::sync::{Arc, Mutex};

    use tracing_subscriber::fmt::MakeWriter;

    use super::*;

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
        let mut loaded = crate::config::load_with_env(&config_path, |_| None).unwrap();
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
}
