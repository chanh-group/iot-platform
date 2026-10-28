use std::fs;
use std::path::PathBuf;

use iot_server::config::{self, ConfigError, LogLevel};
use tempfile::TempDir;

const DB: &str = "[database]\nurl = \"sqlite://./data/iot.db\"\n";

fn write_config(dir: &TempDir, body: &str) -> PathBuf {
    let path = dir.path().join("conf.toml");
    fs::write(&path, format!("config_version = 1\n{body}")).unwrap();
    path
}

#[test]
fn valid_config_loads_with_contract_defaults() {
    let dir = TempDir::new().unwrap();
    let path = write_config(&dir, DB);

    let loaded = config::load_with_env(&path, |_| None).unwrap();
    let canonical_dir = fs::canonicalize(dir.path()).unwrap();

    assert_eq!(loaded.config.server.name, "iot-server");
    assert_eq!(loaded.data_dir, canonical_dir.join("data"));
    assert!(loaded.database_url.starts_with("sqlite:///"));
    assert!(loaded.database_url.ends_with("/data/iot.db"));
    assert_eq!(loaded.revision, 0);
    assert_eq!(loaded.config.logging.level, LogLevel::Info);
}

#[test]
fn unknown_field_is_rejected_with_path() {
    let dir = TempDir::new().unwrap();
    let path = write_config(&dir, &format!("{DB}\n[server]\nbogus = 1\n"));

    let error = config::load_with_env(&path, |_| None).unwrap_err();
    assert!(matches!(&error, ConfigError::Parse { .. }), "{error:?}");
    let message = error.to_string();
    assert!(message.contains("unknown field"), "{message}");
    assert!(message.contains("bogus"), "{message}");
}

#[test]
fn database_url_and_url_env_are_exclusive() {
    let dir = TempDir::new().unwrap();
    let path = write_config(
        &dir,
        "[database]\nurl = \"sqlite://./data/iot.db\"\nurl_env = \"IOT_DB_BOTH\"\n",
    );

    let error = config::load_with_env(&path, |name| {
        (name == "IOT_DB_BOTH").then(|| "sqlite://./data/other.db".to_owned())
    })
    .unwrap_err();
    assert!(matches!(&error, ConfigError::Invalid(_)), "{error:?}");
    assert!(error.to_string().contains("exactly one of url or url_env"));
}

#[test]
fn missing_database_env_fails_with_variable_name() {
    let dir = TempDir::new().unwrap();
    let path = write_config(&dir, "[database]\nurl_env = \"IOT_DB_MISSING_FOR_TEST\"\n");

    let error = config::load_with_env(&path, |_| None).unwrap_err();
    let message = error.to_string();
    assert!(message.contains("IOT_DB_MISSING_FOR_TEST"), "{message}");
}

#[test]
fn out_of_range_value_reports_its_path() {
    let dir = TempDir::new().unwrap();
    let path = write_config(&dir, &format!("{DB}\n[mqtt]\nmax_connections = 0\n"));

    let error = config::load_with_env(&path, |_| None).unwrap_err();
    let message = error.to_string();
    assert!(message.contains("mqtt.max_connections"), "{message}");
    assert!(message.contains("must be in 1..=100000"), "{message}");
}

#[test]
fn database_outside_data_dir_is_rejected() {
    let dir = TempDir::new().unwrap();
    let path = write_config(&dir, "[database]\nurl = \"sqlite://../outside.db\"\n");

    let error = config::load_with_env(&path, |_| None).unwrap_err();
    assert!(error.to_string().contains("outside data_dir"), "{error}");
}

#[test]
fn runtime_settings_path_outside_data_dir_is_rejected() {
    let dir = TempDir::new().unwrap();
    let path = write_config(
        &dir,
        &format!("{DB}\n[runtime]\nsettings_path = \"../runtime.toml\"\n"),
    );

    let error = config::load_with_env(&path, |_| None).unwrap_err();
    assert!(
        error.to_string().contains("runtime.settings_path"),
        "{error}"
    );
}

#[test]
fn broken_runtime_file_fails_startup() {
    let dir = TempDir::new().unwrap();
    let path = write_config(&dir, DB);
    fs::create_dir_all(dir.path().join("data")).unwrap();
    // `queue_capacity` is not a hot field, so runtime.toml must reject it.
    fs::write(
        dir.path().join("data/runtime.toml"),
        "revision = 3\n[telemetry]\nqueue_capacity = 1\n",
    )
    .unwrap();

    let error = config::load_with_env(&path, |_| None).unwrap_err();
    assert!(matches!(&error, ConfigError::Parse { .. }), "{error:?}");
    assert!(error.to_string().contains("runtime.toml"), "{error}");
}

#[test]
fn runtime_overlay_merges_hot_fields() {
    let dir = TempDir::new().unwrap();
    let path = write_config(&dir, DB);
    fs::create_dir_all(dir.path().join("data")).unwrap();
    fs::write(
        dir.path().join("data/runtime.toml"),
        "revision = 3\n\
         [logging]\nlevel = \"debug\"\n\
         [telemetry]\nretention_days = 14\nbatch_size = 200\nflush_interval_ms = 500\n",
    )
    .unwrap();

    let loaded = config::load_with_env(&path, |_| None).unwrap();
    assert_eq!(loaded.revision, 3);
    assert_eq!(loaded.config.logging.level, LogLevel::Debug);
    assert_eq!(loaded.config.telemetry.retention_days, 14);
    assert_eq!(loaded.config.telemetry.batch_size, 200);
    assert_eq!(loaded.config.telemetry.flush_interval_ms, 500);
}

#[test]
fn metrics_token_requires_at_least_32_bytes() {
    let dir = TempDir::new().unwrap();
    let path = write_config(
        &dir,
        &format!(
            "{DB}\n[http]\nmetrics = true\nmetrics_bearer_token_env = \"IOT_METRICS_FOR_TEST\"\n"
        ),
    );

    let error = config::load_with_env(&path, |_| Some("short".to_owned())).unwrap_err();
    assert!(error.to_string().contains("at least 32 bytes"), "{error}");

    let token = "x".repeat(32);
    let loaded = config::load_with_env(&path, |_| Some(token.clone())).unwrap();
    assert_eq!(loaded.metrics_token.as_deref(), Some(token.as_str()));
}

#[test]
fn redaction_hides_database_url_and_secret_env_names() {
    let dir = TempDir::new().unwrap();
    let path = write_config(
        &dir,
        &format!(
            "{DB}\n[http]\nmetrics = true\nmetrics_bearer_token_env = \"IOT_METRICS_FOR_TEST\"\n"
        ),
    );
    let token = "x".repeat(32);
    let loaded = config::load_with_env(&path, |_| Some(token.clone())).unwrap();

    let value = config::redact(&loaded.config).unwrap();
    assert_eq!(value["database"]["url"], "<redacted>");
    assert_eq!(value["http"]["metrics_bearer_token_env"], "<redacted>");
    assert!(!value.to_string().contains("iot.db"));
}

#[test]
fn shipped_conf_toml_is_valid() {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("conf.toml");

    let loaded = config::load_with_env(&path, |_| None).unwrap();
    assert_eq!(loaded.revision, 0);
    assert!(loaded.database_url.ends_with("/data/iot.db"));
}
