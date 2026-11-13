use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

use tempfile::TempDir;

fn write_config(dir: &TempDir, body: &str) -> PathBuf {
    let path = dir.path().join("conf.toml");
    fs::write(
        &path,
        format!("config_version = 1\n[database]\nurl = \"sqlite://./data/iot.db\"\n{body}"),
    )
    .unwrap();
    path
}

fn write_postgres_config(dir: &TempDir, url: &str) -> PathBuf {
    let path = dir.path().join("postgres-conf.toml");
    fs::write(
        &path,
        format!("config_version = 1\n[database]\nurl = {url:?}\n"),
    )
    .unwrap();
    path
}

fn run_check(config_path: &Path) -> Output {
    Command::new(env!("CARGO_BIN_EXE_iot-server"))
        .args(["check", "--config"])
        .arg(config_path)
        .env("PGSSLMODE", "disable")
        .output()
        .unwrap()
}

#[test]
fn check_validates_without_creating_data_dir() {
    let dir = TempDir::new().unwrap();
    let path = write_config(&dir, "");

    let output = run_check(&path);
    assert_eq!(
        output.status.code(),
        Some(0),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(!dir.path().join("data").exists());
}

#[test]
fn check_returns_config_exit_code_for_invalid_config() {
    let dir = TempDir::new().unwrap();
    let path = write_config(&dir, "[mqtt]\nmax_connections = 0\n");

    let output = run_check(&path);
    assert_eq!(
        output.status.code(),
        Some(3),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(String::from_utf8_lossy(&output.stderr).contains("mqtt.max_connections"));
    assert!(!dir.path().join("data").exists());
}

#[test]
fn check_validates_postgres_url_and_tls_without_connecting() {
    let dir = TempDir::new().unwrap();
    let secure = write_postgres_config(
        &dir,
        "postgresql://user:sentinel-password@localhost/iot_test?sslmode=verify-full",
    );
    assert_eq!(run_check(&secure).status.code(), Some(0));

    let local = write_postgres_config(
        &dir,
        "postgresql://user:local@localhost/iot_test?sslmode=disable",
    );
    assert_eq!(run_check(&local).status.code(), Some(0));

    for url in [
        "postgresql://user:local@localhost/iot_test",
        "postgresql://user:sentinel-password@localhost/iot_test?sslmode=prefer",
        "postgresql://user:local@localhost/iot_test?sslmode=typo",
        "postgresql://user:local@localhost/iot_test?sslmode=verify-full&port=invalid",
    ] {
        let config = write_postgres_config(&dir, url);
        let output = run_check(&config);
        assert_eq!(output.status.code(), Some(3), "{url}");
        assert!(
            !String::from_utf8_lossy(&output.stderr).contains("sentinel-password"),
            "database URL secrets must remain redacted"
        );
    }
    assert!(!dir.path().join("data").exists());
}
