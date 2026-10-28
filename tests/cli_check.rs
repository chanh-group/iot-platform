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

fn run_check(config_path: &Path) -> Output {
    Command::new(env!("CARGO_BIN_EXE_iot-server"))
        .args(["check", "--config"])
        .arg(config_path)
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
