use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::time::Duration;

mod common;

use common::{PASSWORD, free_port, wait_ready};
use iot_server::config;
use iot_server::runtime::data_dir_lock::DataDirLock;
use reqwest::StatusCode;
use serde_json::json;
use tempfile::TempDir;
use tokio::io::AsyncWriteExt;

struct Server {
    child: Option<Child>,
}

impl Server {
    fn spawn(config_path: &Path) -> Self {
        let child = Command::new(env!("CARGO_BIN_EXE_iot-server"))
            .args(["run", "--config"])
            .arg(config_path)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        Self { child: Some(child) }
    }

    fn id(&self) -> u32 {
        self.child.as_ref().unwrap().id()
    }

    fn wait_with_output(mut self) -> io::Result<Output> {
        self.child.take().unwrap().wait_with_output()
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

fn write_config(dir: &TempDir, port: u16) -> PathBuf {
    let path = dir.path().join("conf.toml");
    fs::write(
        &path,
        format!(
            "config_version = 1\n\
             [server]\n\
             data_dir = \"./data\"\n\
             shutdown_timeout_ms = 5000\n\
             [http]\n\
             listen = \"127.0.0.1:{port}\"\n\
             [database]\n\
             url = \"sqlite://./data/iot.db\"\n"
        ),
    )
    .unwrap();
    path
}

fn run_cli(args: &[&str], config_path: &Path) -> Output {
    Command::new(env!("CARGO_BIN_EXE_iot-server"))
        .args(args)
        .arg("--config")
        .arg(config_path)
        .output()
        .unwrap()
}

#[tokio::test]
async fn sigterm_drains_in_flight_request_and_releases_lock() {
    let dir = TempDir::new().unwrap();
    let port = free_port();
    let config_path = write_config(&dir, port);

    let migrate = run_cli(&["migrate"], &config_path);
    assert!(
        migrate.status.success(),
        "migrate failed: {}",
        String::from_utf8_lossy(&migrate.stderr)
    );

    let password_file = dir.path().join("password.txt");
    fs::write(&password_file, PASSWORD).unwrap();
    let create = Command::new(env!("CARGO_BIN_EXE_iot-server"))
        .args(["admin", "create", "--config"])
        .arg(&config_path)
        .args(["--password-file"])
        .arg(&password_file)
        .output()
        .unwrap();
    assert!(
        create.status.success(),
        "admin create failed: {}",
        String::from_utf8_lossy(&create.stderr)
    );

    let server = Server::spawn(&config_path);
    let base = format!("http://127.0.0.1:{port}");
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .unwrap();
    wait_ready(&client, &base).await;

    let login = tokio::spawn({
        let client = client.clone();
        let url = format!("{base}/api/v1/auth/login");
        async move {
            client
                .post(url)
                .header("content-type", "application/json")
                .header("x-iot-csrf", "1")
                .json(&json!({ "username": "admin", "password": PASSWORD }))
                .send()
                .await
        }
    });
    tokio::time::sleep(Duration::from_millis(50)).await;

    let kill = Command::new("kill")
        .args(["-TERM", &server.id().to_string()])
        .status()
        .unwrap();
    assert!(kill.success());

    let response = tokio::time::timeout(Duration::from_secs(5), login)
        .await
        .expect("login did not finish")
        .expect("login task panicked")
        .expect("login request failed");
    assert_eq!(response.status(), StatusCode::OK);

    let output = tokio::time::timeout(
        Duration::from_secs(10),
        tokio::task::spawn_blocking(move || server.wait_with_output()),
    )
    .await
    .expect("server did not exit")
    .expect("wait task panicked")
    .expect("wait failed");

    assert!(output.status.success(), "exit {:?}", output.status);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(!stderr.contains("panicked"), "{stderr}");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("server starting"), "{stdout}");
    assert!(stdout.contains("server stopped"), "{stdout}");

    let loaded = config::load_with_env(&config_path, |_| None).unwrap();
    let _lock = DataDirLock::try_new(&loaded.data_dir).expect("lock was not released");
}

#[tokio::test]
async fn second_signal_exits_immediately_without_graceful_cleanup() {
    let dir = TempDir::new().unwrap();
    let port = free_port();
    let config_path = write_config(&dir, port);

    let server = Server::spawn(&config_path);
    let base = format!("http://127.0.0.1:{port}");
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .unwrap();
    wait_ready(&client, &base).await;

    // An unfinished login body keeps one request in flight, so graceful
    // shutdown stays busy until the second signal forces the exit.
    let mut pending = tokio::net::TcpStream::connect(("127.0.0.1", port))
        .await
        .unwrap();
    pending
        .write_all(
            b"POST /api/v1/auth/login HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nX-IoT-CSRF: 1\r\nContent-Length: 64\r\n\r\n{\"username\":\"admin\"",
        )
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    let pid = server.id().to_string();
    let first = Command::new("kill").args(["-TERM", &pid]).status().unwrap();
    assert!(first.success());
    tokio::time::sleep(Duration::from_millis(50)).await;
    let second = Command::new("kill").args(["-TERM", &pid]).status().unwrap();
    assert!(second.success());

    let output = tokio::time::timeout(
        Duration::from_secs(5),
        tokio::task::spawn_blocking(move || server.wait_with_output()),
    )
    .await
    .expect("server did not exit after the second signal")
    .expect("wait task panicked")
    .expect("wait failed");

    assert_eq!(output.status.code(), Some(1), "exit {:?}", output.status);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(!stderr.contains("panicked"), "{stderr}");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("second shutdown signal"), "{stdout}");
    assert!(!stdout.contains("server stopped"), "{stdout}");

    let loaded = config::load_with_env(&config_path, |_| None).unwrap();
    let _lock = DataDirLock::try_new(&loaded.data_dir).expect("lock was not released");
}
