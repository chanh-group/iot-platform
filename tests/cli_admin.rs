use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

use iot_server::config;
use iot_server::core::auth;
use iot_server::storage::sqlite::pool;
use tempfile::TempDir;

fn write_config(dir: &TempDir) -> PathBuf {
    let path = dir.path().join("conf.toml");
    fs::write(
        &path,
        "config_version = 1\n\
         [server]\n\
         data_dir = \"./data\"\n\
         [database]\n\
         url = \"sqlite://./data/iot.db\"\n\
         [auth]\n\
         password_min_length = 12\n",
    )
    .unwrap();
    path
}

fn run(args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_iot-server"))
        .args(args)
        .output()
        .unwrap()
}

fn config_arg(path: &Path) -> &str {
    path.to_str().unwrap()
}

#[tokio::test]
async fn migrate_does_not_create_a_default_admin() {
    let dir = TempDir::new().unwrap();
    let config_path = write_config(&dir);

    let output = run(&["migrate", "--config", config_arg(&config_path)]);
    assert!(output.status.success(), "{output:?}");

    let loaded = config::load_with_env(&config_path, |_| None).unwrap();
    let pool = pool::connect(&loaded).await.unwrap();
    let users: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM users")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(users, 0);
    pool.close().await;
}

#[tokio::test]
async fn admin_create_is_single_and_audited() {
    let dir = TempDir::new().unwrap();
    let config_path = write_config(&dir);
    let password_path = dir.path().join("password.txt");
    let password = "correct horse battery staple";
    fs::write(&password_path, format!("{password}\n")).unwrap();

    let migrate = run(&["migrate", "--config", config_arg(&config_path)]);
    assert!(migrate.status.success(), "{migrate:?}");

    let args = [
        "admin",
        "create",
        "--config",
        config_arg(&config_path),
        "--password-file",
        password_path.to_str().unwrap(),
    ];
    let created = run(&args);
    assert!(created.status.success(), "{created:?}");
    let stdout = String::from_utf8_lossy(&created.stdout);
    assert!(stdout.contains("created admin 'admin'"), "{stdout}");
    assert!(!stdout.contains(password));

    let second = run(&args);
    assert_eq!(second.status.code(), Some(6), "{second:?}");

    let loaded = config::load_with_env(&config_path, |_| None).unwrap();
    let pool = pool::connect(&loaded).await.unwrap();
    let (count, username, password_hash): (i64, String, String) =
        sqlx::query_as("SELECT COUNT(*), username, password_hash FROM users")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(count, 1);
    assert_eq!(username, "admin");
    assert!(password_hash.starts_with("$argon2id$"));
    assert!(auth::verify_password(&password_hash, password));

    let (actor_type, details): (String, Option<String>) = sqlx::query_as(
        "SELECT actor_type, details_json FROM audit_events WHERE action = 'admin.create'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(actor_type, "system");
    let details = details.unwrap();
    assert!(details.contains("\"username\":\"admin\""), "{details}");
    assert!(!details.contains(password));

    pool.close().await;
}

#[test]
fn invalid_username_is_a_clap_usage_error() {
    let output = run(&["admin", "create", "--username", "BadName"]);
    assert_eq!(output.status.code(), Some(2), "{output:?}");
}

#[test]
fn short_password_is_rejected_before_touching_the_database() {
    let dir = TempDir::new().unwrap();
    let config_path = write_config(&dir);
    let password_path = dir.path().join("password.txt");
    fs::write(&password_path, "short\n").unwrap();

    let output = run(&[
        "admin",
        "create",
        "--config",
        config_arg(&config_path),
        "--password-file",
        password_path.to_str().unwrap(),
    ]);
    assert_eq!(output.status.code(), Some(1), "{output:?}");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("at least 12 characters"), "{stderr}");
    assert!(!dir.path().join("data").exists());
}
