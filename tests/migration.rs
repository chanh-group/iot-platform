use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

use iot_server::config;
use iot_server::runtime::data_dir_lock::DataDirLock;
use iot_server::storage::sqlite::{migrate, pool};
use tempfile::TempDir;

const TABLES: [&str; 9] = [
    "users",
    "sessions",
    "devices",
    "device_credentials",
    "telemetry_events",
    "device_attributes",
    "commands",
    "audit_events",
    "_sqlx_migrations",
];

fn write_config(dir: &TempDir) -> PathBuf {
    let path = dir.path().join("conf.toml");
    fs::write(
        &path,
        "config_version = 1\n[database]\nurl = \"sqlite://./data/iot.db\"\n",
    )
    .unwrap();
    path
}

fn run_migrate(config_path: &Path) -> Output {
    Command::new(env!("CARGO_BIN_EXE_iot-server"))
        .args(["migrate", "--config"])
        .arg(config_path)
        .output()
        .unwrap()
}

#[tokio::test]
async fn migration_creates_schema_and_is_idempotent() {
    let dir = TempDir::new().unwrap();
    let path = write_config(&dir);
    let loaded = config::load_with_env(&path, |_| None).unwrap();
    let _lock = DataDirLock::try_new(&loaded.data_dir).unwrap();
    let pool = pool::connect(&loaded).await.unwrap();

    migrate::run(&pool).await.unwrap();
    migrate::run(&pool).await.unwrap();

    let tables: Vec<String> =
        sqlx::query_scalar("SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name")
            .fetch_all(&pool)
            .await
            .unwrap();
    for table in TABLES {
        assert!(
            tables.iter().any(|name| name == table),
            "missing table {table}: {tables:?}"
        );
    }

    let applied: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM _sqlx_migrations")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(applied, 1);

    pool.close().await;
}

#[tokio::test]
async fn every_pool_connection_gets_contract_pragmas() {
    let dir = TempDir::new().unwrap();
    let path = write_config(&dir);
    let loaded = config::load_with_env(&path, |_| None).unwrap();
    let _lock = DataDirLock::try_new(&loaded.data_dir).unwrap();
    let pool = pool::connect(&loaded).await.unwrap();
    migrate::run(&pool).await.unwrap();

    let mut first = pool.acquire().await.unwrap();
    let mut second = pool.acquire().await.unwrap();

    for connection in [&mut first, &mut second] {
        let journal: String = sqlx::query_scalar("PRAGMA journal_mode")
            .fetch_one(&mut **connection)
            .await
            .unwrap();
        let synchronous: i64 = sqlx::query_scalar("PRAGMA synchronous")
            .fetch_one(&mut **connection)
            .await
            .unwrap();
        let foreign_keys: i64 = sqlx::query_scalar("PRAGMA foreign_keys")
            .fetch_one(&mut **connection)
            .await
            .unwrap();
        let busy_timeout: i64 = sqlx::query_scalar("PRAGMA busy_timeout")
            .fetch_one(&mut **connection)
            .await
            .unwrap();
        let autocheckpoint: i64 = sqlx::query_scalar("PRAGMA wal_autocheckpoint")
            .fetch_one(&mut **connection)
            .await
            .unwrap();

        assert_eq!(journal, "wal");
        assert_eq!(synchronous, 2, "FULL");
        assert_eq!(foreign_keys, 1);
        assert_eq!(busy_timeout, 5_000);
        assert_eq!(autocheckpoint, 1_000);
    }

    drop(first);
    drop(second);
    pool.close().await;
}

#[test]
fn migrate_cli_is_idempotent() {
    let dir = TempDir::new().unwrap();
    let path = write_config(&dir);

    for attempt in 1..=2 {
        let output = run_migrate(&path);
        assert_eq!(
            output.status.code(),
            Some(0),
            "attempt {attempt} stderr: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    let runtime = tokio::runtime::Runtime::new().unwrap();
    let applied: i64 = runtime.block_on(async {
        let loaded = config::load_with_env(&path, |_| None).unwrap();
        let pool = sqlx::SqlitePool::connect(&loaded.database_url)
            .await
            .unwrap();
        let applied = sqlx::query_scalar("SELECT COUNT(*) FROM _sqlx_migrations")
            .fetch_one(&pool)
            .await
            .unwrap();
        pool.close().await;
        applied
    });
    assert_eq!(applied, 1);
}

#[test]
fn second_process_exits_5_while_lock_is_held() {
    let dir = TempDir::new().unwrap();
    let path = write_config(&dir);
    let loaded = config::load_with_env(&path, |_| None).unwrap();
    let lock = DataDirLock::try_new(&loaded.data_dir).unwrap();

    let output = run_migrate(&path);
    assert_eq!(
        output.status.code(),
        Some(5),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("locked"),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    drop(lock);
    let output = run_migrate(&path);
    assert_eq!(
        output.status.code(),
        Some(0),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn invalid_config_exits_3_without_creating_data_dir() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("conf.toml");
    fs::write(
        &path,
        "config_version = 1\n[database]\nurl = \"sqlite://./data/iot.db\"\n[mqtt]\nmax_connections = 0\n",
    )
    .unwrap();

    let output = run_migrate(&path);
    assert_eq!(
        output.status.code(),
        Some(3),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(!dir.path().join("data").exists());
}

#[test]
fn database_open_failure_exits_4() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("conf.toml");
    fs::write(
        &path,
        "config_version = 1\n[database]\nurl = \"sqlite://./data\"\n",
    )
    .unwrap();

    let output = run_migrate(&path);
    assert_eq!(
        output.status.code(),
        Some(4),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(dir.path().join("data").is_dir());
}
