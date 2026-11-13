use std::net::IpAddr;
use std::process::{Command, Stdio};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::time::Duration;

use sqlx::PgPool;
use tempfile::TempDir;
use tokio::{net::TcpStream, time::timeout};

const FIXTURE_DATABASE: &str = "iot_test";

#[tokio::test]
#[ignore = "requires an isolated PostgreSQL fixture; run tests/postgres_migration_failure.sh"]
async fn postgres_migration_failure_exits_before_http_or_mqtt_listeners_open() {
    let url = std::env::var("IOT_TEST_POSTGRES_URL").expect("isolated PostgreSQL URL");
    let container = std::env::var("IOT_TEST_POSTGRES_CONTAINER")
        .expect("fixture container name from tests/postgres_migration_failure.sh");
    assert_postgres_fixture(&url, &container).await;

    let pool = PgPool::connect(&url).await.unwrap();
    let (database, schema): (String, String) =
        sqlx::query_as("SELECT current_database(), current_schema()")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(
        database, FIXTURE_DATABASE,
        "refusing a non-fixture database"
    );
    assert_eq!(schema, "public", "refusing a non-fixture schema");

    // A successful but incorrect recorded checksum makes SQLx fail before applying DDL.
    sqlx::query(
        "CREATE TABLE _sqlx_migrations (
            version BIGINT PRIMARY KEY,
            description TEXT NOT NULL,
            installed_on TIMESTAMPTZ NOT NULL DEFAULT now(),
            success BOOLEAN NOT NULL,
            checksum BYTEA NOT NULL,
            execution_time BIGINT NOT NULL
        )",
    )
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO _sqlx_migrations (version, description, success, checksum, execution_time)
         VALUES (1, 'init', TRUE, decode('00', 'hex'), 0)",
    )
    .execute(&pool)
    .await
    .unwrap();
    pool.close().await;

    let dir = TempDir::new().unwrap();
    let http_port = free_port();
    let mqtt_port = loop {
        let port = free_port();
        if port != http_port {
            break port;
        }
    };
    let config = dir.path().join("conf.toml");
    std::fs::write(
        &config,
        format!(
            "config_version = 1\n[server]\ndata_dir = './data'\n\
             [http]\nlisten = '127.0.0.1:{http_port}'\n\
             [mqtt]\nlisten = '127.0.0.1:{mqtt_port}'\n\
             [database]\nurl = '{url}'\n"
        ),
    )
    .unwrap();

    let listener_opened = Arc::new(AtomicBool::new(false));
    let http_probe = monitor_listener(http_port, Arc::clone(&listener_opened));
    let mqtt_probe = monitor_listener(mqtt_port, Arc::clone(&listener_opened));
    let mut child = Command::new(env!("CARGO_BIN_EXE_iot-server"))
        .args(["run", "--config"])
        .arg(&config)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let status = match timeout(Duration::from_secs(20), async {
        loop {
            if let Some(status) = child.try_wait().unwrap() {
                return status;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    {
        Ok(status) => status,
        Err(_) => {
            let _ = child.kill();
            let _ = child.wait();
            http_probe.abort();
            mqtt_probe.abort();
            panic!("server did not exit after migration failure; child was killed");
        }
    };
    http_probe.abort();
    mqtt_probe.abort();

    assert_eq!(
        status.code(),
        Some(4),
        "server did not fail closed on migration error"
    );
    let pool = PgPool::connect(&url).await.unwrap();
    let conflict: (bool, Vec<u8>) =
        sqlx::query_as("SELECT success, checksum FROM _sqlx_migrations WHERE version = 1")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(conflict, (true, vec![0]));
    pool.close().await;
    assert!(
        !listener_opened.load(Ordering::Acquire),
        "HTTP or MQTT accepted a connection before migration failure"
    );
    assert_port_available(http_port);
    assert_port_available(mqtt_port);
}

fn monitor_listener(port: u16, opened: Arc<AtomicBool>) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            if TcpStream::connect(("127.0.0.1", port)).await.is_ok() {
                opened.store(true, Ordering::Release);
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
}

async fn assert_postgres_fixture(url: &str, container: &str) {
    let image = docker_output(&["inspect", "--format", "{{.Config.Image}}", container]).await;
    assert_eq!(image.trim(), "postgres:18.6-bookworm");
    let label = docker_output(&[
        "inspect",
        "--format",
        "{{ index .Config.Labels \"io.iot.test-fixture\" }}",
        container,
    ])
    .await;
    assert_eq!(label.trim(), "postgres-migration-failure");

    let mapped_ports = docker_output(&[
        "inspect",
        "--format",
        "{{json (index .NetworkSettings.Ports \"5432/tcp\")}}",
        container,
    ])
    .await;
    let mapped_ports: serde_json::Value = serde_json::from_str(&mapped_ports).unwrap();
    let bindings = mapped_ports
        .as_array()
        .filter(|bindings| bindings.len() == 1)
        .expect("fixture must have exactly one PostgreSQL port mapping");
    let host: IpAddr = bindings[0]["HostIp"]
        .as_str()
        .expect("fixture port mapping must have a host IP")
        .parse()
        .expect("fixture port mapping host IP must be valid");
    let port: u16 = bindings[0]["HostPort"]
        .as_str()
        .expect("fixture port mapping must have a host port")
        .parse()
        .expect("fixture mapped port must be valid");
    assert!(
        host.is_loopback(),
        "fixture PostgreSQL port must bind loopback"
    );
    validate_fixture_url(url, host, port).expect("test URL must target this fixture mapping");

    let pool = PgPool::connect(url).await.unwrap();
    let (database, schema): (String, String) =
        sqlx::query_as("SELECT current_database(), current_schema()")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(database, FIXTURE_DATABASE, "unexpected database fixture");
    assert_eq!(schema, "public", "unexpected schema fixture");
    pool.close().await;
}

fn validate_fixture_url(
    url: &str,
    mapped_host: IpAddr,
    mapped_port: u16,
) -> Result<(), &'static str> {
    let (scheme, url) = url.split_once("://").ok_or("invalid PostgreSQL URL")?;
    if !matches!(scheme, "postgres" | "postgresql") {
        return Err("invalid PostgreSQL URL scheme");
    }
    let (endpoint, query) = url.split_once('?').ok_or("missing PostgreSQL URL query")?;
    if query != "sslmode=disable" {
        return Err("unexpected PostgreSQL URL query");
    }
    let (authority, database) = endpoint.rsplit_once('/').ok_or("missing database name")?;
    if database != FIXTURE_DATABASE {
        return Err("unexpected database name");
    }
    let (_, host_port) = authority
        .rsplit_once('@')
        .ok_or("missing URL credentials")?;
    let (host, port) = host_port
        .rsplit_once(':')
        .ok_or("missing PostgreSQL port")?;
    let host = host
        .strip_prefix('[')
        .and_then(|host| host.strip_suffix(']'))
        .unwrap_or(host);
    let host: IpAddr = host.parse().map_err(|_| "invalid PostgreSQL URL host")?;
    let port: u16 = port.parse().map_err(|_| "invalid PostgreSQL URL port")?;
    if !host.is_loopback() || host != mapped_host || port != mapped_port {
        return Err("PostgreSQL URL does not match the fixture mapping");
    }
    Ok(())
}

#[test]
fn fixture_url_guard_accepts_only_the_mapped_endpoint_and_database() {
    let host = IpAddr::from([127, 0, 0, 1]);
    let valid = "postgresql://test:secret@127.0.0.1:54321/iot_test?sslmode=disable";
    assert_eq!(validate_fixture_url(valid, host, 54321), Ok(()));
    for invalid in [
        "postgresql://test:secret@127.0.0.1:54321/iot_test?sslmode=disable&host=127.0.0.1",
        "postgresql://test:secret@127.0.0.1:54321/iot_test?sslmode=disable&port=54321",
        "postgresql://test:secret@127.0.0.1:54321/iot_test?sslmode=disable&dbname=iot_test",
        "postgresql://test:secret@192.0.2.1:54321/iot_test?sslmode=disable",
        "postgresql://test:secret@127.0.0.1:54322/iot_test?sslmode=disable",
        "postgresql://test:secret@127.0.0.1:54321/other?sslmode=disable",
    ] {
        assert!(validate_fixture_url(invalid, host, 54321).is_err());
    }
}

async fn docker_output(args: &[&str]) -> String {
    let output = Command::new("docker").args(args).output().unwrap();
    assert!(output.status.success(), "docker {args:?} failed");
    String::from_utf8(output.stdout).unwrap()
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

fn assert_port_available(port: u16) {
    let listener = std::net::TcpListener::bind(("127.0.0.1", port))
        .unwrap_or_else(|error| panic!("listener port {port} is still bound: {error}"));
    drop(listener);
}
