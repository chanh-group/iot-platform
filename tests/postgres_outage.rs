mod common;

use std::{
    process::{Child, Command, Stdio},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use common::{mqtt_connect, mqtt_publish, mqtt_subscribe, mqtt_wait_puback};
use rumqttc::{Event, Packet, QoS};
use serde_json::Value;
use sqlx::{PgPool, postgres::PgPoolOptions};
use tempfile::TempDir;
use tokio::{net::TcpListener, time::timeout};
use uuid::Uuid;

const FIXTURE_DATABASE: &str = "iot_test";
const FIXTURE_SCHEMA: &str = "public";

#[tokio::test]
async fn postgres_startup_retries_are_bounded_and_listeners_stay_closed() {
    const ATTEMPTS: usize = 5;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let accepted = Arc::new(AtomicUsize::new(0));
    let accepted_by_task = Arc::clone(&accepted);
    let accept_task = tokio::spawn(async move {
        while accepted_by_task.load(Ordering::Acquire) < ATTEMPTS {
            let (stream, _) = timeout(Duration::from_secs(5), listener.accept())
                .await
                .expect("receive bounded PostgreSQL connection attempt")
                .expect("accept fake PostgreSQL connection");
            accepted_by_task.fetch_add(1, Ordering::AcqRel);
            drop(stream);
        }
    });

    let dir = TempDir::new().unwrap();
    let http_port = free_port();
    let mqtt_port = free_port();
    let config = dir.path().join("conf.toml");
    std::fs::write(
        &config,
        format!(
            "config_version = 1\n[server]\ndata_dir = './data'\n\
             [http]\nlisten = '127.0.0.1:{http_port}'\n\
             [mqtt]\nlisten = '127.0.0.1:{mqtt_port}'\n\
             [database]\nurl = 'postgresql://iot:startup-sentinel@127.0.0.1:{port}/iot_test?sslmode=disable'\n\
             connect_timeout_ms = 100\n"
        ),
    )
    .unwrap();

    let started = Instant::now();
    let output = tokio::task::spawn_blocking(move || {
        Command::new(env!("CARGO_BIN_EXE_iot-server"))
            .args(["run", "--config"])
            .arg(config)
            .stdout(Stdio::null())
            .output()
            .unwrap()
    })
    .await
    .unwrap();
    let elapsed = started.elapsed();
    accept_task.await.unwrap();

    assert_eq!(output.status.code(), Some(4));
    assert_eq!(accepted.load(Ordering::Acquire), ATTEMPTS);
    assert!(elapsed < Duration::from_secs(4), "startup took {elapsed:?}");
    assert!(!String::from_utf8_lossy(&output.stderr).contains("startup-sentinel"));
    assert_port_available(http_port);
    assert_port_available(mqtt_port);
}

#[tokio::test]
#[ignore = "requires Docker and an isolated postgres:18.6-bookworm fixture; run tests/postgres_recovery.sh"]
async fn postgres_outage_degrades_readiness_never_fakes_stored_and_recovers() {
    let url = std::env::var("IOT_TEST_POSTGRES_URL").expect("isolated PostgreSQL test URL");
    let container = std::env::var("IOT_TEST_POSTGRES_CONTAINER")
        .expect("fixture container name from tests/postgres_outage.sh");
    assert_postgres_fixture(&url, &container).await;

    let dir = TempDir::new().unwrap();
    let http_port = free_port();
    let mqtt_port = free_port();
    let config = dir.path().join("conf.toml");
    std::fs::write(
        &config,
        format!(
            "config_version = 1\n[server]\ndata_dir = './data'\nshutdown_timeout_ms = 3000\n\
             [http]\nlisten = '127.0.0.1:{http_port}'\n\
             [mqtt]\nlisten = '127.0.0.1:{mqtt_port}'\n\
             [database]\nurl_env = 'IOT_TEST_POSTGRES_URL'\nconnect_timeout_ms = 500\n\
             [telemetry]\nqueue_capacity = 16\nbatch_size = 1\nflush_interval_ms = 10\n\
             retry_initial_ms = 50\nretry_max_ms = 100\n"
        ),
    )
    .unwrap();
    let migrated = Command::new(env!("CARGO_BIN_EXE_iot-server"))
        .args(["migrate", "--config"])
        .arg(&config)
        .env("IOT_TEST_POSTGRES_URL", &url)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .unwrap();
    assert!(migrated.success(), "PostgreSQL migrations must succeed");

    let pool = PgPool::connect(&url).await.unwrap();
    let device_id = Uuid::now_v7().to_string();
    let token = format!(
        "iot_{}",
        iot_server::core::auth::generate_session_token().unwrap()
    );
    let now = iot_server::core::auth::now_ms();
    sqlx::query(
        "INSERT INTO devices (id, name, metadata_json, enabled, created_at, updated_at)
         VALUES ($1, $2, '{}'::jsonb, TRUE, $3, $3)",
    )
    .bind(&device_id)
    .bind(format!("postgres-outage-{device_id}"))
    .bind(now)
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO device_credentials (id, device_id, token_hash, status, created_at)
         VALUES ($1, $2, $3, 'active', $4)",
    )
    .bind(Uuid::now_v7().to_string())
    .bind(&device_id)
    .bind(iot_server::core::auth::hash_token(&token).as_slice())
    .bind(now)
    .execute(&pool)
    .await
    .unwrap();

    let mut server = start_server(&config, &url, http_port).await;
    let (client, mut eventloop, _) = mqtt_connect(
        format!("127.0.0.1:{mqtt_port}").parse().unwrap(),
        &device_id,
        &device_id,
        &token,
        true,
    )
    .await;
    let ack_topic = format!("devices/{device_id}/telemetry/ack");
    let telemetry_topic = format!("devices/{device_id}/telemetry");
    assert!(matches!(
        mqtt_subscribe(&client, &mut eventloop, &ack_topic, QoS::AtLeastOnce).await,
        rumqttc::SubscribeReasonCode::Success(_)
    ));

    docker(&["stop", "--time", "1", &container]).await;
    wait_ready_status(http_port, reqwest::StatusCode::SERVICE_UNAVAILABLE).await;

    let event_id = format!("postgres-outage-{}", Uuid::now_v7());
    let payload = format!(r#"{{"event_id":"{event_id}","values":{{"x":1}}}}"#);
    mqtt_publish(
        &client,
        &telemetry_topic,
        QoS::AtLeastOnce,
        false,
        payload.as_bytes(),
    )
    .await;
    mqtt_wait_puback(&mut eventloop, Duration::from_secs(5)).await;
    let failed = receive_telemetry_ack(&mut eventloop, &ack_topic, Duration::from_secs(10)).await;
    assert_eq!(failed["status"], "retryable");
    assert_eq!(failed["code"], "storage_unavailable");
    assert_eq!(
        wait_ready_status(http_port, reqwest::StatusCode::SERVICE_UNAVAILABLE).await,
        reqwest::StatusCode::SERVICE_UNAVAILABLE
    );
    assert!(
        receive_telemetry_ack_optional(&mut eventloop, &ack_topic, Duration::from_millis(400))
            .await
            .is_none(),
        "the failed write must not produce a later success ACK while PostgreSQL is down"
    );

    docker(&["start", &container]).await;
    wait_for_postgres(&url).await;
    wait_for_event(&pool, &device_id, &event_id).await;
    let recovery_command_id = Uuid::now_v7().to_string();
    let command_now = iot_server::core::auth::now_ms();
    sqlx::query(
        "INSERT INTO commands
         (id, device_id, name, params_json, status, created_at, expires_at, next_attempt_at)
         VALUES ($1, $2, 'recovery-probe', '{}'::jsonb, 'pending', $3, $4, $3)",
    )
    .bind(&recovery_command_id)
    .bind(&device_id)
    .bind(command_now)
    .bind(command_now + 60_000)
    .execute(&pool)
    .await
    .unwrap();
    assert_eq!(
        wait_ready_status(http_port, reqwest::StatusCode::OK).await,
        reqwest::StatusCode::OK
    );
    let dispatch_attempts: i64 = sqlx::query_scalar("SELECT attempts FROM commands WHERE id = $1")
        .bind(&recovery_command_id)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert!(
        dispatch_attempts > 0,
        "command storage must recover before readiness"
    );

    mqtt_publish(
        &client,
        &telemetry_topic,
        QoS::AtLeastOnce,
        false,
        payload.as_bytes(),
    )
    .await;
    mqtt_wait_puback(&mut eventloop, Duration::from_secs(5)).await;
    let recovered =
        receive_telemetry_ack(&mut eventloop, &ack_topic, Duration::from_secs(10)).await;
    assert_eq!(recovered["status"], "duplicate");
    let row_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM telemetry_events WHERE device_id = $1 AND event_id = $2",
    )
    .bind(&device_id)
    .bind(&event_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(row_count, 1);

    server.kill();
    pool.close().await;
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
    assert_eq!(label.trim(), "postgres-outage");

    let pool = PgPool::connect(url).await.unwrap();
    let (database, schema): (String, String) =
        sqlx::query_as("SELECT current_database(), current_schema()")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(database, FIXTURE_DATABASE, "unexpected database fixture");
    assert_eq!(schema, FIXTURE_SCHEMA, "unexpected schema fixture");
    pool.close().await;
}

async fn start_server(config: &std::path::Path, url: &str, http_port: u16) -> ChildServer {
    let child = Command::new(env!("CARGO_BIN_EXE_iot-server"))
        .args(["run", "--config"])
        .arg(config)
        .env("IOT_TEST_POSTGRES_URL", url)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let mut server = ChildServer(Some(child));
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(2))
        .build()
        .unwrap();
    let mut last_probe = String::from("no response");
    if timeout(Duration::from_secs(20), async {
        loop {
            assert!(server.try_wait().is_none(), "server exited during startup");
            match client
                .get(format!("http://127.0.0.1:{http_port}/readyz"))
                .send()
                .await
            {
                Ok(response) if response.status().is_success() => return,
                Ok(response) => last_probe = format!("HTTP {}", response.status()),
                Err(error) => last_probe = error.to_string(),
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .is_err()
    {
        panic!("PostgreSQL app did not become ready: {last_probe}");
    }
    server
}

struct ChildServer(Option<Child>);

impl ChildServer {
    fn try_wait(&mut self) -> Option<std::process::ExitStatus> {
        self.0.as_mut().unwrap().try_wait().unwrap()
    }

    fn kill(&mut self) {
        if let Some(mut child) = self.0.take() {
            child.kill().unwrap();
            child.wait().unwrap();
        }
    }
}

impl Drop for ChildServer {
    fn drop(&mut self) {
        self.kill();
    }
}

async fn wait_ready_status(port: u16, expected: reqwest::StatusCode) -> reqwest::StatusCode {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(2))
        .build()
        .unwrap();
    let mut last_probe = String::from("no response");
    if timeout(Duration::from_secs(15), async {
        loop {
            match client
                .get(format!("http://127.0.0.1:{port}/readyz"))
                .send()
                .await
            {
                Ok(response) => {
                    let status = response.status();
                    let body = response.text().await.unwrap_or_default();
                    if status == expected {
                        return status;
                    }
                    last_probe = format!("HTTP {status}: {body}");
                }
                Err(error) => last_probe = error.to_string(),
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .is_err()
    {
        panic!("readiness did not reach {expected}: {last_probe}");
    }
    expected
}

async fn receive_telemetry_ack(
    eventloop: &mut rumqttc::EventLoop,
    topic: &str,
    duration: Duration,
) -> Value {
    receive_telemetry_ack_optional(eventloop, topic, duration)
        .await
        .expect("receive telemetry application ACK")
}

async fn receive_telemetry_ack_optional(
    eventloop: &mut rumqttc::EventLoop,
    topic: &str,
    duration: Duration,
) -> Option<Value> {
    timeout(duration, async {
        loop {
            if let Event::Incoming(Packet::Publish(publish)) = eventloop.poll().await.unwrap()
                && publish.topic == topic
            {
                return serde_json::from_slice(&publish.payload).unwrap();
            }
        }
    })
    .await
    .ok()
}

async fn wait_for_postgres(url: &str) {
    let mut last_error = String::from("connection has not been attempted");
    if timeout(Duration::from_secs(30), async {
        loop {
            match PgPoolOptions::new()
                .acquire_timeout(Duration::from_millis(500))
                .connect(url)
                .await
            {
                Ok(pool) => match sqlx::query_scalar::<_, i32>("SELECT 1")
                    .fetch_one(&pool)
                    .await
                {
                    Ok(_) => {
                        pool.close().await;
                        return;
                    }
                    Err(error) => last_error = error.to_string(),
                },
                Err(error) => last_error = error.to_string(),
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .is_err()
    {
        let state = docker_output(&[
            "inspect",
            "--format",
            "{{.State.Status}} {{.State.Health.Status}} {{json .NetworkSettings.Ports}}",
            &std::env::var("IOT_TEST_POSTGRES_CONTAINER").unwrap(),
        ])
        .await;
        panic!("PostgreSQL fixture did not recover: {last_error}; container={state}");
    }
}

async fn wait_for_event(pool: &PgPool, device_id: &str, event_id: &str) {
    timeout(Duration::from_secs(15), async {
        loop {
            let count: i64 = sqlx::query_scalar(
                "SELECT COUNT(*) FROM telemetry_events WHERE device_id = $1 AND event_id = $2",
            )
            .bind(device_id)
            .bind(event_id)
            .fetch_one(pool)
            .await
            .unwrap();
            if count == 1 {
                return;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("pending telemetry commits after PostgreSQL recovery");
}

async fn docker(args: &[&str]) {
    let output = docker_command(args).await;
    assert!(
        output.status.success(),
        "Docker command failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

async fn docker_output(args: &[&str]) -> String {
    let output = docker_command(args).await;
    assert!(
        output.status.success(),
        "Docker inspect failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).unwrap()
}

async fn docker_command(args: &[&str]) -> std::process::Output {
    let args = args.iter().map(|arg| (*arg).to_owned()).collect::<Vec<_>>();
    tokio::task::spawn_blocking(move || Command::new("docker").args(args).output().unwrap())
        .await
        .unwrap()
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
        .expect("database startup failure must not bind application listeners");
    drop(listener);
}
