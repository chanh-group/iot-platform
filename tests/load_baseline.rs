mod common;

use std::collections::HashMap;
use std::fs;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::{Context, ensure};
use common::TestApp;
use iot_server::core::device::{DeviceAudit, NewDevice};
use iot_server::storage::runtime::StorageRuntime;
use rumqttc::{AsyncClient, Event, EventLoop, MqttOptions, Packet, QoS};
use serde_json::Value;
use sqlx::{PgPool, postgres::PgPoolOptions};
use tempfile::TempDir;
use tokio::sync::{mpsc, watch};
use uuid::Uuid;

const CONNECTIONS: usize = 1_000;
const TICK: Duration = Duration::from_secs(1);
const METRICS_TOKEN: &str = "load-test-metrics-token-not-for-production";

#[derive(Default)]
struct Results {
    sent: AtomicUsize,
    mqtt_pubacks: AtomicUsize,
    application_acks: AtomicUsize,
    stored: AtomicUsize,
    rejected: AtomicUsize,
    payload_bytes: AtomicUsize,
    latencies_ms: Mutex<Vec<f64>>,
    connack_latencies_ms: Mutex<Vec<f64>>,
    connect_attempts_inflight: AtomicUsize,
    connect_attempts_peak: AtomicUsize,
}

struct LoadClient {
    index: usize,
    device_id: String,
    token: String,
    address: SocketAddr,
}

#[derive(Clone)]
struct LoadRun {
    ready: mpsc::Sender<()>,
    start: watch::Receiver<Option<Instant>>,
    results: Arc<Results>,
    duration: Duration,
}

/// Counts a publish only when it is created inside the measurement window.
struct PublishWindow {
    end: Instant,
    sent: usize,
}

impl PublishWindow {
    fn claim(&mut self, now: Instant) -> Option<usize> {
        if now >= self.end {
            return None;
        }
        self.sent += 1;
        Some(self.sent)
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 16)]
#[ignore = "manual SQLite load baseline; run tests/load/run.sh"]
async fn one_thousand_qos1_clients_measure_ack_latency_and_resource_growth() -> anyhow::Result<()> {
    let duration = Duration::from_secs(
        std::env::var("IOT_LOAD_DURATION_SECONDS")
            .unwrap_or_else(|_| "60".to_owned())
            .parse()?,
    );
    ensure!(!duration.is_zero(), "load duration must be positive");

    let app = TestApp::builder()
        .metrics_token(METRICS_TOKEN)
        .start()
        .await;
    let mut devices = Vec::with_capacity(CONNECTIONS);
    for index in 0..CONNECTIONS {
        devices.push(app.create_device(&format!("load-{index}"), true).await);
    }

    let root = app.config.data_dir.clone();
    let disk_before = data_bytes(&root)?;
    let server_pid = std::process::id();
    let peak_rss_kib = Arc::new(Mutex::new(current_rss_kib(server_pid)?));
    let sampling = {
        let peak_rss_kib = Arc::clone(&peak_rss_kib);
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_millis(250));
            loop {
                interval.tick().await;
                if let Ok(current) = current_rss_kib(server_pid)
                    && let Ok(mut peak) = peak_rss_kib.lock()
                {
                    *peak = (*peak).max(current);
                }
            }
        })
    };

    let results = Arc::new(Results::default());
    let (ready_tx, mut ready_rx) = mpsc::channel(CONNECTIONS);
    let (start_tx, start_rx) = watch::channel(None);
    let mqtt_addr = app.mqtt_addr;
    let mut clients = tokio::task::JoinSet::new();
    for (index, device) in devices.into_iter().enumerate() {
        let client = LoadClient {
            index,
            device_id: device.id,
            token: device.token,
            address: mqtt_addr,
        };
        let run = LoadRun {
            ready: ready_tx.clone(),
            start: start_rx.clone(),
            results: Arc::clone(&results),
            duration,
        };
        clients.spawn(async move { load_device(client, run).await });
    }

    drop(ready_tx);
    for _ in 0..CONNECTIONS {
        tokio::select! {
            ready = ready_rx.recv() => {
                ready.context("load client exited before ready")?;
            }
            client = clients.join_next() => {
                client.context("no load clients remain")?.context("load client task panicked")??;
                anyhow::bail!("load client exited before ready");
            }
        }
    }
    let start = Instant::now() + Duration::from_secs(2);
    start_tx
        .send(Some(start))
        .map_err(|_| anyhow::anyhow!("load clients exited before start"))?;
    let end = start + duration;
    tokio::time::sleep_until(tokio::time::Instant::from_std(end)).await;
    while let Some(client) = clients.join_next().await {
        client.context("load client task panicked")??;
    }
    sampling.abort();

    let results =
        Arc::try_unwrap(results).map_err(|_| anyhow::anyhow!("load result still shared"))?;
    let mut latencies_ms = std::mem::take(
        &mut *results
            .latencies_ms
            .lock()
            .map_err(|_| anyhow::anyhow!("load latency mutex poisoned"))?,
    );
    latencies_ms.sort_by(f64::total_cmp);
    let p95 = percentile(&latencies_ms, 0.95);
    let mut connack_latencies_ms = std::mem::take(
        &mut *results
            .connack_latencies_ms
            .lock()
            .map_err(|_| anyhow::anyhow!("CONNACK latency mutex poisoned"))?,
    );
    connack_latencies_ms.sort_by(f64::total_cmp);
    let connack_p95 = percentile(&connack_latencies_ms, 0.95);
    ensure!(
        connack_latencies_ms.len() == CONNECTIONS,
        "did not measure a CONNACK for every client"
    );
    let disk_after = data_bytes(&root)?;
    let peak_rss_kib = *peak_rss_kib
        .lock()
        .map_err(|_| anyhow::anyhow!("RSS sample mutex poisoned"))?;
    let cgroup_memory_peak_kib = cgroup_memory_peak_kib();
    let expected = results.sent.load(Ordering::Relaxed);
    let mqtt_pubacks = results.mqtt_pubacks.load(Ordering::Relaxed);
    let application_acks = results.application_acks.load(Ordering::Relaxed);
    let stored = results.stored.load(Ordering::Relaxed);
    let rejected = results.rejected.load(Ordering::Relaxed);
    let committed_rows: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM telemetry_events")
        .fetch_one(&app.pool)
        .await?;
    let metrics = reqwest::Client::new()
        .get(format!("{}/metrics", app.base))
        .bearer_auth(METRICS_TOKEN)
        .send()
        .await?
        .error_for_status()?
        .text()
        .await?;
    let telemetry_ack_dropped = metric_value(&metrics, "iot_telemetry_ack_dropped_total");
    let presence_hook_backlog = metric_value(&metrics, "iot_mqtt_presence_hook_inflight_max");
    let connect_attempts_peak = results.connect_attempts_peak.load(Ordering::Relaxed);
    ensure!(
        presence_hook_backlog > 0,
        "presence hook backlog was not observed"
    );

    println!(
        "load_baseline={{\"connections\":{CONNECTIONS},\"duration_seconds\":{},\
         \"target_publish_requests_per_second\":{CONNECTIONS},\
         \"publish_requests_created_in_window\":{expected},\
         \"mqtt_pubacks\":{},\"application_acks\":{},\"stored\":{},\
         \"rejected\":{},\"ack_rate_percent\":{:.3},\"application_ack_p95_ms\":{p95:.3},\
         \"peak_rss_kib\":{peak_rss_kib},\"sqlite_growth_bytes\":{}}}",
        duration.as_secs(),
        mqtt_pubacks,
        application_acks,
        stored,
        rejected,
        application_acks as f64 * 100.0 / expected.max(1) as f64,
        disk_after.saturating_sub(disk_before),
    );
    println!(
        "load_details={{\"committed_rows\":{committed_rows},\"telemetry_ack_dropped\":{telemetry_ack_dropped},\
         \"publish_requests_per_second\":{:.3}}}",
        expected as f64 / duration.as_secs_f64(),
    );
    println!(
        "load_connect={{\"connack_p95_ms\":{connack_p95:.3},\
         \"connect_attempts_pending_connack_peak\":{connect_attempts_peak},\
         \"presence_hook_inflight_peak\":{presence_hook_backlog}}}"
    );
    if let Some(peak) = cgroup_memory_peak_kib {
        println!("load_cgroup_memory_peak_kib={peak}");
    }
    ensure!(
        mqtt_pubacks == expected,
        "QoS 1 PUBACK count differs from sends"
    );
    ensure!(
        application_acks <= expected,
        "more application ACKs than sends"
    );
    ensure!(stored + rejected == application_acks);
    ensure!(rejected == 0, "valid load telemetry was rejected");
    ensure!(
        committed_rows == i64::try_from(expected)?,
        "not all QoS 1 telemetry was committed"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 16)]
#[ignore = "requires isolated Docker PostgreSQL fixture; run tests/load/postgres-fixture.sh"]
async fn one_thousand_qos1_clients_measure_postgres_load() -> anyhow::Result<()> {
    let duration = load_duration()?;
    let mut fixture = PostgresLoadFixture::new().await?;
    let devices = fixture.seed_devices().await?;
    let server_pid = fixture.start_server().await?;
    println!(
        "postgres_load_config={{\"pool_max_connections\":{},\"connect_timeout_ms\":{},\
         \"connect_attempts_max\":5,\"telemetry_retry_initial_ms\":{},\
         \"telemetry_retry_max_ms\":{},\"qos\":1}}",
        fixture.pool_max_connections,
        fixture.connect_timeout_ms,
        fixture.telemetry_retry_initial_ms,
        fixture.telemetry_retry_max_ms,
    );
    let device_ids = devices.iter().map(|(id, _)| id.clone()).collect::<Vec<_>>();
    let rows_before: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM telemetry_events WHERE device_id = ANY($1)")
            .bind(&device_ids)
            .fetch_one(&fixture.pool)
            .await?;
    ensure!(rows_before == 0, "load devices already have telemetry");
    let peak_rss_kib = Arc::new(Mutex::new(current_rss_kib(server_pid)?));
    let sampling = {
        let peak_rss_kib = Arc::clone(&peak_rss_kib);
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_millis(250));
            loop {
                interval.tick().await;
                if let Ok(current) = current_rss_kib(server_pid)
                    && let Ok(mut peak) = peak_rss_kib.lock()
                {
                    *peak = (*peak).max(current);
                }
            }
        })
    };

    let results = Arc::new(Results::default());
    let (ready_tx, mut ready_rx) = mpsc::channel(CONNECTIONS);
    let (start_tx, start_rx) = watch::channel(None);
    let mut clients = tokio::task::JoinSet::new();
    for (index, (device_id, token)) in devices.into_iter().enumerate() {
        let client = LoadClient {
            index,
            device_id,
            token,
            address: fixture.mqtt_addr,
        };
        let run = LoadRun {
            ready: ready_tx.clone(),
            start: start_rx.clone(),
            results: Arc::clone(&results),
            duration,
        };
        clients.spawn(async move { load_device(client, run).await });
    }
    drop(ready_tx);
    for _ in 0..CONNECTIONS {
        tokio::select! {
            ready = ready_rx.recv() => { ready.context("load client exited before ready")?; }
            client = clients.join_next() => {
                client.context("no load clients remain")?.context("load client task panicked")??;
                anyhow::bail!("load client exited before ready");
            }
        }
    }
    let database_size_before: i64 =
        sqlx::query_scalar("SELECT pg_database_size(current_database())")
            .fetch_one(&fixture.pool)
            .await?;
    let start = Instant::now() + Duration::from_secs(2);
    start_tx
        .send(Some(start))
        .map_err(|_| anyhow::anyhow!("load clients exited before start"))?;
    let end = start + duration;
    tokio::time::sleep_until(tokio::time::Instant::from_std(end)).await;
    let drain_started = Instant::now();
    while let Some(client) = clients.join_next().await {
        client.context("load client task panicked")??;
    }
    let drain_duration = drain_started.elapsed();
    sampling.abort();

    let results =
        Arc::try_unwrap(results).map_err(|_| anyhow::anyhow!("load result still shared"))?;
    let mut latencies_ms = std::mem::take(
        &mut *results
            .latencies_ms
            .lock()
            .map_err(|_| anyhow::anyhow!("load latency mutex poisoned"))?,
    );
    latencies_ms.sort_by(f64::total_cmp);
    let p95 = percentile(&latencies_ms, 0.95);
    let mut connack_latencies_ms = std::mem::take(
        &mut *results
            .connack_latencies_ms
            .lock()
            .map_err(|_| anyhow::anyhow!("CONNACK latency mutex poisoned"))?,
    );
    connack_latencies_ms.sort_by(f64::total_cmp);
    let connack_p95 = percentile(&connack_latencies_ms, 0.95);
    ensure!(
        connack_latencies_ms.len() == CONNECTIONS,
        "missing CONNACK samples"
    );
    let database_size_after: i64 =
        sqlx::query_scalar("SELECT pg_database_size(current_database())")
            .fetch_one(&fixture.pool)
            .await?;
    let committed_rows: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM telemetry_events WHERE device_id = ANY($1)")
            .bind(&device_ids)
            .fetch_one(&fixture.pool)
            .await?;
    let peak_rss_kib = *peak_rss_kib
        .lock()
        .map_err(|_| anyhow::anyhow!("RSS sample mutex poisoned"))?;
    let cgroup_memory_peak_kib = cgroup_memory_peak_kib();
    let expected = results.sent.load(Ordering::Relaxed);
    let mqtt_pubacks = results.mqtt_pubacks.load(Ordering::Relaxed);
    let application_acks = results.application_acks.load(Ordering::Relaxed);
    let stored = results.stored.load(Ordering::Relaxed);
    let rejected = results.rejected.load(Ordering::Relaxed);
    let payload_bytes = results.payload_bytes.load(Ordering::Relaxed);
    let metrics = reqwest::Client::new()
        .get(format!("{}/metrics", fixture.base))
        .bearer_auth(METRICS_TOKEN)
        .send()
        .await?
        .error_for_status()?
        .text()
        .await?;
    let telemetry_ack_dropped = metric_value(&metrics, "iot_telemetry_ack_dropped_total");
    let presence_hook_backlog = metric_value(&metrics, "iot_mqtt_presence_hook_inflight_max");
    ensure!(
        presence_hook_backlog > 0,
        "presence hook backlog was not observed"
    );
    println!(
        "postgres_load={{\"connections\":{CONNECTIONS},\"window_seconds\":{},\
         \"telemetry_rows_before_for_run_devices\":{rows_before},\
         \"drain_seconds\":{:.3},\"target_publish_requests_per_second\":{CONNECTIONS},\
         \"publish_requests_created_in_window\":{expected},\"mqtt_pubacks\":{mqtt_pubacks},\
         \"application_acks\":{application_acks},\"stored\":{stored},\"rejected\":{rejected},\
         \"ack_rate_percent\":{:.3},\"application_ack_p95_ms\":{p95:.3},\
         \"mean_payload_bytes\":{:.3},\
         \"server_peak_rss_kib\":{peak_rss_kib},\"database_size_before_bytes\":{},\
         \"database_size_after_bytes\":{},\"database_growth_bytes\":{}}}",
        duration.as_secs(),
        drain_duration.as_secs_f64(),
        application_acks as f64 * 100.0 / expected.max(1) as f64,
        payload_bytes as f64 / expected.max(1) as f64,
        database_size_before,
        database_size_after,
        database_size_after.saturating_sub(database_size_before),
    );
    println!(
        "postgres_load_details={{\"committed_rows_for_run_devices\":{committed_rows},\
         \"telemetry_ack_dropped\":{telemetry_ack_dropped},\"publish_requests_per_second\":{:.3},\
         \"connack_p95_ms\":{connack_p95:.3},\"pending_connack_peak\":{},\
         \"presence_hook_inflight_peak\":{presence_hook_backlog}}}",
        expected as f64 / duration.as_secs_f64(),
        results.connect_attempts_peak.load(Ordering::Relaxed),
    );
    if let Some(peak) = cgroup_memory_peak_kib {
        println!("load_cgroup_memory_peak_kib={peak}");
    }
    ensure!(
        mqtt_pubacks == expected,
        "QoS 1 PUBACK count differs from sends"
    );
    ensure!(
        application_acks <= expected,
        "more application ACKs than sends"
    );
    ensure!(stored + rejected == application_acks);
    ensure!(rejected == 0, "valid load telemetry was rejected");
    ensure!(
        committed_rows == i64::try_from(expected)?,
        "not all run telemetry committed"
    );
    Ok(())
}

fn load_duration() -> anyhow::Result<Duration> {
    let duration = Duration::from_secs(
        std::env::var("IOT_LOAD_DURATION_SECONDS")
            .unwrap_or_else(|_| "60".to_owned())
            .parse()?,
    );
    ensure!(!duration.is_zero(), "load duration must be positive");
    Ok(duration)
}

struct PostgresLoadFixture {
    _dir: TempDir,
    config: PathBuf,
    base: String,
    mqtt_addr: SocketAddr,
    pool: PgPool,
    pool_max_connections: u64,
    connect_timeout_ms: u64,
    telemetry_retry_initial_ms: u64,
    telemetry_retry_max_ms: u64,
    server: Option<Child>,
}

impl PostgresLoadFixture {
    async fn new() -> anyhow::Result<Self> {
        let url = std::env::var("IOT_LOAD_POSTGRES_URL")?;
        let password = std::env::var("IOT_LOAD_POSTGRES_PASSWORD")?;
        let port = std::env::var("IOT_LOAD_POSTGRES_PORT")?;
        let fixture_id = std::env::var("IOT_LOAD_POSTGRES_CONTAINER_ID")?;
        let fixture_label = std::env::var("IOT_LOAD_POSTGRES_FIXTURE_LABEL")?;
        let image_id = std::env::var("IOT_LOAD_POSTGRES_IMAGE_ID")?;
        let expected_url = format!(
            "postgres://iot_load:{password}@127.0.0.1:{port}/iot_load_fixture?sslmode=disable"
        );
        ensure!(
            url == expected_url,
            "PostgreSQL URL is not the fixture endpoint or contains options"
        );
        ensure!(
            !url.contains("dbname=") && !url.contains('#'),
            "database override parameters are forbidden"
        );
        verify_docker_fixture(&fixture_id, &fixture_label, &image_id, &port)?;

        let pool = PgPoolOptions::new()
            .max_connections(16)
            .acquire_timeout(Duration::from_secs(5))
            .connect(&url)
            .await?;
        let (database, schema): (String, String) =
            sqlx::query_as("SELECT current_database(), current_schema()")
                .fetch_one(&pool)
                .await?;
        ensure!(
            database == "iot_load_fixture",
            "unexpected fixture database"
        );
        ensure!(schema == "public", "unexpected fixture schema");
        let telemetry_table: Option<String> =
            sqlx::query_scalar("SELECT to_regclass('public.telemetry_events')::text")
                .fetch_one(&pool)
                .await?;
        ensure!(telemetry_table.is_none(), "fixture database is not fresh");

        let dir = tempfile::tempdir()?;
        let http_port = free_port()?;
        let mqtt_port = loop {
            let candidate = free_port()?;
            if candidate != http_port {
                break candidate;
            }
        };
        let config = dir.path().join("conf.toml");
        fs::write(
            &config,
            format!(
                "config_version = 1\n\
                 [server]\ndata_dir = './data'\nshutdown_timeout_ms = 5000\n\
                 [http]\nlisten = '127.0.0.1:{http_port}'\ndashboard = false\n\
                 metrics = true\nmetrics_bearer_token_env = 'IOT_LOAD_METRICS_TOKEN'\n\
                 [mqtt]\nlisten = '127.0.0.1:{mqtt_port}'\n\
                 [database]\nurl_env = 'IOT_LOAD_POSTGRES_URL'\n"
            ),
        )?;

        let migration = Command::new(env!("CARGO_BIN_EXE_iot-server"))
            .args(["migrate", "--config"])
            .arg(&config)
            .output()?;
        ensure!(
            migration.status.success(),
            "PostgreSQL migration failed: {}",
            String::from_utf8_lossy(&migration.stderr)
        );
        let password_file = dir.path().join("admin-password");
        fs::write(&password_file, "load-baseline-test-password\n")?;
        let admin = Command::new(env!("CARGO_BIN_EXE_iot-server"))
            .args(["admin", "create", "--username", "load-admin", "--config"])
            .arg(&config)
            .args(["--password-file"])
            .arg(&password_file)
            .output()?;
        ensure!(
            admin.status.success(),
            "admin bootstrap failed: {}",
            String::from_utf8_lossy(&admin.stderr)
        );

        let loaded = iot_server::config::load(&config)?;
        let pool_max_connections = loaded.config.database.max_connections;
        let connect_timeout_ms = loaded.config.database.connect_timeout_ms;
        let telemetry_retry_initial_ms = loaded.config.telemetry.retry_initial_ms;
        let telemetry_retry_max_ms = loaded.config.telemetry.retry_max_ms;
        let storage = StorageRuntime::connect(&loaded).await?;
        let user = storage
            .repositories
            .admin
            .find_user("load-admin")
            .await?
            .context("load fixture admin missing after bootstrap")?;
        let fixture = Self {
            _dir: dir,
            config,
            base: format!("http://127.0.0.1:{http_port}"),
            mqtt_addr: format!("127.0.0.1:{mqtt_port}").parse()?,
            pool,
            pool_max_connections,
            connect_timeout_ms,
            telemetry_retry_initial_ms,
            telemetry_retry_max_ms,
            server: None,
        };
        drop(user);
        drop(storage);
        Ok(fixture)
    }

    async fn seed_devices(&self) -> anyhow::Result<Vec<(String, String)>> {
        let loaded = iot_server::config::load(&self.config)?;
        let storage = StorageRuntime::connect(&loaded).await?;
        let admin = storage
            .repositories
            .admin
            .find_user("load-admin")
            .await?
            .context("load fixture admin missing")?;
        let mut devices = Vec::with_capacity(CONNECTIONS);
        for index in 0..CONNECTIONS {
            let id = Uuid::now_v7().to_string();
            let token = format!("iot_{}", iot_server::core::auth::generate_session_token()?);
            let credential_id = Uuid::now_v7().to_string();
            let token_hash = iot_server::core::auth::hash_token(&token);
            let now = iot_server::core::auth::now_ms();
            storage
                .repositories
                .devices
                .create(
                    &NewDevice {
                        id: &id,
                        name: &format!("load-{index}"),
                        metadata_json: "{}",
                        enabled: true,
                        created_at: now,
                        credential_id: &credential_id,
                        token_hash: &token_hash,
                    },
                    &DeviceAudit {
                        actor_id: &admin.id,
                        request_id: &format!("load-seed-{index}"),
                        remote_ip: "127.0.0.1",
                    },
                )
                .await?;
            devices.push((id, token));
        }
        storage.close().await;
        Ok(devices)
    }

    async fn start_server(&mut self) -> anyhow::Result<u32> {
        let log = fs::File::create(self._dir.path().join("server.log"))?;
        let stderr = log.try_clone()?;
        let child = Command::new(env!("CARGO_BIN_EXE_iot-server"))
            .args(["run", "--config"])
            .arg(&self.config)
            .env("IOT_LOAD_METRICS_TOKEN", METRICS_TOKEN)
            .stdout(Stdio::from(log))
            .stderr(Stdio::from(stderr))
            .spawn()?;
        let pid = child.id();
        self.server = Some(child);
        let client = reqwest::Client::builder()
            .timeout(Duration::from_millis(500))
            .build()?;
        for _ in 0..240 {
            if self
                .server
                .as_mut()
                .context("server process missing")?
                .try_wait()?
                .is_some()
            {
                anyhow::bail!("PostgreSQL load server exited before readiness");
            }
            if client
                .get(format!("{}/readyz", self.base))
                .send()
                .await
                .is_ok_and(|response| response.status().is_success())
            {
                return Ok(pid);
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
        anyhow::bail!("PostgreSQL load server did not become ready")
    }
}

impl Drop for PostgresLoadFixture {
    fn drop(&mut self) {
        if let Some(mut server) = self.server.take() {
            let _ = server.kill();
            let _ = server.wait();
        }
    }
}

fn verify_docker_fixture(id: &str, label: &str, image_id: &str, port: &str) -> anyhow::Result<()> {
    let inspect = |format: &str| -> anyhow::Result<String> {
        let output = Command::new("docker")
            .args(["inspect", "--format", format, id])
            .output()?;
        ensure!(output.status.success(), "cannot inspect fixture container");
        Ok(String::from_utf8(output.stdout)?.trim().to_owned())
    };
    ensure!(inspect("{{.Id}}")? == id, "container ID mismatch");
    ensure!(
        inspect("{{.Config.Image}}")? == "postgres:18.6-bookworm",
        "unexpected fixture image"
    );
    ensure!(
        inspect("{{.Image}}")? == image_id,
        "fixture image ID mismatch"
    );
    ensure!(
        inspect("{{index .Config.Labels \"org.thingsboardrust.fixture\"}}")? == label,
        "fixture label mismatch"
    );
    ensure!(
        inspect("{{(index (index .NetworkSettings.Ports \"5432/tcp\") 0).HostPort}}")? == port,
        "URL port does not map to fixture container"
    );
    Ok(())
}

fn free_port() -> anyhow::Result<u16> {
    Ok(std::net::TcpListener::bind("127.0.0.1:0")?
        .local_addr()?
        .port())
}

async fn load_device(client: LoadClient, run: LoadRun) -> anyhow::Result<()> {
    let LoadClient {
        index,
        device_id,
        token,
        address,
    } = client;
    let LoadRun {
        ready,
        mut start,
        results,
        duration,
    } = run;
    let mut options = MqttOptions::new(&device_id, address.ip().to_string(), address.port());
    options
        .set_keep_alive(Duration::from_secs(30))
        .set_clean_session(true)
        .set_credentials(&device_id, token);
    let (client, mut eventloop) = AsyncClient::new(options, 32);
    let ack_topic = format!("devices/{device_id}/telemetry/ack");
    client.subscribe(&ack_topic, QoS::AtLeastOnce).await?;
    let connect_started = Instant::now();
    let pending = results
        .connect_attempts_inflight
        .fetch_add(1, Ordering::Relaxed)
        .saturating_add(1);
    results
        .connect_attempts_peak
        .fetch_max(pending, Ordering::Relaxed);
    let connack = wait_connack(&mut eventloop, connect_started).await;
    results
        .connect_attempts_inflight
        .fetch_sub(1, Ordering::Relaxed);
    results
        .connack_latencies_ms
        .lock()
        .map_err(|_| anyhow::anyhow!("CONNACK latency mutex poisoned"))?
        .push(connack?);
    wait_subscribed(&mut eventloop).await?;
    ready.send(()).await?;
    while start.borrow().is_none() {
        start.changed().await?;
    }
    let start_at = start.borrow().context("load start time missing")?;
    let end = start_at + duration;

    let first_tick = start_at + Duration::from_millis(index as u64);
    let mut interval = tokio::time::interval_at(tokio::time::Instant::from_std(first_tick), TICK);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut window = PublishWindow { end, sent: 0 };
    let mut completed = 0usize;
    let mut pending = HashMap::<String, Instant>::new();
    let deadline = end + Duration::from_secs(10);

    loop {
        if Instant::now() >= end && completed == window.sent {
            break;
        }
        tokio::select! {
            _ = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)) => break,
            _ = interval.tick() => {
                // Check the clock here, not in a select guard: a tick can become
                // ready after the guard passed while both branches were pending.
                if let Some(sent) = window.claim(Instant::now()) {
                    let event_id = format!("load-{index}-{sent}");
                    let raw = format!(r#"{{"event_id":"{event_id}","values":{{"load":{sent}}}}}"#);
                    results.payload_bytes.fetch_add(raw.len(), Ordering::Relaxed);
                    pending.insert(event_id, Instant::now());
                    client.publish(
                        format!("devices/{device_id}/telemetry"),
                        QoS::AtLeastOnce,
                        false,
                        raw,
                    ).await?;
                }
            }
            event = eventloop.poll() => match event? {
                Event::Incoming(Packet::SubAck(_)) => {}
                Event::Incoming(Packet::PubAck(_)) => {
                    results.mqtt_pubacks.fetch_add(1, Ordering::Relaxed);
                }
                Event::Incoming(Packet::Publish(message)) if message.topic == ack_topic => {
                    let ack: Value = serde_json::from_slice(&message.payload)?;
                    let id = ack["event_id"].as_str().context("ACK missing event_id")?;
                    if let Some(submitted) = pending.remove(id) {
                        completed += 1;
                        results.application_acks.fetch_add(1, Ordering::Relaxed);
                        results
                            .latencies_ms
                            .lock()
                            .map_err(|_| anyhow::anyhow!("load latency mutex poisoned"))?
                            .push(submitted.elapsed().as_secs_f64() * 1_000.0);
                        match ack["status"].as_str() {
                            Some("stored") => { results.stored.fetch_add(1, Ordering::Relaxed); }
                            _ => { results.rejected.fetch_add(1, Ordering::Relaxed); }
                        }
                    }
                }
                _ => {}
            }
        }
    }
    results.sent.fetch_add(window.sent, Ordering::Relaxed);
    Ok(())
}

#[test]
fn delayed_ticks_after_the_measurement_window_are_not_published() {
    let start = Instant::now();
    let end = start + Duration::from_secs(60);
    let mut window = PublishWindow { end, sent: 0 };

    for second in 0..60 {
        assert_eq!(
            window.claim(start + Duration::from_secs(second)),
            Some(second as usize + 1)
        );
    }

    // A slow client waking after `end` must not backfill the ticks it missed.
    assert_eq!(window.claim(end), None);
    assert_eq!(window.claim(end + Duration::from_secs(5)), None);
    assert_eq!(window.sent, 60);
}

async fn wait_connack(eventloop: &mut EventLoop, started: Instant) -> anyhow::Result<f64> {
    let latency = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if let Event::Incoming(Packet::ConnAck(_)) = eventloop.poll().await? {
                return Ok::<_, rumqttc::ConnectionError>(
                    started.elapsed().as_secs_f64() * 1_000.0,
                );
            }
        }
    })
    .await
    .context("timed out waiting for MQTT CONNACK")??;
    Ok(latency)
}

async fn wait_subscribed(eventloop: &mut EventLoop) -> anyhow::Result<()> {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if let Event::Incoming(Packet::SubAck(_)) = eventloop.poll().await? {
                return Ok::<_, rumqttc::ConnectionError>(());
            }
        }
    })
    .await
    .context("timed out waiting for subscription ACK")??;
    Ok(())
}

fn percentile(sorted: &[f64], fraction: f64) -> f64 {
    if sorted.is_empty() {
        return f64::NAN;
    }
    sorted[((sorted.len() - 1) as f64 * fraction).ceil() as usize]
}

fn data_bytes(root: &std::path::Path) -> anyhow::Result<u64> {
    Ok(["iot.db", "iot.db-wal", "iot.db-shm"]
        .into_iter()
        .filter_map(|name| fs::metadata(root.join(name)).ok())
        .map(|metadata| metadata.len())
        .sum())
}

fn current_rss_kib(pid: u32) -> anyhow::Result<u64> {
    let status = fs::read_to_string(format!("/proc/{pid}/status"))?;
    let rss = status
        .lines()
        .find_map(|line| line.strip_prefix("VmRSS:"))
        .context("VmRSS missing from process status")?
        .split_whitespace()
        .next()
        .context("VmRSS value missing")?
        .parse()?;
    Ok(rss)
}

fn cgroup_memory_peak_kib() -> Option<u64> {
    fs::read_to_string("/sys/fs/cgroup/memory.peak")
        .ok()?
        .trim()
        .parse::<u64>()
        .ok()
        .map(|bytes| bytes / 1_024)
}

fn metric_value(exposition: &str, name: &str) -> u64 {
    exposition
        .lines()
        .find_map(|line| {
            let (metric, value) = line.split_once(' ')?;
            (metric == name).then(|| value.parse().ok()).flatten()
        })
        .unwrap_or_default()
}

#[cfg(target_os = "linux")]
#[test]
fn postgres_fixture_interrupt_cleans_only_its_test_group_and_container() -> anyhow::Result<()> {
    use std::os::unix::fs::PermissionsExt;
    use std::os::unix::process::CommandExt;
    use std::thread;

    let fixture_dir = tempfile::tempdir()?;
    let bin = fixture_dir.path().join("bin");
    fs::create_dir(&bin)?;
    let docker = bin.join("docker");
    fs::write(
        &docker,
        r##"#!/bin/sh
set -eu
case "$1:$2" in
    run:*)
        shift
        while [ "$#" -gt 0 ]; do
            case "$1" in
                --label)
                    shift
                    printf '%s' "${1#*=}" > "$FIXTURE_TEST_DIR/label"
                    ;;
                -p)
                    shift
                    printf '%s' "$1" | cut -d: -f2 > "$FIXTURE_TEST_DIR/port"
                    ;;
            esac
            shift
        done
        printf '%s\n' fixture-container-id
        ;;
    exec:*) exit 0 ;;
    image:inspect) printf '%s\n' sha256:fixture-image ;;
    inspect:*)
        case "$3" in
            '{{.Id}}') printf '%s\n' fixture-container-id ;;
            '{{.Image}}') printf '%s\n' sha256:fixture-image ;;
            '{{.Config.Image}}') printf '%s\n' postgres:18.6-bookworm ;;
            '{{index .Config.Labels "org.thingsboardrust.fixture"}}') cat "$FIXTURE_TEST_DIR/label" ;;
            '{{(index (index .NetworkSettings.Ports "5432/tcp") 0).HostPort}}') cat "$FIXTURE_TEST_DIR/port" ;;
            *) exit 2 ;;
        esac
        ;;
    rm:*) printf '%s\n' "$*" >> "$FIXTURE_TEST_DIR/docker-rm.log" ;;
    version:*) printf '%s\n' 29.9.test ;;
    *) exit 2 ;;
esac
"##,
    )?;
    let fake_cargo = bin.join("cargo");
    fs::write(
        &fake_cargo,
        "#!/bin/sh\nprintf '%s\\n' \"$$\" > \"$FIXTURE_TEST_DIR/test-child.pid\"\nexec /bin/sleep 300\n",
    )?;
    for executable in [&docker, &fake_cargo] {
        fs::set_permissions(executable, fs::Permissions::from_mode(0o755))?;
    }
    let path = std::env::join_paths(std::iter::once(bin.clone()).chain(std::env::split_paths(
        &std::env::var_os("PATH").context("PATH missing")?,
    )))?;
    let script = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/load/postgres-fixture.sh");
    let mut runner_command = Command::new("sh");
    runner_command.process_group(0);
    let runner = runner_command
        .arg(script)
        .env("PATH", path)
        .env("FIXTURE_TEST_DIR", fixture_dir.path())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()?;
    let child_pid_file = fixture_dir.path().join("test-child.pid");
    let deadline = Instant::now() + Duration::from_secs(10);
    while !child_pid_file.exists() && Instant::now() < deadline {
        thread::sleep(Duration::from_millis(20));
    }
    ensure!(child_pid_file.exists(), "fake cargo did not start");
    let child_pid: u32 = fs::read_to_string(&child_pid_file)?.trim().parse()?;

    let mut outside = Command::new("/bin/sleep").arg("300").spawn()?;
    let signal = Command::new("kill")
        .args(["-TERM", "--", &format!("-{}", runner.id())])
        .status()?;
    ensure!(
        signal.success(),
        "failed to terminate fixture runner process group"
    );
    let output = runner.wait_with_output()?;
    ensure!(
        output.status.code() == Some(143),
        "runner exit status: {:?}",
        output.status.code()
    );
    ensure!(
        !linux_process_is_running(child_pid),
        "test-group child survived runner interrupt"
    );
    ensure!(
        linux_process_is_running(outside.id()),
        "fixture cleanup signalled a process outside its test group"
    );
    let cleanup = fs::read_to_string(fixture_dir.path().join("docker-rm.log"))?;
    ensure!(
        cleanup
            .lines()
            .any(|line| line == "rm -fv fixture-container-id"),
        "fixture container was not cleaned by exact ID"
    );
    outside.kill()?;
    outside.wait()?;
    Ok(())
}

#[cfg(target_os = "linux")]
fn linux_process_is_running(pid: u32) -> bool {
    fs::read_to_string(format!("/proc/{pid}/stat"))
        .ok()
        .and_then(|stat| stat.split_once(") ").map(|(_, rest)| rest.to_owned()))
        .and_then(|rest| rest.chars().next())
        .is_some_and(|state| state != 'Z')
}
