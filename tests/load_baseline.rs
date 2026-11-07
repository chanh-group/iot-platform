mod common;

use std::collections::HashMap;
use std::fs;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::{Context, ensure};
use common::TestApp;
use rumqttc::{AsyncClient, Event, EventLoop, MqttOptions, Packet, QoS};
use serde_json::Value;
use tokio::sync::{mpsc, watch};

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
#[ignore = "manual load baseline; run with cargo test --test load_baseline -- --ignored --nocapture"]
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
    let peak_rss_kib = Arc::new(Mutex::new(current_rss_kib()?));
    let sampling = {
        let peak_rss_kib = Arc::clone(&peak_rss_kib);
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_millis(250));
            loop {
                interval.tick().await;
                if let Ok(current) = current_rss_kib()
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

fn current_rss_kib() -> anyhow::Result<u64> {
    let status = fs::read_to_string("/proc/self/status")?;
    let rss = status
        .lines()
        .find_map(|line| line.strip_prefix("VmRSS:"))
        .context("VmRSS missing from /proc/self/status")?
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
