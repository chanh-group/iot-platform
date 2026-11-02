//! Server supervisor: lock, migrate, serve HTTP, handle signals (`02` §6).

use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::path::Path;
use std::process::{self, ExitCode};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use tokio::net::TcpListener;
use tokio::signal::unix::{self, SignalKind};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::config::{self, LoadedConfig};
use crate::core::command::spawn_dispatcher;
#[cfg(test)]
use crate::core::device_channel::DeviceChannel;
use crate::core::events::EventBus;
use crate::core::presence::Presence;
use crate::core::telemetry::{self, TelemetryPayload};
use crate::http::{AppState, ServiceReadiness, router};
use crate::mqtt::server::MqttServer;
use crate::observability::logging;
use crate::runtime::data_dir_lock::DataDirLock;
use crate::runtime::retention;
use crate::runtime::{EXIT_CONFIG, EXIT_LOCK, EXIT_MIGRATION, EXIT_RUNTIME, shutdown};
use crate::storage::sqlite::commands::SqliteCommandRepository;
use crate::storage::sqlite::telemetry::SqliteTelemetryRepository;
use crate::storage::sqlite::{migrate, pool};
#[cfg(test)]
use tokio::sync::{Notify, oneshot};

#[cfg(test)]
struct SupervisorTestControls {
    primary_kicker: Arc<dyn DeviceChannel>,
    emergency_kicker: Arc<dyn DeviceChannel>,
    observer: Option<oneshot::Sender<SupervisorTestObservation>>,
    mqtt_listener_stop_gate: Option<(Arc<Notify>, Arc<Notify>)>,
    mqtt_exit_gate: Option<(Arc<Notify>, Arc<Notify>)>,
}

#[cfg(test)]
struct SupervisorTestObservation {
    mqtt_ready: Arc<AtomicBool>,
    ready: Arc<AtomicBool>,
    device_channel: Arc<dyn DeviceChannel>,
    mqtt_addr: SocketAddr,
    http_addr: SocketAddr,
}

/// Runs the server until SIGINT/SIGTERM, then drains HTTP within the deadline.
pub async fn run(config_path: &Path, loaded: LoadedConfig) -> ExitCode {
    #[cfg(test)]
    return run_inner(config_path, loaded, None).await;
    #[cfg(not(test))]
    run_inner(config_path, loaded).await
}

async fn run_inner(
    config_path: &Path,
    loaded: LoadedConfig,
    #[cfg(test)] mut test_controls: Option<SupervisorTestControls>,
) -> ExitCode {
    #[cfg(debug_assertions)]
    pause_before_data_dir_lock_for_test().await;

    let locked_data_dir = loaded.data_dir.clone();
    let lock = match DataDirLock::try_new(&loaded.data_dir) {
        Ok(lock) => lock,
        Err(error) => {
            eprintln!("iot-server: cannot acquire data dir lock: {error}");
            return ExitCode::from(EXIT_LOCK);
        }
    };

    let loaded = match config::load(config_path) {
        Ok(loaded) if loaded.data_dir == locked_data_dir => loaded,
        Ok(_) => {
            eprintln!(
                "iot-server: server.data_dir changed while acquiring its lock; retry startup"
            );
            return ExitCode::from(EXIT_CONFIG);
        }
        Err(error) => {
            eprintln!("iot-server: cannot reload config under data dir lock: {error}");
            return ExitCode::from(EXIT_CONFIG);
        }
    };
    if let Err(error) = logging::init(&loaded) {
        eprintln!("iot-server: cannot initialize logging: {error}");
        return ExitCode::FAILURE;
    }

    let pool = match pool::connect(&loaded).await {
        Ok(pool) => pool,
        Err(error) => {
            tracing::error!(%error, "cannot open database");
            return ExitCode::from(EXIT_MIGRATION);
        }
    };

    if let Err(error) = migrate::run(&pool).await {
        tracing::error!(%error, "migration failed");
        pool.close().await;
        return ExitCode::from(EXIT_MIGRATION);
    }

    let telemetry_config = &loaded.config.telemetry;
    let events = EventBus::new();
    let telemetry_ready = Arc::new(AtomicBool::new(true));
    let telemetry_cancel = CancellationToken::new();
    let limits = match TelemetryPayload::try_from(telemetry_config) {
        Ok(limits) => limits,
        Err(error) => {
            tracing::error!(%error, "invalid telemetry limits");
            pool.close().await;
            return ExitCode::from(EXIT_MIGRATION);
        }
    };
    let (ingest, mut ingest_task) = telemetry::spawn_worker(
        Arc::new(SqliteTelemetryRepository::new(pool.clone())),
        events.clone(),
        telemetry_config.queue_capacity as usize,
        telemetry_config.max_batch_bytes as usize,
        loaded.runtime_config.clone(),
        telemetry::IngestWorkerControl {
            cancellation: telemetry_cancel.clone(),
            readiness: Arc::clone(&telemetry_ready),
        },
    );

    let mqtt_ready = Arc::new(AtomicBool::new(false));
    let presence = Arc::new(Presence::new());
    #[cfg(test)]
    let mqtt_start = if let Some(controls) = test_controls.as_ref() {
        MqttServer::start_with_ingest_and_test_kickers(
            &loaded.config.mqtt,
            Arc::clone(&mqtt_ready),
            pool.clone(),
            Arc::clone(&presence),
            (
                ingest.clone(),
                limits,
                telemetry_config.queue_capacity as usize,
            ),
            Arc::clone(&controls.primary_kicker),
            Arc::clone(&controls.emergency_kicker),
        )
        .await
    } else {
        MqttServer::start_with_ingest(
            &loaded.config.mqtt,
            Arc::clone(&mqtt_ready),
            pool.clone(),
            Arc::clone(&presence),
            ingest.clone(),
            limits,
            telemetry_config.queue_capacity as usize,
        )
        .await
    };
    #[cfg(not(test))]
    let mqtt_start = MqttServer::start_with_ingest(
        &loaded.config.mqtt,
        Arc::clone(&mqtt_ready),
        pool.clone(),
        Arc::clone(&presence),
        ingest.clone(),
        limits,
        telemetry_config.queue_capacity as usize,
    )
    .await;

    let mut mqtt = match mqtt_start {
        Ok(server) => server,
        Err(error) => {
            tracing::error!(%error, "cannot start mqtt listener");
            telemetry_cancel.cancel();
            drop(ingest);
            ingest_task.abort();
            pool.close().await;
            return ExitCode::FAILURE;
        }
    };

    #[cfg(test)]
    if let Some(gate) = test_controls
        .as_ref()
        .and_then(|controls| controls.mqtt_listener_stop_gate.clone())
    {
        mqtt.set_listener_stop_gate_for_test(gate);
    }

    let listener = match TcpListener::bind(loaded.config.http.listen).await {
        Ok(listener) => listener,
        Err(error) => {
            tracing::error!(%error, "cannot bind http listener");
            mqtt.abort();
            telemetry_cancel.cancel();
            drop(ingest);
            ingest_task.abort();
            pool.close().await;
            return ExitCode::FAILURE;
        }
    };
    let listen_addr = match listener.local_addr() {
        Ok(addr) => addr,
        Err(error) => {
            tracing::error!(%error, "cannot read http listener address");
            mqtt.abort();
            telemetry_cancel.cancel();
            drop(ingest);
            ingest_task.abort();
            pool.close().await;
            return ExitCode::FAILURE;
        }
    };

    logging::startup(config_path, &loaded, listen_addr);

    let shutdown_timeout = Duration::from_millis(loaded.config.server.shutdown_timeout_ms);
    let command_ready = Arc::new(AtomicBool::new(false));
    let command_cancel = CancellationToken::new();
    let mut command_task = spawn_dispatcher(
        Arc::new(SqliteCommandRepository::new(pool.clone())),
        mqtt.device_channel(),
        loaded.config.commands.clone(),
        mqtt.pending_command_acks(),
        mqtt.take_command_wakes(),
        command_cancel.clone(),
        Arc::clone(&command_ready),
    );
    let state = AppState::new(
        pool.clone(),
        Arc::new(loaded),
        ServiceReadiness {
            mqtt: Arc::clone(&mqtt_ready),
            telemetry: telemetry_ready,
            commands: command_ready,
        },
        events,
        presence,
        mqtt.control(),
    );
    let retention_cancel = CancellationToken::new();
    let mut retention_task = retention::spawn(
        pool.clone(),
        state.config.runtime_config.clone(),
        retention_cancel.clone(),
    );
    let app = router::build(state.clone());

    let cancel = CancellationToken::new();
    let server_cancel = cancel.clone();
    let mut server = tokio::spawn(async move {
        axum::serve(
            listener,
            app.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .with_graceful_shutdown(server_cancel.cancelled_owned())
        .await
    });

    #[cfg(test)]
    if let Some(controls) = test_controls.as_mut()
        && let Some(observer) = controls.observer.take()
    {
        let _ = observer.send(SupervisorTestObservation {
            mqtt_ready: Arc::clone(&mqtt_ready),
            ready: Arc::clone(&state.ready),
            device_channel: mqtt.device_channel(),
            mqtt_addr: mqtt.local_addr(),
            http_addr: listen_addr,
        });
    }

    let mut sigterm = match unix::signal(SignalKind::terminate()) {
        Ok(signal) => signal,
        Err(error) => {
            tracing::error!(%error, "cannot listen for SIGTERM");
            stop_retention_worker(&mut retention_task, &retention_cancel, shutdown_timeout).await;
            command_cancel.cancel();
            command_task.abort();
            let _ = command_task.await;
            server.abort();
            mqtt.abort();
            telemetry_cancel.cancel();
            drop(ingest);
            ingest_task.abort();
            state.pool.close().await;
            return ExitCode::FAILURE;
        }
    };
    let mut sigint = match unix::signal(SignalKind::interrupt()) {
        Ok(signal) => signal,
        Err(error) => {
            tracing::error!(%error, "cannot listen for SIGINT");
            stop_retention_worker(&mut retention_task, &retention_cancel, shutdown_timeout).await;
            command_cancel.cancel();
            command_task.abort();
            let _ = command_task.await;
            server.abort();
            mqtt.abort();
            telemetry_cancel.cancel();
            drop(ingest);
            ingest_task.abort();
            state.pool.close().await;
            return ExitCode::FAILURE;
        }
    };

    let (signal_tx, mut signal_rx) = mpsc::channel(1);
    tokio::spawn(async move {
        tokio::select! {
            _ = sigterm.recv() => {}
            _ = sigint.recv() => {}
        }
        let _ = signal_tx.send(()).await;
        tokio::select! {
            _ = sigterm.recv() => {}
            _ = sigint.recv() => {}
        }
        tracing::warn!("second shutdown signal, exiting now");
        process::exit(i32::from(EXIT_RUNTIME));
    });

    let task_exit = loop {
        match wait_for_exit(
            &mut server,
            mqtt.wait(),
            &mut ingest_task,
            &mut command_task,
            &mut retention_task,
            &mut signal_rx,
        )
        .await
        {
            TaskExit::Retention(result) => {
                match result {
                    Ok(()) => tracing::warn!("retention task stopped unexpectedly"),
                    Err(error) => tracing::error!(%error, "retention task failed"),
                }
                crate::observability::metrics::Metrics::global()
                    .telemetry_retention_errors_total
                    .inc();
                retention_task = retention::spawn(
                    pool.clone(),
                    state.config.runtime_config.clone(),
                    retention_cancel.clone(),
                );
            }
            exit => break exit,
        }
    };

    #[cfg(test)]
    if let TaskExit::Mqtt(_) = &task_exit
        && let Some((reached, release)) = test_controls
            .as_ref()
            .and_then(|controls| controls.mqtt_exit_gate.as_ref())
    {
        reached.notify_one();
        release.notified().await;
    }

    shutdown::begin_http(&state.ready, &cancel);
    let mqtt_fenced = mqtt.stop_accepting();
    match task_exit {
        TaskExit::Signal => {}
        TaskExit::Http(result) => {
            match result {
                Ok(Ok(())) => tracing::error!("http server stopped unexpectedly"),
                Ok(Err(error)) => tracing::error!(%error, "http server failed"),
                Err(error) => tracing::error!(%error, "http server task panicked"),
            }
            let deadline = Instant::now() + shutdown_timeout;
            stop_retention_worker(
                &mut retention_task,
                &retention_cancel,
                shutdown_remaining(deadline),
            )
            .await;
            stop_command_worker(
                &mut command_task,
                &command_cancel,
                shutdown_remaining(deadline),
            )
            .await;
            telemetry_cancel.cancel();
            drop(ingest);
            if tokio::time::timeout(shutdown_remaining(deadline), &mut ingest_task)
                .await
                .is_err()
            {
                ingest_task.abort();
            }
            let _ = stop_mqtt_workers(&mut mqtt, shutdown_remaining(deadline)).await;
            state.pool.close().await;
            return ExitCode::FAILURE;
        }
        TaskExit::Mqtt(result) => {
            match result {
                Ok(()) => tracing::error!("mqtt listener stopped unexpectedly"),
                Err(error) => tracing::error!(%error, "mqtt listener failed"),
            }
            let deadline = Instant::now() + shutdown_timeout;
            stop_retention_worker(
                &mut retention_task,
                &retention_cancel,
                shutdown_remaining(deadline),
            )
            .await;
            stop_command_worker(
                &mut command_task,
                &command_cancel,
                shutdown_remaining(deadline),
            )
            .await;
            telemetry_cancel.cancel();
            drop(ingest);
            if tokio::time::timeout(shutdown_remaining(deadline), &mut ingest_task)
                .await
                .is_err()
            {
                ingest_task.abort();
            }
            let _ = stop_mqtt_workers(&mut mqtt, shutdown_remaining(deadline)).await;
            let outcome =
                shutdown::http(&state.ready, &cancel, server, shutdown_remaining(deadline)).await;
            state.pool.close().await;
            if let Err(error) = outcome {
                tracing::error!(%error, "http shutdown after mqtt failure failed");
            }
            return ExitCode::FAILURE;
        }
        TaskExit::Telemetry(result) => {
            let deadline = Instant::now() + shutdown_timeout;
            command_cancel.cancel();
            stop_retention_worker(
                &mut retention_task,
                &retention_cancel,
                shutdown_remaining(deadline),
            )
            .await;
            telemetry_cancel.cancel();
            let exit = shutdown_after_telemetry_worker_exit(
                &state.telemetry_ready,
                &result,
                || mqtt.abort(),
                &state.ready,
                &cancel,
                server,
                shutdown_remaining(deadline),
            )
            .await;
            stop_command_worker(
                &mut command_task,
                &command_cancel,
                shutdown_remaining(deadline),
            )
            .await;
            drop(ingest);
            state.pool.close().await;
            drop(lock);
            return exit;
        }
        TaskExit::Command(result) => {
            match result {
                Ok(()) => tracing::error!("command dispatcher stopped unexpectedly"),
                Err(error) => tracing::error!(%error, "command dispatcher task panicked"),
            }
            let deadline = Instant::now() + shutdown_timeout;
            command_cancel.cancel();
            stop_retention_worker(
                &mut retention_task,
                &retention_cancel,
                shutdown_remaining(deadline),
            )
            .await;
            telemetry_cancel.cancel();
            drop(ingest);
            if tokio::time::timeout(shutdown_remaining(deadline), &mut ingest_task)
                .await
                .is_err()
            {
                ingest_task.abort();
            }
            let _ = stop_mqtt_workers(&mut mqtt, shutdown_remaining(deadline)).await;
            let _ =
                shutdown::http(&state.ready, &cancel, server, shutdown_remaining(deadline)).await;
            state.pool.close().await;
            drop(lock);
            return ExitCode::FAILURE;
        }
        TaskExit::Retention(_) => {
            tracing::error!("retention task escaped its supervisor restart loop");
            return ExitCode::FAILURE;
        }
    }

    let deadline = Instant::now() + shutdown_timeout;
    command_cancel.cancel();
    let retention_stopped = stop_retention_worker(
        &mut retention_task,
        &retention_cancel,
        shutdown_remaining(deadline),
    )
    .await;
    let command_stopped = stop_command_worker(
        &mut command_task,
        &command_cancel,
        shutdown_remaining(deadline),
    )
    .await;
    telemetry_cancel.cancel();
    let telemetry_stopped = match tokio::time::timeout(
        shutdown_remaining(deadline),
        &mut ingest_task,
    )
    .await
    {
        Ok(Ok(())) => true,
        Ok(Err(error)) => {
            tracing::error!(%error, items_remaining = ingest.pending_count(), "telemetry worker failed during shutdown");
            false
        }
        Err(_) => {
            tracing::warn!(
                items_remaining = ingest.pending_count(),
                "telemetry worker did not drain before shutdown deadline"
            );
            ingest_task.abort();
            let _ = (&mut ingest_task).await;
            false
        }
    };
    drop(ingest);
    let mqtt_workers_stopped = stop_mqtt_workers(&mut mqtt, shutdown_remaining(deadline)).await;
    let outcome = shutdown::wait_http(server, shutdown_remaining(deadline)).await;
    state.pool.close().await;
    drop(lock);

    match (
        outcome,
        command_stopped,
        retention_stopped,
        mqtt_workers_stopped && mqtt_fenced,
        telemetry_stopped,
    ) {
        (Ok(()), true, true, true, true) => {
            logging::shutdown("signal");
            ExitCode::SUCCESS
        }
        (Err(error), _, _, _, _) => {
            tracing::error!(%error, "shutdown failed");
            ExitCode::FAILURE
        }
        (_, _, false, _, _) => {
            tracing::error!("retention worker did not stop before shutdown deadline");
            ExitCode::FAILURE
        }
        (_, false, _, _, _) => {
            tracing::error!("command dispatcher did not stop before shutdown deadline");
            ExitCode::FAILURE
        }
        (_, _, _, false, _) => {
            tracing::error!("MQTT workers did not drain before shutdown deadline");
            ExitCode::FAILURE
        }
        (_, _, _, _, false) => {
            tracing::error!("telemetry worker did not drain before shutdown deadline");
            ExitCode::FAILURE
        }
    }
}

#[cfg(debug_assertions)]
async fn pause_before_data_dir_lock_for_test() {
    let Some(marker) = std::env::var_os("IOT_TEST_PAUSE_BEFORE_DATA_DIR_LOCK") else {
        return;
    };
    let marker = std::path::PathBuf::from(marker);
    if let Err(error) = std::fs::write(&marker, b"paused") {
        eprintln!("iot-server: cannot create test startup marker: {error}");
        return;
    }
    while marker.exists() {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

fn shutdown_remaining(deadline: Instant) -> Duration {
    deadline.saturating_duration_since(Instant::now())
}

async fn stop_mqtt_workers(mqtt: &mut MqttServer, timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    let telemetry_acks_drained = mqtt
        .drain_telemetry_ack_worker(shutdown_remaining(deadline))
        .await;
    let sessions_kicked = mqtt
        .kick_connected_sessions(shutdown_remaining(deadline))
        .await;
    tokio::time::sleep(shutdown_remaining(deadline).min(Duration::from_millis(50))).await;
    mqtt.stop_listener().await;
    let workers_drained = mqtt.drain_workers(shutdown_remaining(deadline)).await;
    telemetry_acks_drained && sessions_kicked && workers_drained
}

#[derive(Debug)]
enum TaskExit {
    Signal,
    Http(Result<io::Result<()>, tokio::task::JoinError>),
    Mqtt(anyhow::Result<()>),
    Telemetry(Result<(), tokio::task::JoinError>),
    Command(Result<(), tokio::task::JoinError>),
    Retention(Result<(), tokio::task::JoinError>),
}

async fn wait_for_exit<MqttExit>(
    server: &mut tokio::task::JoinHandle<io::Result<()>>,
    mqtt: MqttExit,
    ingest_task: &mut tokio::task::JoinHandle<()>,
    command_task: &mut tokio::task::JoinHandle<()>,
    retention_task: &mut tokio::task::JoinHandle<()>,
    signal_rx: &mut mpsc::Receiver<()>,
) -> TaskExit
where
    MqttExit: Future<Output = anyhow::Result<()>>,
{
    tokio::select! {
        biased;
        _ = signal_rx.recv() => TaskExit::Signal,
        result = &mut *server => TaskExit::Http(result),
        result = mqtt => TaskExit::Mqtt(result),
        result = &mut *ingest_task => TaskExit::Telemetry(result),
        result = &mut *command_task => TaskExit::Command(result),
        result = &mut *retention_task => TaskExit::Retention(result),
    }
}

async fn stop_retention_worker(
    task: &mut tokio::task::JoinHandle<()>,
    cancel: &CancellationToken,
    timeout: Duration,
) -> bool {
    cancel.cancel();
    match tokio::time::timeout(timeout, &mut *task).await {
        Ok(Ok(())) => true,
        Ok(Err(error)) => {
            tracing::error!(%error, "retention task failed during shutdown");
            false
        }
        Err(_) => {
            task.abort();
            let _ = task.await;
            false
        }
    }
}

async fn stop_command_worker(
    task: &mut tokio::task::JoinHandle<()>,
    cancel: &CancellationToken,
    timeout: Duration,
) -> bool {
    cancel.cancel();
    match tokio::time::timeout(timeout, &mut *task).await {
        Ok(Ok(())) => true,
        Ok(Err(error)) => {
            tracing::error!(%error, "command dispatcher task failed during shutdown");
            false
        }
        Err(_) => {
            task.abort();
            let _ = task.await;
            false
        }
    }
}

async fn shutdown_after_telemetry_worker_exit(
    readiness: &AtomicBool,
    result: &Result<(), tokio::task::JoinError>,
    stop_mqtt: impl FnOnce(),
    http_readiness: &AtomicBool,
    cancel: &CancellationToken,
    server: tokio::task::JoinHandle<io::Result<()>>,
    timeout: Duration,
) -> ExitCode {
    readiness.store(false, Ordering::Release);
    if result.is_err() {
        tracing::error!("telemetry worker task failed");
    } else {
        tracing::error!("telemetry worker stopped unexpectedly");
    }
    stop_mqtt();
    if let Err(error) = shutdown::http(http_readiness, cancel, server, timeout).await {
        tracing::error!(%error, "http shutdown after telemetry worker failure failed");
    }
    ExitCode::FAILURE
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::process::Command;

    use axum::http::StatusCode;
    use reqwest::header::{COOKIE, SET_COOKIE};
    use rumqttc::{AsyncClient, ConnectReturnCode, Event, Packet, QoS};
    use serde_json::{Value, json};

    #[tokio::test]
    async fn unexpected_telemetry_worker_exit_stops_services_and_fails() {
        let telemetry_readiness = AtomicBool::new(true);
        let http_readiness = AtomicBool::new(true);
        let cancellation = CancellationToken::new();
        let http_stopped = Arc::new(AtomicBool::new(false));
        let http_stopped_task = Arc::clone(&http_stopped);
        let http_cancel = cancellation.clone();
        let mut server = tokio::spawn(async move {
            http_cancel.cancelled().await;
            http_stopped_task.store(true, Ordering::Release);
            Ok(())
        });
        let mut ingest_task = tokio::spawn(async {});
        let mut command_task = tokio::spawn(std::future::pending::<()>());
        let mut retention_task = tokio::spawn(std::future::pending::<()>());
        let (_signal_tx, mut signal_rx) = mpsc::channel(1);
        let mqtt_exit = std::future::pending::<anyhow::Result<()>>();

        let exit = wait_for_exit(
            &mut server,
            mqtt_exit,
            &mut ingest_task,
            &mut command_task,
            &mut retention_task,
            &mut signal_rx,
        )
        .await;
        let TaskExit::Telemetry(result) = exit else {
            panic!("expected telemetry worker exit, got {exit:?}");
        };
        let mqtt_stopped = AtomicBool::new(false);
        let code = shutdown_after_telemetry_worker_exit(
            &telemetry_readiness,
            &result,
            || mqtt_stopped.store(true, Ordering::Release),
            &http_readiness,
            &cancellation,
            server,
            Duration::from_secs(1),
        )
        .await;

        assert_eq!(code, ExitCode::FAILURE);
        assert!(!telemetry_readiness.load(Ordering::Acquire));
        assert!(mqtt_stopped.load(Ordering::Acquire));
        assert!(http_stopped.load(Ordering::Acquire));
        assert!(!http_readiness.load(Ordering::Acquire));
        retention_task.abort();
    }

    #[tokio::test]
    async fn unexpected_command_dispatcher_exit_is_supervised() {
        let mut server = tokio::spawn(std::future::pending::<io::Result<()>>());
        let mqtt_exit = std::future::pending::<anyhow::Result<()>>();
        let mut ingest_task = tokio::spawn(std::future::pending::<()>());
        let mut command_task = tokio::spawn(async {});
        let mut retention_task = tokio::spawn(std::future::pending::<()>());
        let (_signal_tx, mut signal_rx) = mpsc::channel(1);

        let exit = wait_for_exit(
            &mut server,
            mqtt_exit,
            &mut ingest_task,
            &mut command_task,
            &mut retention_task,
            &mut signal_rx,
        )
        .await;
        assert!(matches!(exit, TaskExit::Command(Ok(()))));
        server.abort();
        ingest_task.abort();
        retention_task.abort();
    }

    #[tokio::test]
    async fn mqtt_listener_and_ack_failures_follow_supervisor_cleanup_once() {
        for ack_failure in [false, true] {
            let config = crate::config::schema::MqttConfig {
                listen: "127.0.0.1:0".parse().unwrap(),
                ..crate::config::schema::MqttConfig::default()
            };
            let mqtt_ready = Arc::new(AtomicBool::new(false));
            let mut mqtt = MqttServer::start(
                &config,
                Arc::clone(&mqtt_ready),
                sqlx::SqlitePool::connect_lazy("sqlite::memory:").unwrap(),
                Arc::new(Presence::new()),
            )
            .await
            .unwrap();
            if ack_failure {
                mqtt.abort_command_ack_worker_for_test();
            } else {
                mqtt.control()
                    .failure
                    .report_failure("injected listener failure");
            }

            let mut http = tokio::spawn(std::future::pending::<io::Result<()>>());
            let mut ingest = tokio::spawn(std::future::pending::<()>());
            let mut command = tokio::spawn(std::future::pending::<()>());
            let mut retention_task = tokio::spawn(std::future::pending::<()>());
            let (_signal_tx, mut signal_rx) = mpsc::channel(1);
            let exit = tokio::time::timeout(
                Duration::from_secs(2),
                wait_for_exit(
                    &mut http,
                    mqtt.wait(),
                    &mut ingest,
                    &mut command,
                    &mut retention_task,
                    &mut signal_rx,
                ),
            )
            .await
            .expect("supervisor should observe MQTT worker failure");
            assert!(matches!(exit, TaskExit::Mqtt(Err(_))));
            assert!(
                !mqtt_ready.load(Ordering::Acquire),
                "MQTT failure must lower readiness"
            );
            let _ = stop_mqtt_workers(&mut mqtt, Duration::from_secs(1)).await;
            http.abort();
            ingest.abort();
            command.abort();
            retention_task.abort();
        }
    }

    struct GatedFailingKick {
        started: Arc<Notify>,
        release: Arc<Notify>,
        first: AtomicBool,
        calls: Arc<std::sync::atomic::AtomicUsize>,
    }

    #[async_trait::async_trait]
    impl DeviceChannel for GatedFailingKick {
        async fn publish(
            &self,
            _device_id: &str,
            _topic: &str,
            _payload: &[u8],
        ) -> Result<(), crate::core::device_channel::DeviceChannelError> {
            Err(crate::core::device_channel::DeviceChannelError::Unavailable)
        }

        async fn kick(
            &self,
            _device_id: &str,
        ) -> Result<(), crate::core::device_channel::DeviceChannelError> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            if self.first.swap(false, Ordering::Relaxed) {
                self.started.notify_one();
                self.release.notified().await;
            }
            Err(crate::core::device_channel::DeviceChannelError::Unavailable)
        }

        async fn is_online(
            &self,
            _device_id: &str,
        ) -> Result<bool, crate::core::device_channel::DeviceChannelError> {
            Ok(true)
        }
    }

    struct FailingKick(Arc<std::sync::atomic::AtomicUsize>);

    #[async_trait::async_trait]
    impl DeviceChannel for FailingKick {
        async fn publish(
            &self,
            _device_id: &str,
            _topic: &str,
            _payload: &[u8],
        ) -> Result<(), crate::core::device_channel::DeviceChannelError> {
            Err(crate::core::device_channel::DeviceChannelError::Unavailable)
        }

        async fn kick(
            &self,
            _device_id: &str,
        ) -> Result<(), crate::core::device_channel::DeviceChannelError> {
            self.0.fetch_add(1, Ordering::Relaxed);
            Err(crate::core::device_channel::DeviceChannelError::Unavailable)
        }

        async fn is_online(
            &self,
            _device_id: &str,
        ) -> Result<bool, crate::core::device_channel::DeviceChannelError> {
            Ok(true)
        }
    }

    fn a20_test_config(directory: &Path) -> std::path::PathBuf {
        fn free_port() -> u16 {
            std::net::TcpListener::bind("127.0.0.1:0")
                .unwrap()
                .local_addr()
                .unwrap()
                .port()
        }

        let http_port = free_port();
        let mut mqtt_port = free_port();
        while mqtt_port == http_port {
            mqtt_port = free_port();
        }
        let config_path = directory.join("a20-conf.toml");
        std::fs::write(
            &config_path,
            format!(
                "config_version = 1\n\
                 [server]\n\
                 data_dir = \"./data\"\n\
                 shutdown_timeout_ms = 5000\n\
                 [http]\n\
                 listen = \"127.0.0.1:{http_port}\"\n\
                 dashboard = false\n\
                 [mqtt]\n\
                 listen = \"127.0.0.1:{mqtt_port}\"\n\
                 [database]\n\
                 url = \"sqlite://./data/iot.db\"\n\
                 [runtime]\n\
                 settings_path = \"./data/runtime.toml\"\n"
            ),
        )
        .unwrap();
        config_path
    }

    #[tokio::test]
    async fn a20_fail_closed_shutdown_is_verified_in_a_child_process() {
        const CONFIG_ENV: &str = "IOT_A20_CHILD_CONFIG";
        const MARKER_ENV: &str = "IOT_A20_CHILD_MARKER";

        if let (Some(config_path), Some(marker_path)) =
            (std::env::var_os(CONFIG_ENV), std::env::var_os(MARKER_ENV))
        {
            let exit = a20_child_scenario(
                std::path::Path::new(&config_path),
                std::path::Path::new(&marker_path),
            )
            .await;
            assert_eq!(exit, ExitCode::FAILURE);
            return;
        }

        let directory = tempfile::tempdir().unwrap();
        let config_path = a20_test_config(directory.path());
        let marker_path = directory.path().join("a20-verified");
        let output = Command::new(std::env::current_exe().unwrap())
            .args([
                "runtime::supervisor::tests::a20_fail_closed_shutdown_is_verified_in_a_child_process",
                "--exact",
                "--nocapture",
            ])
            .env(CONFIG_ENV, &config_path)
            .env(MARKER_ENV, &marker_path)
            .output()
            .unwrap();

        assert_eq!(
            output.status.code(),
            Some(0),
            "child test harness should pass after observing run_inner's failure code; stdout: {}; stderr: {}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        let child_stdout = String::from_utf8_lossy(&output.stdout);
        assert!(
            child_stdout.contains("running 1 test") && child_stdout.contains("1 passed"),
            "child should run exactly the requested test; stdout: {child_stdout}"
        );
        assert_eq!(
            std::fs::read_to_string(marker_path).unwrap(),
            "A20 evidence: committed disable, post-kick-failure ingress/egress denied while MQTT listener remains open, listeners closed, run_inner ExitCode::FAILURE observed; production entrypoint propagation unverified\n"
        );
    }

    async fn a20_child_scenario(config_path: &Path, marker_path: &Path) -> ExitCode {
        const PASSWORD: &str = "a20-process-test-password";
        const EVENT_ID: &str = "a20-process-fenced-event";

        let loaded = config::load(config_path).unwrap();
        std::fs::create_dir_all(&loaded.data_dir).unwrap();
        let setup_pool = pool::connect(&loaded).await.unwrap();
        migrate::run(&setup_pool).await.unwrap();
        let password_hash = crate::core::auth::hash_password(PASSWORD).unwrap();
        let mut transaction = setup_pool.begin().await.unwrap();
        crate::storage::sqlite::users::create_admin(
            &mut transaction,
            "a20-process-admin",
            &password_hash,
            crate::core::auth::now_ms(),
        )
        .await
        .unwrap();
        transaction.commit().await.unwrap();
        setup_pool.close().await;

        let primary_started = Arc::new(Notify::new());
        let primary_release = Arc::new(Notify::new());
        let primary_calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let emergency_calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let (observer_sender, observer_receiver) = oneshot::channel();
        let mqtt_listener_stop_reached = Arc::new(Notify::new());
        let release_mqtt_listener_stop = Arc::new(Notify::new());
        let mqtt_exit_reached = Arc::new(Notify::new());
        let release_supervisor = Arc::new(Notify::new());
        let controls = SupervisorTestControls {
            primary_kicker: Arc::new(GatedFailingKick {
                started: Arc::clone(&primary_started),
                release: Arc::clone(&primary_release),
                first: AtomicBool::new(true),
                calls: Arc::clone(&primary_calls),
            }),
            emergency_kicker: Arc::new(FailingKick(Arc::clone(&emergency_calls))),
            observer: Some(observer_sender),
            mqtt_listener_stop_gate: Some((
                Arc::clone(&mqtt_listener_stop_reached),
                Arc::clone(&release_mqtt_listener_stop),
            )),
            mqtt_exit_gate: Some((
                Arc::clone(&mqtt_exit_reached),
                Arc::clone(&release_supervisor),
            )),
        };
        let run_config_path = config_path.to_path_buf();
        let supervisor =
            tokio::spawn(async move { run_inner(&run_config_path, loaded, Some(controls)).await });
        let observation = tokio::time::timeout(Duration::from_secs(5), observer_receiver)
            .await
            .expect("supervisor should start both listeners")
            .unwrap();
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(5))
            .build()
            .unwrap();
        let base = format!("http://{}", observation.http_addr);
        let mut ready = false;
        for _ in 0..200 {
            if let Ok(response) = http.get(format!("{base}/readyz")).send().await
                && response.status() == StatusCode::OK
            {
                ready = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        assert!(ready, "child server should become ready before disable");

        let login = http
            .post(format!("{base}/api/v1/auth/login"))
            .header("X-IoT-CSRF", "1")
            .json(&json!({"username":"a20-process-admin", "password":PASSWORD}))
            .send()
            .await
            .unwrap();
        assert_eq!(login.status(), StatusCode::OK);
        let cookie = login
            .headers()
            .get(SET_COOKIE)
            .unwrap()
            .to_str()
            .unwrap()
            .split(';')
            .next()
            .unwrap()
            .to_owned();
        let created = http
            .post(format!("{base}/api/v1/devices"))
            .header(COOKIE, &cookie)
            .header("X-IoT-CSRF", "1")
            .json(&json!({"name":"a20-process-device"}))
            .send()
            .await
            .unwrap();
        assert_eq!(created.status(), StatusCode::CREATED);
        let created: Value = created.json().await.unwrap();
        let device_id = created["device"]["id"].as_str().unwrap().to_owned();
        let device_token = created["credential"]["token"].as_str().unwrap().to_owned();

        let mut options = rumqttc::MqttOptions::new(
            &device_id,
            observation.mqtt_addr.ip().to_string(),
            observation.mqtt_addr.port(),
        );
        options.set_credentials(&device_id, &device_token);
        options.set_keep_alive(Duration::from_secs(60));
        let (mqtt_client, mut eventloop) = AsyncClient::new(options, 10);
        let connack = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if let Event::Incoming(Packet::ConnAck(connack)) = eventloop.poll().await.unwrap() {
                    break connack.code;
                }
            }
        })
        .await
        .expect("device should authenticate before its disable");
        assert_eq!(connack, ConnectReturnCode::Success);

        let disable_http = http.clone();
        let disable_url = format!("{base}/api/v1/devices/{device_id}");
        let disable_cookie = cookie.clone();
        let disable = tokio::spawn(async move {
            disable_http
                .patch(disable_url)
                .header(COOKIE, disable_cookie)
                .header("X-IoT-CSRF", "1")
                .json(&json!({"enabled":false}))
                .send()
                .await
        });
        tokio::time::timeout(Duration::from_secs(5), primary_started.notified())
            .await
            .expect("disable should commit and reach the primary kick");

        let query_pool = pool::connect(&config::load(config_path).unwrap())
            .await
            .unwrap();
        let enabled: bool = sqlx::query_scalar("SELECT enabled FROM devices WHERE id = ?")
            .bind(&device_id)
            .fetch_one(&query_pool)
            .await
            .unwrap();
        assert!(!enabled, "the database disable must commit before a kick");
        assert_eq!(
            http.get(format!("{base}/readyz"))
                .send()
                .await
                .unwrap()
                .status(),
            StatusCode::OK,
            "the test is still before the both-kicks-failed transition"
        );
        tokio::net::TcpStream::connect(observation.mqtt_addr)
            .await
            .expect("listener should still accept TCP while the primary kick is held");

        primary_release.notify_one();
        let response = tokio::time::timeout(Duration::from_secs(5), disable)
            .await
            .expect("disable request should finish after both retry paths fail")
            .unwrap()
            .unwrap();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(
            response.json::<Value>().await.unwrap()["code"],
            "device_kick_failed"
        );
        assert_eq!(primary_calls.load(Ordering::Relaxed), 3);
        assert_eq!(emergency_calls.load(Ordering::Relaxed), 3);

        tokio::time::timeout(
            Duration::from_secs(5),
            mqtt_listener_stop_reached.notified(),
        )
        .await
        .expect("failed kick paths should fence traffic before listener shutdown");
        assert!(!observation.mqtt_ready.load(Ordering::Acquire));
        assert_eq!(
            http.get(format!("{base}/readyz"))
                .send()
                .await
                .unwrap()
                .status(),
            StatusCode::SERVICE_UNAVAILABLE
        );
        tokio::net::TcpStream::connect(observation.mqtt_addr)
            .await
            .expect("listener must remain open while post-failure traffic is checked");
        assert!(
            observation
                .device_channel
                .publish(
                    &device_id,
                    &format!("devices/{device_id}/commands"),
                    b"post-failure-server-publish",
                )
                .await
                .is_err(),
            "new server-originated application traffic must be denied after both kick paths fail"
        );
        mqtt_client
            .publish(
                format!("devices/{device_id}/telemetry"),
                QoS::AtLeastOnce,
                false,
                json!({
                    "event_id": EVENT_ID,
                    "ts": crate::core::auth::now_ms(),
                    "values": {"temperature": 24.0}
                })
                .to_string(),
            )
            .await
            .unwrap();
        let connection_closed = tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                match eventloop.poll().await {
                    Ok(Event::Incoming(Packet::PubAck(_))) => {
                        panic!("post-failure telemetry unexpectedly received PUBACK")
                    }
                    Ok(_) => {}
                    Err(_) => break,
                }
            }
        })
        .await
        .is_ok();
        assert!(
            connection_closed,
            "post-failure device publish must be denied on its still-open session"
        );
        let stored: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM telemetry_events WHERE device_id = ? AND event_id = ?",
        )
        .bind(&device_id)
        .bind(EVENT_ID)
        .fetch_one(&query_pool)
        .await
        .unwrap();
        assert_eq!(stored, 0, "post-failure telemetry must not reach SQL");
        query_pool.close().await;

        release_mqtt_listener_stop.notify_one();
        tokio::time::timeout(Duration::from_secs(5), mqtt_exit_reached.notified())
            .await
            .expect("listener shutdown should reach the supervisor's cleanup gate");
        assert!(!observation.mqtt_ready.load(Ordering::Acquire));
        let readiness = http.get(format!("{base}/readyz")).send().await.unwrap();
        assert_eq!(readiness.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(
            readiness.json::<Value>().await.unwrap()["checks"]["mqtt"],
            "error"
        );
        assert!(
            tokio::net::TcpStream::connect(observation.mqtt_addr)
                .await
                .is_err(),
            "MQTT listener must close before supervisor shutdown completes"
        );

        release_supervisor.notify_one();
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(10), supervisor)
                .await
                .expect("supervisor cleanup should finish")
                .unwrap(),
            ExitCode::FAILURE
        );
        assert!(!observation.ready.load(Ordering::Acquire));
        assert!(
            tokio::net::TcpStream::connect(observation.http_addr)
                .await
                .is_err()
        );

        let loaded = config::load(config_path).unwrap();
        let lock =
            DataDirLock::try_new(&loaded.data_dir).expect("supervisor should release its lock");
        let pool = pool::connect(&loaded).await.unwrap();
        let enabled: bool = sqlx::query_scalar("SELECT enabled FROM devices WHERE id = ?")
            .bind(&device_id)
            .fetch_one(&pool)
            .await
            .unwrap();
        assert!(
            !enabled,
            "disabled state must persist after process shutdown"
        );
        let stored: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM telemetry_events WHERE device_id = ? AND event_id = ?",
        )
        .bind(&device_id)
        .bind(EVENT_ID)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(stored, 0);
        pool.close().await;
        drop(lock);
        std::fs::write(
            marker_path,
            "A20 evidence: committed disable, post-kick-failure ingress/egress denied while MQTT listener remains open, listeners closed, run_inner ExitCode::FAILURE observed; production entrypoint propagation unverified\n",
        )
        .unwrap();
        ExitCode::FAILURE
    }
}
