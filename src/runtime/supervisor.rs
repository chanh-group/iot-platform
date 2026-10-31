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

use crate::config::LoadedConfig;
use crate::core::command::spawn_dispatcher;
use crate::core::events::EventBus;
use crate::core::presence::Presence;
use crate::core::telemetry::{self, TelemetryPayload};
use crate::http::{AppState, ServiceReadiness, router};
use crate::mqtt::server::MqttServer;
use crate::observability::logging;
use crate::runtime::data_dir_lock::DataDirLock;
use crate::runtime::{EXIT_LOCK, EXIT_MIGRATION, EXIT_RUNTIME, shutdown};
use crate::storage::sqlite::commands::SqliteCommandRepository;
use crate::storage::sqlite::telemetry::SqliteTelemetryRepository;
use crate::storage::sqlite::{migrate, pool};

/// Runs the server until SIGINT/SIGTERM, then drains HTTP within the deadline.
pub async fn run(config_path: &Path, loaded: LoadedConfig) -> ExitCode {
    if let Err(error) = logging::init(&loaded) {
        eprintln!("iot-server: cannot initialize logging: {error}");
        return ExitCode::FAILURE;
    }

    let lock = match DataDirLock::try_new(&loaded.data_dir) {
        Ok(lock) => lock,
        Err(error) => {
            tracing::error!(%error, "cannot acquire data dir lock");
            return ExitCode::from(EXIT_LOCK);
        }
    };

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
        telemetry_config.batch_size as usize,
        telemetry_config.max_batch_bytes as usize,
        Duration::from_millis(telemetry_config.flush_interval_ms),
        telemetry::IngestWorkerControl {
            cancellation: telemetry_cancel.clone(),
            readiness: Arc::clone(&telemetry_ready),
        },
    );

    let mqtt_ready = Arc::new(AtomicBool::new(false));
    let presence = Arc::new(Presence::new());
    let mut mqtt = match MqttServer::start_with_ingest(
        &loaded.config.mqtt,
        Arc::clone(&mqtt_ready),
        pool.clone(),
        Arc::clone(&presence),
        ingest.clone(),
        limits,
        telemetry_config.queue_capacity as usize,
    )
    .await
    {
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
            mqtt: mqtt_ready,
            telemetry: telemetry_ready,
            commands: command_ready,
        },
        events,
        presence,
        mqtt.control(),
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

    let mut sigterm = match unix::signal(SignalKind::terminate()) {
        Ok(signal) => signal,
        Err(error) => {
            tracing::error!(%error, "cannot listen for SIGTERM");
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

    match wait_for_exit(
        &mut server,
        mqtt.wait(),
        &mut ingest_task,
        &mut command_task,
        &mut signal_rx,
    )
    .await
    {
        TaskExit::Signal => {}
        TaskExit::Http(result) => {
            match result {
                Ok(Ok(())) => tracing::error!("http server stopped unexpectedly"),
                Ok(Err(error)) => tracing::error!(%error, "http server failed"),
                Err(error) => tracing::error!(%error, "http server task panicked"),
            }
            let deadline = Instant::now() + shutdown_timeout;
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
    }

    let deadline = Instant::now() + shutdown_timeout;
    command_cancel.cancel();
    let command_stopped = stop_command_worker(
        &mut command_task,
        &command_cancel,
        shutdown_remaining(deadline),
    )
    .await;
    telemetry_cancel.cancel();
    drop(ingest);
    let telemetry_stopped =
        match tokio::time::timeout(shutdown_remaining(deadline), &mut ingest_task).await {
            Ok(Ok(())) => true,
            Ok(Err(error)) => {
                tracing::error!(%error, "telemetry worker failed during shutdown");
                false
            }
            Err(_) => {
                tracing::warn!("telemetry worker did not drain before shutdown deadline");
                ingest_task.abort();
                let _ = (&mut ingest_task).await;
                false
            }
        };
    let mqtt_workers_stopped = stop_mqtt_workers(&mut mqtt, shutdown_remaining(deadline)).await;
    let outcome = shutdown::http(&state.ready, &cancel, server, shutdown_remaining(deadline)).await;
    state.pool.close().await;
    drop(lock);

    match (
        outcome,
        command_stopped,
        mqtt_workers_stopped,
        telemetry_stopped,
    ) {
        (Ok(()), true, true, true) => {
            logging::shutdown("signal");
            ExitCode::SUCCESS
        }
        (Err(error), _, _, _) => {
            tracing::error!(%error, "shutdown failed");
            ExitCode::FAILURE
        }
        (_, false, _, _) => {
            tracing::error!("command dispatcher did not stop before shutdown deadline");
            ExitCode::FAILURE
        }
        (_, _, false, _) => {
            tracing::error!("MQTT workers did not drain before shutdown deadline");
            ExitCode::FAILURE
        }
        (_, _, _, false) => {
            tracing::error!("telemetry worker did not drain before shutdown deadline");
            ExitCode::FAILURE
        }
    }
}

fn shutdown_remaining(deadline: Instant) -> Duration {
    deadline.saturating_duration_since(Instant::now())
}

async fn stop_mqtt_workers(mqtt: &mut MqttServer, timeout: Duration) -> bool {
    mqtt.stop_listener().await;
    mqtt.drain_workers(timeout).await
}

#[derive(Debug)]
enum TaskExit {
    Signal,
    Http(Result<io::Result<()>, tokio::task::JoinError>),
    Mqtt(anyhow::Result<()>),
    Telemetry(Result<(), tokio::task::JoinError>),
    Command(Result<(), tokio::task::JoinError>),
}

async fn wait_for_exit<MqttExit>(
    server: &mut tokio::task::JoinHandle<io::Result<()>>,
    mqtt: MqttExit,
    ingest_task: &mut tokio::task::JoinHandle<()>,
    command_task: &mut tokio::task::JoinHandle<()>,
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
        let (_signal_tx, mut signal_rx) = mpsc::channel(1);
        let mqtt_exit = std::future::pending::<anyhow::Result<()>>();

        let exit = wait_for_exit(
            &mut server,
            mqtt_exit,
            &mut ingest_task,
            &mut command_task,
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
    }

    #[tokio::test]
    async fn unexpected_command_dispatcher_exit_is_supervised() {
        let mut server = tokio::spawn(std::future::pending::<io::Result<()>>());
        let mqtt_exit = std::future::pending::<anyhow::Result<()>>();
        let mut ingest_task = tokio::spawn(std::future::pending::<()>());
        let mut command_task = tokio::spawn(async {});
        let (_signal_tx, mut signal_rx) = mpsc::channel(1);

        let exit = wait_for_exit(
            &mut server,
            mqtt_exit,
            &mut ingest_task,
            &mut command_task,
            &mut signal_rx,
        )
        .await;
        assert!(matches!(exit, TaskExit::Command(Ok(()))));
        server.abort();
        ingest_task.abort();
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
            let (_signal_tx, mut signal_rx) = mpsc::channel(1);
            let exit = tokio::time::timeout(
                Duration::from_secs(2),
                wait_for_exit(
                    &mut http,
                    mqtt.wait(),
                    &mut ingest,
                    &mut command,
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
        }
    }
}
