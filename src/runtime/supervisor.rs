//! Server supervisor: lock, migrate, serve HTTP, handle signals (`02` §6).

use std::net::SocketAddr;
use std::path::Path;
use std::process::{self, ExitCode};
use std::sync::Arc;
use std::time::Duration;

use tokio::net::TcpListener;
use tokio::signal::unix::{self, SignalKind};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::config::LoadedConfig;
use crate::http::{AppState, router};
use crate::observability::logging;
use crate::runtime::data_dir_lock::DataDirLock;
use crate::runtime::{EXIT_LOCK, EXIT_MIGRATION, EXIT_RUNTIME, shutdown};
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

    let listener = match TcpListener::bind(loaded.config.http.listen).await {
        Ok(listener) => listener,
        Err(error) => {
            tracing::error!(%error, "cannot bind http listener");
            pool.close().await;
            return ExitCode::FAILURE;
        }
    };
    let listen_addr = match listener.local_addr() {
        Ok(addr) => addr,
        Err(error) => {
            tracing::error!(%error, "cannot read http listener address");
            pool.close().await;
            return ExitCode::FAILURE;
        }
    };

    logging::startup(config_path, &loaded, listen_addr);

    let shutdown_timeout = Duration::from_millis(loaded.config.server.shutdown_timeout_ms);
    let state = AppState::new(pool.clone(), Arc::new(loaded));
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
            server.abort();
            state.pool.close().await;
            return ExitCode::FAILURE;
        }
    };
    let mut sigint = match unix::signal(SignalKind::interrupt()) {
        Ok(signal) => signal,
        Err(error) => {
            tracing::error!(%error, "cannot listen for SIGINT");
            server.abort();
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

    tokio::select! {
        biased;
        _ = signal_rx.recv() => {}
        result = &mut server => {
            match result {
                Ok(Ok(())) => tracing::error!("http server stopped unexpectedly"),
                Ok(Err(error)) => tracing::error!(%error, "http server failed"),
                Err(error) => tracing::error!(%error, "http server task panicked"),
            }
            state.pool.close().await;
            return ExitCode::FAILURE;
        }
    }

    let outcome = shutdown::http(&state.ready, &cancel, server, shutdown_timeout).await;
    state.pool.close().await;
    drop(lock);

    match outcome {
        Ok(()) => {
            logging::shutdown("signal");
            ExitCode::SUCCESS
        }
        Err(error) => {
            tracing::error!(%error, "shutdown failed");
            ExitCode::FAILURE
        }
    }
}
