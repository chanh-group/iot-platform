//! Graceful HTTP drain within the configured deadline (`01` §10).

use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

/// Failures while draining the HTTP server.
#[derive(Debug, thiserror::Error)]
pub enum ShutdownError {
    #[error("shutdown deadline exceeded")]
    Deadline,
    #[error("http server panicked: {0}")]
    Panic(#[from] tokio::task::JoinError),
    #[error("http server failed: {0}")]
    Server(#[from] std::io::Error),
}

/// Flips readiness, stops accepting connections, and waits for in-flight requests.
pub async fn http(
    ready: &AtomicBool,
    cancel: &CancellationToken,
    server: JoinHandle<std::io::Result<()>>,
    timeout: Duration,
) -> Result<(), ShutdownError> {
    begin_http(ready, cancel);
    wait_http(server, timeout).await
}

/// Marks the service unavailable and signals Axum to stop accepting connections.
pub fn begin_http(ready: &AtomicBool, cancel: &CancellationToken) {
    ready.store(false, Ordering::Release);
    cancel.cancel();
}

/// Waits for requests already accepted by Axum to complete within the remaining deadline.
pub async fn wait_http(
    mut server: JoinHandle<std::io::Result<()>>,
    timeout: Duration,
) -> Result<(), ShutdownError> {
    match tokio::time::timeout(timeout, &mut server).await {
        Ok(Ok(Ok(()))) => Ok(()),
        Ok(Ok(Err(error))) => Err(ShutdownError::Server(error)),
        Ok(Err(error)) => Err(ShutdownError::Panic(error)),
        Err(_) => {
            server.abort();
            Err(ShutdownError::Deadline)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn deadline_aborts_the_server_and_flips_readiness() {
        let ready = AtomicBool::new(true);
        let cancel = CancellationToken::new();
        let server = tokio::spawn(async {
            std::future::pending::<()>().await;
            Ok(())
        });

        let error = http(&ready, &cancel, server, Duration::from_millis(10))
            .await
            .unwrap_err();

        assert!(matches!(error, ShutdownError::Deadline));
        assert!(!ready.load(Ordering::Acquire));
        assert!(cancel.is_cancelled());
    }

    #[tokio::test]
    async fn panicking_server_is_reported() {
        let ready = AtomicBool::new(true);
        let cancel = CancellationToken::new();
        let server = tokio::spawn(async { panic!("boom") });

        let error = http(&ready, &cancel, server, Duration::from_secs(1))
            .await
            .unwrap_err();

        assert!(matches!(error, ShutdownError::Panic(_)));
    }
}
