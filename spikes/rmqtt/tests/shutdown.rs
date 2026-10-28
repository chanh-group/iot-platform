mod common;

use common::*;
use rmqtt_spike::SpikeServer;
use std::net::SocketAddr;
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::time::{Instant, sleep, timeout};

#[tokio::test]
async fn shutdown() {
    let server = start().await;
    let addr = server.addr();
    let (client, eventloop, _) = connect_ok(&server, DEVICE_A, DEVICE_A, TOKEN_A, true).await;

    server.abort();
    assert_port_closed(addr).await;
    drop(client);
    drop(eventloop);

    // Rebinding the same port proves the listener was released, not just
    // that the accept loop stopped.
    let restarted = timeout(
        Duration::from_secs(5),
        SpikeServer::start_on(addr, config()),
    )
    .await
    .expect("restart timed out")
    .expect("restart on the same port failed");
    let (client2, eventloop2, _) = connect_ok(&restarted, DEVICE_A, DEVICE_A, TOKEN_A, true).await;
    restarted.abort();
    drop(client2);
    drop(eventloop2);
}

async fn assert_port_closed(addr: SocketAddr) {
    let deadline = Instant::now() + Duration::from_secs(3);
    loop {
        if TcpStream::connect(addr).await.is_err() {
            return;
        }
        if Instant::now() >= deadline {
            panic!("{addr} still accepts connections after abort");
        }
        sleep(Duration::from_millis(25)).await;
    }
}
