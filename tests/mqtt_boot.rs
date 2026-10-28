mod common;

use std::time::Duration;

use common::TestApp;
use reqwest::StatusCode;

#[tokio::test]
async fn mqtt_listener_accepts_connections_and_readiness_tracks_it() {
    let server = TestApp::start().await;

    tokio::net::TcpStream::connect(server.mqtt_addr)
        .await
        .unwrap();

    server.abort_mqtt();
    for _ in 0..20 {
        let response = reqwest::Client::new()
            .get(format!("{}/readyz", server.base))
            .send()
            .await
            .unwrap();
        if response.status() == StatusCode::SERVICE_UNAVAILABLE {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    panic!("readyz remained ready after the MQTT listener stopped");
}
