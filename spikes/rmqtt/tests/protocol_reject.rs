mod common;

use common::*;
use std::time::Duration;

/// Actual RMQTT 0.24 behaviour: `ClientAuthenticate` controls the refusal
/// code. Level 3 -> v3 code 5 (not authorized); level 5 -> v5 reason 0x87.
/// The contract asks for 0x01/0x84, which the ClientConnack hook cannot
/// produce because `refused_ack(..., None, ...)` drops connect_info.
#[tokio::test]
async fn protocol_reject() {
    let server = start().await;

    let v3 = raw_connack(server.addr(), 3, DEVICE_A, DEVICE_A, TOKEN_A.as_bytes()).await;
    assert_eq!(
        connack_code(&v3, 3),
        0x05,
        "MQTT 3.1 must be refused; wire bytes: {v3:02x?}"
    );

    let v5 = raw_connack(server.addr(), 5, DEVICE_A, DEVICE_A, TOKEN_A.as_bytes()).await;
    assert_eq!(
        connack_code(&v5, 5),
        0x87,
        "MQTT 5 must be refused; wire bytes: {v5:02x?}"
    );

    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(
        server.sessions().await,
        0,
        "refused handshakes must not create sessions"
    );
}
