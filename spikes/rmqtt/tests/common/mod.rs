#![allow(dead_code)]

use std::collections::HashMap;
use std::net::SocketAddr;
use std::time::{Duration, Instant};

use rmqtt_spike::{Event, SpikeConfig, SpikeServer};
use rumqttc::{
    AsyncClient, ConnectReturnCode, ConnectionError, Event as MqttEvent, EventLoop, MqttOptions,
    Packet, Publish, QoS, SubscribeReasonCode,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::time::timeout;

pub const DEVICE_A: &str = "019c0e1e-0000-7000-8000-00000000000a";
pub const DEVICE_B: &str = "019c0e1e-0000-7000-8000-00000000000b";
pub const TOKEN_A: &str = "iot_spike_token_for_device_a_000000000000";
pub const TOKEN_B: &str = "iot_spike_token_for_device_b_000000000000";

pub fn config() -> SpikeConfig {
    SpikeConfig {
        devices: HashMap::from([
            (DEVICE_A.to_owned(), TOKEN_A.to_owned()),
            (DEVICE_B.to_owned(), TOKEN_B.to_owned()),
        ]),
        ..SpikeConfig::default()
    }
}

pub async fn start() -> SpikeServer {
    SpikeServer::start(config()).await.unwrap()
}

pub async fn start_with(config: SpikeConfig) -> SpikeServer {
    SpikeServer::start(config).await.unwrap()
}

pub async fn connect_ok(
    server: &SpikeServer,
    client_id: &str,
    username: &str,
    token: &str,
    clean_session: bool,
) -> (AsyncClient, EventLoop, bool) {
    let options = options_for(server.addr(), client_id, username, token, clean_session);
    let (client, mut eventloop) = AsyncClient::new(options, 32);
    let session_present = wait_connack(&mut eventloop).await;
    (client, eventloop, session_present)
}

pub async fn wait_connack(eventloop: &mut EventLoop) -> bool {
    match next_event(eventloop, Duration::from_secs(5)).await {
        Ok(MqttEvent::Incoming(Packet::ConnAck(ack))) => {
            assert_eq!(ack.code, ConnectReturnCode::Success);
            ack.session_present
        }
        Ok(other) => panic!("expected CONNACK, got {other:?}"),
        Err(error) => panic!("connect failed: {error:?}"),
    }
}

pub fn options_for(
    addr: SocketAddr,
    client_id: &str,
    username: &str,
    token: &str,
    clean_session: bool,
) -> MqttOptions {
    let mut options = MqttOptions::new(client_id, addr.ip().to_string(), addr.port());
    options
        .set_keep_alive(Duration::from_secs(30))
        .set_clean_session(clean_session)
        .set_credentials(username, token);
    options
}

pub async fn connect_refused(
    server: &SpikeServer,
    client_id: &str,
    username: &str,
    token: &str,
) -> ConnectReturnCode {
    let options = options_for(server.addr(), client_id, username, token, true);
    let (client, mut eventloop) = AsyncClient::new(options, 8);
    let result = next_event(&mut eventloop, Duration::from_secs(5)).await;
    drop(client);
    match result {
        Err(error) => match *error {
            ConnectionError::ConnectionRefused(code) => code,
            other => panic!("expected refused connect, got {other:?}"),
        },
        Ok(other) => panic!("expected refused connect, got {other:?}"),
    }
}

pub async fn next_event(
    eventloop: &mut EventLoop,
    dur: Duration,
) -> Result<MqttEvent, Box<ConnectionError>> {
    match timeout(dur, eventloop.poll()).await {
        Ok(result) => result.map_err(Box::new),
        Err(_) => panic!("timeout waiting for MQTT event"),
    }
}

pub async fn subscribe(
    client: &AsyncClient,
    eventloop: &mut EventLoop,
    filter: &str,
) -> Vec<SubscribeReasonCode> {
    subscribe_with_qos(client, eventloop, filter, QoS::AtLeastOnce).await
}

pub async fn subscribe_with_qos(
    client: &AsyncClient,
    eventloop: &mut EventLoop,
    filter: &str,
    qos: QoS,
) -> Vec<SubscribeReasonCode> {
    client.subscribe(filter, qos).await.unwrap();
    loop {
        match next_event(eventloop, Duration::from_secs(5)).await {
            Ok(MqttEvent::Incoming(Packet::SubAck(ack))) => return ack.return_codes,
            Ok(_) => {}
            Err(error) => panic!("connection lost waiting SUBACK: {error:?}"),
        }
    }
}

pub async fn publish_qos1(
    client: &AsyncClient,
    eventloop: &mut EventLoop,
    topic: &str,
    retain: bool,
    payload: &[u8],
) {
    client
        .publish(topic, QoS::AtLeastOnce, retain, payload)
        .await
        .unwrap();
    match wait_puback(eventloop, Duration::from_secs(5)).await {
        Ok(()) => {}
        Err(error) => panic!("connection lost waiting PUBACK for {topic}: {error:?}"),
    }
}

/// Publish a QoS 1 packet and report whether PUBACK arrived before the
/// connection closed. Retained-cap rejections close the session.
pub async fn publish_maybe_closed(
    client: &AsyncClient,
    eventloop: &mut EventLoop,
    topic: &str,
    retain: bool,
    payload: &[u8],
) -> bool {
    client
        .publish(topic, QoS::AtLeastOnce, retain, payload)
        .await
        .unwrap();
    wait_puback(eventloop, Duration::from_secs(2)).await.is_ok()
}

async fn wait_puback(eventloop: &mut EventLoop, dur: Duration) -> Result<(), Box<ConnectionError>> {
    let deadline = Instant::now() + dur;
    loop {
        let left = deadline.saturating_duration_since(Instant::now());
        if left.is_zero() {
            panic!("timeout waiting for PUBACK");
        }
        match next_event(eventloop, left).await {
            Ok(MqttEvent::Incoming(Packet::PubAck(_))) => return Ok(()),
            Ok(_) => {}
            Err(error) => return Err(error),
        }
    }
}

pub async fn wait_publish(eventloop: &mut EventLoop, dur: Duration) -> Publish {
    let deadline = Instant::now() + dur;
    loop {
        let left = deadline.saturating_duration_since(Instant::now());
        if left.is_zero() {
            panic!("timeout waiting for PUBLISH");
        }
        match next_event(eventloop, left).await {
            Ok(MqttEvent::Incoming(Packet::Publish(publish))) => return publish,
            Ok(_) => {}
            Err(error) => panic!("connection lost waiting PUBLISH: {error:?}"),
        }
    }
}

/// Assert that the broker does not deliver a PUBLISH within `dur`.
pub async fn expect_no_publish(eventloop: &mut EventLoop, dur: Duration) {
    let deadline = Instant::now() + dur;
    loop {
        let left = deadline.saturating_duration_since(Instant::now());
        if left.is_zero() {
            return;
        }
        match timeout(left, eventloop.poll()).await {
            Ok(Ok(MqttEvent::Incoming(Packet::Publish(publish)))) => {
                panic!(
                    "unexpected PUBLISH on {}: {:?}",
                    publish.topic, publish.payload
                )
            }
            Ok(Ok(_)) | Err(_) => {}
            Ok(Err(error)) => panic!("connection lost while checking silence: {error:?}"),
        }
    }
}

pub async fn wait_event<F>(server: &SpikeServer, mut pred: F, dur: Duration) -> Event
where
    F: FnMut(&Event) -> bool,
{
    let deadline = Instant::now() + dur;
    loop {
        if let Some(event) = server.events().await.into_iter().find(|event| pred(event)) {
            return event;
        }
        if Instant::now() >= deadline {
            panic!("event not seen; events={:?}", server.events().await);
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

pub async fn wait_sessions(server: &SpikeServer, expected: usize, dur: Duration) {
    let deadline = Instant::now() + dur;
    loop {
        if server.sessions().await == expected {
            return;
        }
        if Instant::now() >= deadline {
            panic!("sessions={} != {expected}", server.sessions().await);
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Wait until the client side observes a closed/broken connection.
pub async fn wait_closed(eventloop: &mut EventLoop, dur: Duration) {
    let deadline = Instant::now() + dur;
    loop {
        let left = deadline.saturating_duration_since(Instant::now());
        if left.is_zero() {
            panic!("connection stayed open");
        }
        match timeout(left, eventloop.poll()).await {
            Ok(Ok(_)) => {}
            Ok(Err(_)) => return,
            Err(_) => panic!("connection stayed open"),
        }
    }
}

/// Send a raw CONNECT and return the bytes the broker answers with.
/// MQTT 3.1 (level 3) is not supported by rumqttc, so protocol rejection is
/// checked at the wire level.
pub async fn raw_connack(
    addr: SocketAddr,
    proto_ver: u8,
    client_id: &str,
    username: &str,
    password: &[u8],
) -> Vec<u8> {
    let mut stream = timeout(Duration::from_secs(5), tokio::net::TcpStream::connect(addr))
        .await
        .unwrap()
        .unwrap();
    stream
        .write_all(&connect_packet(proto_ver, client_id, username, password))
        .await
        .unwrap();
    let mut buf = [0u8; 64];
    let read = timeout(Duration::from_secs(5), stream.read(&mut buf))
        .await
        .unwrap()
        .unwrap();
    buf[..read].to_vec()
}

fn push_string(buf: &mut Vec<u8>, value: &[u8]) {
    buf.extend_from_slice(&(value.len() as u16).to_be_bytes());
    buf.extend_from_slice(value);
}

fn connect_packet(proto_ver: u8, client_id: &str, username: &str, password: &[u8]) -> Vec<u8> {
    let protocol_name: &[u8] = if proto_ver == 3 { b"MQIsdp" } else { b"MQTT" };
    let mut body = Vec::new();
    body.extend_from_slice(&(protocol_name.len() as u16).to_be_bytes());
    body.extend_from_slice(protocol_name);
    body.push(proto_ver);
    body.push(0x02 | 0x80 | 0x40); // clean session + username + password
    body.extend_from_slice(&30u16.to_be_bytes());
    if proto_ver == 5 {
        body.push(0); // empty CONNECT properties
    }
    push_string(&mut body, client_id.as_bytes());
    push_string(&mut body, username.as_bytes());
    push_string(&mut body, password);

    let mut packet = vec![0x10];
    let mut remaining = body.len();
    loop {
        let mut byte = (remaining % 128) as u8;
        remaining /= 128;
        if remaining > 0 {
            byte |= 0x80;
        }
        packet.push(byte);
        if remaining == 0 {
            break;
        }
    }
    packet.extend_from_slice(&body);
    packet
}

pub fn connack_code(bytes: &[u8], proto_ver: u8) -> u8 {
    assert!(bytes.len() >= 4, "short CONNACK: {bytes:02x?}");
    assert_eq!(bytes[0], 0x20, "not a CONNACK: {bytes:02x?}");
    if proto_ver == 5 {
        assert!(bytes.len() >= 5, "short MQTT 5 CONNACK: {bytes:02x?}");
        assert_eq!(
            bytes[1], 3,
            "unexpected MQTT 5 CONNACK length: {bytes:02x?}"
        );
        assert_eq!(bytes[2], 0, "session present must be false");
        assert_eq!(bytes[4], 0, "expected empty properties: {bytes:02x?}");
        bytes[3]
    } else {
        assert_eq!(
            bytes[1], 2,
            "unexpected MQTT 3 CONNACK length: {bytes:02x?}"
        );
        assert_eq!(bytes[2], 0, "session present must be false");
        bytes[3]
    }
}
