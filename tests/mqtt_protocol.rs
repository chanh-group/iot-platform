mod common;

use std::time::Duration;

use common::{
    TestApp, mqtt_connect, mqtt_connect_refused, mqtt_next, mqtt_options, mqtt_publish,
    mqtt_subscribe, mqtt_wait_closed, mqtt_wait_puback, mqtt_wait_publish, wait_until,
};
use iot_server::config::schema::MqttConfig;
use rumqttc::{
    AsyncClient, ConnectReturnCode, Event as MqttEvent, LastWill, Packet, QoS, SubscribeReasonCode,
};
use sqlx::{PgPool, SqlitePool};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

const WAIT: Duration = Duration::from_secs(5);

mod protocol_postgres {
    use std::{
        net::{IpAddr, SocketAddr},
        process::{Child, Command, Stdio},
    };

    use sqlx::postgres::PgPool;
    use tempfile::TempDir;

    pub struct Device {
        pub id: String,
        pub token: String,
    }

    pub struct Fixture {
        pub addr: SocketAddr,
        pub device: Device,
        pub config: std::path::PathBuf,
        _child: ChildGuard,
        _dir: TempDir,
    }

    struct ChildGuard(Child);

    impl Drop for ChildGuard {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }

    pub async fn start(device_name: &str, mqtt_config: &str, telemetry_config: &str) -> Fixture {
        let url = std::env::var("IOT_TEST_POSTGRES_URL").expect("runner PostgreSQL URL");
        let container = std::env::var("IOT_TEST_POSTGRES_CONTAINER").expect("runner container");
        let port: u16 = std::env::var("IOT_TEST_POSTGRES_PORT")
            .expect("runner mapped port")
            .parse()
            .expect("valid mapped port");
        let inspect = |format: &str| {
            let output = Command::new("docker")
                .args(["inspect", "--format", format, &container])
                .output()
                .expect("inspect guarded PostgreSQL fixture");
            assert!(output.status.success(), "Docker fixture inspection failed");
            String::from_utf8(output.stdout)
                .expect("Docker output is UTF-8")
                .trim()
                .to_owned()
        };
        assert_eq!(inspect("{{.Config.Image}}"), "postgres:18.6-bookworm");
        assert_eq!(
            inspect("{{ index .Config.Labels \"io.iot.test-fixture\" }}"),
            "mqtt-protocol"
        );
        let mapped: serde_json::Value = serde_json::from_str(&inspect(
            "{{json (index .NetworkSettings.Ports \"5432/tcp\")}}",
        ))
        .expect("parse mapped PostgreSQL port");
        let mappings = mapped
            .as_array()
            .filter(|m| m.len() == 1)
            .expect("one PostgreSQL mapping");
        let host: IpAddr = mappings[0]["HostIp"]
            .as_str()
            .expect("mapped host")
            .parse()
            .expect("valid host");
        let mapped_port: u16 = mappings[0]["HostPort"]
            .as_str()
            .expect("mapped port")
            .parse()
            .expect("valid port");
        assert!(host.is_loopback(), "PostgreSQL mapping must bind loopback");
        assert_eq!(mapped_port, port);
        validate_url(&url, host, port).expect("URL must target guarded fixture");

        let dir = TempDir::new().expect("temporary server config directory");
        let http_port = crate::common::free_port();
        let mqtt_port = loop {
            let p = crate::common::free_port();
            if p != http_port {
                break p;
            }
        };
        let config = dir.path().join("conf.toml");
        std::fs::write(&config, format!("config_version=1\n[server]\ndata_dir='./data'\nshutdown_timeout_ms=1000\n[http]\nlisten='127.0.0.1:{http_port}'\n[mqtt]\nlisten='127.0.0.1:{mqtt_port}'\n{mqtt_config}[database]\nurl_env='IOT_TEST_POSTGRES_URL'\n[telemetry]\nqueue_capacity=16\nbatch_size=1\n{telemetry_config}")).expect("write test config");
        let pool = PgPool::connect(&url)
            .await
            .expect("connect fixture database");
        let (database, schema): (String, String) =
            sqlx::query_as("SELECT current_database(), current_schema()")
                .fetch_one(&pool)
                .await
                .expect("verify fixture before migration");
        assert_eq!((database.as_str(), schema.as_str()), ("iot_test", "public"));
        pool.close().await;
        let output = Command::new(env!("CARGO_BIN_EXE_iot-server"))
            .args(["migrate", "--config"])
            .arg(&config)
            .env("IOT_TEST_POSTGRES_URL", &url)
            .output()
            .expect("run migrations");
        assert!(
            output.status.success(),
            "migration failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let password = "protocol-test-password";
        let password_file = dir.path().join("admin-password");
        std::fs::write(&password_file, password).expect("write password file");
        let output = Command::new(env!("CARGO_BIN_EXE_iot-server"))
            .args(["admin", "create", "--username", "admin", "--config"])
            .arg(&config)
            .args(["--password-file"])
            .arg(&password_file)
            .env("IOT_TEST_POSTGRES_URL", &url)
            .output()
            .expect("create fixture admin");
        assert!(
            output.status.success(),
            "admin creation failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let child = Command::new(env!("CARGO_BIN_EXE_iot-server"))
            .args(["run", "--config"])
            .arg(&config)
            .env("IOT_TEST_POSTGRES_URL", &url)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("start server");
        let mut child = ChildGuard(child);
        let base = format!("http://127.0.0.1:{http_port}");
        let client = reqwest::Client::builder()
            .cookie_store(true)
            .build()
            .expect("HTTP client");
        let mut ready = false;
        for _ in 0..100 {
            assert!(
                child.0.try_wait().expect("check server").is_none(),
                "server exited"
            );
            if client
                .get(format!("{base}/readyz"))
                .send()
                .await
                .is_ok_and(|r| r.status().is_success())
            {
                ready = true;
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        assert!(ready, "server did not become ready");
        let response = client
            .post(format!("{base}/api/v1/auth/login"))
            .header("X-IoT-CSRF", "1")
            .json(&serde_json::json!({"username":"admin","password":password}))
            .send()
            .await
            .expect("login");
        assert!(response.status().is_success(), "fixture login failed");
        let cookie = response
            .headers()
            .get(reqwest::header::SET_COOKIE)
            .expect("login cookie")
            .to_str()
            .expect("cookie UTF-8")
            .split(';')
            .next()
            .expect("cookie value")
            .to_owned();
        let _: serde_json::Value = response.json().await.expect("login response");
        let response = client
            .post(format!("{base}/api/v1/devices"))
            .header("Cookie", cookie)
            .header("X-IoT-CSRF", "1")
            .json(&serde_json::json!({"name":device_name,"metadata":{}}))
            .send()
            .await
            .expect("create device");
        assert_eq!(response.status(), reqwest::StatusCode::CREATED);
        let body: serde_json::Value = response.json().await.expect("device response");
        Fixture {
            addr: format!("127.0.0.1:{mqtt_port}")
                .parse()
                .expect("MQTT address"),
            device: Device {
                id: body["device"]["id"].as_str().expect("device id").to_owned(),
                token: body["credential"]["token"]
                    .as_str()
                    .expect("device token")
                    .to_owned(),
            },
            _child: child,
            config,
            _dir: dir,
        }
    }

    fn validate_url(url: &str, mapped_host: IpAddr, mapped_port: u16) -> Result<(), &'static str> {
        let (scheme, rest) = url.split_once("://").ok_or("invalid URL")?;
        if scheme != "postgresql" {
            return Err("unexpected scheme");
        }
        let (endpoint, query) = rest.split_once('?').ok_or("missing query")?;
        if query != "sslmode=disable" {
            return Err("unexpected query");
        }
        let (authority, database) = endpoint.rsplit_once('/').ok_or("missing database")?;
        if database != "iot_test" {
            return Err("unexpected database");
        }
        let (_, host_port) = authority.rsplit_once('@').ok_or("missing credentials")?;
        let (host, port) = host_port.rsplit_once(':').ok_or("missing port")?;
        let host: IpAddr = host.parse().map_err(|_| "invalid host")?;
        let port: u16 = port.parse().map_err(|_| "invalid port")?;
        if !host.is_loopback() || host != mapped_host || port != mapped_port {
            return Err("URL endpoint differs from fixture mapping");
        }
        Ok(())
    }

    #[test]
    fn postgres_fixture_url_guard_rejects_endpoint_and_query_overrides() {
        let host = IpAddr::from([127, 0, 0, 1]);
        assert_eq!(
            validate_url(
                "postgresql://u:p@127.0.0.1:54321/iot_test?sslmode=disable",
                host,
                54321
            ),
            Ok(())
        );
        for url in [
            "postgresql://u:p@127.0.0.1:54321/iot_test?sslmode=disable&host=127.0.0.1",
            "postgresql://u:p@127.0.0.1:54321/iot_test?sslmode=disable&port=54321",
            "postgresql://u:p@127.0.0.1:54321/iot_test?sslmode=disable&dbname=iot_test",
            "postgresql://u:p@192.0.2.1:54321/iot_test?sslmode=disable",
            "postgresql://u:p@127.0.0.1:54322/iot_test?sslmode=disable",
            "postgresql://u:p@127.0.0.1:54321/other?sslmode=disable",
            "postgres://u:p@127.0.0.1:54321/iot_test?sslmode=disable",
        ] {
            assert!(
                validate_url(url, host, 54321).is_err(),
                "accepted unsafe URL: {url}"
            );
        }
    }
}

fn push_mqtt_string(body: &mut Vec<u8>, value: &str) {
    body.extend_from_slice(&(value.len() as u16).to_be_bytes());
    body.extend_from_slice(value.as_bytes());
}

fn mqtt_packet(header: u8, body: Vec<u8>) -> Vec<u8> {
    let mut packet = vec![header];
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

async fn raw_mqtt_connect(
    address: std::net::SocketAddr,
    device_id: &str,
    token: &str,
) -> TcpStream {
    let mut body = Vec::new();
    push_mqtt_string(&mut body, "MQTT");
    body.extend_from_slice(&[4, 0xc2]); // MQTT 3.1.1, clean session, username, password
    body.extend_from_slice(&30u16.to_be_bytes());
    push_mqtt_string(&mut body, device_id);
    push_mqtt_string(&mut body, device_id);
    push_mqtt_string(&mut body, token);

    let mut stream = TcpStream::connect(address).await.unwrap();
    stream.write_all(&mqtt_packet(0x10, body)).await.unwrap();
    let mut connack = [0u8; 4];
    tokio::time::timeout(WAIT, stream.read_exact(&mut connack))
        .await
        .expect("broker should answer CONNECT")
        .unwrap();
    assert_eq!(connack, [0x20, 0x02, 0x00, 0x00]);
    stream
}

#[tokio::test]
async fn reconnect_succeeds_if_previous_session_drops_a_queued_kick() {
    let config = MqttConfig {
        max_subscriptions_per_client: 1_024,
        ..MqttConfig::default()
    };
    let app = TestApp::builder().mqtt_config(config).start().await;
    let device = app.create_device("protocol-kick-race", true).await;

    for round in 1..=8u16 {
        let mut old = raw_mqtt_connect(app.mqtt_addr, &device.id, &device.token).await;
        let mut body = round.to_be_bytes().to_vec();
        for index in 0..1_024 {
            push_mqtt_string(
                &mut body,
                &format!("devices/{}/data/probe-{index}", device.id),
            );
            body.push(1);
        }
        old.write_all(&mqtt_packet(0x82, body)).await.unwrap();
        drop(old);

        let new = raw_mqtt_connect(app.mqtt_addr, &device.id, &device.token).await;
        drop(new);
        wait_until(
            || !app.presence.is_online(&device.id).unwrap_or(true),
            WAIT,
            "takeover session disconnect",
        )
        .await;
    }
}

async fn next_telemetry_ack(
    eventloop: &mut rumqttc::EventLoop,
    ack_topic: &str,
    timeout: Duration,
) -> rumqttc::Publish {
    tokio::time::timeout(timeout, async {
        loop {
            if let MqttEvent::Incoming(Packet::Publish(publish)) = eventloop.poll().await.unwrap()
                && publish.topic == ack_topic
            {
                return publish;
            }
        }
    })
    .await
    .expect("receive telemetry ACK before timeout")
}

async fn publish_telemetry_and_wait_for_puback(
    client: &AsyncClient,
    eventloop: &mut rumqttc::EventLoop,
    topic: &str,
    ack_topic: &str,
    payload: &[u8],
    acknowledgements: &mut Vec<rumqttc::Publish>,
) {
    client
        .publish(topic, QoS::AtLeastOnce, false, payload)
        .await
        .unwrap();
    tokio::time::timeout(WAIT, async {
        loop {
            match eventloop.poll().await.unwrap() {
                MqttEvent::Incoming(Packet::PubAck(_)) => return,
                MqttEvent::Incoming(Packet::Publish(publish)) if publish.topic == ack_topic => {
                    acknowledgements.push(publish);
                }
                _ => {}
            }
        }
    })
    .await
    .expect("receive telemetry publish PUBACK before timeout");
}

#[tokio::test]
async fn qos_levels_and_own_device_wildcard_round_trip() {
    let app = TestApp::start().await;
    let device = app.create_device("protocol-qos", true).await;
    qos_round_trip(app.mqtt_addr, &device.id, &device.token).await;
}

#[tokio::test]
#[ignore = "requires disposable PostgreSQL fixture; run tests/protocol_postgres.sh"]
async fn qos_levels_and_own_device_wildcard_round_trip_postgres() {
    let fixture = protocol_postgres::start("protocol-qos", "", "").await;
    qos_round_trip(fixture.addr, &fixture.device.id, &fixture.device.token).await;
}

async fn qos_round_trip(addr: std::net::SocketAddr, device_id: &str, token: &str) {
    let (client, mut eventloop, _) = mqtt_connect(addr, device_id, device_id, token, true).await;
    let filter = format!("devices/{device_id}/data/#");

    assert_eq!(
        mqtt_subscribe(&client, &mut eventloop, &filter, QoS::ExactlyOnce).await,
        SubscribeReasonCode::Success(QoS::ExactlyOnce)
    );

    for (qos, suffix, payload) in [
        (QoS::AtMostOnce, "qos0", b"zero".as_slice()),
        (QoS::AtLeastOnce, "qos1", b"one".as_slice()),
        (QoS::ExactlyOnce, "qos2", b"two".as_slice()),
    ] {
        let topic = format!("devices/{device_id}/data/{suffix}");
        mqtt_publish(&client, &topic, qos, false, payload).await;
        let received = mqtt_wait_publish(&mut eventloop, WAIT).await;
        assert_eq!(received.topic, topic);
        assert_eq!(received.payload.as_ref(), payload);
        assert_eq!(received.qos, qos);
    }
}

#[tokio::test]
async fn max_inflight_per_client_limits_unacknowledged_qos1_deliveries() {
    let config = MqttConfig {
        max_inflight_per_client: 1,
        ..MqttConfig::default()
    };
    let app = TestApp::builder().mqtt_config(config).start().await;
    let device = app.create_device("protocol-inflight", true).await;
    max_inflight_round_trip(
        app.mqtt_addr,
        &device.id,
        &device.token,
        app.mqtt_config.max_inflight_per_client,
        TelemetryCounter::Sqlite(&app.pool),
    )
    .await;
}

#[tokio::test]
#[ignore = "requires disposable PostgreSQL fixture; run tests/protocol_postgres.sh"]
async fn max_inflight_per_client_limits_unacknowledged_qos1_deliveries_postgres() {
    let fixture =
        protocol_postgres::start("protocol-inflight", "max_inflight_per_client=1\n", "").await;
    let url = std::env::var("IOT_TEST_POSTGRES_URL").expect("runner PostgreSQL URL");
    let pool = PgPool::connect(&url)
        .await
        .expect("connect guarded fixture database");
    let configured = iot_server::config::load(&fixture.config)
        .unwrap()
        .config
        .mqtt
        .max_inflight_per_client;
    max_inflight_round_trip(
        fixture.addr,
        &fixture.device.id,
        &fixture.device.token,
        configured,
        TelemetryCounter::Postgres(&pool),
    )
    .await;
    pool.close().await;
}

enum TelemetryCounter<'a> {
    Sqlite(&'a SqlitePool),
    Postgres(&'a PgPool),
}

impl TelemetryCounter<'_> {
    async fn count(&self, device_id: &str) -> i64 {
        match self {
            Self::Sqlite(pool) => {
                sqlx::query_scalar("SELECT COUNT(*) FROM telemetry_events WHERE device_id = ?")
                    .bind(device_id)
                    .fetch_one(*pool)
                    .await
                    .unwrap()
            }
            Self::Postgres(pool) => {
                sqlx::query_scalar("SELECT COUNT(*) FROM telemetry_events WHERE device_id = $1")
                    .bind(device_id)
                    .fetch_one(*pool)
                    .await
                    .unwrap()
            }
        }
    }
}

async fn max_inflight_round_trip(
    address: std::net::SocketAddr,
    device_id: &str,
    token: &str,
    max_inflight_per_client: u64,
    counter: TelemetryCounter<'_>,
) {
    assert_eq!(max_inflight_per_client, 1);
    let ack_topic = format!("devices/{device_id}/telemetry/ack");
    let telemetry_topic = format!("devices/{device_id}/telemetry");
    let mut options = mqtt_options(address, device_id, device_id, token, true);
    options.set_manual_acks(true);
    let (client, mut eventloop) = AsyncClient::new(options, 32);
    assert!(matches!(
        mqtt_next(&mut eventloop, WAIT).await.unwrap(),
        MqttEvent::Incoming(Packet::ConnAck(ack)) if ack.code == ConnectReturnCode::Success
    ));
    assert_eq!(
        mqtt_subscribe(&client, &mut eventloop, &ack_topic, QoS::AtLeastOnce).await,
        SubscribeReasonCode::Success(QoS::AtLeastOnce)
    );

    let mut acknowledgements = Vec::new();
    for index in 0..3 {
        let payload = format!(r#"{{"event_id":"inflight-{index}","values":{{"x":{index}}}}}"#);
        publish_telemetry_and_wait_for_puback(
            &client,
            &mut eventloop,
            &telemetry_topic,
            &ack_topic,
            payload.as_bytes(),
            &mut acknowledgements,
        )
        .await;
    }

    tokio::time::timeout(WAIT, async {
        loop {
            let count = counter.count(device_id).await;
            if count == 3 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("all three telemetry events should commit");

    if acknowledgements.is_empty() {
        acknowledgements.push(next_telemetry_ack(&mut eventloop, &ack_topic, WAIT).await);
    }
    assert_eq!(
        acknowledgements.len(),
        1,
        "only one QoS 1 delivery may remain unacknowledged"
    );
    let first = acknowledgements.pop().unwrap();
    assert_eq!(first.qos, QoS::AtLeastOnce);

    assert!(
        tokio::time::timeout(
            Duration::from_millis(500),
            next_telemetry_ack(&mut eventloop, &ack_topic, WAIT),
        )
        .await
        .is_err(),
        "broker must wait for PUBACK before sending another QoS 1 message"
    );

    client.ack(&first).await.unwrap();
    let second = next_telemetry_ack(&mut eventloop, &ack_topic, WAIT).await;
    assert_ne!(second.pkid, first.pkid);
    assert!(
        tokio::time::timeout(
            Duration::from_millis(500),
            next_telemetry_ack(&mut eventloop, &ack_topic, WAIT),
        )
        .await
        .is_err(),
        "the next inflight slot must remain occupied until its PUBACK"
    );

    client.ack(&second).await.unwrap();
    let third = next_telemetry_ack(&mut eventloop, &ack_topic, WAIT).await;
    assert_ne!(third.pkid, second.pkid);
    client.ack(&third).await.unwrap();
}

#[tokio::test]
async fn retained_last_will_is_replayed_after_ungraceful_disconnect() {
    let app = TestApp::start().await;
    let device = app.create_device("protocol-will", true).await;
    let status = format!("devices/{}/status", device.id);
    let mut options = mqtt_options(app.mqtt_addr, &device.id, &device.id, &device.token, false);
    options.set_last_will(LastWill::new(&status, b"offline", QoS::AtLeastOnce, true));
    let (client, mut eventloop) = AsyncClient::new(options, 16);
    assert!(matches!(
        mqtt_next(&mut eventloop, WAIT).await.unwrap(),
        MqttEvent::Incoming(Packet::ConnAck(ack)) if ack.code == ConnectReturnCode::Success
    ));

    drop(client);
    drop(eventloop);
    wait_until(
        || {
            !app.presence
                .is_online(&device.id)
                .expect("presence state should remain healthy")
        },
        WAIT,
        "device disconnect",
    )
    .await;
    tokio::time::timeout(WAIT, async {
        while app.retained_count().await != 1 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the broker should store the retained will");

    let (client, mut eventloop, _) =
        mqtt_connect(app.mqtt_addr, &device.id, &device.id, &device.token, true).await;
    assert_eq!(
        mqtt_subscribe(&client, &mut eventloop, &status, QoS::AtLeastOnce).await,
        SubscribeReasonCode::Success(QoS::AtLeastOnce)
    );
    let will = mqtt_wait_publish(&mut eventloop, WAIT).await;
    assert_eq!(will.topic, status);
    assert_eq!(will.payload.as_ref(), b"offline");
    assert!(will.retain);
}

#[tokio::test]
async fn persistent_session_reconnects_then_expires() {
    let config = MqttConfig {
        max_session_expiry_ms: 500,
        ..MqttConfig::default()
    };
    let app = TestApp::builder().mqtt_config(config).start().await;
    let device = app.create_device("protocol-session", true).await;
    persistent_session_round_trip(app.mqtt_addr, &app.presence, &device.id, &device.token).await;
}

#[tokio::test]
#[ignore = "requires disposable PostgreSQL fixture; run tests/protocol_postgres.sh"]
async fn persistent_session_reconnects_then_expires_postgres() {
    use std::sync::{Arc, atomic::AtomicBool};

    let fixture = protocol_postgres::start("protocol-session", "", "").await;
    let loaded = iot_server::config::load(&fixture.config).unwrap();
    let storage = iot_server::storage::runtime::StorageRuntime::connect(&loaded)
        .await
        .unwrap();
    let telemetry = &loaded.config.telemetry;
    let cancel = tokio_util::sync::CancellationToken::new();
    let (ingest, task) = iot_server::core::telemetry::spawn_worker(
        storage.repositories.telemetry.clone(),
        iot_server::core::events::EventBus::new(),
        telemetry.queue_capacity as usize,
        telemetry.max_batch_bytes as usize,
        loaded.runtime_config.clone(),
        iot_server::core::telemetry::IngestWorkerControl {
            cancellation: cancel.clone(),
            readiness: Arc::new(AtomicBool::new(true)),
        },
    );
    let mut config = loaded.config.mqtt.clone();
    config.listen = "127.0.0.1:0".parse().unwrap();
    config.max_session_expiry_ms = 500;
    let presence = Arc::new(iot_server::core::presence::Presence::new());
    let mqtt = iot_server::mqtt::server::MqttServer::start_with_ingest(
        &config,
        Arc::new(AtomicBool::new(false)),
        iot_server::mqtt::server::MqttRepositories {
            devices: storage.repositories.devices.clone(),
            commands: storage.repositories.commands.clone(),
            attributes: storage.repositories.attributes.clone(),
        },
        Arc::clone(&presence),
        (
            ingest,
            iot_server::core::telemetry::TelemetryPayload::try_from(telemetry).unwrap(),
            telemetry.queue_capacity as usize,
        ),
    )
    .await
    .unwrap();
    persistent_session_round_trip(
        mqtt.local_addr(),
        &presence,
        &fixture.device.id,
        &fixture.device.token,
    )
    .await;
    mqtt.abort();
    drop(mqtt);
    cancel.cancel();
    task.await.unwrap();
    storage.close().await;
}

async fn persistent_session_round_trip(
    address: std::net::SocketAddr,
    presence: &iot_server::core::presence::Presence,
    device_id: &str,
    token: &str,
) {
    let filter = format!("devices/{device_id}/data/#");

    let (client, mut eventloop, present) =
        mqtt_connect(address, device_id, device_id, token, false).await;
    assert!(!present);
    assert_eq!(
        mqtt_subscribe(&client, &mut eventloop, &filter, QoS::AtLeastOnce).await,
        SubscribeReasonCode::Success(QoS::AtLeastOnce)
    );
    drop(client);
    drop(eventloop);
    wait_until(
        || {
            !presence
                .is_online(device_id)
                .expect("presence state should remain healthy")
        },
        WAIT,
        "session disconnect",
    )
    .await;

    let (client, mut eventloop, present) =
        mqtt_connect(address, device_id, device_id, token, false).await;
    assert!(
        present,
        "an unexpired clean_session=false session should resume"
    );
    let topic = format!("devices/{device_id}/data/resumed");
    mqtt_publish(&client, &topic, QoS::AtLeastOnce, false, b"resumed").await;
    assert_eq!(
        mqtt_wait_publish(&mut eventloop, WAIT)
            .await
            .payload
            .as_ref(),
        b"resumed"
    );
    drop(client);
    drop(eventloop);
    wait_until(
        || {
            !presence
                .is_online(device_id)
                .expect("presence state should remain healthy")
        },
        WAIT,
        "second session disconnect",
    )
    .await;

    tokio::time::sleep(Duration::from_millis(700)).await;
    let (_client, _eventloop, present) =
        mqtt_connect(address, device_id, device_id, token, false).await;
    assert!(!present, "an expired persistent session must be discarded");
}

#[tokio::test]
async fn broker_restart_discards_persistent_sessions() {
    let mut app = TestApp::start().await;
    let device = app.create_device("protocol-restart", true).await;
    let filter = format!("devices/{}/data/#", device.id);
    let (client, mut eventloop, _) =
        mqtt_connect(app.mqtt_addr, &device.id, &device.id, &device.token, false).await;
    mqtt_subscribe(&client, &mut eventloop, &filter, QoS::AtLeastOnce).await;
    drop(client);
    drop(eventloop);
    wait_until(
        || {
            !app.presence
                .is_online(&device.id)
                .expect("presence state should remain healthy")
        },
        WAIT,
        "session disconnect",
    )
    .await;

    app.restart_mqtt().await;
    let (_client, _eventloop, present) =
        mqtt_connect(app.mqtt_addr, &device.id, &device.id, &device.token, false).await;
    assert!(
        !present,
        "broker sessions are in-memory and must not survive restart"
    );
}

#[tokio::test]
async fn subscription_limit_is_enforced_by_the_listener() {
    let config = MqttConfig {
        max_subscriptions_per_client: 1,
        ..MqttConfig::default()
    };
    let app = TestApp::builder().mqtt_config(config).start().await;
    let device = app.create_device("protocol-sub-limit", true).await;
    let (client, mut eventloop, _) =
        mqtt_connect(app.mqtt_addr, &device.id, &device.id, &device.token, true).await;

    let status = format!("devices/{}/status", device.id);
    let data = format!("devices/{}/data/#", device.id);
    assert_eq!(
        mqtt_subscribe(&client, &mut eventloop, &status, QoS::AtLeastOnce).await,
        SubscribeReasonCode::Success(QoS::AtLeastOnce)
    );
    assert_eq!(
        mqtt_subscribe(&client, &mut eventloop, &data, QoS::AtLeastOnce).await,
        SubscribeReasonCode::Failure
    );
}

#[tokio::test]
async fn topic_level_limit_is_enforced_by_the_listener() {
    let config = MqttConfig {
        max_topic_levels: 4,
        ..MqttConfig::default()
    };
    let app = TestApp::builder().mqtt_config(config).start().await;
    let device = app.create_device("protocol-topic-limit", true).await;
    topic_level_limit_round_trip(app.mqtt_addr, &device.id, &device.token).await;
}

#[tokio::test]
#[ignore = "requires disposable PostgreSQL fixture; run tests/protocol_postgres.sh"]
async fn topic_level_limit_is_enforced_by_the_listener_postgres() {
    let fixture =
        protocol_postgres::start("protocol-topic-limit", "max_topic_levels=4\n", "").await;
    topic_level_limit_round_trip(fixture.addr, &fixture.device.id, &fixture.device.token).await;
}

async fn topic_level_limit_round_trip(addr: std::net::SocketAddr, device_id: &str, token: &str) {
    let (client, mut eventloop, _) = mqtt_connect(addr, device_id, device_id, token, true).await;
    let filter = format!("devices/{device_id}/data/#");
    let too_deep_filter = format!("devices/{device_id}/data/x/y/#");
    assert_eq!(
        mqtt_subscribe(&client, &mut eventloop, &too_deep_filter, QoS::AtLeastOnce).await,
        SubscribeReasonCode::Failure
    );
    mqtt_subscribe(&client, &mut eventloop, &filter, QoS::AtLeastOnce).await;

    let allowed = format!("devices/{device_id}/data/x");
    mqtt_publish(&client, &allowed, QoS::AtLeastOnce, false, b"ok").await;
    assert_eq!(mqtt_wait_publish(&mut eventloop, WAIT).await.topic, allowed);

    let too_deep = format!("devices/{device_id}/data/x/y");
    mqtt_publish(&client, &too_deep, QoS::AtLeastOnce, false, b"deny").await;
    mqtt_wait_closed(&mut eventloop, WAIT).await;
}

#[tokio::test]
async fn packet_limit_is_enforced_by_the_listener() {
    let config = MqttConfig {
        max_packet_bytes: 1_024,
        ..MqttConfig::default()
    };
    let app = TestApp::builder().mqtt_config(config).start().await;
    let device = app.create_device("protocol-packet-limit", true).await;
    packet_limit_round_trip(app.mqtt_addr, &device.id, &device.token).await;
}

#[tokio::test]
#[ignore = "requires disposable PostgreSQL fixture; run tests/protocol_postgres.sh"]
async fn packet_limit_is_enforced_by_the_listener_postgres() {
    let fixture = protocol_postgres::start(
        "protocol-packet-limit",
        "max_packet_bytes=1024\n",
        "max_payload_bytes=1024\n",
    )
    .await;
    packet_limit_round_trip(fixture.addr, &fixture.device.id, &fixture.device.token).await;
}

async fn packet_limit_round_trip(addr: std::net::SocketAddr, device_id: &str, token: &str) {
    let (client, mut eventloop, _) = mqtt_connect(addr, device_id, device_id, token, true).await;
    let topic = format!("devices/{device_id}/data/large");
    let payload = [b'x'; 2_048];

    mqtt_publish(&client, &topic, QoS::AtLeastOnce, false, &payload).await;
    mqtt_wait_closed(&mut eventloop, WAIT).await;
}

#[tokio::test]
async fn client_id_limit_rejects_only_when_the_canonical_id_exceeds_it() {
    let device_id = uuid::Uuid::now_v7().to_string();
    assert_eq!(device_id.len(), 36);
    let token = format!(
        "iot_{}",
        iot_server::core::auth::generate_session_token().unwrap()
    );
    // A below-schema test value isolates the broker limit from UUID authentication.
    let restrictive_config = MqttConfig {
        max_client_id_len: 35,
        ..MqttConfig::default()
    };
    let mut app = TestApp::builder()
        .mqtt_config(restrictive_config)
        .start()
        .await;
    let device = app
        .create_device_with_identity(&device_id, &token, "protocol-client-id-limit", true)
        .await;

    mqtt_connect_refused(app.mqtt_addr, &device.id, &device.id, &device.token).await;

    let permissive_config = MqttConfig {
        max_client_id_len: 36,
        ..MqttConfig::default()
    };
    app.restart_mqtt_with_config(permissive_config).await;
    let (client, eventloop, _) =
        mqtt_connect(app.mqtt_addr, &device.id, &device.id, &device.token, true).await;
    drop(client);
    drop(eventloop);
}

#[tokio::test]
async fn connection_limit_rejects_an_additional_device() {
    let config = MqttConfig {
        max_connections: 1,
        ..MqttConfig::default()
    };
    let app = TestApp::builder().mqtt_config(config).start().await;
    let first = app.create_device("protocol-connection-a", true).await;
    let second = app.create_device("protocol-connection-b", true).await;
    let (_client, _eventloop, _) =
        mqtt_connect(app.mqtt_addr, &first.id, &first.id, &first.token, true).await;
    assert_eq!(
        mqtt_connect_refused(app.mqtt_addr, &second.id, &second.id, &second.token).await,
        ConnectReturnCode::NotAuthorized,
        "the listener should explicitly refuse a connection above its configured cap"
    );
}

#[tokio::test]
async fn offline_session_queue_obeys_its_configured_cap() {
    let config = MqttConfig {
        max_queued_messages_per_client: 1,
        ..MqttConfig::default()
    };
    let app = TestApp::builder().mqtt_config(config).start().await;
    let device = app.create_device("protocol-queue-limit", true).await;
    let ack_topic = format!("devices/{}/telemetry/ack", device.id);
    let (client, mut eventloop, _) =
        mqtt_connect(app.mqtt_addr, &device.id, &device.id, &device.token, false).await;
    mqtt_subscribe(&client, &mut eventloop, &ack_topic, QoS::AtLeastOnce).await;

    let mut lock = app.pool.acquire().await.unwrap();
    sqlx::query("BEGIN IMMEDIATE")
        .execute(&mut *lock)
        .await
        .unwrap();
    for event_id in ["queued-1", "queued-2"] {
        let raw = format!(r#"{{"event_id":"{event_id}","values":{{"x":1}}}}"#);
        let topic = format!("devices/{}/telemetry", device.id);
        mqtt_publish(&client, &topic, QoS::AtLeastOnce, false, raw.as_bytes()).await;
        mqtt_wait_puback(&mut eventloop, WAIT).await;
    }

    drop(client);
    drop(eventloop);
    wait_until(
        || {
            !app.presence
                .is_online(&device.id)
                .expect("presence state should remain healthy")
        },
        WAIT,
        "session disconnect before commit",
    )
    .await;
    sqlx::query("COMMIT").execute(&mut *lock).await.unwrap();
    drop(lock);

    tokio::time::timeout(WAIT, async {
        loop {
            let rows: i64 =
                sqlx::query_scalar("SELECT COUNT(*) FROM telemetry_events WHERE device_id = ?")
                    .bind(&device.id)
                    .fetch_one(&app.pool)
                    .await
                    .unwrap();
            if rows == 2 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("both publishes should commit after releasing the test lock");
    tokio::time::sleep(Duration::from_millis(100)).await;

    let (client, mut eventloop, present) =
        mqtt_connect(app.mqtt_addr, &device.id, &device.id, &device.token, false).await;
    assert!(present, "the persistent subscription should resume");
    let queued = mqtt_wait_publish(&mut eventloop, WAIT).await;
    assert_eq!(queued.topic, ack_topic);
    let extra = tokio::time::timeout(Duration::from_millis(250), async {
        loop {
            if let MqttEvent::Incoming(Packet::Publish(publish)) = eventloop.poll().await.unwrap()
                && publish.topic == ack_topic
            {
                return;
            }
        }
    })
    .await;
    assert!(
        extra.is_err(),
        "the offline queue must retain no more than one ACK"
    );
    drop(client);
}
