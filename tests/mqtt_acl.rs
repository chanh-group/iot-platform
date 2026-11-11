//! P2-03: default-deny ACL matrix over a real broker (T12–T14, A04).

mod common;

use std::time::Duration;
use std::{
    net::IpAddr,
    process::{Child, Command, Stdio},
};

use common::{
    TestApp, TestDevice, mqtt_connect, mqtt_next, mqtt_wait_closed, raw_publish_denied,
    wait_presence,
};
use rumqttc::{AsyncClient, Event as MqttEvent, EventLoop, Packet, QoS, SubscribeReasonCode};
use sqlx::PgPool;
use tempfile::TempDir;

const WAIT: Duration = Duration::from_secs(5);

#[tokio::test]
async fn own_topics_are_allowed_for_pub_and_sub() {
    own_topics_are_allowed(Backend::Sqlite(Box::new(TestApp::start().await))).await;
}

#[tokio::test]
#[ignore = "requires disposable PostgreSQL fixture; run tests/acl_postgres.sh"]
async fn own_topics_are_allowed_for_pub_and_sub_postgres() {
    own_topics_are_allowed(Backend::postgres().await).await;
}

async fn own_topics_are_allowed(server: Backend) {
    let device = server.create_device("acl-own").await;
    let (client, mut eventloop, _) = mqtt_connect(
        server.mqtt_addr(),
        &device.id,
        &device.id,
        &device.token,
        true,
    )
    .await;

    for filter in [
        format!("devices/{}/telemetry/ack", device.id),
        format!("devices/{}/commands", device.id),
        format!("devices/{}/status", device.id),
        format!("devices/{}/data/#", device.id),
        format!("devices/{}/data/sensors/+", device.id),
    ] {
        assert_eq!(
            subscribe(&client, &mut eventloop, &filter).await,
            SubscribeReasonCode::Success(QoS::AtLeastOnce),
            "{filter}"
        );
    }

    for topic in [
        format!("devices/{}/telemetry", device.id),
        format!("devices/{}/attributes", device.id),
        format!("devices/{}/commands/ack", device.id),
        format!("devices/{}/status", device.id),
        format!("devices/{}/data/sensors/room", device.id),
    ] {
        publish_ok(&client, &mut eventloop, &topic).await;
    }
}

enum Backend {
    Sqlite(Box<TestApp>),
    Postgres(PostgresApp),
}

impl Backend {
    fn mqtt_addr(&self) -> std::net::SocketAddr {
        match self {
            Self::Sqlite(app) => app.mqtt_addr,
            Self::Postgres(app) => app.mqtt_addr,
        }
    }

    async fn create_device(&self, name: &str) -> TestDevice {
        match self {
            Self::Sqlite(app) => app.create_device(name, true).await,
            Self::Postgres(app) => app.create_device(name).await,
        }
    }

    async fn wait_presence(&self, id: &str, online: bool) {
        match self {
            Self::Sqlite(app) => wait_presence(app, id, online).await,
            Self::Postgres(app) => app.wait_presence(id, online).await,
        }
    }
}

struct PostgresApp {
    _dir: TempDir,
    base: String,
    mqtt_addr: std::net::SocketAddr,
    token: String,
    cookie: String,
    _child: ChildGuard,
}

struct ChildGuard(Child);

impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

impl Backend {
    async fn postgres() -> Self {
        let url = std::env::var("IOT_TEST_POSTGRES_URL").expect("runner PostgreSQL URL");
        let container = std::env::var("IOT_TEST_POSTGRES_CONTAINER").expect("runner container");
        let expected_port: u16 = std::env::var("IOT_TEST_POSTGRES_PORT")
            .expect("runner mapped port")
            .parse()
            .expect("valid mapped port");
        assert_postgres_fixture(&url, &container, expected_port);
        let dir = tempfile::tempdir().expect("temp config dir");
        let http_port = common::free_port();
        let mqtt_port = loop {
            let port = common::free_port();
            if port != http_port {
                break port;
            }
        };
        let config = dir.path().join("conf.toml");
        std::fs::write(&config, format!("config_version=1\n[server]\ndata_dir='./data'\nshutdown_timeout_ms=1000\n[http]\nlisten='127.0.0.1:{http_port}'\n[mqtt]\nlisten='127.0.0.1:{mqtt_port}'\n[database]\nurl_env='IOT_TEST_POSTGRES_URL'\n[telemetry]\nqueue_capacity=16\nbatch_size=1\n"))
            .expect("write test config");
        let pool = PgPool::connect(&url)
            .await
            .expect("connect fixture database");
        let (database, schema): (String, String) =
            sqlx::query_as("SELECT current_database(), current_schema()")
                .fetch_one(&pool)
                .await
                .expect("verify fixture database before migration");
        assert_eq!(database, "iot_test");
        assert_eq!(schema, "public");
        pool.close().await;
        let output = Command::new(env!("CARGO_BIN_EXE_iot-server"))
            .args(["migrate", "--config"])
            .arg(&config)
            .env("IOT_TEST_POSTGRES_URL", &url)
            .output()
            .expect("run migration");
        assert!(
            output.status.success(),
            "migration failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let password = "acl-test-password";
        let password_file = dir.path().join("admin-password");
        std::fs::write(&password_file, password).expect("write admin password");
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
            tokio::time::sleep(Duration::from_millis(50)).await;
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
            .unwrap()
            .to_str()
            .unwrap()
            .split(';')
            .next()
            .unwrap()
            .to_owned();
        let _login: serde_json::Value = response.json().await.unwrap();
        let token = "1".to_owned();
        let app = PostgresApp {
            _dir: dir,
            base,
            mqtt_addr: format!("127.0.0.1:{mqtt_port}").parse().unwrap(),
            token,
            cookie,
            _child: child,
        };
        Self::Postgres(app)
    }
}

impl PostgresApp {
    async fn create_device(&self, name: &str) -> TestDevice {
        let response = reqwest::Client::new()
            .post(format!("{}/api/v1/devices", self.base))
            .header("Cookie", &self.cookie)
            .header("X-IoT-CSRF", &self.token)
            .json(&serde_json::json!({"name":name,"metadata":{}}))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), reqwest::StatusCode::CREATED);
        let body: serde_json::Value = response.json().await.unwrap();
        TestDevice {
            id: body["device"]["id"].as_str().unwrap().to_owned(),
            token: body["credential"]["token"].as_str().unwrap().to_owned(),
        }
    }

    async fn wait_presence(&self, id: &str, online: bool) {
        let client = reqwest::Client::new();
        tokio::time::timeout(WAIT, async {
            loop {
                let response = client
                    .get(format!("{}/api/v1/devices/{id}", self.base))
                    .header("Cookie", &self.cookie)
                    .send()
                    .await
                    .expect("query device presence");
                assert!(response.status().is_success(), "presence query failed");
                let body: serde_json::Value = response.json().await.unwrap();
                if body["online"].as_bool() == Some(online) {
                    return;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("broker presence did not reach expected state");
    }
}

fn assert_postgres_fixture(url: &str, container: &str, expected_port: u16) {
    let image = docker_inspect("{{.Config.Image}}", container);
    assert_eq!(image.trim(), "postgres:18.6-bookworm");
    let label = docker_inspect(
        "{{ index .Config.Labels \"io.iot.test-fixture\" }}",
        container,
    );
    assert_eq!(label.trim(), "mqtt-acl");
    let mapped: serde_json::Value = serde_json::from_str(&docker_inspect(
        "{{json (index .NetworkSettings.Ports \"5432/tcp\")}}",
        container,
    ))
    .expect("parse container port mappings");
    let bindings = mapped
        .as_array()
        .filter(|bindings| bindings.len() == 1)
        .expect("fixture must have exactly one PostgreSQL port mapping");
    let host: IpAddr = bindings[0]["HostIp"]
        .as_str()
        .expect("mapped host")
        .parse()
        .expect("valid mapped host");
    let port: u16 = bindings[0]["HostPort"]
        .as_str()
        .expect("mapped port")
        .parse()
        .expect("valid mapped port");
    assert!(host.is_loopback(), "PostgreSQL mapping must bind loopback");
    assert_eq!(port, expected_port);
    validate_fixture_url(url, host, port).expect("URL must target guarded fixture");
}

fn docker_inspect(format: &str, container: &str) -> String {
    let output = Command::new("docker")
        .args(["inspect", "--format", format, container])
        .output()
        .expect("inspect PostgreSQL fixture");
    assert!(output.status.success(), "Docker fixture inspection failed");
    String::from_utf8(output.stdout).expect("Docker output is UTF-8")
}

fn validate_fixture_url(
    url: &str,
    mapped_host: IpAddr,
    mapped_port: u16,
) -> Result<(), &'static str> {
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
        validate_fixture_url(
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
            validate_fixture_url(url, host, 54321).is_err(),
            "accepted unsafe URL: {url}"
        );
    }
}

#[tokio::test]
async fn subscribe_outside_own_prefix_is_refused_and_keeps_the_connection() {
    subscribe_outside_own_prefix(Backend::Sqlite(Box::new(TestApp::start().await))).await;
}

#[tokio::test]
#[ignore = "requires disposable PostgreSQL fixture; run tests/acl_postgres.sh"]
async fn subscribe_outside_own_prefix_is_refused_and_keeps_the_connection_postgres() {
    subscribe_outside_own_prefix(Backend::postgres().await).await;
}

async fn subscribe_outside_own_prefix(server: Backend) {
    let a = server.create_device("acl-sub-a").await;
    let b = server.create_device("acl-sub-b").await;
    let (client, mut eventloop, _) =
        mqtt_connect(server.mqtt_addr(), &a.id, &a.id, &a.token, true).await;

    // T13/T14 plus every filter that would widen the read set.
    for filter in [
        format!("devices/{}/commands", b.id),
        format!("devices/{}/data/#", b.id),
        "devices/+/telemetry".to_owned(),
        "devices/#".to_owned(),
        a.id.clone(),
        format!("devices/{}/#", a.id),
        format!("devices/{}/telemetry", a.id),
        format!("devices/{}/attributes", a.id),
        "$SYS/#".to_owned(),
        format!("$share/group/devices/{}/data/#", a.id),
    ] {
        assert_eq!(
            subscribe(&client, &mut eventloop, &filter).await,
            SubscribeReasonCode::Failure,
            "{filter}"
        );
    }

    // Denied filters do not close the connection.
    assert_eq!(
        subscribe(&client, &mut eventloop, &format!("devices/{}/data/#", a.id)).await,
        SubscribeReasonCode::Success(QoS::AtLeastOnce)
    );
}

#[tokio::test]
async fn cross_device_publish_is_denied_not_routed_and_disconnects() {
    cross_device_publish(Backend::Sqlite(Box::new(TestApp::start().await))).await;
}

#[tokio::test]
#[ignore = "requires disposable PostgreSQL fixture; run tests/acl_postgres.sh"]
async fn cross_device_publish_is_denied_not_routed_and_disconnects_postgres() {
    cross_device_publish(Backend::postgres().await).await;
}

async fn cross_device_publish(server: Backend) {
    let a = server.create_device("acl-pub-a").await;
    let b = server.create_device("acl-pub-b").await;

    let (b_client, mut b_eventloop, _) =
        mqtt_connect(server.mqtt_addr(), &b.id, &b.id, &b.token, true).await;
    assert_eq!(
        subscribe(
            &b_client,
            &mut b_eventloop,
            &format!("devices/{}/data/#", b.id)
        )
        .await,
        SubscribeReasonCode::Success(QoS::AtLeastOnce)
    );

    // T12: A cannot write into B's namespace and B never sees the message.
    let (a_client, mut a_eventloop, _) =
        mqtt_connect(server.mqtt_addr(), &a.id, &a.id, &a.token, true).await;
    a_client
        .publish(
            &format!("devices/{}/data/attack", b.id),
            QoS::AtLeastOnce,
            false,
            b"{}".as_slice(),
        )
        .await
        .unwrap();
    mqtt_wait_closed(&mut a_eventloop, WAIT).await;
    assert!(
        tokio::time::timeout(Duration::from_millis(300), b_eventloop.poll())
            .await
            .is_err(),
        "denied publish must not be routed to device B"
    );
}

#[tokio::test]
async fn server_only_and_system_topics_are_denied_for_publish() {
    server_only_publish_denials(Backend::Sqlite(Box::new(TestApp::start().await))).await;
}

#[tokio::test]
#[ignore = "requires disposable PostgreSQL fixture; run tests/acl_postgres.sh"]
async fn server_only_and_system_topics_are_denied_for_publish_postgres() {
    server_only_publish_denials(Backend::postgres().await).await;
}

async fn server_only_publish_denials(server: Backend) {
    let telemetry_ack = server.create_device("acl-deny-ack").await;
    let commands = server.create_device("acl-deny-commands").await;
    let sys = server.create_device("acl-deny-sys").await;
    let shared = server.create_device("acl-deny-shared").await;
    assert_publish_denied(
        &server,
        &telemetry_ack,
        &format!("devices/{}/telemetry/ack", telemetry_ack.id),
    )
    .await;
    assert_publish_denied(
        &server,
        &commands,
        &format!("devices/{}/commands", commands.id),
    )
    .await;
    assert_publish_denied(&server, &sys, "$SYS/brokers").await;
    assert_publish_denied(
        &server,
        &shared,
        &format!("$share/group/devices/{}/data/x", shared.id),
    )
    .await;

    // Wildcards in a publish topic never reach the broker through rumqttc.
    let data_wildcard = server.create_device("acl-deny-data-wildcard").await;
    let telemetry_wildcard = server.create_device("acl-deny-telemetry-wildcard").await;
    for (device, topic) in [
        (
            &data_wildcard,
            format!("devices/{}/data/+", data_wildcard.id),
        ),
        (
            &telemetry_wildcard,
            format!("devices/{}/telemetry/#", telemetry_wildcard.id),
        ),
    ] {
        assert!(
            raw_publish_denied(
                server.mqtt_addr(),
                &device.id,
                &device.id,
                &device.token,
                &topic,
                false,
            )
            .await,
            "{topic}"
        );
    }
}

#[tokio::test]
async fn retained_publish_to_server_topics_is_denied() {
    retained_publish_denial(Backend::Sqlite(Box::new(TestApp::start().await))).await;
}

#[tokio::test]
#[ignore = "requires disposable PostgreSQL fixture; run tests/acl_postgres.sh"]
async fn retained_publish_to_server_topics_is_denied_postgres() {
    retained_publish_denial(Backend::postgres().await).await;
}

async fn retained_publish_denial(server: Backend) {
    let device = server.create_device("acl-retained").await;
    let (client, mut eventloop, _) = mqtt_connect(
        server.mqtt_addr(),
        &device.id,
        &device.id,
        &device.token,
        true,
    )
    .await;

    client
        .publish(
            &format!("devices/{}/commands", device.id),
            QoS::AtLeastOnce,
            true,
            b"{}".as_slice(),
        )
        .await
        .unwrap();
    mqtt_wait_closed(&mut eventloop, WAIT).await;
}

async fn subscribe(
    client: &AsyncClient,
    eventloop: &mut EventLoop,
    filter: &str,
) -> SubscribeReasonCode {
    client.subscribe(filter, QoS::AtLeastOnce).await.unwrap();
    loop {
        if let MqttEvent::Incoming(Packet::SubAck(ack)) = mqtt_next(eventloop, WAIT).await.unwrap()
        {
            return ack.return_codes[0];
        }
    }
}

async fn publish_ok(client: &AsyncClient, eventloop: &mut EventLoop, topic: &str) {
    client
        .publish(topic, QoS::AtLeastOnce, false, b"{}".as_slice())
        .await
        .unwrap();
    loop {
        if let MqttEvent::Incoming(Packet::PubAck(_)) = mqtt_next(eventloop, WAIT).await.unwrap() {
            return;
        }
    }
}

async fn assert_publish_denied(server: &Backend, device: &TestDevice, topic: &str) {
    let (client, mut eventloop, _) = mqtt_connect(
        server.mqtt_addr(),
        &device.id,
        &device.id,
        &device.token,
        true,
    )
    .await;
    server.wait_presence(&device.id, true).await;
    client
        .publish(topic, QoS::AtLeastOnce, false, b"{}".as_slice())
        .await
        .unwrap();
    mqtt_wait_closed(&mut eventloop, WAIT).await;
    server.wait_presence(&device.id, false).await;
    drop(client);
}
