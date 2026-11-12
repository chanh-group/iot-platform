use std::{
    net::{IpAddr, SocketAddr},
    process::{Child, Command, Stdio},
    time::Duration,
};

use tempfile::TempDir;

pub struct TestDevice {
    pub id: String,
    pub token: String,
}

pub struct PostgresApp {
    base: String,
    mqtt_addr: SocketAddr,
    cookie: String,
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

impl PostgresApp {
    pub async fn start() -> Self {
        let url = std::env::var("IOT_TEST_POSTGRES_URL").expect("runner PostgreSQL URL");
        let container = std::env::var("IOT_TEST_POSTGRES_CONTAINER").expect("runner container");
        let expected_port: u16 = std::env::var("IOT_TEST_POSTGRES_PORT")
            .expect("runner mapped port")
            .parse()
            .expect("valid mapped port");
        assert_postgres_fixture(&url, &container, expected_port);

        let dir = tempfile::tempdir().expect("temp config dir");
        let http_port = crate::common::free_port();
        let mqtt_port = loop {
            let port = crate::common::free_port();
            if port != http_port {
                break port;
            }
        };
        let config = dir.path().join("conf.toml");
        std::fs::write(&config, format!("config_version=1\n[server]\ndata_dir='./data'\nshutdown_timeout_ms=1000\n[http]\nlisten='127.0.0.1:{http_port}'\n[mqtt]\nlisten='127.0.0.1:{mqtt_port}'\n[database]\nurl_env='IOT_TEST_POSTGRES_URL'\n[telemetry]\nqueue_capacity=16\nbatch_size=1\n"))
            .expect("write test config");

        let pool = sqlx::PgPool::connect(&url)
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
        let password_file = dir.path().join("admin-password");
        std::fs::write(&password_file, "mqtt-auth-test-password").expect("write password");
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
            .json(&serde_json::json!({"username":"admin","password":"mqtt-auth-test-password"}))
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
        Self {
            _dir: dir,
            base,
            mqtt_addr: format!("127.0.0.1:{mqtt_port}").parse().unwrap(),
            cookie,
            _child: child,
        }
    }

    pub fn mqtt_addr(&self) -> SocketAddr {
        self.mqtt_addr
    }

    pub async fn create_device(&self, name: &str) -> TestDevice {
        let response = reqwest::Client::new()
            .post(format!("{}/api/v1/devices", self.base))
            .header("Cookie", &self.cookie)
            .header("X-IoT-CSRF", "1")
            .json(&serde_json::json!({"name":name,"metadata":{}}))
            .send()
            .await
            .expect("create test device");
        assert_eq!(response.status(), reqwest::StatusCode::CREATED);
        let body: serde_json::Value = response.json().await.expect("device response");
        TestDevice {
            id: body["device"]["id"].as_str().expect("device id").to_owned(),
            token: body["credential"]["token"]
                .as_str()
                .expect("device token")
                .to_owned(),
        }
    }

    pub async fn assert_no_devices_online(&self) {
        let response = reqwest::Client::new()
            .get(format!("{}/api/v1/overview", self.base))
            .header("Cookie", &self.cookie)
            .send()
            .await
            .expect("query overview");
        assert!(response.status().is_success(), "overview query failed");
        let body: serde_json::Value = response.json().await.expect("overview");
        assert_eq!(body["devices"]["online"], 0);
    }
}

fn assert_postgres_fixture(url: &str, container: &str, expected_port: u16) {
    assert_eq!(
        docker_inspect("{{.Config.Image}}", container).trim(),
        "postgres:18.6-bookworm"
    );
    assert_eq!(
        docker_inspect(
            "{{ index .Config.Labels \"io.iot.test-fixture\" }}",
            container
        )
        .trim(),
        "mqtt-auth"
    );
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

#[cfg(test)]
mod tests {
    use super::validate_fixture_url;
    use std::net::IpAddr;

    #[test]
    fn fixture_url_guard_rejects_overrides() {
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
}
