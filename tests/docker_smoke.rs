use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Duration;

use anyhow::{Context, bail};
use reqwest::{Client, StatusCode};
use rumqttc::{AsyncClient, Event, MqttOptions, Packet, QoS};
use serde_json::{Value, json};
use tempfile::TempDir;
use tokio::time::sleep;
use uuid::Uuid;

const IMAGE: &str = "iot-server:local";
const PROJECT_PREFIX: &str = "iot-a17-smoke";

#[tokio::test]
#[ignore = "requires Docker Engine and Compose"]
async fn compose_recreate_preserves_data_and_live_interfaces() -> anyhow::Result<()> {
    let repo = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let mut temp = DockerTestDir::new()?;
    let data_dir = temp.data_dir.clone();
    let password_file = temp.path().join("admin-password");
    let project = format!("{PROJECT_PREFIX}-{}", std::process::id());
    let http_port = free_port()?;
    let mqtt_port = loop {
        let port = free_port()?;
        if port != http_port {
            break port;
        }
    };
    assert!(
        !data_dir.exists(),
        "smoke test must begin without a data directory"
    );
    let compose = Compose::new(
        repo.join("deploy/compose.sqlite.yml"),
        project.clone(),
        data_dir.clone(),
        http_port,
        mqtt_port,
    );

    let mut build = Command::new("docker");
    build
        .current_dir(&repo)
        .args(["build", "-f", "deploy/Dockerfile", "-t", IMAGE, "."]);
    run_checked(&mut build, "build Docker image")?;
    let mut prepare = Command::new(repo.join("deploy/prepare-data.sh"));
    prepare.arg(&data_dir);
    run_checked(&mut prepare, "prepare new bind-mounted data directory")?;

    compose.up()?;
    wait_ready(http_port).await?;
    compose.stop()?;

    let password = format!("A17-{}-{}!", std::process::id(), Uuid::new_v4());
    std::fs::write(&password_file, &password)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&password_file, std::fs::Permissions::from_mode(0o600))?;
    }
    create_admin(&repo, &data_dir, &password_file)?;

    compose.up()?;
    wait_ready(http_port).await?;

    let first_admin = admin_client()?;
    login(&first_admin, http_port, &password).await?;
    let created = json_response(
        first_admin
            .post(format!("http://127.0.0.1:{http_port}/api/v1/devices"))
            .header("x-iot-csrf", "1")
            .json(&json!({"name":"a17-device","metadata":{},"enabled":true}))
            .send()
            .await?,
        StatusCode::CREATED,
    )
    .await?;
    let device_id = created["device"]["id"]
        .as_str()
        .context("device create response omitted device id")?
        .to_owned();
    let token = created["credential"]["token"]
        .as_str()
        .context("device create response omitted credential token")?
        .to_owned();

    let initial_config = get_json(
        &first_admin,
        format!("http://127.0.0.1:{http_port}/api/v1/config"),
    )
    .await?;
    let revision = initial_config["revision"]
        .as_u64()
        .context("config response omitted revision")?;
    let updated_config = json_response(
        first_admin
            .patch(format!(
                "http://127.0.0.1:{http_port}/api/v1/config/runtime"
            ))
            .header("x-iot-csrf", "1")
            .json(&json!({
                "expected_revision": revision,
                "settings": {"telemetry": {"retention_days": 9, "batch_size": 42}}
            }))
            .send()
            .await?,
        StatusCode::OK,
    )
    .await?;
    let runtime_revision = updated_config["revision"]
        .as_u64()
        .context("runtime update response omitted revision")?;

    let first_event = format!("a17-before-{}", Uuid::new_v4());
    assert_eq!(
        publish_telemetry(mqtt_port, &device_id, &token, &first_event, 21.5).await?["status"],
        "stored"
    );
    compose.down()?;

    compose.up()?;
    wait_ready(http_port).await?;

    let second_admin = admin_client()?;
    login(&second_admin, http_port, &password).await?;
    let device = get_json(
        &second_admin,
        format!("http://127.0.0.1:{http_port}/api/v1/devices/{device_id}"),
    )
    .await?;
    assert_eq!(device["id"], device_id);
    assert_eq!(device["name"], "a17-device");

    let persisted_config = get_json(
        &second_admin,
        format!("http://127.0.0.1:{http_port}/api/v1/config"),
    )
    .await?;
    assert_eq!(persisted_config["revision"], runtime_revision);
    assert_eq!(persisted_config["config"]["telemetry"]["retention_days"], 9);
    assert_eq!(persisted_config["config"]["telemetry"]["batch_size"], 42);

    let second_event = format!("a17-after-{}", Uuid::new_v4());
    assert_eq!(
        publish_telemetry(mqtt_port, &device_id, &token, &second_event, 22.5).await?["status"],
        "stored"
    );

    let history = get_json(
        &second_admin,
        format!(
            "http://127.0.0.1:{http_port}/api/v1/devices/{device_id}/telemetry?after_id=0&limit=100"
        ),
    )
    .await?;
    let events = history["items"]
        .as_array()
        .context("telemetry history response omitted items")?;
    assert!(events.iter().any(|event| event["event_id"] == first_event));
    assert!(events.iter().any(|event| event["event_id"] == second_event));

    let backup_dir = temp.path().join("backup");
    let wrong_project = format!("{project}-wrong");
    let wrong_project_backup = run_backup(&compose, &backup_dir, &wrong_project)?;
    assert!(!wrong_project_backup.status.success());
    assert!(!backup_dir.exists());
    wait_ready(http_port).await?;

    compose.stop()?;
    let lock_holder = format!("{project}-lock-holder");
    let mut hold_lock = Command::new("docker");
    hold_lock
        .args([
            "run",
            "--rm",
            "-d",
            "--name",
            &lock_holder,
            "--user",
            "10001:10001",
            "--volume",
        ])
        .arg(format!("{}:/data:Z", data_dir.display()))
        .args([
            "--entrypoint",
            "/usr/bin/flock",
            IMAGE,
            "/data/.lock",
            "sleep",
            "30",
        ]);
    run_checked(&mut hold_lock, "hold the data-directory lock")?;
    let lock_guard = DockerLockHolder(lock_holder.clone());
    let locked_backup = run_backup(&compose, &backup_dir, &project)?;
    assert!(
        !locked_backup.status.success(),
        "backup must fail when the lock is held"
    );
    assert!(!backup_dir.exists());
    let mut stop_lock = Command::new("docker");
    stop_lock.args(["stop", &lock_holder]);
    run_checked(&mut stop_lock, "release the data-directory lock")?;
    drop(lock_guard);

    let backup = run_backup(&compose, &backup_dir, &project)?;
    if !backup.status.success() {
        bail!(
            "could not create backup: {}\n{}",
            String::from_utf8_lossy(&backup.stderr),
            String::from_utf8_lossy(&backup.stdout)
        );
    }
    verify_private_backup(&backup_dir)?;

    compose.down()?;
    drop(compose);
    temp.cleanup_data()?;
    assert!(
        !data_dir.exists(),
        "Docker-owned test data directory remains"
    );
    let temp_path = temp.path().to_path_buf();
    drop(temp);
    assert!(
        !temp_path.exists(),
        "smoke-test temporary directory remains"
    );
    Ok(())
}

fn run_backup(
    compose: &Compose,
    destination: &Path,
    project: &str,
) -> anyhow::Result<std::process::Output> {
    let script = compose
        .file
        .parent()
        .context("compose file has no parent")?
        .join("backup.sh");
    Ok(Command::new(script)
        .arg(destination)
        .env("COMPOSE_PROJECT_NAME", project)
        .env("IOT_DATA_DIR", &compose.data_dir)
        .output()?)
}

fn verify_private_backup(path: &Path) -> anyhow::Result<()> {
    use std::os::unix::fs::{MetadataExt, PermissionsExt};

    let dir = std::fs::metadata(path)?;
    let parent = std::fs::metadata(path.parent().context("backup has no parent")?)?;
    anyhow::ensure!(dir.uid() == parent.uid());
    anyhow::ensure!(dir.gid() == parent.gid());
    anyhow::ensure!(dir.permissions().mode() & 0o777 == 0o700);
    for entry in std::fs::read_dir(path)? {
        let entry = entry?;
        let metadata = entry.metadata()?;
        anyhow::ensure!(metadata.uid() == parent.uid());
        anyhow::ensure!(metadata.gid() == parent.gid());
        anyhow::ensure!(metadata.permissions().mode() & 0o777 == 0o600);
    }
    let output = Command::new("sha256sum")
        .args(["-c", "SHA256SUMS"])
        .current_dir(path)
        .output()?;
    anyhow::ensure!(output.status.success(), "backup checksums did not verify");
    Ok(())
}

#[test]
fn prepare_data_refuses_nonempty_wrong_owner_before_running_docker() -> anyhow::Result<()> {
    use std::os::unix::fs::MetadataExt;
    use std::os::unix::fs::PermissionsExt;

    let temp = TempDir::new()?;
    let data_dir = temp.path().join("existing-data");
    std::fs::create_dir(&data_dir)?;
    let fixture = data_dir.join("keep.db");
    std::fs::write(&fixture, b"preserve")?;
    let metadata = std::fs::metadata(&data_dir)?;
    if metadata.uid() == 10001 && metadata.gid() == 10001 {
        return Ok(());
    }

    let stub_dir = temp.path().join("bin");
    std::fs::create_dir(&stub_dir)?;
    let docker_called = temp.path().join("docker-called");
    let docker_stub = stub_dir.join("docker");
    std::fs::write(
        &docker_stub,
        "#!/bin/sh\nprintf called > \"$DOCKER_STUB_MARKER\"\nexit 0\n",
    )?;
    std::fs::set_permissions(&docker_stub, std::fs::Permissions::from_mode(0o755))?;
    let path = std::env::join_paths(std::iter::once(stub_dir).chain(std::env::split_paths(
        &std::env::var_os("PATH").unwrap_or_default(),
    )))?;
    let script = Path::new(env!("CARGO_MANIFEST_DIR")).join("deploy/prepare-data.sh");
    let output = Command::new(script)
        .arg(&data_dir)
        .env("PATH", path)
        .env("DOCKER_STUB_MARKER", &docker_called)
        .output()?;

    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("refusing"));
    assert!(
        !docker_called.exists(),
        "preflight must reject before any Docker mount"
    );
    assert_eq!(std::fs::read(fixture)?, b"preserve");
    Ok(())
}

struct DockerTestDir {
    temp: TempDir,
    data_dir: PathBuf,
}

struct DockerLockHolder(String);

impl Drop for DockerLockHolder {
    fn drop(&mut self) {
        let _ = Command::new("docker")
            .args(["rm", "-f", &self.0])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status();
    }
}

impl DockerTestDir {
    fn new() -> anyhow::Result<Self> {
        let temp = TempDir::new()?;
        let data_dir = temp.path().join("data");
        Ok(Self { temp, data_dir })
    }

    fn path(&self) -> &Path {
        self.temp.path()
    }

    fn cleanup_data(&mut self) -> anyhow::Result<()> {
        if !self.data_dir.exists() {
            return Ok(());
        }
        let mut command = Command::new("docker");
        command
            .args(["run", "--rm", "--user", "0:0", "--volume"])
            .arg(format!("{}:/fixture:Z", self.temp.path().display()))
            .args(["--entrypoint", "/bin/rm", IMAGE, "-rf", "/fixture/data"]);
        run_checked(&mut command, "remove test-owned data directory")?;
        if self.data_dir.exists() {
            bail!("test-owned data directory was not removed");
        }
        Ok(())
    }
}

impl Drop for DockerTestDir {
    fn drop(&mut self) {
        if let Err(error) = self.cleanup_data() {
            eprintln!("failed to remove Docker smoke-test data: {error:#}");
        }
    }
}

struct Compose {
    file: PathBuf,
    project: String,
    data_dir: PathBuf,
    http_port: u16,
    mqtt_port: u16,
}

impl Compose {
    fn new(
        file: PathBuf,
        project: String,
        data_dir: PathBuf,
        http_port: u16,
        mqtt_port: u16,
    ) -> Self {
        Self {
            file,
            project,
            data_dir,
            http_port,
            mqtt_port,
        }
    }

    fn command(&self, args: &[&str]) -> Command {
        let mut command = Command::new("docker");
        command
            .args(["compose", "--project-name", &self.project, "--file"])
            .arg(&self.file)
            .args(args)
            .env("IOT_DATA_DIR", &self.data_dir)
            .env("IOT_HTTP_PORT", self.http_port.to_string())
            .env("IOT_MQTT_PORT", self.mqtt_port.to_string());
        command
    }

    fn up(&self) -> anyhow::Result<()> {
        let mut command = self.command(&["up", "-d"]);
        run_checked(&mut command, "start Compose service")?;
        Ok(())
    }

    fn down(&self) -> anyhow::Result<()> {
        let mut command = self.command(&["down", "--remove-orphans"]);
        run_checked(&mut command, "stop Compose service")?;
        Ok(())
    }

    fn stop(&self) -> anyhow::Result<()> {
        let mut command = self.command(&["stop", "iot-server"]);
        run_checked(&mut command, "stop Compose service")?;
        Ok(())
    }
}

impl Drop for Compose {
    fn drop(&mut self) {
        let _ = self.command(&["down", "--remove-orphans"]).output();
    }
}

fn run_checked(command: &mut Command, action: &str) -> anyhow::Result<String> {
    let output = command
        .output()
        .with_context(|| format!("could not {action}"))?;
    if !output.status.success() {
        bail!(
            "could not {action}: {}\n{}",
            String::from_utf8_lossy(&output.stderr),
            String::from_utf8_lossy(&output.stdout)
        );
    }
    Ok(String::from_utf8_lossy(&output.stdout).into_owned())
}

fn free_port() -> anyhow::Result<u16> {
    Ok(TcpListener::bind(("127.0.0.1", 0))?.local_addr()?.port())
}

fn create_admin(repo: &Path, data_dir: &Path, password_file: &Path) -> anyhow::Result<()> {
    let mut command = Command::new("docker");
    command
        .current_dir(repo)
        .args(["run", "--rm", "--user", "0:0", "--volume"])
        .arg(format!("{}:/app/data:Z", data_dir.display()))
        .args(["--volume"])
        .arg(format!(
            "{}:/app/conf.toml:ro,Z",
            repo.join("deploy/conf.docker.toml").display()
        ))
        .args(["--volume"])
        .arg(format!(
            "{}:/run/admin-password:ro,Z",
            password_file.display()
        ))
        .args([
            IMAGE,
            "--config",
            "/app/conf.toml",
            "admin",
            "create",
            "--username",
            "smoke-admin",
            "--password-file",
            "/run/admin-password",
        ]);
    run_checked(&mut command, "create test admin")?;
    Ok(())
}

async fn wait_ready(http_port: u16) -> anyhow::Result<()> {
    let url = format!("http://127.0.0.1:{http_port}/readyz");
    let client = Client::new();
    for _ in 0..60 {
        if client
            .get(&url)
            .send()
            .await
            .is_ok_and(|response| response.status().is_success())
        {
            return Ok(());
        }
        sleep(Duration::from_millis(500)).await;
    }
    bail!("Compose service did not become ready: {url}")
}

fn admin_client() -> anyhow::Result<Client> {
    Ok(Client::builder().cookie_store(true).build()?)
}

async fn login(client: &Client, http_port: u16, password: &str) -> anyhow::Result<()> {
    let response = client
        .post(format!("http://127.0.0.1:{http_port}/api/v1/auth/login"))
        .header("x-iot-csrf", "1")
        .json(&json!({"username":"smoke-admin","password":password}))
        .send()
        .await?;
    json_response(response, StatusCode::OK).await?;
    Ok(())
}

async fn get_json(client: &Client, url: String) -> anyhow::Result<Value> {
    json_response(client.get(url).send().await?, StatusCode::OK).await
}

async fn json_response(response: reqwest::Response, expected: StatusCode) -> anyhow::Result<Value> {
    let status = response.status();
    let body = response.text().await?;
    if status != expected {
        bail!("expected HTTP {expected}, got {status}: {body}");
    }
    Ok(serde_json::from_str(&body)?)
}

async fn publish_telemetry(
    port: u16,
    device_id: &str,
    token: &str,
    event_id: &str,
    temperature: f64,
) -> anyhow::Result<Value> {
    let mut options = MqttOptions::new(device_id, "127.0.0.1", port);
    options
        .set_keep_alive(Duration::from_secs(10))
        .set_clean_session(true)
        .set_credentials(device_id, token);
    let (client, mut event_loop) = AsyncClient::new(options, 16);
    let ack_topic = format!("devices/{device_id}/telemetry/ack");
    let payload = serde_json::to_vec(&json!({
        "event_id": event_id,
        "values": {"temperature": temperature}
    }))?;

    let ack = tokio::time::timeout(Duration::from_secs(15), async {
        let mut subscribed = false;
        let mut published = false;
        loop {
            match event_loop.poll().await? {
                Event::Incoming(Packet::ConnAck(ack)) => {
                    if ack.code != rumqttc::ConnectReturnCode::Success {
                        bail!("device MQTT credential rejected: {:?}", ack.code);
                    }
                    client.subscribe(&ack_topic, QoS::AtLeastOnce).await?;
                }
                Event::Incoming(Packet::SubAck(_)) if !subscribed => {
                    subscribed = true;
                    client
                        .publish(
                            format!("devices/{device_id}/telemetry"),
                            QoS::AtLeastOnce,
                            false,
                            payload.clone(),
                        )
                        .await?;
                    published = true;
                }
                Event::Incoming(Packet::Publish(message))
                    if subscribed && published && message.topic == ack_topic =>
                {
                    break Ok::<_, anyhow::Error>(serde_json::from_slice(&message.payload)?);
                }
                _ => {}
            }
        }
    })
    .await??;
    client.disconnect().await?;
    Ok(ack)
}
