mod common;

use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use common::TestApp;
use iot_server::core::auth;
use iot_server::core::telemetry::{self, TelemetryPayload};
use iot_server::storage::sqlite::telemetry as telemetry_storage;
use serde_json::{Value, json};
use tempfile::TempDir;

const ADMIN_PASSWORD: &str = "backup-restore-test-password";
const FAKE_DOCKER: &str = r##"#!/bin/sh
set -eu
case "$1" in
    compose)
        case " $* " in
            *" ps "*) echo test-container ;;
            *" stop "*) exit 0 ;;
            *) exit 2 ;;
        esac
        exit 0
        ;;
    inspect)
        case "$3" in
            *Mounts*) printf '%s\n' "$FAKE_DATA_DIR" ;;
            *) printf 'false\n' ;;
        esac
        exit 0
        ;;
    run) ;;
    *) exit 2 ;;
esac

shift
data_mount=
backup_mount=
while [ "$#" -gt 0 ]; do
    case "$1" in
        --volume)
            case "$2" in
                *:/data:Z) data_mount=${2%:/data:Z} ;;
                *:/backup:Z) backup_mount=${2%:/backup:Z} ;;
            esac
            shift 2
            ;;
        --user|--env|--entrypoint) shift 2 ;;
        --rm) shift ;;
        iot-server:local) shift; break ;;
        *) shift ;;
    esac
done
shift 2
shift 2
script=$1
BACKUP_DATA_ROOT="$data_mount" \
BACKUP_OUTPUT_ROOT="$backup_mount" \
BACKUP_UID="$(id -u)" \
BACKUP_GID="$(id -g)" \
PATH="$FAKE_COPY_BIN:$PATH" \
flock -n "$data_mount/.lock" /bin/sh -c "$script"
"##;
const FAKE_COPY: &str = r##"#!/bin/sh
if [ "${FAIL_BACKUP_COPY:-0}" = 1 ]; then
    printf partial > "$3"
    exit 1
fi
exec /bin/cp "$@"
"##;

struct ServerProcess(Child);

impl ServerProcess {
    fn start(config_path: &Path, working_dir: &Path) -> anyhow::Result<Self> {
        let child = Command::new(env!("CARGO_BIN_EXE_iot-server"))
            .args(["run", "--config"])
            .arg(config_path)
            .current_dir(working_dir)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()?;
        Ok(Self(child))
    }

    async fn wait_ready(&mut self, client: &reqwest::Client, base: &str) -> anyhow::Result<()> {
        for _ in 0..100 {
            if let Some(status) = self.0.try_wait()? {
                anyhow::bail!("restored server exited before ready: {status}");
            }
            if let Ok(response) = client.get(format!("{base}/readyz")).send().await
                && response.status().is_success()
            {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        anyhow::bail!("restored server did not become ready")
    }

    fn stop(&mut self) -> anyhow::Result<()> {
        let status = Command::new("kill")
            .args(["-TERM", &self.0.id().to_string()])
            .status()?;
        anyhow::ensure!(
            status.success(),
            "could not send SIGTERM to restored server"
        );
        let status = self.0.wait()?;
        anyhow::ensure!(
            status.success(),
            "restored server did not stop cleanly: {status}"
        );
        Ok(())
    }
}

impl Drop for ServerProcess {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

fn make_executable(path: &Path) -> anyhow::Result<()> {
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o755))?;
    Ok(())
}

fn run_backup_script(
    repo: &Path,
    data_dir: &Path,
    destination: &Path,
    stub_bin: &Path,
    fail_copy: bool,
) -> anyhow::Result<std::process::Output> {
    let path = std::env::join_paths(std::iter::once(stub_bin.to_path_buf()).chain(
        std::env::split_paths(&std::env::var_os("PATH").unwrap_or_default()),
    ))?;
    Ok(Command::new(repo.join("deploy/backup.sh"))
        .arg(destination)
        .env("PATH", path)
        .env("IOT_DATA_DIR", data_dir)
        .env("FAKE_DATA_DIR", data_dir)
        .env("FAKE_COPY_BIN", stub_bin)
        .env("FAIL_BACKUP_COPY", if fail_copy { "1" } else { "0" })
        .current_dir(repo)
        .output()?)
}

#[tokio::test]
async fn stopped_sqlite_backup_restores_device_telemetry_and_runtime_revision() -> anyhow::Result<()>
{
    let mut app = TestApp::start().await;
    app.create_admin("backup-restore-admin", ADMIN_PASSWORD)
        .await;
    let client = app.client();
    let login = client
        .post(format!("{}/api/v1/auth/login", app.base))
        .header("X-IoT-CSRF", "1")
        .json(&json!({"username":"backup-restore-admin","password":ADMIN_PASSWORD}))
        .send()
        .await?;
    anyhow::ensure!(login.status().is_success(), "source admin login failed");

    let device = app.create_device("backup-restore-device", true).await;
    let timestamp = auth::now_ms();
    let payload = json!({
        "event_id": "backup-restore-event",
        "ts": timestamp,
        "values": {"temperature": 21.5}
    });
    let raw = payload.to_string().into_bytes();
    let limits = TelemetryPayload::try_from(&app.config.config.telemetry)
        .map_err(|error| anyhow::anyhow!(error))?;
    let event = telemetry::validate(&raw, timestamp, limits)
        .map_err(|error| anyhow::anyhow!("{error:?}"))?;
    telemetry_storage::insert_batch(&app.pool, &[(device.id.clone(), event)]).await?;

    let update = client
        .patch(format!("{}/api/v1/config/runtime", app.base))
        .header("X-IoT-CSRF", "1")
        .json(&json!({
            "expected_revision": 0,
            "settings": {"logging":{"level":"debug"}}
        }))
        .send()
        .await?;
    anyhow::ensure!(
        update.status().is_success(),
        "source runtime config update failed"
    );

    let source_data = app.config.data_dir.clone();
    app.stop_for_backup().await;

    let restored_root = TempDir::new()?;
    let restored_data = restored_root.path().join("data");
    std::fs::create_dir(&restored_data)?;
    let stub_bin = restored_root.path().join("bin");
    std::fs::create_dir(&stub_bin)?;
    let fake_docker = stub_bin.join("docker");
    std::fs::write(&fake_docker, FAKE_DOCKER)?;
    make_executable(&fake_docker)?;
    let fake_copy = stub_bin.join("cp");
    std::fs::write(&fake_copy, FAKE_COPY)?;
    make_executable(&fake_copy)?;

    let failed_backup = restored_root.path().join("failed-backup");
    let failed = run_backup_script(&repo_root(), &source_data, &failed_backup, &stub_bin, true)?;
    anyhow::ensure!(
        !failed.status.success(),
        "injected copy error must fail backup"
    );
    anyhow::ensure!(
        !failed_backup.exists(),
        "failed copy was published as a backup"
    );

    let backup_dir = restored_root.path().join("backup-output");
    let backup = run_backup_script(&repo_root(), &source_data, &backup_dir, &stub_bin, false)?;
    anyhow::ensure!(
        backup.status.success(),
        "backup script failed: {}",
        String::from_utf8_lossy(&backup.stderr)
    );
    let checksum = Command::new("sha256sum")
        .args(["-c", "SHA256SUMS"])
        .current_dir(&backup_dir)
        .output()?;
    anyhow::ensure!(
        checksum.status.success(),
        "backup checksum verification failed"
    );
    for name in ["iot.db", "iot.db-wal", "iot.db-shm", "runtime.toml"] {
        let source = backup_dir.join(name);
        if source.is_file() {
            std::fs::copy(source, restored_data.join(name))?;
        }
    }
    drop(app);
    let restored_pool = sqlx::sqlite::SqlitePoolOptions::new()
        .connect_with(
            sqlx::sqlite::SqliteConnectOptions::new()
                .filename(restored_data.join("iot.db"))
                .create_if_missing(false),
        )
        .await?;
    let restored_admins: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM users WHERE username = ?")
        .bind("backup-restore-admin")
        .fetch_one(&restored_pool)
        .await?;
    restored_pool.close().await;
    anyhow::ensure!(restored_admins == 1, "backup is missing its admin record");
    let http_port = common::free_port();
    let config_path = restored_root.path().join("conf.toml");
    std::fs::write(
        &config_path,
        format!(
            "config_version = 1\n\
             [server]\ndata_dir = \"./data\"\n\
             [http]\nlisten = \"127.0.0.1:{http_port}\"\ncookie_secure = false\n\
             [mqtt]\nlisten = \"127.0.0.1:0\"\n\
             [database]\nurl = \"sqlite://./data/iot.db\"\n"
        ),
    )?;
    let base = format!("http://127.0.0.1:{http_port}");
    let client = reqwest::Client::builder().cookie_store(true).build()?;
    let mut server = ServerProcess::start(&config_path, restored_root.path())?;
    server.wait_ready(&client, &base).await?;

    let login = client
        .post(format!("{base}/api/v1/auth/login"))
        .header("X-IoT-CSRF", "1")
        .json(&json!({"username":"backup-restore-admin","password":ADMIN_PASSWORD}))
        .send()
        .await?;
    anyhow::ensure!(
        login.status().is_success(),
        "restored admin login failed: {} {}",
        login.status(),
        login.text().await.unwrap_or_default()
    );

    let device_response = client
        .get(format!("{base}/api/v1/devices/{}", device.id))
        .send()
        .await?
        .error_for_status()?;
    let device_json: Value = device_response.json().await?;
    anyhow::ensure!(device_json["id"] == device.id, "restored device is missing");

    let history: Value = client
        .get(format!(
            "{base}/api/v1/devices/{}/telemetry?after_id=0&limit=100",
            device.id
        ))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    anyhow::ensure!(
        history["items"].as_array().is_some_and(|items| items
            .iter()
            .any(|item| item["event_id"] == "backup-restore-event")),
        "restored telemetry is missing"
    );

    let config: Value = client
        .get(format!("{base}/api/v1/config"))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    anyhow::ensure!(config["revision"] == 1, "runtime revision was not restored");

    server.stop()?;
    Ok(())
}
