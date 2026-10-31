mod common;

use std::path::Path;
use std::process::{Child, Command, Stdio};

use common::{PASSWORD, TestApp, cookie_client, free_port};
use iot_server::core::telemetry::ValidatedTelemetry;
use iot_server::storage::sqlite::telemetry;
use iot_server::storage::sqlite::{pool, users};
use serde_json::{Value, json};

struct ServerProcess(Child);

impl ServerProcess {
    fn start(config_path: &Path) -> Self {
        Self::spawn(config_path, None)
    }

    fn start_paused(config_path: &Path, marker: &Path) -> Self {
        Self::spawn(config_path, Some(marker))
    }

    fn spawn(config_path: &Path, pause_marker: Option<&Path>) -> Self {
        let mut command = Command::new(env!("CARGO_BIN_EXE_iot-server"));
        command
            .args(["run", "--config"])
            .arg(config_path)
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        if let Some(marker) = pause_marker {
            command.env("IOT_TEST_PAUSE_BEFORE_DATA_DIR_LOCK", marker);
        }
        Self(command.spawn().unwrap())
    }

    fn stop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

impl Drop for ServerProcess {
    fn drop(&mut self) {
        self.stop();
    }
}

async fn wait_server(server: &mut ServerProcess, client: &reqwest::Client, base: &str) {
    for _ in 0..100 {
        if let Some(status) = server.0.try_wait().unwrap() {
            panic!("server exited before ready: {status}");
        }
        if let Ok(response) = client.get(format!("{base}/readyz")).send().await
            && response.status() == reqwest::StatusCode::OK
        {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    panic!("server at {base} did not become ready");
}

async fn admin_client(app: &TestApp, username: &str) -> reqwest::Client {
    app.create_admin(username, PASSWORD).await;
    let client = app.client();
    let response = client
        .post(format!("{}/api/v1/auth/login", app.base))
        .header("X-IoT-CSRF", "1")
        .json(&json!({"username":username,"password":PASSWORD}))
        .send()
        .await
        .unwrap();
    assert!(response.status().is_success());
    client
}

async fn patch(
    client: &reqwest::Client,
    app: &TestApp,
    expected_revision: u64,
    settings: Value,
) -> reqwest::Response {
    client
        .patch(format!("{}/api/v1/config/runtime", app.base))
        .header("X-IoT-CSRF", "1")
        .json(&json!({"expected_revision":expected_revision,"settings":settings}))
        .send()
        .await
        .unwrap()
}

#[tokio::test]
async fn runtime_patch_persists_preserves_overrides_and_broadcasts_revisions() {
    let app = TestApp::builder()
        .metrics_token("runtime-config-secret-token-0123456789")
        .start()
        .await;
    let client = admin_client(&app, "runtime-config-admin").await;
    let mut revisions = app.events.subscribe_config_revisions();

    let response = client
        .get(format!("{}/api/v1/config", app.base))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    let body: Value = response.json().await.unwrap();
    assert_eq!(body["revision"], 0);
    assert_eq!(body["restart_required"], json!([]));
    assert_eq!(body["config"]["database"]["url"], "<redacted>");
    assert_eq!(
        body["config"]["http"]["metrics_bearer_token_env"],
        "<redacted>"
    );
    let serialized = body.to_string();
    assert!(!serialized.contains("runtime-config-secret-token-0123456789"));
    assert!(!serialized.contains("TEST_METRICS_BEARER_TOKEN"));
    assert!(!serialized.contains("sqlite://"));

    let response = patch(
        &client,
        &app,
        0,
        json!({
            "logging":{"level":"debug"},
            "telemetry":{"batch_size":200,"retention_days":14}
        }),
    )
    .await;
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    let body: Value = response.json().await.unwrap();
    assert_eq!(body["revision"], 1);
    assert_eq!(body["config"]["logging"]["level"], "debug");
    assert_eq!(body["config"]["telemetry"]["batch_size"], 200);
    assert_eq!(revisions.try_recv().unwrap(), 1);

    let first_runtime_file = std::fs::read_to_string(&app.config.runtime_settings_path).unwrap();
    assert!(first_runtime_file.contains("revision = 1"));
    assert!(first_runtime_file.contains("level = \"debug\""));
    assert!(first_runtime_file.contains("batch_size = 200"));
    assert!(!first_runtime_file.contains("database"));
    assert!(!first_runtime_file.contains("http"));

    let response = patch(
        &client,
        &app,
        1,
        json!({"telemetry":{"flush_interval_ms":250}}),
    )
    .await;
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    let body: Value = response.json().await.unwrap();
    assert_eq!(body["revision"], 2);
    assert_eq!(body["config"]["logging"]["level"], "debug");
    assert_eq!(body["config"]["telemetry"]["batch_size"], 200);
    assert_eq!(body["config"]["telemetry"]["flush_interval_ms"], 250);
    assert_eq!(revisions.try_recv().unwrap(), 2);

    let audit_details: Vec<String> = sqlx::query_scalar(
        "SELECT details_json FROM audit_events WHERE action = 'config.update' ORDER BY id",
    )
    .fetch_all(&app.pool)
    .await
    .unwrap();
    assert_eq!(audit_details.len(), 2);
    let first_audit: Value = serde_json::from_str(&audit_details[0]).unwrap();
    assert_eq!(first_audit["old_revision"], 0);
    assert_eq!(first_audit["new_revision"], 1);
    assert!(
        first_audit["paths"]
            .as_array()
            .unwrap()
            .contains(&json!("logging.level"))
    );
    assert!(
        !audit_details
            .iter()
            .any(|details| details.contains("debug"))
    );
    assert!(
        !audit_details
            .iter()
            .any(|details| details.contains("runtime-config-secret-token"))
    );

    let reloaded = iot_server::config::load_with_env(&app.config_path, |name| {
        (name == "TEST_METRICS_BEARER_TOKEN")
            .then(|| "runtime-config-secret-token-0123456789".to_owned())
    })
    .unwrap();
    let snapshot = reloaded.runtime_config.load();
    assert_eq!(snapshot.revision, 2);
    assert_eq!(
        snapshot.config.logging.level,
        iot_server::config::LogLevel::Debug
    );
    assert_eq!(snapshot.config.telemetry.batch_size, 200);
    assert_eq!(snapshot.config.telemetry.flush_interval_ms, 250);
}

#[tokio::test]
async fn runtime_patch_survives_a_real_server_restart() {
    let dir = tempfile::TempDir::new().unwrap();
    let config_path = dir.path().join("conf.toml");
    let http_port = free_port();
    let mqtt_port = free_port();
    std::fs::write(
        &config_path,
        format!(
            "config_version = 1\n\
             [server]\n\
             data_dir = \"./data\"\n\
             [http]\n\
             listen = \"127.0.0.1:{http_port}\"\n\
             dashboard = false\n\
             [mqtt]\n\
             listen = \"127.0.0.1:{mqtt_port}\"\n\
             [database]\n\
             url = \"sqlite://./data/iot.db\"\n\
             [runtime]\n\
             settings_path = \"./data/runtime.toml\"\n"
        ),
    )
    .unwrap();
    let base = format!("http://127.0.0.1:{http_port}");
    let client = cookie_client();
    let mut server = ServerProcess::start(&config_path);
    wait_server(&mut server, &client, &base).await;

    let loaded = iot_server::config::load(&config_path).unwrap();
    let pool = pool::connect(&loaded).await.unwrap();
    let password_hash = iot_server::core::auth::hash_password(PASSWORD).unwrap();
    let mut transaction = pool.begin().await.unwrap();
    users::create_admin(
        &mut transaction,
        "runtime-restart-admin",
        &password_hash,
        iot_server::core::auth::now_ms(),
    )
    .await
    .unwrap();
    transaction.commit().await.unwrap();
    let login = client
        .post(format!("{base}/api/v1/auth/login"))
        .header("X-IoT-CSRF", "1")
        .json(&json!({"username":"runtime-restart-admin","password":PASSWORD}))
        .send()
        .await
        .unwrap();
    assert!(login.status().is_success());

    let response = client
        .patch(format!("{base}/api/v1/config/runtime"))
        .header("X-IoT-CSRF", "1")
        .json(&json!({
            "expected_revision":0,
            "settings":{"logging":{"level":"debug"},"telemetry":{"retention_days":7}}
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    assert_eq!(response.json::<Value>().await.unwrap()["revision"], 1);
    pool.close().await;

    server.stop();
    let mut restarted = ServerProcess::start(&config_path);
    wait_server(&mut restarted, &client, &base).await;
    let response = client
        .get(format!("{base}/api/v1/config"))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    let body: Value = response.json().await.unwrap();
    assert_eq!(body["revision"], 1);
    assert_eq!(body["config"]["logging"]["level"], "debug");
    assert_eq!(body["config"]["telemetry"]["retention_days"], 7);
    restarted.stop();
}

#[tokio::test]
#[cfg_attr(
    not(debug_assertions),
    ignore = "startup race synchronization hook is debug-profile only"
)]
async fn startup_reloads_runtime_revision_after_waiting_for_the_data_dir_lock() {
    let dir = tempfile::TempDir::new().unwrap();
    let config_path = dir.path().join("conf.toml");
    let http_port = free_port();
    let mqtt_port = free_port();
    std::fs::write(
        &config_path,
        format!(
            "config_version = 1\n\
             [server]\n\
             data_dir = \"./data\"\n\
             [http]\n\
             listen = \"127.0.0.1:{http_port}\"\n\
             dashboard = false\n\
             [mqtt]\n\
             listen = \"127.0.0.1:{mqtt_port}\"\n\
             [database]\n\
             url = \"sqlite://./data/iot.db\"\n\
             [runtime]\n\
             settings_path = \"./data/runtime.toml\"\n"
        ),
    )
    .unwrap();
    let base = format!("http://127.0.0.1:{http_port}");
    let client = cookie_client();
    let mut first = ServerProcess::start(&config_path);
    wait_server(&mut first, &client, &base).await;

    let loaded = iot_server::config::load(&config_path).unwrap();
    let pool = pool::connect(&loaded).await.unwrap();
    let password_hash = iot_server::core::auth::hash_password(PASSWORD).unwrap();
    let mut transaction = pool.begin().await.unwrap();
    users::create_admin(
        &mut transaction,
        "runtime-lock-admin",
        &password_hash,
        iot_server::core::auth::now_ms(),
    )
    .await
    .unwrap();
    transaction.commit().await.unwrap();
    pool.close().await;
    let login = client
        .post(format!("{base}/api/v1/auth/login"))
        .header("X-IoT-CSRF", "1")
        .json(&json!({"username":"runtime-lock-admin","password":PASSWORD}))
        .send()
        .await
        .unwrap();
    assert!(login.status().is_success());

    let response = client
        .patch(format!("{base}/api/v1/config/runtime"))
        .header("X-IoT-CSRF", "1")
        .json(&json!({"expected_revision":0,"settings":{"logging":{"level":"debug"}}}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    assert_eq!(response.json::<Value>().await.unwrap()["revision"], 1);

    let marker = dir.path().join("process-b-loaded-config");
    let mut second = ServerProcess::start_paused(&config_path, &marker);
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            if let Some(status) = second.0.try_wait().unwrap() {
                panic!("second server exited before startup pause: {status}");
            }
            if marker.exists() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("second process should pause after reading revision 1 and before locking");

    let response = client
        .patch(format!("{base}/api/v1/config/runtime"))
        .header("X-IoT-CSRF", "1")
        .json(&json!({"expected_revision":1,"settings":{"telemetry":{"retention_days":9}}}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    assert_eq!(response.json::<Value>().await.unwrap()["revision"], 2);
    first.stop();
    std::fs::remove_file(marker).unwrap();

    wait_server(&mut second, &client, &base).await;
    let response = client
        .get(format!("{base}/api/v1/config"))
        .send()
        .await
        .unwrap();
    let body: Value = response.json().await.unwrap();
    assert_eq!(body["revision"], 2);
    assert_eq!(body["config"]["logging"]["level"], "debug");
    assert_eq!(body["config"]["telemetry"]["retention_days"], 9);

    let response = client
        .patch(format!("{base}/api/v1/config/runtime"))
        .header("X-IoT-CSRF", "1")
        .json(&json!({"expected_revision":2,"settings":{"telemetry":{"flush_interval_ms":250}}}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    assert_eq!(response.json::<Value>().await.unwrap()["revision"], 3);
    second.stop();
}

#[tokio::test]
async fn runtime_patch_distinguishes_unknown_restart_noop_and_invalid_fields() {
    let app = TestApp::start().await;
    let client = admin_client(&app, "runtime-validation-admin").await;
    let mut revisions = app.events.subscribe_config_revisions();
    let original = std::fs::read_to_string(&app.config.runtime_settings_path).ok();
    let audit_count = || async {
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM audit_events WHERE action = 'config.update'",
        )
        .fetch_one(&app.pool)
        .await
        .unwrap()
    };

    let unknown = patch(&client, &app, 0, json!({"logging":{"level":"verbose"}})).await;
    assert_eq!(unknown.status(), reqwest::StatusCode::BAD_REQUEST);
    assert_eq!(
        unknown.json::<Value>().await.unwrap()["code"],
        "validation_error"
    );

    let absent = patch(&client, &app, 0, json!({"telemetry":{"not_a_field":1}})).await;
    assert_eq!(absent.status(), reqwest::StatusCode::BAD_REQUEST);
    assert_eq!(
        absent.json::<Value>().await.unwrap()["code"],
        "validation_error"
    );

    for settings in [
        json!({"logging.level":"debug"}),
        json!({"logging.level":"debug","logging":{"level":"warn"}}),
    ] {
        let flat = patch(&client, &app, 0, settings).await;
        assert_eq!(flat.status(), reqwest::StatusCode::BAD_REQUEST);
        assert_eq!(
            flat.json::<Value>().await.unwrap()["code"],
            "validation_error"
        );
    }

    let restart = patch(
        &client,
        &app,
        0,
        json!({"http":{"listen":"127.0.0.1:9000"}}),
    )
    .await;
    assert_eq!(restart.status(), reqwest::StatusCode::UNPROCESSABLE_ENTITY);
    let restart: Value = restart.json().await.unwrap();
    assert_eq!(restart["code"], "restart_required");
    assert_eq!(
        restart["details"]["restart_required"],
        json!(["http.listen"])
    );

    let invalid = patch(&client, &app, 0, json!({"telemetry":{"batch_size":3000}})).await;
    assert_eq!(invalid.status(), reqwest::StatusCode::BAD_REQUEST);
    assert_eq!(
        invalid.json::<Value>().await.unwrap()["code"],
        "validation_error"
    );
    assert_eq!(app.config.runtime_config.load().revision, 0);
    assert_eq!(
        std::fs::read_to_string(&app.config.runtime_settings_path).ok(),
        original
    );
    assert_eq!(audit_count().await, 0);

    let empty = patch(&client, &app, 0, json!({})).await;
    assert_eq!(empty.status(), reqwest::StatusCode::BAD_REQUEST);
    assert_eq!(
        empty.json::<Value>().await.unwrap()["code"],
        "validation_error"
    );
    assert_eq!(audit_count().await, 0);

    let noop = patch(&client, &app, 0, json!({"logging":{"level":"info"}})).await;
    assert_eq!(noop.status(), reqwest::StatusCode::OK);
    assert_eq!(noop.json::<Value>().await.unwrap()["revision"], 0);
    assert_eq!(
        std::fs::read_to_string(&app.config.runtime_settings_path).ok(),
        original
    );
    assert_eq!(audit_count().await, 0);
    assert!(matches!(
        revisions.try_recv(),
        Err(tokio::sync::broadcast::error::TryRecvError::Empty)
    ));
}

#[tokio::test]
async fn runtime_rename_failure_returns_config_write_failed_without_applying() {
    let app = TestApp::start().await;
    let client = admin_client(&app, "runtime-write-failure-admin").await;
    std::fs::create_dir(&app.config.runtime_settings_path).unwrap();

    let response = patch(&client, &app, 0, json!({"logging":{"level":"debug"}})).await;
    assert_eq!(
        response.status(),
        reqwest::StatusCode::INTERNAL_SERVER_ERROR
    );
    let body: Value = response.json().await.unwrap();
    assert_eq!(body["code"], "config_write_failed");
    assert_eq!(body["details"]["applied"], false);
    assert_eq!(body["details"]["stage"], "rename");
    assert_eq!(app.config.runtime_config.load().revision, 0);
    assert_eq!(
        app.config.runtime_config.load().config.logging.level,
        iot_server::config::LogLevel::Info
    );
    let temp_files = std::fs::read_dir(app.config.runtime_settings_path.parent().unwrap())
        .unwrap()
        .filter_map(Result::ok)
        .filter(|entry| {
            entry
                .file_name()
                .to_string_lossy()
                .starts_with(".runtime.toml.")
        })
        .count();
    assert_eq!(temp_files, 0);
}

#[tokio::test]
async fn concurrent_runtime_patches_with_one_revision_have_one_winner() {
    let app = TestApp::start().await;
    let client = admin_client(&app, "runtime-concurrency-admin").await;
    let first = patch(&client, &app, 0, json!({"logging":{"level":"debug"}}));
    let second = patch(&client, &app, 0, json!({"telemetry":{"retention_days":7}}));
    let (first, second) = tokio::join!(first, second);
    let statuses = [first.status(), second.status()];
    assert_eq!(
        statuses
            .iter()
            .filter(|status| **status == reqwest::StatusCode::OK)
            .count(),
        1
    );
    assert_eq!(
        statuses
            .iter()
            .filter(|status| **status == reqwest::StatusCode::CONFLICT)
            .count(),
        1
    );
    let conflict = if first.status() == reqwest::StatusCode::CONFLICT {
        first
    } else {
        second
    };
    let conflict: Value = conflict.json().await.unwrap();
    assert_eq!(conflict["details"]["current_revision"], 1);
    assert_eq!(app.config.runtime_config.load().revision, 1);
    assert!(
        std::fs::read_to_string(&app.config.runtime_settings_path)
            .unwrap()
            .contains("revision = 1")
    );
}

#[tokio::test]
async fn config_audit_failure_does_not_roll_back_the_published_snapshot() {
    let app = TestApp::start().await;
    let client = admin_client(&app, "runtime-audit-admin").await;
    let mut revisions = app.events.subscribe_config_revisions();
    let audit_errors_before = iot_server::observability::metrics::Metrics::global()
        .audit_write_errors_total
        .get();
    sqlx::query(
        "CREATE TRIGGER fail_runtime_config_audit BEFORE INSERT ON audit_events
         WHEN NEW.action = 'config.update'
         BEGIN SELECT RAISE(FAIL, 'injected audit failure'); END",
    )
    .execute(&app.pool)
    .await
    .unwrap();

    let response = patch(&client, &app, 0, json!({"logging":{"level":"debug"}})).await;
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    assert_eq!(app.config.runtime_config.load().revision, 1);
    assert_eq!(revisions.try_recv().unwrap(), 1);
    assert_eq!(
        iot_server::observability::metrics::Metrics::global()
            .audit_write_errors_total
            .get(),
        audit_errors_before + 1
    );
    assert!(
        std::fs::read_to_string(&app.config.runtime_settings_path)
            .unwrap()
            .contains("level = \"debug\"")
    );
    sqlx::query("DROP TRIGGER fail_runtime_config_audit")
        .execute(&app.pool)
        .await
        .unwrap();

    sqlx::query(
        "CREATE TABLE audit_commit_guard (
             user_id TEXT REFERENCES users(id) DEFERRABLE INITIALLY DEFERRED
         )",
    )
    .execute(&app.pool)
    .await
    .unwrap();
    sqlx::query(
        "CREATE TRIGGER fail_runtime_config_audit_commit AFTER INSERT ON audit_events
         WHEN NEW.action = 'config.update'
         BEGIN INSERT INTO audit_commit_guard(user_id) VALUES ('missing-user'); END",
    )
    .execute(&app.pool)
    .await
    .unwrap();
    let commit_errors_before = iot_server::observability::metrics::Metrics::global()
        .audit_write_errors_total
        .get();
    let response = patch(&client, &app, 1, json!({"telemetry":{"retention_days":7}})).await;
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    assert_eq!(app.config.runtime_config.load().revision, 2);
    assert_eq!(revisions.try_recv().unwrap(), 2);
    assert_eq!(
        iot_server::observability::metrics::Metrics::global()
            .audit_write_errors_total
            .get(),
        commit_errors_before + 1
    );
    let committed_audits: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM audit_events WHERE action = 'config.update'")
            .fetch_one(&app.pool)
            .await
            .unwrap();
    assert_eq!(committed_audits, 0);
    sqlx::query("DROP TRIGGER fail_runtime_config_audit_commit")
        .execute(&app.pool)
        .await
        .unwrap();
    sqlx::query("DROP TABLE audit_commit_guard")
        .execute(&app.pool)
        .await
        .unwrap();
}

#[tokio::test]
async fn audit_and_revision_broadcast_finish_after_patch_request_times_out() {
    let app = TestApp::builder().request_timeout_ms(1_000).start().await;
    let client = admin_client(&app, "runtime-timeout-admin").await;
    let mut revisions = app.events.subscribe_config_revisions();
    let mut blocker = app.pool.acquire().await.unwrap();
    sqlx::query("BEGIN IMMEDIATE")
        .execute(&mut *blocker)
        .await
        .unwrap();

    let first = tokio::time::timeout(
        std::time::Duration::from_secs(3),
        patch(&client, &app, 0, json!({"logging":{"level":"debug"}})),
    )
    .await
    .expect("HTTP middleware should time out the blocked audit response");
    assert_eq!(first.status(), reqwest::StatusCode::REQUEST_TIMEOUT);
    assert_eq!(app.config.runtime_config.load().revision, 1);
    let audit_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM audit_events WHERE action = 'config.update'")
            .fetch_one(&app.pool)
            .await
            .unwrap();
    assert_eq!(audit_count, 0);
    assert!(matches!(
        revisions.try_recv(),
        Err(tokio::sync::broadcast::error::TryRecvError::Empty)
    ));

    let second = patch(&client, &app, 1, json!({"telemetry":{"retention_days":7}}));
    tokio::pin!(second);
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(100), &mut second)
            .await
            .is_err()
    );
    assert_eq!(app.config.runtime_config.load().revision, 1);

    sqlx::query("ROLLBACK")
        .execute(&mut *blocker)
        .await
        .unwrap();
    let second = tokio::time::timeout(std::time::Duration::from_secs(3), &mut second)
        .await
        .expect("next PATCH should proceed after post-commit audit releases the lock");
    assert_eq!(second.status(), reqwest::StatusCode::OK);
    assert_eq!(app.config.runtime_config.load().revision, 2);
    assert_eq!(revisions.try_recv().unwrap(), 1);
    assert_eq!(revisions.try_recv().unwrap(), 2);
    let audit_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM audit_events WHERE action = 'config.update'")
            .fetch_one(&app.pool)
            .await
            .unwrap();
    assert_eq!(audit_count, 2);
}

#[tokio::test]
async fn event_id_can_be_inserted_again_after_retention_deletes_its_old_row() {
    let app = TestApp::start().await;
    let device = app.create_device("runtime-retention-t17", true).await;
    let received_at = iot_server::core::auth::now_ms();
    let event = ValidatedTelemetry {
        event_id: "retention-retry".to_owned(),
        ts: received_at,
        received_at,
        values: serde_json::Map::from_iter([("x".to_owned(), json!(1))]),
        payload_sha256: [7; 32],
        raw_bytes: 10,
    };
    assert_eq!(
        telemetry::insert_batch(&app.pool, &[(device.id.clone(), event.clone())])
            .await
            .unwrap(),
        vec![iot_server::core::telemetry::TelemetryInsertResult::Stored]
    );
    sqlx::query("UPDATE telemetry_events SET received_at = ? WHERE device_id = ? AND event_id = ?")
        .bind(received_at - 40 * 86_400_000)
        .bind(&device.id)
        .bind(&event.event_id)
        .execute(&app.pool)
        .await
        .unwrap();
    assert_eq!(
        telemetry::delete_before(&app.pool, received_at - 30 * 86_400_000, 5_000)
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        telemetry::insert_batch(&app.pool, &[(device.id, event)])
            .await
            .unwrap(),
        vec![iot_server::core::telemetry::TelemetryInsertResult::Stored]
    );
}

#[tokio::test]
async fn malformed_runtime_file_fails_startup_load() {
    let app = TestApp::start().await;
    std::fs::write(
        &app.config.runtime_settings_path,
        "revision = 1\n[telemetry]\nunknown = 2\n",
    )
    .unwrap();
    assert!(iot_server::config::load(&app.config_path).is_err());
}
