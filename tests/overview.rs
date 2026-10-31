mod common;

use common::TestApp;
use iot_server::core::auth;
use iot_server::observability::metrics::{IngestRejectReason, Metrics};
use serde_json::{Value, json};

async fn admin_client(app: &TestApp) -> reqwest::Client {
    app.create_admin("overview-admin", "overview-test-password")
        .await;
    let client = app.client();
    let response = client
        .post(format!("{}/api/v1/auth/login", app.base))
        .header("X-IoT-CSRF", "1")
        .json(&json!({
            "username": "overview-admin",
            "password": "overview-test-password",
        }))
        .send()
        .await
        .unwrap();
    assert!(response.status().is_success());
    client
}

async fn create_command(client: &reqwest::Client, app: &TestApp, device_id: &str) -> String {
    let response = client
        .post(format!("{}/api/v1/devices/{device_id}/commands", app.base))
        .header("X-IoT-CSRF", "1")
        .json(&json!({"name":"reboot"}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::ACCEPTED);
    response.json::<Value>().await.unwrap()["command_id"]
        .as_str()
        .unwrap()
        .to_owned()
}

#[tokio::test]
async fn overview_uses_database_windows_and_live_presence() {
    let app = TestApp::start().await;
    let client = admin_client(&app).await;
    let enabled = app.create_device("overview-enabled", true).await;
    let _disabled = app.create_device("overview-disabled", false).await;
    assert!(
        app.presence
            .connect(&enabled.id, "overview-generation")
            .unwrap()
    );

    let now = auth::now_ms();
    for (suffix, received_at) in [("current", now - 2_000), ("stale", now - 86_402_000)] {
        sqlx::query(
            "INSERT INTO telemetry_events
             (device_id, event_id, ts, received_at, values_json, payload_sha256)
             VALUES (?, ?, ?, ?, '{}', ?)",
        )
        .bind(&enabled.id)
        .bind(format!("overview-{suffix}"))
        .bind(received_at)
        .bind(received_at)
        .bind([0_u8; 32].as_slice())
        .execute(&app.pool)
        .await
        .unwrap();
    }

    let _pending_id = create_command(&client, &app, &enabled.id).await;
    let dispatched_id = create_command(&client, &app, &enabled.id).await;
    let failed_id = create_command(&client, &app, &enabled.id).await;
    let expired_recent_id = create_command(&client, &app, &enabled.id).await;
    let expired_old_id = create_command(&client, &app, &enabled.id).await;
    let failed_at = now - 2_000;
    let expired_recent_at = now - 3_000;
    let expired_old_at = now - 86_402_000;
    sqlx::query("UPDATE commands SET status = 'dispatched' WHERE id = ?")
        .bind(&dispatched_id)
        .execute(&app.pool)
        .await
        .unwrap();
    sqlx::query("UPDATE commands SET status = 'failed', completed_at = ? WHERE id = ?")
        .bind(failed_at)
        .bind(&failed_id)
        .execute(&app.pool)
        .await
        .unwrap();
    for (id, completed_at) in [
        (&expired_recent_id, expired_recent_at),
        (&expired_old_id, expired_old_at),
    ] {
        sqlx::query(
            "UPDATE commands
             SET status = 'expired', completed_at = ?, created_at = ?, expires_at = ? WHERE id = ?",
        )
        .bind(completed_at)
        .bind(completed_at - 1_000)
        .bind(completed_at)
        .bind(id)
        .execute(&app.pool)
        .await
        .unwrap();
    }

    let rejected_before = Metrics::global().ingest_rejected_24h();
    Metrics::global().reject_ingest(IngestRejectReason::InvalidPayload);
    let response = client
        .get(format!("{}/api/v1/overview", app.base))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    let overview: Value = response.json().await.unwrap();
    assert_eq!(overview["devices"]["total"], 2);
    assert_eq!(overview["devices"]["online"], 1);
    assert_eq!(overview["devices"]["disabled"], 1);
    assert_eq!(overview["telemetry"]["events_24h"], 1);
    assert_eq!(overview["telemetry"]["last_received_at"], now - 2_000);
    assert_eq!(overview["commands"]["pending"], 1);
    assert_eq!(overview["commands"]["dispatched"], 1);
    assert_eq!(overview["commands"]["failed_24h"], 1);
    assert_eq!(overview["commands"]["expired_24h"], 1);
    assert_eq!(
        overview["ingest"]["queue_capacity"],
        app.config.config.telemetry.queue_capacity
    );
    assert!(overview["ingest"]["rejected_24h"].as_u64().unwrap() > rejected_before);
    assert_eq!(overview["storage_ok"], true);
}

#[tokio::test]
async fn overview_is_authenticated_and_reports_degraded_storage_readiness() {
    let app = TestApp::start().await;
    let unauthenticated = reqwest::Client::new()
        .get(format!("{}/api/v1/overview", app.base))
        .send()
        .await
        .unwrap();
    assert_eq!(unauthenticated.status(), reqwest::StatusCode::UNAUTHORIZED);

    let client = admin_client(&app).await;
    app.command_ready
        .store(false, std::sync::atomic::Ordering::Release);
    let response = client
        .get(format!("{}/api/v1/overview", app.base))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    let overview: Value = response.json().await.unwrap();
    assert_eq!(overview["storage_ok"], false);
}
