mod common;
#[path = "common/mqtt_auth_postgres.rs"]
mod mqtt_auth_postgres;

use common::TestApp;
use iot_server::core::auth;
use iot_server::observability::metrics::{IngestRejectReason, Metrics};
use mqtt_auth_postgres::PostgresApp;
use serde_json::{Value, json};
use std::time::Duration;

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

async fn assert_overview(
    client: &reqwest::Client,
    base: &str,
    cookie: Option<&str>,
    queue_capacity: u64,
    rejected_before: u64,
    now: i64,
) {
    let mut request = client.get(format!("{base}/api/v1/overview"));
    if let Some(cookie) = cookie {
        request = request.header("Cookie", cookie);
    }
    let response = request.send().await.unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    let overview: Value = response.json().await.unwrap();
    assert_eq!(overview["devices"]["total"], 2);
    assert_eq!(overview["devices"]["online"], 1);
    assert_eq!(overview["devices"]["disabled"], 1);
    assert_eq!(overview["telemetry"]["events_24h"], 1);
    assert_eq!(overview["telemetry"]["last_received_at"], now - 2_000);
    assert_eq!(overview["commands"]["pending"], 1, "{overview}");
    assert_eq!(overview["commands"]["dispatched"], 1);
    assert_eq!(overview["commands"]["failed_24h"], 1);
    assert_eq!(overview["commands"]["expired_24h"], 1);
    assert_eq!(overview["ingest"]["queue_capacity"], queue_capacity);
    assert!(overview["ingest"]["rejected_24h"].as_u64().unwrap() > rejected_before);
    assert_eq!(overview["storage_ok"], true);
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
            "UPDATE commands SET status = 'expired', completed_at = ?, created_at = ?, expires_at = ? WHERE id = ?",
        )
        .bind(completed_at).bind(completed_at - 1_000).bind(completed_at).bind(id)
        .execute(&app.pool).await.unwrap();
    }

    let rejected_before = Metrics::global().ingest_rejected_24h();
    Metrics::global().reject_ingest(IngestRejectReason::InvalidPayload);
    assert_overview(
        &client,
        &app.base,
        None,
        app.config.config.telemetry.queue_capacity,
        rejected_before,
        now,
    )
    .await;
}

#[tokio::test]
#[ignore = "requires disposable PostgreSQL fixture; run tests/overview_postgres.sh"]
async fn overview_uses_database_windows_and_live_presence_postgres() {
    let app = PostgresApp::start().await;
    let enabled = app.create_device("overview-enabled").await;
    let _disabled = app.create_device_disabled("overview-disabled").await;
    let pool = sqlx::PgPool::connect(&std::env::var("IOT_TEST_POSTGRES_URL").unwrap())
        .await
        .unwrap();
    let now = auth::now_ms();
    for (suffix, received_at) in [("current", now - 2_000), ("stale", now - 86_402_000)] {
        let internal_id: i64 = sqlx::query_scalar(
            "UPDATE telemetry_id_allocator SET last_id = last_id + 1 WHERE id = 1 RETURNING last_id",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO telemetry_events
             (internal_id, device_id, event_id, ts, received_at, values_json, payload_sha256)
             VALUES ($1, $2, $3, $4, $5, '{}'::jsonb, $6)",
        )
        .bind(internal_id)
        .bind(&enabled.id)
        .bind(format!("overview-{suffix}"))
        .bind(received_at)
        .bind(received_at)
        .bind([0_u8; 32].as_slice())
        .execute(&pool)
        .await
        .unwrap();
    }
    let client = reqwest::Client::new();
    let base = app.base_url();
    let cookie = app.admin_cookie();
    let command_ids = (0..5)
        .map(|_| uuid::Uuid::now_v7().to_string())
        .collect::<Vec<_>>();
    let failed_at = now - 2_000;
    for (index, id) in command_ids.iter().enumerate() {
        let (status, created_at, expires_at, next_attempt_at, completed_at) = match index {
            0 => ("pending", now, now + 3_600_000, now + 3_600_000, None),
            1 => ("dispatched", now, now + 3_600_000, now + 3_600_000, None),
            2 => (
                "failed",
                failed_at - 1_000,
                failed_at + 60_000,
                failed_at,
                Some(failed_at),
            ),
            3 => (
                "expired",
                now - 4_000,
                now - 3_000,
                now - 3_000,
                Some(now - 3_000),
            ),
            _ => (
                "expired",
                now - 86_403_000,
                now - 86_402_000,
                now - 86_402_000,
                Some(now - 86_402_000),
            ),
        };
        sqlx::query(
            "INSERT INTO commands
             (id, device_id, name, params_json, status, created_at, expires_at, next_attempt_at, completed_at)
             VALUES ($1, $2, 'reboot', '{}'::jsonb, $3, $4, $5, $6, $7)",
        )
        .bind(id)
        .bind(&enabled.id)
        .bind(status)
        .bind(created_at)
        .bind(expires_at)
        .bind(next_attempt_at)
        .bind(completed_at)
        .execute(&pool)
        .await
        .unwrap();
    }
    let (mqtt, mut eventloop, session_present) = common::mqtt_connect(
        app.mqtt_addr(),
        &enabled.id,
        &enabled.id,
        &enabled.token,
        true,
    )
    .await;
    assert!(!session_present);
    let initial = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let response = client
                .get(format!("{base}/api/v1/overview"))
                .header("Cookie", cookie)
                .send()
                .await
                .unwrap();
            let body: Value = response.json().await.unwrap();
            if body["devices"]["online"] == 1 {
                break body;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("MQTT device did not become online");
    sqlx::query("UPDATE commands SET status = 'pending', next_attempt_at = $1 WHERE id = $2")
        .bind(now + 3_600_000)
        .bind(&command_ids[0])
        .execute(&pool)
        .await
        .unwrap();
    let rejected_before = initial["ingest"]["rejected_24h"].as_u64().unwrap();
    let mqtt_task = tokio::spawn(async move {
        loop {
            if eventloop.poll().await.is_err() {
                break;
            }
        }
    });
    mqtt.publish(
        format!("devices/{}/telemetry", enabled.id),
        rumqttc::QoS::AtLeastOnce,
        false,
        b"{",
    )
    .await
    .unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let response = client
                .get(format!("{base}/api/v1/overview"))
                .header("Cookie", cookie)
                .send()
                .await
                .unwrap();
            let body: Value = response.json().await.unwrap();
            if body["devices"]["online"] == 1
                && body["ingest"]["rejected_24h"].as_u64().unwrap() > rejected_before
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("MQTT device did not become online");
    assert_overview(&client, base, Some(cookie), 16, rejected_before, now).await;
    drop(mqtt);
    mqtt_task.abort();
    pool.close().await;
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
