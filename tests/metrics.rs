mod common;

use common::TestApp;
use iot_server::core::auth;
use iot_server::observability::metrics::Metrics;

const TOKEN: &str = "test-metrics-bearer-token-long-enough-0123456789";

#[tokio::test]
async fn metrics_is_404_when_disabled() {
    let app = TestApp::start().await;
    let response = app
        .client()
        .get(format!("{}/metrics", app.base))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn metrics_requires_its_bearer_token_and_exports_core_series() {
    let app = TestApp::builder().metrics_token(TOKEN).start().await;
    let client = reqwest::Client::new();

    let missing = client
        .get(format!("{}/metrics", app.base))
        .send()
        .await
        .unwrap();
    assert_eq!(missing.status(), reqwest::StatusCode::UNAUTHORIZED);

    let wrong = client
        .get(format!("{}/metrics", app.base))
        .bearer_auth("incorrect-token")
        .send()
        .await
        .unwrap();
    assert_eq!(wrong.status(), reqwest::StatusCode::UNAUTHORIZED);

    app.create_admin("metrics-admin", "metrics-test-password")
        .await;
    let session_client = app.client();
    let login = session_client
        .post(format!("{}/api/v1/auth/login", app.base))
        .header("X-IoT-CSRF", "1")
        .json(&serde_json::json!({
            "username": "metrics-admin",
            "password": "metrics-test-password",
        }))
        .send()
        .await
        .unwrap();
    assert!(login.status().is_success());
    let cookie_only = session_client
        .get(format!("{}/metrics", app.base))
        .send()
        .await
        .unwrap();
    assert_eq!(cookie_only.status(), reqwest::StatusCode::UNAUTHORIZED);

    let device = app.create_device("metrics-command", true).await;
    let command = session_client
        .post(format!(
            "{}/api/v1/devices/{}/commands",
            app.base, device.id
        ))
        .header("X-IoT-CSRF", "1")
        .json(&serde_json::json!({"name":"reboot"}))
        .send()
        .await
        .unwrap();
    assert_eq!(command.status(), reqwest::StatusCode::ACCEPTED);
    let command_id = command.json::<serde_json::Value>().await.unwrap()["command_id"]
        .as_str()
        .unwrap()
        .to_owned();

    let response = client
        .get(format!("{}/metrics", app.base))
        .bearer_auth(TOKEN)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    assert!(
        response
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .unwrap()
            .to_str()
            .unwrap()
            .starts_with("text/plain; version=0.0.4")
    );
    let exposition = response.text().await.unwrap();
    assert!(exposition.contains("iot_commands_pending 1\n"));
    for metric in [
        "iot_mqtt_connections_active",
        "iot_ingest_accepted_total",
        "iot_ingest_rejected_total{reason=\"queue_full\"}",
        "iot_retained_cap_rejected_total",
        "iot_telemetry_queue_depth",
        "iot_telemetry_commit_seconds_bucket",
        "iot_telemetry_commit_errors_total",
        "iot_telemetry_retention_errors_total",
        "iot_audit_write_errors_total",
        "iot_commands_pending",
        "iot_commands_expired_total",
        "iot_command_ack_dropped_total",
        "iot_command_ack_invalid_total",
        "iot_command_ack_storage_errors_total",
        "iot_command_publish_errors_total",
        "iot_command_storage_errors_total",
        "iot_command_dispatch_wake_dropped_total",
        "iot_ws_clients",
        "iot_ws_dropped_total",
    ] {
        assert!(exposition.contains(metric), "missing {metric}");
    }
    assert!(!exposition.contains(TOKEN));
    assert!(!exposition.contains("device_id"));

    let expired_before = Metrics::global().commands_expired_total.get();
    let expires_at = auth::now_ms() - 1;
    sqlx::query(
        "UPDATE commands SET created_at = ?, expires_at = ?, next_attempt_at = ? WHERE id = ?",
    )
    .bind(expires_at - 1)
    .bind(expires_at)
    .bind(expires_at - 1)
    .bind(&command_id)
    .execute(&app.pool)
    .await
    .unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(3), async {
        loop {
            let status: String = sqlx::query_scalar("SELECT status FROM commands WHERE id = ?")
                .bind(&command_id)
                .fetch_one(&app.pool)
                .await
                .unwrap();
            if status == "expired" {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("dispatcher should expire the command");
    assert!(Metrics::global().commands_expired_total.get() > expired_before);

    let after_expiry = client
        .get(format!("{}/metrics", app.base))
        .bearer_auth(TOKEN)
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(after_expiry.contains("iot_commands_pending 0\n"));
}
