mod common;

use common::TestApp;

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
    for metric in [
        "iot_mqtt_connections_active",
        "iot_ingest_accepted_total",
        "iot_ingest_rejected_total{reason=\"queue_full\"}",
        "iot_retained_cap_rejected_total",
        "iot_telemetry_queue_depth",
        "iot_telemetry_commit_seconds_bucket",
        "iot_telemetry_commit_errors_total",
        "iot_commands_pending",
        "iot_commands_expired_total",
        "iot_ws_clients",
        "iot_ws_dropped_total",
    ] {
        assert!(exposition.contains(metric), "missing {metric}");
    }
    assert!(!exposition.contains(TOKEN));
    assert!(!exposition.contains("device_id"));
}
