mod common;

use std::time::Duration;

use common::{
    TestApp, mqtt_connect, mqtt_connect_refused, mqtt_publish, mqtt_wait_closed, mqtt_wait_puback,
};
use rumqttc::{ConnectReturnCode, QoS};
use serde_json::{Value, json};

async fn admin_client(app: &TestApp, username: &str) -> reqwest::Client {
    app.create_admin(username, "credential-rotation-test-password")
        .await;
    let client = app.client();
    let response = client
        .post(format!("{}/api/v1/auth/login", app.base))
        .header("X-IoT-CSRF", "1")
        .json(&json!({
            "username": username,
            "password": "credential-rotation-test-password"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    client
}

async fn rotate(client: &reqwest::Client, app: &TestApp, device_id: &str) -> reqwest::Response {
    client
        .post(format!(
            "{}/api/v1/devices/{device_id}/credentials/rotate",
            app.base
        ))
        .header("X-IoT-CSRF", "1")
        .json(&json!({}))
        .send()
        .await
        .unwrap()
}

#[tokio::test]
async fn rotates_an_online_device_and_rejects_the_previous_token() {
    let app = TestApp::start().await;
    let http = admin_client(&app, "credential-online").await;
    let device = app.create_device("credential-online", true).await;
    let old_credential_id: String = sqlx::query_scalar(
        "SELECT id FROM device_credentials WHERE device_id = ? AND status = 'active'",
    )
    .bind(&device.id)
    .fetch_one(&app.pool)
    .await
    .unwrap();
    let (_old_client, mut old_eventloop, _) =
        mqtt_connect(app.mqtt_addr, &device.id, &device.id, &device.token, true).await;

    let response = rotate(&http, &app, &device.id).await;
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    let body: Value = response.json().await.unwrap();
    assert_eq!(body["device_id"], device.id);
    let new_credential_id = body["credential"]["id"].as_str().unwrap();
    let new_token = body["credential"]["token"].as_str().unwrap();
    assert_ne!(new_token, device.token);
    assert_eq!(new_token.len(), 47);

    mqtt_wait_closed(&mut old_eventloop, Duration::from_secs(3)).await;
    assert_eq!(
        mqtt_connect_refused(app.mqtt_addr, &device.id, &device.id, &device.token).await,
        ConnectReturnCode::BadUserNamePassword
    );
    let (_new_client, _new_eventloop, _) =
        mqtt_connect(app.mqtt_addr, &device.id, &device.id, new_token, true).await;

    let old_status: String =
        sqlx::query_scalar("SELECT status FROM device_credentials WHERE id = ?")
            .bind(&old_credential_id)
            .fetch_one(&app.pool)
            .await
            .unwrap();
    let rotated_from: Option<String> =
        sqlx::query_scalar("SELECT rotated_from FROM device_credentials WHERE id = ?")
            .bind(new_credential_id)
            .fetch_one(&app.pool)
            .await
            .unwrap();
    assert_eq!(old_status, "revoked");
    assert_eq!(rotated_from.as_deref(), Some(old_credential_id.as_str()));
    let audit: (String, String) = sqlx::query_as(
        "SELECT action, details_json FROM audit_events WHERE target_id = ? ORDER BY id DESC LIMIT 1",
    )
    .bind(&device.id)
    .fetch_one(&app.pool)
    .await
    .unwrap();
    assert_eq!(audit.0, "credential.rotate");
    assert_eq!(
        serde_json::from_str::<Value>(&audit.1).unwrap(),
        json!({
            "old_credential_id": old_credential_id,
            "new_credential_id": new_credential_id
        })
    );
    assert!(!audit.1.contains(new_token));
}

#[tokio::test]
async fn rotates_disabled_device_without_enabling_it() {
    let app = TestApp::start().await;
    let http = admin_client(&app, "credential-disabled").await;
    let device = app.create_device("credential-disabled", false).await;

    let response = rotate(&http, &app, &device.id).await;
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    let body: Value = response.json().await.unwrap();
    let new_token = body["credential"]["token"].as_str().unwrap();
    assert_ne!(new_token, device.token);
    let enabled: bool = sqlx::query_scalar("SELECT enabled FROM devices WHERE id = ?")
        .bind(&device.id)
        .fetch_one(&app.pool)
        .await
        .unwrap();
    assert!(!enabled);
    assert_eq!(
        mqtt_connect_refused(app.mqtt_addr, &device.id, &device.id, &device.token).await,
        ConnectReturnCode::BadUserNamePassword
    );
    assert_eq!(
        mqtt_connect_refused(app.mqtt_addr, &device.id, &device.id, new_token).await,
        ConnectReturnCode::BadUserNamePassword
    );

    let enabled = http
        .patch(format!("{}/api/v1/devices/{}", app.base, device.id))
        .header("X-IoT-CSRF", "1")
        .json(&json!({"enabled": true}))
        .send()
        .await
        .unwrap();
    assert_eq!(enabled.status(), reqwest::StatusCode::OK);
    let (_client, _eventloop, _) =
        mqtt_connect(app.mqtt_addr, &device.id, &device.id, new_token, true).await;
}

#[tokio::test]
async fn audit_failure_rolls_back_rotation_and_preserves_old_credential() {
    let app = TestApp::start().await;
    let http = admin_client(&app, "credential-audit").await;
    let device = app.create_device("credential-audit", true).await;
    let (old_client, mut old_eventloop, _) =
        mqtt_connect(app.mqtt_addr, &device.id, &device.id, &device.token, true).await;
    let before: (String, Vec<u8>) = sqlx::query_as(
        "SELECT id, token_hash FROM device_credentials WHERE device_id = ? AND status = 'active'",
    )
    .bind(&device.id)
    .fetch_one(&app.pool)
    .await
    .unwrap();
    sqlx::query(
        "CREATE TRIGGER fail_credential_rotate_audit BEFORE INSERT ON audit_events
         WHEN NEW.action = 'credential.rotate'
         BEGIN SELECT RAISE(FAIL, 'injected audit failure'); END",
    )
    .execute(&app.pool)
    .await
    .unwrap();

    let response = rotate(&http, &app, &device.id).await;
    assert_eq!(response.status(), reqwest::StatusCode::SERVICE_UNAVAILABLE);
    let active: (String, Vec<u8>) = sqlx::query_as(
        "SELECT id, token_hash FROM device_credentials WHERE device_id = ? AND status = 'active'",
    )
    .bind(&device.id)
    .fetch_one(&app.pool)
    .await
    .unwrap();
    assert_eq!(active, before);
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM device_credentials WHERE device_id = ?",
        )
        .bind(&device.id)
        .fetch_one(&app.pool)
        .await
        .unwrap(),
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM audit_events WHERE target_id = ? AND action = 'credential.rotate'",
        )
        .bind(&device.id)
        .fetch_one(&app.pool)
        .await
        .unwrap(),
        0
    );
    mqtt_publish(
        &old_client,
        &format!("devices/{}/data/after-rollback", device.id),
        QoS::AtLeastOnce,
        false,
        b"still-connected",
    )
    .await;
    mqtt_wait_puback(&mut old_eventloop, Duration::from_secs(2)).await;
    drop(old_client);
}

#[tokio::test]
async fn missing_device_returns_not_found() {
    let app = TestApp::start().await;
    let http = admin_client(&app, "credential-missing").await;
    let response = rotate(&http, &app, "019c0000-0000-7000-8000-000000000000").await;
    assert_eq!(response.status(), reqwest::StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn rotation_requires_admin_session_and_csrf() {
    let app = TestApp::start().await;
    let device = app.create_device("credential-auth", true).await;
    let url = format!(
        "{}/api/v1/devices/{}/credentials/rotate",
        app.base, device.id
    );

    let unauthenticated = reqwest::Client::new()
        .post(&url)
        .header("X-IoT-CSRF", "1")
        .json(&json!({}))
        .send()
        .await
        .unwrap();
    assert_eq!(unauthenticated.status(), reqwest::StatusCode::UNAUTHORIZED);

    let http = admin_client(&app, "credential-auth").await;
    let csrf_missing = http.post(&url).json(&json!({})).send().await.unwrap();
    assert_eq!(csrf_missing.status(), reqwest::StatusCode::FORBIDDEN);
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM device_credentials WHERE device_id = ?")
            .bind(&device.id)
            .fetch_one(&app.pool)
            .await
            .unwrap(),
        1
    );
}
