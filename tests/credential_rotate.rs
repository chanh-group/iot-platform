mod common;
#[path = "common/mqtt_auth_postgres.rs"]
mod mqtt_auth_postgres;

use std::time::Duration;

use common::{
    TestApp, mqtt_connect, mqtt_connect_refused, mqtt_options, mqtt_publish, mqtt_wait_closed,
    mqtt_wait_puback,
};
use rumqttc::{AsyncClient, ConnectReturnCode, ConnectionError, Event, Packet, QoS};
use serde_json::{Value, json};

async fn assert_online_rotation(
    client: &reqwest::Client,
    base: &str,
    mqtt_addr: std::net::SocketAddr,
    device_id: &str,
    old_token: &str,
) -> (String, String) {
    let (_old_client, mut old_eventloop, _) =
        mqtt_connect(mqtt_addr, device_id, device_id, old_token, true).await;
    let response = client
        .post(format!(
            "{base}/api/v1/devices/{device_id}/credentials/rotate"
        ))
        .header("X-IoT-CSRF", "1")
        .json(&json!({}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    let body: Value = response.json().await.unwrap();
    assert_eq!(body["device_id"], device_id);
    let new_credential_id = body["credential"]["id"].as_str().unwrap().to_owned();
    let new_token = body["credential"]["token"].as_str().unwrap().to_owned();
    assert_ne!(new_token, old_token);
    assert_eq!(new_token.len(), 47);

    mqtt_wait_closed(&mut old_eventloop, Duration::from_secs(3)).await;
    assert_eq!(
        mqtt_connect_refused(mqtt_addr, device_id, device_id, old_token).await,
        ConnectReturnCode::BadUserNamePassword
    );
    let (_new_client, _new_eventloop, _) =
        mqtt_connect(mqtt_addr, device_id, device_id, &new_token, true).await;
    (new_credential_id, new_token)
}

fn assert_rotation_storage(
    old_credential_id: &str,
    new_credential_id: &str,
    new_token: &str,
    old_status: &str,
    rotated_from: Option<&str>,
    audit_action: &str,
    audit_details: &Value,
) {
    assert_eq!(old_status, "revoked");
    assert_eq!(rotated_from, Some(old_credential_id));
    assert_eq!(audit_action, "credential.rotate");
    assert_eq!(
        *audit_details,
        json!({
            "old_credential_id": old_credential_id,
            "new_credential_id": new_credential_id
        })
    );
    assert!(!audit_details.to_string().contains(new_token));
}

#[tokio::test]
#[ignore = "requires disposable PostgreSQL fixture; run tests/credential_rotate_postgres.sh"]
async fn rotates_an_online_device_and_rejects_the_previous_token_postgres() {
    let app = mqtt_auth_postgres::PostgresApp::start().await;
    let http = reqwest::Client::builder()
        .default_headers(reqwest::header::HeaderMap::from_iter([(
            reqwest::header::COOKIE,
            app.admin_cookie().parse().unwrap(),
        )]))
        .build()
        .unwrap();
    let device = app.create_device("credential-rotation-postgres").await;
    let pool = sqlx::PgPool::connect(&std::env::var("IOT_TEST_POSTGRES_URL").unwrap())
        .await
        .unwrap();
    let old_credential_id: String = sqlx::query_scalar(
        "SELECT id FROM device_credentials WHERE device_id = $1 AND status = 'active'",
    )
    .bind(&device.id)
    .fetch_one(&pool)
    .await
    .unwrap();
    let (new_credential_id, new_token) = assert_online_rotation(
        &http,
        app.base_url(),
        app.mqtt_addr(),
        &device.id,
        &device.token,
    )
    .await;

    let old_status: String =
        sqlx::query_scalar("SELECT status FROM device_credentials WHERE id = $1")
            .bind(&old_credential_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    let rotated_from: Option<String> =
        sqlx::query_scalar("SELECT rotated_from FROM device_credentials WHERE id = $1")
            .bind(&new_credential_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    let audit: (String, Value) = sqlx::query_as(
        "SELECT action, details_json FROM audit_events WHERE target_id = $1 ORDER BY id DESC LIMIT 1",
    )
    .bind(&device.id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_rotation_storage(
        &old_credential_id,
        &new_credential_id,
        &new_token,
        &old_status,
        rotated_from.as_deref(),
        &audit.0,
        &audit.1,
    );
}

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
    let (new_credential_id, new_token) =
        assert_online_rotation(&http, &app.base, app.mqtt_addr, &device.id, &device.token).await;

    let old_status: String =
        sqlx::query_scalar("SELECT status FROM device_credentials WHERE id = ?")
            .bind(&old_credential_id)
            .fetch_one(&app.pool)
            .await
            .unwrap();
    let rotated_from: Option<String> =
        sqlx::query_scalar("SELECT rotated_from FROM device_credentials WHERE id = ?")
            .bind(&new_credential_id)
            .fetch_one(&app.pool)
            .await
            .unwrap();
    let audit: (String, String) = sqlx::query_as(
        "SELECT action, details_json FROM audit_events WHERE target_id = ? ORDER BY id DESC LIMIT 1",
    )
    .bind(&device.id)
    .fetch_one(&app.pool)
    .await
    .unwrap();
    assert_rotation_storage(
        &old_credential_id,
        &new_credential_id,
        &new_token,
        &old_status,
        rotated_from.as_deref(),
        &audit.0,
        &serde_json::from_str(&audit.1).unwrap(),
    );
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
#[ignore = "requires tests/credential_rotate_postgres.sh"]
async fn rotates_disabled_device_without_enabling_it_postgres() {
    let app = mqtt_auth_postgres::PostgresApp::start().await;
    let http = reqwest::Client::builder()
        .default_headers(reqwest::header::HeaderMap::from_iter([(
            reqwest::header::COOKIE,
            app.admin_cookie().parse().unwrap(),
        )]))
        .build()
        .unwrap();
    let device = app.create_device_disabled("credential-disabled").await;

    let response = http
        .post(format!(
            "{}/api/v1/devices/{}/credentials/rotate",
            app.base_url(),
            device.id
        ))
        .header("X-IoT-CSRF", "1")
        .json(&json!({}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    let body: Value = response.json().await.unwrap();
    let new_token = body["credential"]["token"].as_str().unwrap();
    assert_ne!(new_token, device.token);

    let pool = sqlx::PgPool::connect(&std::env::var("IOT_TEST_POSTGRES_URL").unwrap())
        .await
        .unwrap();
    let enabled: bool = sqlx::query_scalar("SELECT enabled FROM devices WHERE id = $1")
        .bind(&device.id)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert!(!enabled);
    assert_eq!(
        mqtt_connect_refused(app.mqtt_addr(), &device.id, &device.id, &device.token).await,
        ConnectReturnCode::BadUserNamePassword
    );
    assert_eq!(
        mqtt_connect_refused(app.mqtt_addr(), &device.id, &device.id, new_token).await,
        ConnectReturnCode::BadUserNamePassword
    );

    let enabled = http
        .patch(format!("{}/api/v1/devices/{}", app.base_url(), device.id))
        .header("X-IoT-CSRF", "1")
        .json(&json!({"enabled": true}))
        .send()
        .await
        .unwrap();
    assert_eq!(enabled.status(), reqwest::StatusCode::OK);
    let (_client, _eventloop, _) =
        mqtt_connect(app.mqtt_addr(), &device.id, &device.id, new_token, true).await;
}

#[tokio::test]
async fn audit_failure_rolls_back_rotation_and_preserves_old_credential() {
    let app = TestApp::start().await;
    let http = admin_client(&app, "credential-audit").await;
    let device = app.create_device("credential-audit", true).await;
    let (old_client, _old_eventloop, _) =
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
    drop(old_client);
    // The kick worker releases the rollback fence asynchronously after the HTTP error.
    let (reconnected, mut eventloop) = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let options = mqtt_options(app.mqtt_addr, &device.id, &device.id, &device.token, true);
            let (client, mut eventloop) = AsyncClient::new(options, 32);
            match eventloop.poll().await {
                Ok(Event::Incoming(Packet::ConnAck(ack)))
                    if ack.code == ConnectReturnCode::Success =>
                {
                    break (client, eventloop);
                }
                Err(ConnectionError::ConnectionRefused(ConnectReturnCode::NotAuthorized)) => {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
                other => panic!("unexpected MQTT connection result: {other:?}"),
            }
        }
    })
    .await
    .expect("old credential should reconnect after the rollback fence clears");
    mqtt_publish(
        &reconnected,
        &format!("devices/{}/data/after-rollback", device.id),
        QoS::AtLeastOnce,
        false,
        b"still-connected",
    )
    .await;
    mqtt_wait_puback(&mut eventloop, Duration::from_secs(2)).await;
    drop(reconnected);
}

#[tokio::test]
async fn missing_device_returns_not_found() {
    let app = TestApp::start().await;
    let http = admin_client(&app, "credential-missing").await;
    assert_missing_device_returns_not_found(&http, &app.base).await;
}

async fn assert_missing_device_returns_not_found(http: &reqwest::Client, base: &str) {
    let response = http
        .post(format!(
            "{base}/api/v1/devices/019c0000-0000-7000-8000-000000000000/credentials/rotate"
        ))
        .header("X-IoT-CSRF", "1")
        .json(&json!({}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::NOT_FOUND);
}

#[tokio::test]
#[ignore = "requires tests/credential_rotate_postgres.sh"]
async fn missing_device_returns_not_found_postgres() {
    let app = mqtt_auth_postgres::PostgresApp::start().await;
    let http = reqwest::Client::builder()
        .default_headers(reqwest::header::HeaderMap::from_iter([(
            reqwest::header::COOKIE,
            app.admin_cookie().parse().unwrap(),
        )]))
        .build()
        .unwrap();
    assert_missing_device_returns_not_found(&http, app.base_url()).await;
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
