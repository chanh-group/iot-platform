mod common;

use common::TestApp;
use iot_server::core::auth;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use uuid::Uuid;

async fn admin_client(app: &TestApp, username: &str) -> reqwest::Client {
    app.create_admin(username, "command-test-password").await;
    let client = app.client();
    let response = client
        .post(format!("{}/api/v1/auth/login", app.base))
        .header("X-IoT-CSRF", "1")
        .json(&json!({"username": username, "password": "command-test-password"}))
        .send()
        .await
        .unwrap();
    assert!(response.status().is_success());
    client
}

async fn create_command(
    client: &reqwest::Client,
    app: &TestApp,
    device_id: &str,
    body: &str,
) -> reqwest::Response {
    client
        .post(format!("{}/api/v1/devices/{device_id}/commands", app.base))
        .header("X-IoT-CSRF", "1")
        .header("X-Request-ID", "command-api-123")
        .header(reqwest::header::CONTENT_TYPE, "application/json")
        .body(body.to_owned())
        .send()
        .await
        .unwrap()
}

#[tokio::test]
async fn create_commits_command_and_redacted_audit_before_returning_202() {
    let app = TestApp::start().await;
    let device = app.create_device("command-create", true).await;
    let http = admin_client(&app, "command-create-admin").await;
    let response = create_command(
        &http,
        &app,
        &device.id,
        r#"{"name":"relay.toggle-1","params":{"value":"private-param"},"idempotency_key":"private-key"}"#,
    )
    .await;

    assert_eq!(response.status(), reqwest::StatusCode::ACCEPTED);
    let location = response.headers()[reqwest::header::LOCATION]
        .to_str()
        .unwrap()
        .to_owned();
    let body: Value = response.json().await.unwrap();
    let id = body["command_id"].as_str().unwrap();
    assert_eq!(body["device_id"], device.id);
    assert_eq!(body["status"], "pending");
    assert_eq!(location, format!("/api/v1/commands/{id}"));
    assert!(body["expires_at"].as_i64().unwrap() > auth::now_ms());

    let command: Value = http
        .get(format!("{}{location}", app.base))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(command["id"], id);
    assert_eq!(command["params"], json!({"value":"private-param"}));
    assert_eq!(command["status"], "pending");
    assert_eq!(command["attempts"], 0);
    assert_eq!(
        command["expires_at"].as_i64().unwrap() - command["created_at"].as_i64().unwrap(),
        60_000
    );
    assert!(command.get("idempotency_key").is_none());
    assert!(command.get("request_sha256").is_none());

    let (action, target_id, request_id, details): (String, String, Option<String>, String) =
        sqlx::query_as(
            "SELECT action, target_id, request_id, details_json FROM audit_events
             WHERE target_id = ? AND action = 'command.create'",
        )
        .bind(id)
        .fetch_one(&app.pool)
        .await
        .unwrap();
    assert_eq!(action, "command.create");
    assert_eq!(target_id, id);
    assert_eq!(request_id.as_deref(), Some("command-api-123"));
    let details: Value = serde_json::from_str(&details).unwrap();
    assert_eq!(details["name"], "relay.toggle-1");
    assert_eq!(details["expires_at"], body["expires_at"]);
    assert!(!details.to_string().contains("private-param"));
    assert!(!details.to_string().contains("private-key"));
}

#[tokio::test]
async fn command_endpoints_require_admin_and_post_requires_csrf() {
    let app = TestApp::start().await;
    let device = app.create_device("command-auth", true).await;
    let path = format!("/api/v1/devices/{}/commands", device.id);
    let unauthenticated = reqwest::Client::new();
    for response in [
        unauthenticated
            .post(format!("{}{}", app.base, path))
            .header("X-IoT-CSRF", "1")
            .json(&json!({"name":"reboot"}))
            .send()
            .await
            .unwrap(),
        unauthenticated
            .get(format!("{}{}", app.base, path))
            .send()
            .await
            .unwrap(),
        unauthenticated
            .get(format!("{}/api/v1/commands/missing", app.base))
            .send()
            .await
            .unwrap(),
    ] {
        assert_eq!(response.status(), reqwest::StatusCode::UNAUTHORIZED);
        assert_eq!(
            response.json::<Value>().await.unwrap()["code"],
            "unauthenticated"
        );
    }

    let http = admin_client(&app, "command-auth-admin").await;
    let response = http
        .post(format!("{}{}", app.base, path))
        .json(&json!({"name":"reboot"}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::FORBIDDEN);
    assert_eq!(
        response.json::<Value>().await.unwrap()["code"],
        "csrf_failed"
    );
}

#[tokio::test]
async fn create_validates_fields_and_device_state() {
    let app = TestApp::start().await;
    let device = app.create_device("command-validation", true).await;
    let disabled = app.create_device("command-disabled", false).await;
    let http = admin_client(&app, "command-validation-admin").await;

    let mut invalid = vec![
        "{".to_owned(),
        r#"{"name":"Reboot"}"#.to_owned(),
        r#"{"name":"1reboot"}"#.to_owned(),
        r#"{"name":"reboot","params":null}"#.to_owned(),
        r#"{"name":"reboot","ttl_ms":999}"#.to_owned(),
        r#"{"name":"reboot","ttl_ms":86400001}"#.to_owned(),
        r#"{"name":"reboot","ttl_ms":1.5}"#.to_owned(),
        r#"{"name":"reboot","idempotency_key":"bad\tkey"}"#.to_owned(),
        r#"{"name":"reboot","unknown":true}"#.to_owned(),
    ];
    invalid.push(format!(
        r#"{{"name":"reboot","params":{{"value":"{}"}}}}"#,
        "x".repeat(16_385)
    ));
    for body in invalid {
        let response = create_command(&http, &app, &device.id, &body).await;
        assert_eq!(
            response.status(),
            reqwest::StatusCode::BAD_REQUEST,
            "{body}"
        );
        let error: Value = response.json().await.unwrap();
        assert_eq!(
            error["code"],
            if body == "{" {
                "invalid_json"
            } else {
                "validation_error"
            },
            "{body}"
        );
    }

    let missing = create_command(
        &http,
        &app,
        "019c0000-0000-7000-8000-000000000000",
        r#"{"name":"reboot"}"#,
    )
    .await;
    assert_eq!(missing.status(), reqwest::StatusCode::NOT_FOUND);
    assert_eq!(
        missing.json::<Value>().await.unwrap()["code"],
        "device_not_found"
    );

    let disabled = create_command(&http, &app, &disabled.id, r#"{"name":"reboot"}"#).await;
    assert_eq!(disabled.status(), reqwest::StatusCode::CONFLICT);
    assert_eq!(
        disabled.json::<Value>().await.unwrap()["code"],
        "device_disabled"
    );
}

#[tokio::test]
async fn idempotency_uses_raw_request_bytes_and_does_not_audit_replays() {
    let app = TestApp::start().await;
    let device = app.create_device("command-idempotency", true).await;
    let http = admin_client(&app, "command-idempotency-admin").await;
    let request =
        r#"{"name":"reboot","params":{"n":1},"ttl_ms":60000,"idempotency_key":"same-key"}"#;

    let first = create_command(&http, &app, &device.id, request).await;
    assert_eq!(first.status(), reqwest::StatusCode::ACCEPTED);
    let first_body: Value = first.json().await.unwrap();
    let replay = create_command(&http, &app, &device.id, request).await;
    assert_eq!(replay.status(), reqwest::StatusCode::ACCEPTED);
    assert_eq!(replay.headers()["idempotent-replayed"], "true");
    let replay_body: Value = replay.json().await.unwrap();
    assert_eq!(replay_body["command_id"], first_body["command_id"]);

    let same_value_different_bytes = "{\n \"idempotency_key\":\"same-key\", \"ttl_ms\":60000, \"params\":{\"n\":1}, \"name\":\"reboot\"\n}";
    let conflict = create_command(&http, &app, &device.id, same_value_different_bytes).await;
    assert_eq!(conflict.status(), reqwest::StatusCode::CONFLICT);
    assert_eq!(
        conflict.json::<Value>().await.unwrap()["code"],
        "idempotency_conflict"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM commands WHERE device_id = ? AND idempotency_key = 'same-key'",
        )
        .bind(&device.id)
        .fetch_one(&app.pool)
        .await
        .unwrap(),
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM audit_events WHERE action = 'command.create'",
        )
        .fetch_one(&app.pool)
        .await
        .unwrap(),
        1
    );
}

#[tokio::test]
async fn idempotency_replay_precedes_current_ttl_validation() {
    let app = TestApp::builder().command_max_ttl_ms(60_000).start().await;
    let device = app.create_device("command-ttl-replay", true).await;
    let http = admin_client(&app, "command-ttl-replay-admin").await;
    let raw_request = r#"{"name":"reboot","ttl_ms":120000,"idempotency_key":"config-replay"}"#;
    let command_id = Uuid::now_v7().to_string();
    let created_at = auth::now_ms();
    sqlx::query(
        "INSERT INTO commands (
            id, device_id, name, params_json, status, created_at, expires_at,
            next_attempt_at, idempotency_key, request_sha256
         ) VALUES (?, ?, 'reboot', '{}', 'pending', ?, ?, ?, 'config-replay', ?)",
    )
    .bind(&command_id)
    .bind(&device.id)
    .bind(created_at)
    .bind(created_at + 120_000)
    .bind(created_at)
    .bind(Sha256::digest(raw_request.as_bytes()).as_slice())
    .execute(&app.pool)
    .await
    .unwrap();

    let conflict = create_command(
        &http,
        &app,
        &device.id,
        "{ \"name\":\"reboot\", \"ttl_ms\":120000, \"idempotency_key\":\"config-replay\" }",
    )
    .await;
    assert_eq!(conflict.status(), reqwest::StatusCode::CONFLICT);
    assert_eq!(
        conflict.json::<Value>().await.unwrap()["code"],
        "idempotency_conflict"
    );

    let replay = create_command(&http, &app, &device.id, raw_request).await;
    assert_eq!(replay.status(), reqwest::StatusCode::ACCEPTED);
    assert_eq!(replay.headers()["idempotent-replayed"], "true");
    let replay: Value = replay.json().await.unwrap();
    assert_eq!(replay["command_id"], command_id);
}

#[tokio::test]
async fn command_list_paginates_filters_and_detail_is_scoped_to_contract_fields() {
    let app = TestApp::start().await;
    let device = app.create_device("command-list", true).await;
    let http = admin_client(&app, "command-list-admin").await;
    let mut ids = Vec::new();
    for index in 0..3 {
        let response = create_command(
            &http,
            &app,
            &device.id,
            &format!(r#"{{"name":"reboot","idempotency_key":"key-{index}"}}"#),
        )
        .await;
        assert_eq!(response.status(), reqwest::StatusCode::ACCEPTED);
        ids.push(
            response.json::<Value>().await.unwrap()["command_id"]
                .as_str()
                .unwrap()
                .to_owned(),
        );
    }
    sqlx::query("UPDATE commands SET created_at = ? WHERE device_id = ?")
        .bind(auth::now_ms() - 1_000)
        .bind(&device.id)
        .execute(&app.pool)
        .await
        .unwrap();

    let list_url = format!("{}/api/v1/devices/{}/commands", app.base, device.id);
    let first: Value = http
        .get(&list_url)
        .query(&[("limit", "2")])
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(first["items"].as_array().unwrap().len(), 2);
    let cursor = first["next_cursor"].as_str().unwrap();
    let second_response = http
        .get(&list_url)
        .query(&[("limit", "2"), ("cursor", cursor)])
        .send()
        .await
        .unwrap();
    assert_eq!(second_response.status(), reqwest::StatusCode::OK);
    let second: Value = second_response.json().await.unwrap();
    assert_eq!(second["items"].as_array().unwrap().len(), 1);
    assert!(second["next_cursor"].is_null());
    let mut page_ids: Vec<String> = first["items"]
        .as_array()
        .unwrap()
        .iter()
        .map(|item| item["id"].as_str().unwrap().to_owned())
        .collect();
    page_ids.push(second["items"][0]["id"].as_str().unwrap().to_owned());
    let expected_ids: Vec<String> = sqlx::query_scalar(
        "SELECT id FROM commands WHERE device_id = ? ORDER BY created_at DESC, id DESC",
    )
    .bind(&device.id)
    .fetch_all(&app.pool)
    .await
    .unwrap();
    assert_eq!(page_ids, expected_ids);

    sqlx::query("UPDATE commands SET status = 'dispatched', dispatched_at = ? WHERE id = ?")
        .bind(auth::now_ms())
        .bind(&ids[0])
        .execute(&app.pool)
        .await
        .unwrap();
    let filtered: Value = http
        .get(&list_url)
        .query(&[("status", "dispatched")])
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(filtered["items"].as_array().unwrap().len(), 1);
    assert_eq!(filtered["items"][0]["id"], ids[0]);

    let mismatched_cursor = http
        .get(&list_url)
        .query(&[("status", "dispatched"), ("cursor", cursor)])
        .send()
        .await
        .unwrap();
    assert_eq!(mismatched_cursor.status(), reqwest::StatusCode::BAD_REQUEST);
    assert_eq!(
        mismatched_cursor.json::<Value>().await.unwrap()["code"],
        "invalid_cursor"
    );

    let detail: Value = http
        .get(format!("{}/api/v1/commands/{}", app.base, ids[0]))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(detail["status"], "dispatched");
    assert!(detail["dispatched_at"].as_i64().is_some());
    assert_eq!(detail["attempts"], 0);
    assert!(detail.get("request_sha256").is_none());

    let missing = http
        .get(format!("{}/api/v1/commands/{}", app.base, Uuid::now_v7()))
        .send()
        .await
        .unwrap();
    assert_eq!(missing.status(), reqwest::StatusCode::NOT_FOUND);
    assert_eq!(
        missing.json::<Value>().await.unwrap()["code"],
        "command_not_found"
    );

    let missing_device = http
        .get(format!(
            "{}/api/v1/devices/019c0000-0000-7000-8000-000000000000/commands",
            app.base
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(missing_device.status(), reqwest::StatusCode::NOT_FOUND);
    assert_eq!(
        missing_device.json::<Value>().await.unwrap()["code"],
        "device_not_found"
    );
}

#[tokio::test]
async fn concurrent_creates_do_not_exceed_pending_queue_limit() {
    let app = TestApp::start().await;
    let device = app.create_device("command-limit", true).await;
    let http = admin_client(&app, "command-limit-admin").await;
    let limit = app.config.config.commands.max_pending_per_device;
    assert_eq!(limit, 100);
    insert_active_commands(&app.pool, &device.id, limit - 1).await;

    let first_client = http.clone();
    let first_app = &app;
    let first_device_id = device.id.clone();
    let second_client = http.clone();
    let second_app = &app;
    let second_device_id = device.id.clone();
    let (first, second) = tokio::join!(
        create_command(
            &first_client,
            first_app,
            &first_device_id,
            r#"{"name":"reboot","idempotency_key":"race-a"}"#,
        ),
        create_command(
            &second_client,
            second_app,
            &second_device_id,
            r#"{"name":"reboot","idempotency_key":"race-b"}"#,
        ),
    );
    let statuses = [first.status(), second.status()];
    assert_eq!(
        statuses
            .iter()
            .filter(|status| **status == reqwest::StatusCode::ACCEPTED)
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
    let body: Value = conflict.json().await.unwrap();
    assert_eq!(body["code"], "command_queue_full");
    assert_eq!(body["details"]["limit"], limit);
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM commands WHERE device_id = ? AND status IN ('pending', 'dispatched')",
        )
        .bind(&device.id)
        .fetch_one(&app.pool)
        .await
        .unwrap(),
        i64::try_from(limit).unwrap()
    );
}

async fn insert_active_commands(pool: &sqlx::SqlitePool, device_id: &str, count: u64) {
    let now = auth::now_ms();
    let mut tx = pool.begin().await.unwrap();
    for index in 0..count {
        sqlx::query(
            "INSERT INTO commands (
                id, device_id, name, params_json, status, created_at, expires_at, next_attempt_at
             ) VALUES (?, ?, 'reboot', '{}', ?, ?, ?, ?)",
        )
        .bind(Uuid::now_v7().to_string())
        .bind(device_id)
        .bind(if index == 0 { "dispatched" } else { "pending" })
        .bind(now)
        .bind(now + 60_000)
        .bind(now)
        .execute(&mut *tx)
        .await
        .unwrap();
    }
    tx.commit().await.unwrap();
}

#[tokio::test]
async fn audit_failure_rolls_back_command_and_returns_storage_error() {
    let app = TestApp::start().await;
    let device = app.create_device("command-audit", true).await;
    let http = admin_client(&app, "command-audit-admin").await;
    sqlx::query(
        "CREATE TRIGGER fail_command_audit BEFORE INSERT ON audit_events
         WHEN NEW.action = 'command.create'
         BEGIN SELECT RAISE(FAIL, 'injected audit failure'); END",
    )
    .execute(&app.pool)
    .await
    .unwrap();

    let response = create_command(
        &http,
        &app,
        &device.id,
        r#"{"name":"reboot","idempotency_key":"audit-failure"}"#,
    )
    .await;
    assert_eq!(response.status(), reqwest::StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(
        response.json::<Value>().await.unwrap()["code"],
        "storage_unavailable"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM commands WHERE device_id = ?")
            .bind(&device.id)
            .fetch_one(&app.pool)
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM audit_events WHERE action = 'command.create'",
        )
        .fetch_one(&app.pool)
        .await
        .unwrap(),
        0
    );
}
