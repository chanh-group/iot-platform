use std::fs;
use std::process::Command;
use std::sync::Arc;

use iot_server::config::{self, DatabaseBackend};
use iot_server::core::attributes::ValidatedAttributes;
use iot_server::core::auth::NewAdminSession;
use iot_server::core::command::{
    AckApplyOutcome, CommandAck, CommandAckStatus, CommandAudit, CommandStatus, NewCommand,
};
use iot_server::core::device::{DeviceAudit, DeviceCredentialRotation, DevicePatch, NewDevice};
use iot_server::core::storage::ConfigAudit;
use iot_server::core::telemetry::{
    TelemetryHistoryRange, TelemetryInsertResult, TelemetryRepository, ValidatedTelemetry,
};
use iot_server::storage::postgres::telemetry::PostgresTelemetryRepository;
use iot_server::storage::runtime::StorageRuntime;
use serde_json::{Map, Value, json};
use tempfile::TempDir;
use uuid::Uuid;

#[tokio::test]
#[ignore = "requires an isolated PostgreSQL 18.6 database; set IOT_TEST_POSTGRES_URL"]
async fn postgres_migrations_and_repositories_preserve_storage_contracts() {
    let url = std::env::var("IOT_TEST_POSTGRES_URL").expect("test PostgreSQL URL");
    assert!(
        url.split('?')
            .next()
            .is_some_and(|url| url.ends_with("/iot_test")),
        "integration test URL must use the dedicated iot_test database"
    );
    let override_url = format!("{url}&dbname=postgres");
    let override_pool = sqlx::PgPool::connect(&override_url).await.unwrap();
    let (override_database, override_schema): (String, String) =
        sqlx::query_as("SELECT current_database(), current_schema()")
            .fetch_one(&override_pool)
            .await
            .unwrap();
    assert_eq!(override_database, "postgres");
    assert!(!is_fixture_target(&override_database, &override_schema));
    override_pool.close().await;

    let reset_pool = sqlx::PgPool::connect(&url).await.unwrap();
    let (database, schema): (String, String) =
        sqlx::query_as("SELECT current_database(), current_schema()")
            .fetch_one(&reset_pool)
            .await
            .unwrap();
    assert!(
        is_fixture_target(&database, &schema),
        "refusing to reset a non-fixture target"
    );
    sqlx::query("DROP SCHEMA public CASCADE")
        .execute(&reset_pool)
        .await
        .unwrap();
    sqlx::query("CREATE SCHEMA public")
        .execute(&reset_pool)
        .await
        .unwrap();
    reset_pool.close().await;

    let dir = TempDir::new().unwrap();
    let config_path = dir.path().join("conf.toml");
    fs::write(
        &config_path,
        "config_version = 1\n[server]\ndata_dir = './data'\n[database]\nurl_env = 'IOT_TEST_POSTGRES_URL'\n",
    )
    .unwrap();
    let migrate = Command::new(env!("CARGO_BIN_EXE_iot-server"))
        .args(["migrate", "--config"])
        .arg(&config_path)
        .status()
        .unwrap();
    assert!(migrate.success());
    let loaded = config::load_with_env(&config_path, |name| {
        (name == "IOT_TEST_POSTGRES_URL").then(|| url.clone())
    })
    .unwrap();
    assert_eq!(loaded.database_backend, DatabaseBackend::Postgres);

    let storage = StorageRuntime::connect(&loaded).await.unwrap();
    storage.migrate().await.unwrap();
    storage.migrate().await.unwrap();
    let repositories = &storage.repositories;
    repositories.storage.health_check().await.unwrap();

    assert!(
        repositories
            .admin
            .find_user("admin")
            .await
            .unwrap()
            .is_none()
    );
    let password_path = dir.path().join("password.txt");
    fs::write(&password_path, "postgres-test-password\n").unwrap();
    let created = Command::new(env!("CARGO_BIN_EXE_iot-server"))
        .args(["admin", "create", "--username", "admin", "--config"])
        .arg(&config_path)
        .arg("--password-file")
        .arg(&password_path)
        .status()
        .unwrap();
    assert!(created.success());
    let admin = repositories
        .admin
        .find_user("ADMIN")
        .await
        .unwrap()
        .unwrap();
    let token_hash = [7_u8; 32];
    let now = 2_000_i64;
    repositories
        .admin
        .create_session_and_audit(NewAdminSession {
            user: &admin,
            token_hash: &token_hash,
            now,
            expires_at: now + 60_000,
            remote_ip: "127.0.0.1",
            user_agent: Some("postgres integration test"),
            request_id: "postgres-login",
        })
        .await
        .unwrap();
    let session = repositories
        .admin
        .verify_session(&token_hash, now + 1)
        .await
        .unwrap()
        .unwrap();
    assert!(
        repositories
            .admin
            .session_is_live(&session.id, now + 1)
            .await
            .unwrap()
    );
    repositories
        .admin
        .revoke_session_and_audit(&session, now + 2, "postgres-logout", "127.0.0.1")
        .await
        .unwrap();
    assert!(
        !repositories
            .admin
            .session_is_live(&session.id, now + 3)
            .await
            .unwrap()
    );

    let device_id = Uuid::now_v7().to_string();
    let credential_id = Uuid::now_v7().to_string();
    let device_audit = DeviceAudit {
        actor_id: &admin.id,
        request_id: "postgres-device",
        remote_ip: "127.0.0.1",
    };
    let device_token_hash = [8_u8; 32];
    repositories
        .devices
        .create(
            &NewDevice {
                id: &device_id,
                name: "postgres-device",
                metadata_json: "{}",
                enabled: true,
                created_at: now,
                credential_id: &credential_id,
                token_hash: &device_token_hash,
            },
            &device_audit,
        )
        .await
        .unwrap();
    let row = repositories.devices.get(&device_id).await.unwrap().unwrap();
    assert_eq!(row.metadata_json, "{}");
    assert_eq!(
        repositories
            .devices
            .active_credential(&device_id)
            .await
            .unwrap()
            .unwrap()
            .credential_id,
        credential_id
    );
    assert!(
        repositories
            .devices
            .kick_state(&device_id)
            .await
            .unwrap()
            .unwrap()
            .enabled
    );
    repositories
        .devices
        .update_last_seen(&device_id, now + 1)
        .await
        .unwrap();
    assert_eq!(
        repositories
            .devices
            .list(Some("postgres-device"), Some(true), None, 10)
            .await
            .unwrap()
            .len(),
        1
    );

    let mut attribute_values = Map::new();
    attribute_values.insert("firmware".to_owned(), json!("1.2.3"));
    repositories
        .attributes
        .upsert(
            &device_id,
            &ValidatedAttributes {
                device_ts: Some(now),
                updated_at: now,
                values: attribute_values,
            },
        )
        .await
        .unwrap();
    let attributes = repositories
        .attributes
        .latest(&device_id, None, 10)
        .await
        .unwrap();
    assert_eq!(attributes.len(), 1);
    assert_eq!(
        serde_json::from_str::<Value>(&attributes[0].value_json).unwrap(),
        json!("1.2.3")
    );

    let event = ValidatedTelemetry {
        event_id: "postgres-event-1".to_owned(),
        ts: now,
        received_at: now,
        values: Map::from_iter([("temperature".to_owned(), json!(21))]),
        payload_sha256: [9_u8; 32],
        raw_bytes: 16,
    };
    assert_eq!(
        repositories
            .telemetry
            .insert_batch(&[(device_id.clone(), event.clone())])
            .await
            .unwrap(),
        [TelemetryInsertResult::Stored]
    );
    assert_eq!(
        repositories
            .telemetry
            .insert_batch(&[(device_id.clone(), event.clone())])
            .await
            .unwrap(),
        [TelemetryInsertResult::Duplicate]
    );
    let mut changed_event = event.clone();
    changed_event.payload_sha256 = [10_u8; 32];
    assert_eq!(
        repositories
            .telemetry
            .insert_batch(&[(device_id.clone(), changed_event)])
            .await
            .unwrap(),
        [TelemetryInsertResult::Conflict]
    );
    let checkpoint = repositories
        .telemetry_read
        .committed_through()
        .await
        .unwrap();
    assert!(checkpoint >= 3);
    let rollback_events = vec![
        (
            device_id.clone(),
            ValidatedTelemetry {
                event_id: "postgres-event-rollback".to_owned(),
                ..event.clone()
            },
        ),
        (
            Uuid::now_v7().to_string(),
            ValidatedTelemetry {
                event_id: "postgres-missing-device".to_owned(),
                ..event.clone()
            },
        ),
    ];
    assert!(
        repositories
            .telemetry
            .insert_batch(&rollback_events)
            .await
            .is_err()
    );
    assert_eq!(
        repositories
            .telemetry_read
            .committed_through()
            .await
            .unwrap(),
        checkpoint
    );
    let history = repositories
        .telemetry_read
        .history(
            &device_id,
            TelemetryHistoryRange {
                from: 0,
                to: checkpoint,
                commit_order: true,
            },
            None,
            &[],
            10,
        )
        .await
        .unwrap();
    assert_eq!(history.len(), 1);
    assert_eq!(
        repositories
            .telemetry_read
            .latest(&device_id, &[])
            .await
            .unwrap()
            .len(),
        1
    );

    let telemetry_pool = sqlx::PgPool::connect(&url).await.unwrap();
    let concurrent_repository = Arc::new(PostgresTelemetryRepository::new(telemetry_pool.clone()));
    let prefix_before = repositories
        .telemetry_read
        .committed_through()
        .await
        .unwrap();
    let mut first_writer = telemetry_pool.begin().await.unwrap();
    let first_id: i64 = sqlx::query_scalar(
        "UPDATE telemetry_id_allocator SET last_id = last_id + 1
         WHERE id = 1 RETURNING last_id",
    )
    .fetch_one(&mut *first_writer)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO telemetry_events
         (internal_id, device_id, event_id, ts, received_at, values_json, payload_sha256)
         VALUES ($1, $2, $3, $4, $5, $6, $7)",
    )
    .bind(first_id)
    .bind(&device_id)
    .bind("postgres-interleave-t1")
    .bind(now + 1)
    .bind(now + 1)
    .bind(json!({"temperature": 22}))
    .bind([14_u8; 32].as_slice())
    .execute(&mut *first_writer)
    .await
    .unwrap();
    let second_event = ValidatedTelemetry {
        event_id: "postgres-interleave-t2".to_owned(),
        ts: now + 2,
        received_at: now + 2,
        values: Map::from_iter([("temperature".to_owned(), json!(23))]),
        payload_sha256: [15_u8; 32],
        raw_bytes: 16,
    };
    let second_writer_repository = Arc::clone(&concurrent_repository);
    let second_device_id = device_id.clone();
    let second_writer = tokio::spawn(async move {
        second_writer_repository
            .insert_batch(&[(second_device_id, second_event)])
            .await
    });
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            let waiting: bool = sqlx::query_scalar(
                "SELECT EXISTS(
                     SELECT 1 FROM pg_stat_activity
                     WHERE wait_event_type = 'Lock'
                       AND query LIKE '%telemetry_id_allocator%'
                 )",
            )
            .fetch_one(&telemetry_pool)
            .await
            .unwrap();
            if waiting {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("second telemetry writer should wait on the allocator row");
    assert_eq!(
        repositories
            .telemetry_read
            .committed_through()
            .await
            .unwrap(),
        prefix_before
    );
    first_writer.commit().await.unwrap();
    assert_eq!(
        second_writer.await.unwrap().unwrap(),
        [TelemetryInsertResult::Stored]
    );
    let prefix_after = repositories
        .telemetry_read
        .committed_through()
        .await
        .unwrap();
    assert_eq!(prefix_after, first_id + 1);
    telemetry_pool.close().await;

    let request = json!({
        "name": "restart",
        "params": {"reason": "test"},
        "ttl_ms": 60_000,
        "idempotency_key": "postgres-command-1"
    });
    let request_hash = [11_u8; 32];
    let command = repositories
        .commands
        .create(NewCommand {
            device_id: &device_id,
            request: request.clone(),
            request_sha256: request_hash,
            default_ttl_ms: 60_000,
            max_ttl_ms: 86_400_000,
            max_pending_per_device: 10,
            audit: CommandAudit {
                actor_id: &admin.id,
                request_id: "postgres-command",
                remote_ip: "127.0.0.1",
            },
        })
        .await
        .unwrap();
    let command_id = command.command.id.clone();
    assert_eq!(command.command.status, CommandStatus::Pending);
    assert_eq!(
        repositories
            .commands
            .list(&device_id, Some(CommandStatus::Pending), None, 10)
            .await
            .unwrap()
            .len(),
        1
    );
    repositories
        .commands
        .defer_offline(
            &command_id,
            command.command.created_at + 1,
            command.command.created_at + 100,
        )
        .await
        .unwrap();
    assert_eq!(
        repositories
            .commands
            .wake_device(&device_id, command.command.created_at + 2)
            .await
            .unwrap(),
        1
    );
    let due = repositories
        .commands
        .due(command.command.created_at + 2, 10)
        .await
        .unwrap();
    assert_eq!(due.len(), 1);
    assert_eq!(
        repositories
            .commands
            .due_at(
                command.command.created_at + 2,
                command.command.created_at + 2,
                10
            )
            .await
            .unwrap()
            .len(),
        1
    );
    let replay = repositories
        .commands
        .create(NewCommand {
            device_id: &device_id,
            request,
            request_sha256: request_hash,
            default_ttl_ms: 60_000,
            max_ttl_ms: 86_400_000,
            max_pending_per_device: 10,
            audit: CommandAudit {
                actor_id: &admin.id,
                request_id: "postgres-command-replay",
                remote_ip: "127.0.0.1",
            },
        })
        .await
        .unwrap();
    assert!(replay.replayed);
    assert!(
        repositories
            .commands
            .can_dispatch(&command_id, auth_now())
            .await
            .unwrap()
    );
    assert_eq!(repositories.storage.pending_commands().await.unwrap(), 1);
    let dispatch_at = command.command.created_at + 1;
    assert_eq!(
        repositories
            .commands
            .record_dispatch_attempt(
                &command_id,
                dispatch_at,
                dispatch_at,
                dispatch_at + 1_000,
                true,
            )
            .await
            .unwrap()
            .unwrap()
            .status,
        CommandStatus::Dispatched
    );
    assert_eq!(
        repositories
            .commands
            .wake_all(dispatch_at + 2, 10)
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        repositories
            .commands
            .apply_ack(
                &device_id,
                &CommandAck {
                    command_id: command_id.clone(),
                    status: CommandAckStatus::Succeeded,
                    result: Some(json!({"ok": true})),
                    error: None,
                },
                dispatch_at + 1,
            )
            .await
            .unwrap(),
        AckApplyOutcome::Applied
    );
    assert_eq!(
        repositories
            .commands
            .get(&command_id)
            .await
            .unwrap()
            .unwrap()
            .status,
        CommandStatus::Succeeded
    );
    let expiring = repositories
        .commands
        .create(NewCommand {
            device_id: &device_id,
            request: json!({"name":"expire-me","ttl_ms":1_000}),
            request_sha256: [13_u8; 32],
            default_ttl_ms: 60_000,
            max_ttl_ms: 86_400_000,
            max_pending_per_device: 10,
            audit: CommandAudit {
                actor_id: &admin.id,
                request_id: "postgres-command-expire",
                remote_ip: "127.0.0.1",
            },
        })
        .await
        .unwrap();
    let expired = repositories
        .commands
        .expire_due(expiring.command.expires_at, 10, &[])
        .await
        .unwrap();
    assert_eq!(expired.len(), 1);
    assert_eq!(expired[0].status, CommandStatus::Expired);
    let counts = repositories
        .storage
        .overview(0, expiring.command.expires_at + 1)
        .await
        .unwrap();
    assert_eq!(counts.devices_total, 1);
    assert_eq!(counts.telemetry_events_24h, 3);
    assert_eq!(counts.commands_expired_24h, 1);
    assert_eq!(repositories.storage.pending_commands().await.unwrap(), 0);

    let updated = repositories
        .devices
        .patch(
            &device_id,
            DevicePatch {
                name: Some("postgres-device-updated".to_owned()),
                metadata_json: None,
                enabled: Some(false),
            },
            now + 10,
            &device_audit,
            Box::new(|| {}),
        )
        .await
        .unwrap();
    assert!(!updated.enabled);
    let (committed_tx, committed_rx) = tokio::sync::oneshot::channel();
    let rotated_credential_id = Uuid::now_v7().to_string();
    repositories
        .devices
        .rotate_credential(
            DeviceCredentialRotation {
                device_id: &device_id,
                expected_credential_id: &credential_id,
                new_credential_id: &rotated_credential_id,
                token_hash: &[12_u8; 32],
                now: now + 11,
                audit: device_audit,
            },
            Box::new(move || {
                let _ = committed_tx.send(());
            }),
        )
        .await
        .unwrap();
    committed_rx.await.unwrap();

    repositories
        .storage
        .audit_config_update(ConfigAudit {
            occurred_at: now + 12,
            actor_id: &admin.id,
            request_id: "postgres-config",
            remote_ip: "127.0.0.1",
            old_revision: 1,
            new_revision: 2,
            changed_fields: &["http.dashboard".to_owned()],
        })
        .await
        .unwrap();
    repositories
        .telemetry_retention
        .delete_before(now + 1, 10)
        .await
        .unwrap();
    assert!(
        repositories
            .sessions
            .delete_expired_sessions(auth_now())
            .await
            .unwrap()
            > 0
    );
    storage.close().await;

    fn auth_now() -> i64 {
        iot_server::core::auth::now_ms()
    }
}

fn is_fixture_target(database: &str, schema: &str) -> bool {
    database == "iot_test" && schema == "public"
}

#[test]
fn postgres_fixture_guard_rejects_database_overrides() {
    assert!(is_fixture_target("iot_test", "public"));
    assert!(!is_fixture_target("production", "public"));
    assert!(!is_fixture_target("iot_test", "tenant_schema"));
}
