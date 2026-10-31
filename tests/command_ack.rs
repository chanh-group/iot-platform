mod common;

use std::time::Duration;

use common::{TestApp, mqtt_connect, mqtt_next, mqtt_subscribe, mqtt_wait_publish};
use iot_server::config::schema::CommandsConfig;
use iot_server::core::auth;
use iot_server::core::command::{
    AckApplyOutcome, Command, CommandAck, CommandAckStatus, CommandCursor, CommandDispatcher,
    CommandRepository, CommandRepositoryError, CommandStatus, CommandWakeReceiver, CreateOutcome,
    NewCommand, spawn_ack_worker,
};
use iot_server::core::device_channel::{DeviceChannel, DeviceChannelError};
use iot_server::storage::sqlite::commands::SqliteCommandRepository;
use rumqttc::{Event as MqttEvent, Packet, QoS, SubscribeReasonCode};
use serde_json::{Value, json};
use sqlx::SqlitePool;
use tokio::sync::{Notify, mpsc};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

async fn insert_command(
    pool: &SqlitePool,
    device_id: &str,
    status: &str,
    expires_at: i64,
) -> String {
    let id = Uuid::now_v7().to_string();
    let created_at = expires_at.saturating_sub(60_000);
    sqlx::query(
        "INSERT INTO commands (
            id, device_id, name, params_json, status, created_at, expires_at, next_attempt_at
         ) VALUES (?, ?, 'reboot', '{}', ?, ?, ?, ?)",
    )
    .bind(&id)
    .bind(device_id)
    .bind(status)
    .bind(created_at)
    .bind(expires_at)
    .bind(created_at)
    .execute(pool)
    .await
    .unwrap();
    id
}

async fn publish_ack(
    client: &rumqttc::AsyncClient,
    eventloop: &mut rumqttc::EventLoop,
    topic: &str,
    payload: &Value,
) {
    client
        .publish(
            topic,
            QoS::AtLeastOnce,
            false,
            payload.to_string().into_bytes(),
        )
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if matches!(
                mqtt_next(eventloop, Duration::from_secs(2)).await.unwrap(),
                MqttEvent::Incoming(Packet::PubAck(_))
            ) {
                break;
            }
        }
    })
    .await
    .expect("device ACK publish should receive MQTT PUBACK");
}

async fn wait_status(pool: &SqlitePool, command_id: &str, expected: &str) {
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let status: String = sqlx::query_scalar("SELECT status FROM commands WHERE id = ?")
                .bind(command_id)
                .fetch_one(pool)
                .await
                .unwrap();
            if status == expected {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("command ACK should be persisted");
}

struct BlockingAckRepository {
    inner: SqliteCommandRepository,
    started: Notify,
    release: Notify,
    completed: Notify,
}

struct OfflineCommandChannel;

fn closed_wakes() -> CommandWakeReceiver {
    let (sender, receiver) = mpsc::channel(1);
    drop(sender);
    CommandWakeReceiver::new(receiver, std::sync::Arc::new(Notify::new()))
}

#[async_trait::async_trait]
impl DeviceChannel for OfflineCommandChannel {
    async fn publish(
        &self,
        _device_id: &str,
        _topic: &str,
        _payload: &[u8],
    ) -> Result<(), DeviceChannelError> {
        Err(DeviceChannelError::Unavailable)
    }

    async fn kick(&self, _device_id: &str) -> Result<(), DeviceChannelError> {
        Ok(())
    }

    async fn is_online(&self, _device_id: &str) -> Result<bool, DeviceChannelError> {
        Ok(false)
    }
}

#[async_trait::async_trait]
impl CommandRepository for BlockingAckRepository {
    async fn create(
        &self,
        command: NewCommand<'_>,
    ) -> Result<CreateOutcome, CommandRepositoryError> {
        self.inner.create(command).await
    }

    async fn get(&self, id: &str) -> Result<Option<Command>, CommandRepositoryError> {
        self.inner.get(id).await
    }

    async fn list(
        &self,
        device_id: &str,
        status: Option<CommandStatus>,
        after: Option<CommandCursor>,
        limit: i64,
    ) -> Result<Vec<Command>, CommandRepositoryError> {
        self.inner.list(device_id, status, after, limit).await
    }

    async fn due(&self, now: i64, limit: i64) -> Result<Vec<Command>, CommandRepositoryError> {
        self.inner.due(now, limit).await
    }

    async fn due_at(
        &self,
        scheduled_at: i64,
        now: i64,
        limit: i64,
    ) -> Result<Vec<Command>, CommandRepositoryError> {
        self.inner.due_at(scheduled_at, now, limit).await
    }

    async fn can_dispatch(
        &self,
        command_id: &str,
        now: i64,
    ) -> Result<bool, CommandRepositoryError> {
        self.inner.can_dispatch(command_id, now).await
    }

    async fn defer_offline(
        &self,
        command_id: &str,
        now: i64,
        next_attempt_at: i64,
    ) -> Result<(), CommandRepositoryError> {
        self.inner
            .defer_offline(command_id, now, next_attempt_at)
            .await
    }

    async fn wake_device(&self, device_id: &str, now: i64) -> Result<u64, CommandRepositoryError> {
        self.inner.wake_device(device_id, now).await
    }

    async fn wake_all(&self, now: i64, limit: i64) -> Result<u64, CommandRepositoryError> {
        self.inner.wake_all(now, limit).await
    }

    async fn record_dispatch_attempt(
        &self,
        command_id: &str,
        attempted_at: i64,
        finished_at: i64,
        next_attempt_at: i64,
        published: bool,
    ) -> Result<(), CommandRepositoryError> {
        self.inner
            .record_dispatch_attempt(
                command_id,
                attempted_at,
                finished_at,
                next_attempt_at,
                published,
            )
            .await
    }

    async fn expire_due(
        &self,
        now: i64,
        limit: i64,
        pending_ack_ids: &[String],
    ) -> Result<u64, CommandRepositoryError> {
        self.inner.expire_due(now, limit, pending_ack_ids).await
    }

    async fn apply_ack(
        &self,
        device_id: &str,
        ack: &CommandAck,
        received_at: i64,
    ) -> Result<AckApplyOutcome, CommandRepositoryError> {
        self.started.notify_one();
        self.release.notified().await;
        let result = self.inner.apply_ack(device_id, ack, received_at).await;
        self.completed.notify_one();
        result
    }
}

#[tokio::test]
async fn mqtt_failed_ack_persists_result_and_error_after_acl_allow() {
    let app = TestApp::start().await;
    let device = app.create_device("command-ack-device", true).await;
    let other_device = app.create_device("command-ack-spoof", true).await;
    let (client, mut eventloop, _) =
        mqtt_connect(app.mqtt_addr, &device.id, &device.id, &device.token, true).await;
    let command_topic = format!("devices/{}/commands", device.id);
    assert_eq!(
        mqtt_subscribe(&client, &mut eventloop, &command_topic, QoS::AtLeastOnce).await,
        SubscribeReasonCode::Success(QoS::AtLeastOnce)
    );

    let command_id =
        insert_command(&app.pool, &device.id, "pending", auth::now_ms() + 60_000).await;
    let command = mqtt_wait_publish(&mut eventloop, Duration::from_secs(4)).await;
    let command_payload: Value = serde_json::from_slice(&command.payload).unwrap();
    assert_eq!(command_payload["command_id"], command_id);

    let (other_client, mut other_eventloop, _) = mqtt_connect(
        app.mqtt_addr,
        &other_device.id,
        &other_device.id,
        &other_device.token,
        true,
    )
    .await;
    let other_ack_topic = format!("devices/{}/commands/ack", other_device.id);
    publish_ack(
        &other_client,
        &mut other_eventloop,
        &other_ack_topic,
        &json!({
            "command_id": command_id,
            "status": "succeeded",
            "result": {"spoofed":true}
        }),
    )
    .await;

    let ack_topic = format!("devices/{}/commands/ack", device.id);
    publish_ack(
        &client,
        &mut eventloop,
        &ack_topic,
        &json!({"command_id":command_id,"status":"pending"}),
    )
    .await;
    let status: String = sqlx::query_scalar("SELECT status FROM commands WHERE id = ?")
        .bind(&command_id)
        .fetch_one(&app.pool)
        .await
        .unwrap();
    assert_eq!(status, "dispatched");

    publish_ack(
        &client,
        &mut eventloop,
        &ack_topic,
        &json!({
            "command_id": command_id,
            "status": "failed",
            "result": {"attempt": 2},
            "error": "relay did not respond"
        }),
    )
    .await;
    wait_status(&app.pool, &command_id, "failed").await;

    let row: (Option<i64>, Option<String>, Option<String>) =
        sqlx::query_as("SELECT completed_at, result_json, error FROM commands WHERE id = ?")
            .bind(command_id)
            .fetch_one(&app.pool)
            .await
            .unwrap();
    assert!(row.0.is_some());
    assert_eq!(row.1.as_deref(), Some(r#"{"attempt":2}"#));
    assert_eq!(row.2.as_deref(), Some("relay did not respond"));
}

#[tokio::test]
async fn retained_command_ack_is_denied_before_queueing() {
    let app = TestApp::start().await;
    let device = app.create_device("command-retained-ack", true).await;
    let (client, mut eventloop, _) =
        mqtt_connect(app.mqtt_addr, &device.id, &device.id, &device.token, true).await;
    let topic = format!("devices/{}/commands/ack", device.id);
    let payload = json!({
        "command_id": Uuid::now_v7().to_string(),
        "status": "succeeded",
    });
    client
        .publish(
            topic,
            QoS::AtLeastOnce,
            true,
            payload.to_string().into_bytes(),
        )
        .await
        .unwrap();
    common::mqtt_wait_closed(&mut eventloop, Duration::from_secs(3)).await;
    assert_eq!(app.retained_count().await, 0);
}

#[tokio::test]
async fn duplicate_unknown_cross_device_and_late_acks_are_atomic() {
    let app = TestApp::start().await;
    let device = app.create_device("command-ack-state", true).await;
    let other = app.create_device("command-ack-other", true).await;
    let now = auth::now_ms();
    let live_id = insert_command(&app.pool, &device.id, "dispatched", now + 60_000).await;
    let repository = SqliteCommandRepository::new(app.pool.clone());

    let ack = CommandAck {
        command_id: live_id.clone(),
        status: CommandAckStatus::Succeeded,
        result: Some(json!({"version":1})),
        error: None,
    };
    assert_eq!(
        repository.apply_ack(&other.id, &ack, now).await.unwrap(),
        AckApplyOutcome::WrongDevice
    );
    let unknown = CommandAck {
        command_id: Uuid::now_v7().to_string(),
        ..ack.clone()
    };
    assert_eq!(
        repository
            .apply_ack(&device.id, &unknown, now)
            .await
            .unwrap(),
        AckApplyOutcome::UnknownCommand
    );
    assert_eq!(
        repository.apply_ack(&device.id, &ack, now).await.unwrap(),
        AckApplyOutcome::Applied
    );
    let completed_before: (Option<i64>, Option<String>) =
        sqlx::query_as("SELECT completed_at, result_json FROM commands WHERE id = ?")
            .bind(&live_id)
            .fetch_one(&app.pool)
            .await
            .unwrap();
    let duplicate = CommandAck {
        status: CommandAckStatus::Failed,
        result: Some(json!({"version":2})),
        error: Some("must not replace terminal result".to_owned()),
        ..ack
    };
    assert_eq!(
        repository
            .apply_ack(&device.id, &duplicate, now + 1)
            .await
            .unwrap(),
        AckApplyOutcome::Duplicate
    );
    let completed_after: (Option<i64>, Option<String>, String, Option<String>) = sqlx::query_as(
        "SELECT completed_at, result_json, status, error FROM commands WHERE id = ?",
    )
    .bind(&live_id)
    .fetch_one(&app.pool)
    .await
    .unwrap();
    assert_eq!(completed_after.0, completed_before.0);
    assert_eq!(completed_after.1, completed_before.1);
    assert_eq!(completed_after.2, "succeeded");
    assert_eq!(completed_after.3, None);

    let expired_id = insert_command(&app.pool, &device.id, "pending", now - 1).await;
    let late_ack = CommandAck {
        command_id: expired_id.clone(),
        status: CommandAckStatus::Succeeded,
        result: Some(json!({"late":true})),
        error: None,
    };
    assert_eq!(
        repository
            .apply_ack(&device.id, &late_ack, now)
            .await
            .unwrap(),
        AckApplyOutcome::ExpiredAndLateRecorded
    );
    let late: (String, Option<i64>, Option<i64>, Option<String>) = sqlx::query_as(
        "SELECT status, completed_at, late_ack_at, late_ack_json
         FROM commands WHERE id = ?",
    )
    .bind(&expired_id)
    .fetch_one(&app.pool)
    .await
    .unwrap();
    assert_eq!(late.0, "expired");
    assert_eq!(late.1, Some(now - 1));
    assert_eq!(late.2, Some(now));
    assert!(late.3.unwrap().contains("late"));
    let on_time_but_already_terminal = CommandAck {
        command_id: expired_id.clone(),
        status: CommandAckStatus::Succeeded,
        result: Some(json!({"must_not_reopen":true})),
        error: None,
    };
    assert_eq!(
        repository
            .apply_ack(&device.id, &on_time_but_already_terminal, now - 2)
            .await
            .unwrap(),
        AckApplyOutcome::Duplicate
    );
    let final_status: String = sqlx::query_scalar("SELECT status FROM commands WHERE id = ?")
        .bind(expired_id)
        .fetch_one(&app.pool)
        .await
        .unwrap();
    assert_eq!(final_status, "expired");
}

#[tokio::test]
async fn on_time_queued_ack_defers_expiry_while_worker_is_blocked() {
    let pool = SqlitePool::connect("sqlite::memory:").await.unwrap();
    iot_server::storage::sqlite::migrate::run(&pool)
        .await
        .unwrap();
    let device_id = Uuid::now_v7().to_string();
    let before_send = auth::now_ms();
    let expires_at = before_send + 5_000;
    sqlx::query(
        "INSERT INTO devices (id, name, enabled, created_at, updated_at)
         VALUES (?, 'queued-ack-expiry', 1, ?, ?)",
    )
    .bind(&device_id)
    .bind(before_send - 60_000)
    .bind(before_send - 60_000)
    .execute(&pool)
    .await
    .unwrap();
    let command_id = insert_command(&pool, &device_id, "pending", expires_at).await;
    let repository = SqliteCommandRepository::new(pool.clone());
    let gated = std::sync::Arc::new(BlockingAckRepository {
        inner: repository.clone(),
        started: Notify::new(),
        release: Notify::new(),
        completed: Notify::new(),
    });
    let cancel = CancellationToken::new();
    let (handle, worker) = spawn_ack_worker(gated.clone(), cancel.clone());
    let payload = json!({
        "command_id": command_id,
        "status": "succeeded",
        "result": {"queued": true},
    })
    .to_string();

    handle.try_send(&device_id, payload.as_bytes());
    let after_send = auth::now_ms();
    tokio::time::timeout(Duration::from_secs(1), gated.started.notified())
        .await
        .expect("ACK worker should pick up the queued ACK");
    let dispatcher = CommandDispatcher::new(
        std::sync::Arc::new(repository.clone()),
        std::sync::Arc::new(OfflineCommandChannel),
        CommandsConfig::default(),
        handle.pending(),
        closed_wakes(),
    );
    dispatcher.run_once(expires_at + 1).await.unwrap();
    let before_ack_persistence: String =
        sqlx::query_scalar("SELECT status FROM commands WHERE id = ?")
            .bind(&command_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(before_ack_persistence, "pending");
    gated.release.notify_one();
    tokio::time::timeout(Duration::from_secs(1), gated.completed.notified())
        .await
        .expect("ACK worker should persist after the expiry sweep");

    let row: (String, i64, Option<i64>, Option<String>) = sqlx::query_as(
        "SELECT status, completed_at, late_ack_at, result_json FROM commands WHERE id = ?",
    )
    .bind(&command_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(row.0, "succeeded");
    assert!((before_send..=after_send).contains(&row.1));
    assert_eq!(row.2, None);
    assert_eq!(row.3.as_deref(), Some(r#"{"queued":true}"#));

    cancel.cancel();
    worker.await.unwrap();
}

#[tokio::test]
async fn ack_races_with_dispatch_and_expiry_never_reopens_a_terminal_command() {
    let app = TestApp::start().await;
    let device = app.create_device("command-ack-races", true).await;
    let repository = SqliteCommandRepository::new(app.pool.clone());
    let now = auth::now_ms();
    let dispatch_race_id = insert_command(&app.pool, &device.id, "pending", now + 60_000).await;
    let ack = CommandAck {
        command_id: dispatch_race_id.clone(),
        status: CommandAckStatus::Succeeded,
        result: Some(json!({"ok":true})),
        error: None,
    };
    let (ack_result, dispatch_result) = tokio::join!(
        repository.apply_ack(&device.id, &ack, now),
        repository.record_dispatch_attempt(&dispatch_race_id, now, now, now + 1_000, true,),
    );
    assert_eq!(ack_result.unwrap(), AckApplyOutcome::Applied);
    dispatch_result.unwrap();
    let terminal: (String, Option<String>) =
        sqlx::query_as("SELECT status, result_json FROM commands WHERE id = ?")
            .bind(&dispatch_race_id)
            .fetch_one(&app.pool)
            .await
            .unwrap();
    assert_eq!(terminal.0, "succeeded");
    assert_eq!(terminal.1.as_deref(), Some(r#"{"ok":true}"#));

    let expiry_race_id = insert_command(&app.pool, &device.id, "pending", now - 1).await;
    let late_ack = CommandAck {
        command_id: expiry_race_id.clone(),
        status: CommandAckStatus::Failed,
        result: None,
        error: Some("expired".to_owned()),
    };
    let (_expired_count, late_result) = tokio::join!(
        repository.expire_due(now, 100, &[]),
        repository.apply_ack(&device.id, &late_ack, now),
    );
    assert!(matches!(
        late_result.unwrap(),
        AckApplyOutcome::LateRecorded | AckApplyOutcome::ExpiredAndLateRecorded
    ));
    let state: (String, Option<i64>, Option<String>) =
        sqlx::query_as("SELECT status, late_ack_at, late_ack_json FROM commands WHERE id = ?")
            .bind(expiry_race_id)
            .fetch_one(&app.pool)
            .await
            .unwrap();
    assert_eq!(state.0, "expired");
    assert_eq!(state.1, Some(now));
    assert!(state.2.is_some());
}

#[tokio::test]
async fn ack_storage_failure_does_not_create_a_terminal_state() {
    let app = TestApp::start().await;
    let device = app.create_device("command-ack-storage", true).await;
    let now = auth::now_ms();
    let command_id = insert_command(&app.pool, &device.id, "dispatched", now + 60_000).await;
    sqlx::query(
        "CREATE TRIGGER fail_command_ack BEFORE UPDATE OF status ON commands
         WHEN NEW.status IN ('succeeded', 'failed')
         BEGIN SELECT RAISE(FAIL, 'injected ACK storage failure'); END",
    )
    .execute(&app.pool)
    .await
    .unwrap();
    let repository = SqliteCommandRepository::new(app.pool.clone());
    let ack = CommandAck {
        command_id: command_id.clone(),
        status: CommandAckStatus::Succeeded,
        result: None,
        error: None,
    };
    assert!(repository.apply_ack(&device.id, &ack, now).await.is_err());
    let status: String = sqlx::query_scalar("SELECT status FROM commands WHERE id = ?")
        .bind(command_id)
        .fetch_one(&app.pool)
        .await
        .unwrap();
    assert_eq!(status, "dispatched");
}
