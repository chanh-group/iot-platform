mod common;

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use common::{TestApp, mqtt_connect, mqtt_subscribe, mqtt_wait_publish, wait_until};
use iot_server::config::schema::CommandsConfig;
use iot_server::core::auth;
use iot_server::core::command::{
    CommandDispatcher, CommandRepository, CommandWakeReceiver, PendingCommandAcks,
};
use iot_server::core::device_channel::{DeviceChannel, DeviceChannelError};
use iot_server::storage::sqlite::commands::SqliteCommandRepository;
use rumqttc::{Event as MqttEvent, Packet, QoS, SubscribeReasonCode};
use serde_json::{Value, json};
use sqlx::SqlitePool;
use tokio::sync::{Notify, mpsc};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

async fn admin_client(app: &TestApp) -> reqwest::Client {
    app.create_admin("command-offline-admin", "command-offline-password")
        .await;
    let client = app.client();
    let response = client
        .post(format!("{}/api/v1/auth/login", app.base))
        .header("X-IoT-CSRF", "1")
        .json(&json!({
            "username": "command-offline-admin",
            "password": "command-offline-password"
        }))
        .send()
        .await
        .unwrap();
    assert!(response.status().is_success());
    client
}

async fn create_command(client: &reqwest::Client, app: &TestApp, device_id: &str) -> Value {
    let response = client
        .post(format!("{}/api/v1/devices/{device_id}/commands", app.base))
        .header("X-IoT-CSRF", "1")
        .json(&json!({"name":"reboot","params":{"delay_s":5}}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::ACCEPTED);
    response.json().await.unwrap()
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
    .expect("dispatcher should persist the published state");
}

#[tokio::test]
async fn offline_command_stays_in_sql_then_restart_recovery_publishes_same_id() {
    let mut app = TestApp::start().await;
    let device = app.create_device("command-offline", true).await;
    let http = admin_client(&app).await;
    let created = create_command(&http, &app, &device.id).await;
    let command_id = created["command_id"].as_str().unwrap().to_owned();

    let status: String = sqlx::query_scalar("SELECT status FROM commands WHERE id = ?")
        .bind(&command_id)
        .fetch_one(&app.pool)
        .await
        .unwrap();
    assert_eq!(status, "pending");
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT attempts FROM commands WHERE id = ?")
            .bind(&command_id)
            .fetch_one(&app.pool)
            .await
            .unwrap(),
        0
    );

    app.restart_mqtt().await;
    let (client, mut eventloop, _) =
        mqtt_connect(app.mqtt_addr, &device.id, &device.id, &device.token, true).await;
    let topic = format!("devices/{}/commands", device.id);
    assert_eq!(
        mqtt_subscribe(&client, &mut eventloop, &topic, QoS::AtLeastOnce).await,
        SubscribeReasonCode::Success(QoS::AtLeastOnce)
    );

    let publish = mqtt_wait_publish(&mut eventloop, Duration::from_secs(5)).await;
    assert_eq!(publish.topic, topic);
    assert_eq!(publish.qos, QoS::AtLeastOnce);
    assert!(!publish.retain);
    let payload: Value = serde_json::from_slice(&publish.payload).unwrap();
    assert_eq!(payload["command_id"], command_id);
    assert_eq!(payload["name"], "reboot");
    assert_eq!(payload["params"], json!({"delay_s":5}));
    assert_eq!(
        payload["created_at"],
        created["expires_at"].as_i64().unwrap() - 60_000
    );

    wait_until(
        || app.presence.is_online(&device.id).unwrap_or(false),
        Duration::from_secs(1),
        "device reconnect",
    )
    .await;
    wait_status(&app.pool, &command_id, "dispatched").await;
}

#[tokio::test]
async fn reconnect_resends_with_the_persisted_command_id() {
    let app = TestApp::start().await;
    let device = app.create_device("command-resend", true).await;
    let (client, mut eventloop, _) =
        mqtt_connect(app.mqtt_addr, &device.id, &device.id, &device.token, true).await;
    let topic = format!("devices/{}/commands", device.id);
    assert_eq!(
        mqtt_subscribe(&client, &mut eventloop, &topic, QoS::AtLeastOnce).await,
        SubscribeReasonCode::Success(QoS::AtLeastOnce)
    );
    let http = admin_client(&app).await;
    let created = create_command(&http, &app, &device.id).await;
    let command_id = created["command_id"].as_str().unwrap().to_owned();
    let first = mqtt_wait_publish(&mut eventloop, Duration::from_secs(4)).await;
    assert_eq!(
        serde_json::from_slice::<Value>(&first.payload).unwrap()["command_id"],
        command_id
    );

    drop(client);
    drop(eventloop);
    wait_until(
        || !app.presence.is_online(&device.id).unwrap_or(true),
        Duration::from_secs(2),
        "device disconnect",
    )
    .await;
    let (client, mut eventloop, _) =
        mqtt_connect(app.mqtt_addr, &device.id, &device.id, &device.token, true).await;
    assert_eq!(
        mqtt_subscribe(&client, &mut eventloop, &topic, QoS::AtLeastOnce).await,
        SubscribeReasonCode::Success(QoS::AtLeastOnce)
    );
    let resent = mqtt_wait_publish(&mut eventloop, Duration::from_secs(5)).await;
    let payload: Value = serde_json::from_slice(&resent.payload).unwrap();
    assert_eq!(payload["command_id"], command_id);
}

#[tokio::test]
async fn clean_reconnect_redelivers_after_a_late_command_subscription() {
    let app = TestApp::builder()
        .command_dispatch_timing(100, 10_000)
        .start()
        .await;
    let device = app.create_device("command-late-subscription", true).await;
    let http = admin_client(&app).await;
    let response = http
        .post(format!(
            "{}/api/v1/devices/{}/commands",
            app.base, device.id
        ))
        .header("X-IoT-CSRF", "1")
        .json(&json!({"name":"reboot","params":{},"ttl_ms":4_000}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::ACCEPTED);
    let command: Value = response.json().await.unwrap();
    let command_id = command["command_id"].as_str().unwrap().to_owned();
    let expires_at = command["expires_at"].as_i64().unwrap();

    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            let next_attempt_at: i64 =
                sqlx::query_scalar("SELECT next_attempt_at FROM commands WHERE id = ?")
                    .bind(&command_id)
                    .fetch_one(&app.pool)
                    .await
                    .unwrap();
            if next_attempt_at > auth::now_ms() + 5_000 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("offline dispatcher should defer the command before reconnect");

    let (client, mut eventloop, session_present) =
        mqtt_connect(app.mqtt_addr, &device.id, &device.id, &device.token, true).await;
    assert!(
        !session_present,
        "clean reconnect starts without subscriptions"
    );
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            let attempts: i64 = sqlx::query_scalar("SELECT attempts FROM commands WHERE id = ?")
                .bind(&command_id)
                .fetch_one(&app.pool)
                .await
                .unwrap();
            if attempts >= 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("connection wake should make the first publish attempt before subscribe");
    tokio::time::sleep(Duration::from_millis(100)).await;

    let topic = format!("devices/{}/commands", device.id);
    client.subscribe(&topic, QoS::AtLeastOnce).await.unwrap();
    let mut received = None;
    let mut subscribed = false;
    tokio::time::timeout(Duration::from_secs(2), async {
        while !subscribed || received.is_none() {
            match eventloop.poll().await.expect("poll MQTT event") {
                MqttEvent::Incoming(Packet::SubAck(ack)) => {
                    assert_eq!(
                        ack.return_codes[0],
                        SubscribeReasonCode::Success(QoS::AtLeastOnce)
                    );
                    subscribed = true;
                }
                MqttEvent::Incoming(Packet::Publish(publish)) if publish.topic == topic => {
                    received = Some(publish);
                }
                _ => {}
            }
        }
    })
    .await
    .expect("successful command subscription should trigger immediate redelivery");

    let publish = received.expect("receive command after subscription");
    let payload: Value = serde_json::from_slice(&publish.payload).unwrap();
    assert_eq!(payload["command_id"], command_id);
    assert!(
        auth::now_ms() < expires_at,
        "command must arrive before expiry"
    );
}

struct TestChannel {
    online: bool,
    publish_error: bool,
    entered: Option<Arc<Notify>>,
    resume: Option<Arc<Notify>>,
    online_checks: AtomicUsize,
    published: AtomicUsize,
    payloads: Mutex<Vec<Vec<u8>>>,
}

#[async_trait::async_trait]
impl DeviceChannel for TestChannel {
    async fn publish(
        &self,
        _device_id: &str,
        _topic: &str,
        payload: &[u8],
    ) -> Result<(), DeviceChannelError> {
        self.published.fetch_add(1, Ordering::Relaxed);
        self.payloads.lock().unwrap().push(payload.to_vec());
        if self.publish_error {
            Err(DeviceChannelError::Unavailable)
        } else {
            Ok(())
        }
    }

    async fn kick(&self, _device_id: &str) -> Result<(), DeviceChannelError> {
        Ok(())
    }

    async fn is_online(&self, _device_id: &str) -> Result<bool, DeviceChannelError> {
        if self.online_checks.fetch_add(1, Ordering::Relaxed) == 0 {
            if let Some(entered) = &self.entered {
                entered.notify_one();
            }
            if let Some(resume) = &self.resume {
                resume.notified().await;
            }
        }
        Ok(self.online)
    }
}

async fn fixture() -> (SqlitePool, SqliteCommandRepository, String, String) {
    let pool = SqlitePool::connect("sqlite::memory:").await.unwrap();
    iot_server::storage::sqlite::migrate::run(&pool)
        .await
        .unwrap();
    let device_id = Uuid::now_v7().to_string();
    let now = auth::now_ms();
    sqlx::query(
        "INSERT INTO devices (id, name, enabled, created_at, updated_at)
         VALUES (?, 'dispatcher-test', 1, ?, ?)",
    )
    .bind(&device_id)
    .bind(now)
    .bind(now)
    .execute(&pool)
    .await
    .unwrap();
    let command_id = Uuid::now_v7().to_string();
    sqlx::query(
        "INSERT INTO commands (
            id, device_id, name, params_json, status, created_at, expires_at, next_attempt_at
         ) VALUES (?, ?, 'reboot', '{}', 'pending', ?, ?, ?)",
    )
    .bind(&command_id)
    .bind(&device_id)
    .bind(now)
    .bind(now + 60_000)
    .bind(now)
    .execute(&pool)
    .await
    .unwrap();
    (
        pool.clone(),
        SqliteCommandRepository::new(pool),
        device_id,
        command_id,
    )
}

fn config() -> CommandsConfig {
    CommandsConfig {
        dispatch_interval_ms: 100,
        retry_initial_ms: 100,
        retry_max_ms: 1_000,
        ..CommandsConfig::default()
    }
}

fn closed_wakes() -> CommandWakeReceiver {
    let (sender, receiver) = mpsc::channel(1);
    drop(sender);
    CommandWakeReceiver::new(receiver, Arc::new(Notify::new()))
}

#[tokio::test]
async fn failed_publish_only_advances_retry_metadata_and_keeps_pending() {
    let (pool, repository, _device_id, command_id) = fixture().await;
    let now = auth::now_ms();
    let channel = Arc::new(TestChannel {
        online: true,
        publish_error: true,
        entered: None,
        resume: None,
        online_checks: AtomicUsize::new(0),
        published: AtomicUsize::new(0),
        payloads: Mutex::new(Vec::new()),
    });
    let dispatcher = CommandDispatcher::new(
        Arc::new(repository),
        channel,
        config(),
        PendingCommandAcks::default(),
        closed_wakes(),
    );
    dispatcher.run_once(now).await.unwrap();
    let row: (String, i64, i64) =
        sqlx::query_as("SELECT status, attempts, next_attempt_at FROM commands WHERE id = ?")
            .bind(command_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(row.0, "pending");
    assert_eq!(row.1, 1);
    assert!(row.2 > now);
}

#[tokio::test]
async fn reconnect_wake_dispatches_before_the_next_poll_for_a_short_ttl_command() {
    let (pool, repository, device_id, command_id) = fixture().await;
    let now = auth::now_ms();
    sqlx::query("UPDATE commands SET expires_at = ?, next_attempt_at = ? WHERE id = ?")
        .bind(now + 2_000)
        .bind(now + 10_000)
        .bind(&command_id)
        .execute(&pool)
        .await
        .unwrap();

    let (wake_sender, wake_receiver) = mpsc::channel(1);
    let channel = Arc::new(TestChannel {
        online: true,
        publish_error: false,
        entered: None,
        resume: None,
        online_checks: AtomicUsize::new(0),
        published: AtomicUsize::new(0),
        payloads: Mutex::new(Vec::new()),
    });
    let dispatcher = CommandDispatcher::new(
        Arc::new(repository),
        channel.clone(),
        CommandsConfig {
            dispatch_interval_ms: 10_000,
            retry_initial_ms: 10_000,
            retry_max_ms: 10_000,
            ..CommandsConfig::default()
        },
        PendingCommandAcks::default(),
        CommandWakeReceiver::new(wake_receiver, Arc::new(Notify::new())),
    );
    let cancel = CancellationToken::new();
    let task = tokio::spawn(dispatcher.run(cancel.clone(), Arc::new(AtomicBool::new(false))));

    wake_sender.send(device_id).await.unwrap();
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            let status: String = sqlx::query_scalar("SELECT status FROM commands WHERE id = ?")
                .bind(&command_id)
                .fetch_one(&pool)
                .await
                .unwrap();
            if status == "dispatched" {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("reconnect wake should dispatch before the short TTL expires");
    assert_eq!(channel.published.load(Ordering::Relaxed), 1);

    cancel.cancel();
    task.await.unwrap();
}

#[tokio::test]
async fn full_wake_queue_fallback_dispatches_a_short_ttl_command() {
    let (pool, repository, device_id, command_id) = fixture().await;
    let now = auth::now_ms();
    sqlx::query("UPDATE commands SET expires_at = ?, next_attempt_at = ? WHERE id = ?")
        .bind(now + 2_000)
        .bind(now + 10_000)
        .bind(&command_id)
        .execute(&pool)
        .await
        .unwrap();

    let (wake_sender, wake_receiver) = mpsc::channel(1);
    let wake_overflow = Arc::new(Notify::new());
    wake_sender.try_send("unrelated-device".to_owned()).unwrap();
    assert!(wake_sender.try_send(device_id).is_err());
    wake_overflow.notify_one();

    let channel = Arc::new(TestChannel {
        online: true,
        publish_error: false,
        entered: None,
        resume: None,
        online_checks: AtomicUsize::new(0),
        published: AtomicUsize::new(0),
        payloads: Mutex::new(Vec::new()),
    });
    let dispatcher = CommandDispatcher::new(
        Arc::new(repository),
        channel.clone(),
        CommandsConfig {
            dispatch_interval_ms: 10_000,
            retry_initial_ms: 10_000,
            retry_max_ms: 10_000,
            ..CommandsConfig::default()
        },
        PendingCommandAcks::default(),
        CommandWakeReceiver::new(wake_receiver, wake_overflow),
    );
    let cancel = CancellationToken::new();
    let task = tokio::spawn(dispatcher.run(cancel.clone(), Arc::new(AtomicBool::new(false))));

    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            let status: String = sqlx::query_scalar("SELECT status FROM commands WHERE id = ?")
                .bind(&command_id)
                .fetch_one(&pool)
                .await
                .unwrap();
            if status == "dispatched" {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("overflow recovery should dispatch before the short TTL expires");
    assert_eq!(channel.published.load(Ordering::Relaxed), 1);

    cancel.cancel();
    task.await.unwrap();
}

#[tokio::test]
async fn overflow_drain_expires_a_full_batch_that_ages_out_while_dispatch_is_blocked() {
    let (pool, repository, device_id, _) = fixture().await;
    let now = auth::now_ms();
    for _ in 1..100 {
        let command_id = Uuid::now_v7().to_string();
        sqlx::query(
            "INSERT INTO commands (
                id, device_id, name, params_json, status, created_at, expires_at, next_attempt_at
             ) VALUES (?, ?, 'reboot', '{}', 'pending', ?, ?, ?)",
        )
        .bind(&command_id)
        .bind(&device_id)
        .bind(now)
        .bind(now + 60_000)
        .bind(now + 10_000)
        .execute(&pool)
        .await
        .unwrap();
    }
    let expires_at = auth::now_ms() + 1_000;
    sqlx::query("UPDATE commands SET expires_at = ?, next_attempt_at = ? WHERE device_id = ?")
        .bind(expires_at)
        .bind(auth::now_ms() + 10_000)
        .bind(&device_id)
        .execute(&pool)
        .await
        .unwrap();

    let (entered, resume) = (Arc::new(Notify::new()), Arc::new(Notify::new()));
    let channel = Arc::new(TestChannel {
        online: true,
        publish_error: false,
        entered: Some(Arc::clone(&entered)),
        resume: Some(Arc::clone(&resume)),
        online_checks: AtomicUsize::new(0),
        published: AtomicUsize::new(0),
        payloads: Mutex::new(Vec::new()),
    });
    let (wake_sender, wake_receiver) = mpsc::channel(1);
    let wake_overflow = Arc::new(Notify::new());
    wake_sender.try_send("unrelated-device".to_owned()).unwrap();
    assert!(wake_sender.try_send(device_id).is_err());
    wake_overflow.notify_one();

    let dispatcher = CommandDispatcher::new(
        Arc::new(repository),
        channel,
        CommandsConfig {
            dispatch_interval_ms: 10_000,
            retry_initial_ms: 10_000,
            retry_max_ms: 10_000,
            ..CommandsConfig::default()
        },
        PendingCommandAcks::default(),
        CommandWakeReceiver::new(wake_receiver, wake_overflow),
    );
    let cancel = CancellationToken::new();
    let task = tokio::spawn(dispatcher.run(cancel.clone(), Arc::new(AtomicBool::new(false))));

    tokio::time::timeout(Duration::from_millis(400), entered.notified())
        .await
        .expect("overflow drain should reach the first due command");
    let remaining = expires_at.saturating_sub(auth::now_ms()).max(0);
    tokio::time::sleep(Duration::from_millis(
        u64::try_from(remaining).unwrap() + 50,
    ))
    .await;
    resume.notify_one();

    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            let expired: i64 =
                sqlx::query_scalar("SELECT COUNT(*) FROM commands WHERE status = 'expired'")
                    .fetch_one(&pool)
                    .await
                    .unwrap();
            if expired == 100 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("fresh expiry sweeps should advance past the stale due batch");

    cancel.cancel();
    tokio::time::timeout(Duration::from_secs(1), task)
        .await
        .expect("dispatcher cancellation should not be trapped in overflow drain")
        .unwrap();
}

#[tokio::test]
async fn disabled_device_commands_are_not_dispatched() {
    let (pool, repository, device_id, _command_id) = fixture().await;
    sqlx::query("UPDATE devices SET enabled = 0 WHERE id = ?")
        .bind(&device_id)
        .execute(&pool)
        .await
        .unwrap();
    let channel = Arc::new(TestChannel {
        online: true,
        publish_error: false,
        entered: None,
        resume: None,
        online_checks: AtomicUsize::new(0),
        published: AtomicUsize::new(0),
        payloads: Mutex::new(Vec::new()),
    });
    let dispatcher = CommandDispatcher::new(
        Arc::new(repository),
        channel.clone(),
        config(),
        PendingCommandAcks::default(),
        closed_wakes(),
    );
    dispatcher.run_once(auth::now_ms()).await.unwrap();
    assert_eq!(channel.published.load(Ordering::Relaxed), 0);
}

#[tokio::test]
async fn dispatcher_rechecks_expiry_after_online_check_before_publish() {
    let (pool, repository, _device_id, command_id) = fixture().await;
    let started = Arc::new(Notify::new());
    let resume = Arc::new(Notify::new());
    let channel = Arc::new(TestChannel {
        online: true,
        publish_error: false,
        entered: Some(Arc::clone(&started)),
        resume: Some(Arc::clone(&resume)),
        online_checks: AtomicUsize::new(0),
        published: AtomicUsize::new(0),
        payloads: Mutex::new(Vec::new()),
    });
    let dispatcher = CommandDispatcher::new(
        Arc::new(repository),
        channel.clone(),
        config(),
        PendingCommandAcks::default(),
        closed_wakes(),
    );
    let now = auth::now_ms();
    let task = tokio::spawn(async move { dispatcher.run_once(now - 1).await });
    tokio::time::timeout(Duration::from_secs(1), started.notified())
        .await
        .expect("dispatcher checks online state");
    sqlx::query("UPDATE commands SET expires_at = ? WHERE id = ?")
        .bind(auth::now_ms() - 1)
        .bind(&command_id)
        .execute(&pool)
        .await
        .unwrap();
    resume.notify_one();
    task.await.unwrap().unwrap();
    assert_eq!(channel.published.load(Ordering::Relaxed), 0);
    let repository = SqliteCommandRepository::new(pool.clone());
    repository
        .expire_due(auth::now_ms(), 100, &[])
        .await
        .unwrap();
    let status: String = sqlx::query_scalar("SELECT status FROM commands WHERE id = ?")
        .bind(command_id)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(status, "expired");
}

#[tokio::test]
async fn dispatched_command_recovers_from_sql_after_dispatcher_restart() {
    let (pool, repository, _device_id, command_id) = fixture().await;
    let now = auth::now_ms();
    sqlx::query(
        "UPDATE commands SET status = 'dispatched', dispatched_at = ?, attempts = 1,
             last_attempt_at = ?, next_attempt_at = ? WHERE id = ?",
    )
    .bind(now - 100)
    .bind(now - 100)
    .bind(now - 1)
    .bind(&command_id)
    .execute(&pool)
    .await
    .unwrap();
    let channel = Arc::new(TestChannel {
        online: true,
        publish_error: false,
        entered: None,
        resume: None,
        online_checks: AtomicUsize::new(0),
        published: AtomicUsize::new(0),
        payloads: Mutex::new(Vec::new()),
    });

    CommandDispatcher::new(
        Arc::new(repository),
        channel.clone(),
        config(),
        PendingCommandAcks::default(),
        closed_wakes(),
    )
    .run_once(now)
    .await
    .unwrap();

    assert_eq!(channel.published.load(Ordering::Relaxed), 1);
    let payload: Value = serde_json::from_slice(&channel.payloads.lock().unwrap()[0]).unwrap();
    assert_eq!(payload["command_id"], command_id);
    assert!(payload.get("device_id").is_none());
    let row: (String, i64) = sqlx::query_as("SELECT status, attempts FROM commands WHERE id = ?")
        .bind(command_id)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(row, ("dispatched".to_owned(), 2));
}

#[tokio::test]
async fn command_write_failure_lowers_readiness_until_a_command_write_succeeds() {
    let app = TestApp::start().await;
    let device = app.create_device("command-readiness", true).await;
    let (client, mut eventloop, _) =
        mqtt_connect(app.mqtt_addr, &device.id, &device.id, &device.token, true).await;
    let topic = format!("devices/{}/commands", device.id);
    assert_eq!(
        mqtt_subscribe(&client, &mut eventloop, &topic, QoS::AtLeastOnce).await,
        SubscribeReasonCode::Success(QoS::AtLeastOnce)
    );
    let now = auth::now_ms();
    let command_id = Uuid::now_v7().to_string();
    sqlx::query(
        "INSERT INTO commands (
            id, device_id, name, params_json, status, created_at, expires_at, next_attempt_at
         ) VALUES (?, ?, 'readiness-probe', '{}', 'pending', ?, ?, ?)",
    )
    .bind(&command_id)
    .bind(&device.id)
    .bind(now)
    .bind(now + 60_000)
    .bind(now)
    .execute(&app.pool)
    .await
    .unwrap();
    sqlx::query(
        "CREATE TRIGGER fail_command_dispatch BEFORE UPDATE OF attempts ON commands
         WHEN NEW.name = 'readiness-probe'
         BEGIN SELECT RAISE(FAIL, 'injected dispatcher write failure'); END",
    )
    .execute(&app.pool)
    .await
    .unwrap();

    let command = mqtt_wait_publish(&mut eventloop, Duration::from_secs(2)).await;
    assert_eq!(command.topic, topic);
    wait_until(
        || !app.command_ready.load(Ordering::Acquire),
        Duration::from_secs(2),
        "command storage readiness to drop after failed write",
    )
    .await;
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT 1")
            .fetch_one(&app.pool)
            .await
            .unwrap(),
        1
    );
    let ready = reqwest::Client::new()
        .get(format!("{}/readyz", app.base))
        .send()
        .await
        .unwrap();
    assert_eq!(ready.status(), reqwest::StatusCode::SERVICE_UNAVAILABLE);
    let checks: Value = ready.json().await.unwrap();
    assert_eq!(checks["checks"]["database"], "ok");
    assert_eq!(checks["checks"]["commands"], "error");

    sqlx::query("UPDATE commands SET next_attempt_at = ? WHERE id = ?")
        .bind(auth::now_ms() + 60_000)
        .bind(&command_id)
        .execute(&app.pool)
        .await
        .unwrap();
    let empty_device = app.create_device("command-readiness-empty", true).await;
    let (_empty_client, _empty_eventloop, _) = mqtt_connect(
        app.mqtt_addr,
        &empty_device.id,
        &empty_device.id,
        &empty_device.token,
        true,
    )
    .await;
    wait_until(
        || app.presence.is_online(&empty_device.id).unwrap_or(false),
        Duration::from_secs(1),
        "empty device reconnect",
    )
    .await;
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert!(
        !app.command_ready.load(Ordering::Acquire),
        "a no-op wake must not clear command storage write failure"
    );

    sqlx::query("DROP TRIGGER fail_command_dispatch")
        .execute(&app.pool)
        .await
        .unwrap();
    sqlx::query("UPDATE commands SET next_attempt_at = ? WHERE id = ?")
        .bind(auth::now_ms())
        .bind(&command_id)
        .execute(&app.pool)
        .await
        .unwrap();
    wait_until(
        || app.command_ready.load(Ordering::Acquire),
        Duration::from_secs(2),
        "command storage readiness to recover after successful write",
    )
    .await;
    let status: String = sqlx::query_scalar("SELECT status FROM commands WHERE id = ?")
        .bind(command_id)
        .fetch_one(&app.pool)
        .await
        .unwrap();
    assert_eq!(status, "dispatched");
}
