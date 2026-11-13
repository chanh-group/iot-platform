mod common;
#[path = "common/mqtt_auth_postgres.rs"]
mod postgres;

use common::{
    TestApp, mqtt_connect, mqtt_subscribe, mqtt_wait_closed, mqtt_wait_publish, wait_presence,
};
use iot_server::observability::metrics::Metrics;
use rumqttc::{Event, Packet, QoS};
use std::time::Duration;

static RETAINED_CAP_TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

async fn publish_qos1(
    client: &rumqttc::AsyncClient,
    eventloop: &mut rumqttc::EventLoop,
    topic: &str,
    retain: bool,
    payload: &[u8],
) {
    client
        .publish(topic, QoS::AtLeastOnce, retain, payload)
        .await
        .expect("queue MQTT publish");
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if matches!(
                eventloop.poll().await.expect("poll MQTT event"),
                Event::Incoming(Packet::PubAck(_))
            ) {
                break;
            }
        }
    })
    .await
    .expect("receive MQTT PUBACK before timeout");
}

#[tokio::test]
async fn attributes_are_latest_per_key_and_invalid_or_retained_updates_are_dropped() {
    let app = TestApp::start().await;
    let device = app.create_device("attributes-flow", true).await;
    app.create_admin("attributes-admin", "attributes-test-password")
        .await;
    let http = app.client();
    let login = http
        .post(format!("{}/api/v1/auth/login", app.base))
        .header("X-IoT-CSRF", "1")
        .json(&serde_json::json!({"username":"attributes-admin","password":"attributes-test-password"}))
        .send()
        .await
        .expect("login test admin");
    assert!(login.status().is_success());
    run_attributes_latest_case(
        &app.base,
        None,
        app.mqtt_addr,
        &device.id,
        &device.token,
        http.clone(),
    )
    .await;

    let url = format!("{}/api/v1/devices/{}/attributes", app.base, device.id);
    sqlx::query("ALTER TABLE device_attributes RENAME TO unavailable_attributes")
        .execute(&app.pool)
        .await
        .unwrap();
    let unavailable = http.get(&url).send().await.unwrap();
    assert_eq!(
        unavailable.status(),
        reqwest::StatusCode::SERVICE_UNAVAILABLE
    );
    assert_eq!(
        unavailable.json::<serde_json::Value>().await.unwrap()["code"],
        "storage_unavailable"
    );
    sqlx::query("ALTER TABLE devices RENAME TO unavailable_devices")
        .execute(&app.pool)
        .await
        .unwrap();
    let unavailable = http.get(&url).send().await.unwrap();
    assert_eq!(
        unavailable.status(),
        reqwest::StatusCode::SERVICE_UNAVAILABLE
    );
    assert_eq!(
        unavailable.json::<serde_json::Value>().await.unwrap()["code"],
        "storage_unavailable"
    );
    sqlx::query("ALTER TABLE sessions RENAME TO unavailable_sessions")
        .execute(&app.pool)
        .await
        .unwrap();
    let unavailable = http.get(&url).send().await.unwrap();
    assert_eq!(
        unavailable.status(),
        reqwest::StatusCode::SERVICE_UNAVAILABLE
    );
    let body: serde_json::Value = unavailable.json().await.unwrap();
    assert_eq!(body["code"], "storage_unavailable");
    assert!(!body.to_string().contains("unavailable_sessions"));
}

#[tokio::test]
#[ignore = "requires tests/attributes_postgres.sh"]
async fn attributes_are_latest_per_key_and_invalid_or_retained_updates_are_dropped_postgres() {
    let app = postgres::PostgresApp::start().await;
    let device = app.create_device("attributes-flow").await;
    run_attributes_latest_case(
        app.base_url(),
        Some(app.admin_cookie()),
        app.mqtt_addr(),
        &device.id,
        &device.token,
        reqwest::Client::new(),
    )
    .await;
    assert_postgres_attribute_storage_failures(app.base_url(), app.admin_cookie(), &device.id)
        .await;
}

async fn assert_postgres_attribute_storage_failures(base: &str, cookie: &str, device_id: &str) {
    let url = std::env::var("IOT_TEST_POSTGRES_URL").expect("guarded PostgreSQL URL");
    let pool = sqlx::PgPool::connect(&url)
        .await
        .expect("connect guarded PostgreSQL fixture");
    let endpoint = format!("{base}/api/v1/devices/{device_id}/attributes");
    let http = reqwest::Client::new();
    for (rename_away, rename_back, unavailable) in [
        (
            "ALTER TABLE device_attributes RENAME TO unavailable_attributes",
            "ALTER TABLE unavailable_attributes RENAME TO device_attributes",
            "unavailable_attributes",
        ),
        (
            "ALTER TABLE devices RENAME TO unavailable_devices",
            "ALTER TABLE unavailable_devices RENAME TO devices",
            "unavailable_devices",
        ),
        (
            "ALTER TABLE sessions RENAME TO unavailable_sessions",
            "ALTER TABLE unavailable_sessions RENAME TO sessions",
            "unavailable_sessions",
        ),
    ] {
        sqlx::query(rename_away).execute(&pool).await.unwrap();
        let response = http
            .get(&endpoint)
            .header("Cookie", cookie)
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), reqwest::StatusCode::SERVICE_UNAVAILABLE);
        let body: serde_json::Value = response.json().await.unwrap();
        assert_eq!(body["code"], "storage_unavailable");
        if unavailable == "unavailable_sessions" {
            assert!(!body.to_string().contains(unavailable));
        }
        sqlx::query(rename_back).execute(&pool).await.unwrap();
    }
    pool.close().await;
}

async fn run_attributes_latest_case(
    base: &str,
    cookie: Option<&str>,
    mqtt_addr: std::net::SocketAddr,
    device_id: &str,
    token: &str,
    http: reqwest::Client,
) {
    let topic = format!("devices/{device_id}/attributes");
    let (client, mut eventloop, _) =
        common::mqtt_connect(mqtt_addr, device_id, device_id, token, true).await;

    publish_qos1(
        &client,
        &mut eventloop,
        &topic,
        false,
        br#"{"values":{"firmware":"1.0","interval_s":30}}"#,
    )
    .await;
    let url = format!("{base}/api/v1/devices/{device_id}/attributes");
    let get_attributes = || {
        let mut request = http.get(&url);
        if let Some(cookie) = cookie {
            request = request.header("Cookie", cookie);
        }
        request
    };
    assert_eq!(
        reqwest::Client::new()
            .get(&url)
            .send()
            .await
            .expect("request attributes without admin auth")
            .status(),
        reqwest::StatusCode::UNAUTHORIZED
    );
    let attributes = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let response = get_attributes().send().await.expect("query attributes");
            if response.status().is_success() {
                let body: serde_json::Value = response.json().await.expect("attribute response");
                if body["items"]
                    .as_array()
                    .is_some_and(|items| items.len() == 2)
                {
                    break body;
                }
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("initial attributes should be stored before timeout");
    assert_eq!(attributes["items"][0]["value"], "1.0");
    assert_eq!(attributes["items"][1]["value"], 30);

    // The server runs in a subprocess for PostgreSQL, so verify rejection through persisted state.
    publish_qos1(
        &client,
        &mut eventloop,
        &topic,
        false,
        br#"{"values":{"firmware":"\u0000"}}"#,
    )
    .await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    let unchanged: serde_json::Value = get_attributes()
        .send()
        .await
        .expect("query attributes after NUL update")
        .json()
        .await
        .expect("attribute response after NUL update");
    assert_eq!(unchanged["items"][0]["value"], "1.0");
    assert_eq!(unchanged["items"].as_array().unwrap().len(), 2);

    let device_ts = iot_server::core::auth::now_ms();
    let update = serde_json::json!({"ts": device_ts, "values": {"firmware": "2.0"}}).to_string();
    publish_qos1(&client, &mut eventloop, &topic, false, update.as_bytes()).await;
    let latest = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let body: serde_json::Value = get_attributes()
                .send()
                .await
                .expect("query latest attributes")
                .json()
                .await
                .expect("latest attribute response");
            if body["items"][0]["value"] == "2.0" {
                break body;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("firmware update should be stored before timeout");
    assert_eq!(latest["items"].as_array().unwrap().len(), 2);
    assert_eq!(latest["items"][0]["key"], "firmware");
    assert_eq!(latest["items"][0]["device_ts"], device_ts);
    assert!(latest["items"][0]["updated_at"].is_i64());
    assert_eq!(latest["items"][1]["value"], 30);
    assert_eq!(latest["items"][1]["device_ts"], serde_json::Value::Null);

    let missing = get_attributes_for(
        &http,
        cookie,
        &format!("{base}/api/v1/devices/missing/attributes"),
    )
    .send()
    .await
    .expect("request missing device attributes");
    assert_eq!(missing.status(), reqwest::StatusCode::NOT_FOUND);
    assert_eq!(
        missing.json::<serde_json::Value>().await.unwrap()["code"],
        "device_not_found"
    );

    publish_qos1(
        &client,
        &mut eventloop,
        &topic,
        false,
        br#"{"values":{"invalid":null}}"#,
    )
    .await;
    let after_invalid: serde_json::Value = get_attributes()
        .send()
        .await
        .expect("query attributes after null update")
        .json()
        .await
        .expect("attribute response after null update");
    assert_eq!(after_invalid["items"].as_array().unwrap().len(), 2);
    assert_eq!(after_invalid["items"][0]["value"], "2.0");

    client
        .publish(&topic, QoS::AtLeastOnce, true, br#"{"values":{"x":1}}"#)
        .await
        .expect("queue retained attributes publish");
    common::mqtt_wait_closed(&mut eventloop, Duration::from_secs(2)).await;
    let after_retained: serde_json::Value = get_attributes()
        .send()
        .await
        .expect("query attributes after retained update")
        .json()
        .await
        .expect("attribute response after retained update");
    assert_eq!(after_retained["items"].as_array().unwrap().len(), 2);
    assert_eq!(after_retained["items"][0]["value"], "2.0");

    for start in [0, 100] {
        let values: serde_json::Map<String, serde_json::Value> = (start..start + 100)
            .map(|i| (format!("key{i:03}"), serde_json::json!(i)))
            .collect();
        let payload = serde_json::json!({"values": values}).to_string();
        publish_qos1(&client, &mut eventloop, &topic, false, payload.as_bytes()).await;
    }
    let all_attributes = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let body: serde_json::Value = get_attributes()
                .send()
                .await
                .expect("query paginated attributes")
                .json()
                .await
                .expect("paginated attribute response");
            if body["items"]
                .as_array()
                .is_some_and(|items| items.len() == 50)
            {
                break body;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("first attribute page should be available");
    assert_eq!(all_attributes["items"].as_array().unwrap().len(), 50);
    let cursor = all_attributes["next_cursor"].as_str().unwrap();
    let second = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let body: serde_json::Value = get_attributes()
                .query(&[("cursor", cursor), ("limit", "200")])
                .send()
                .await
                .unwrap()
                .json()
                .await
                .unwrap();
            if body["items"]
                .as_array()
                .is_some_and(|items| items.len() == 152)
            {
                break body;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("all attribute pages should include both updates");
    assert_eq!(second["items"].as_array().unwrap().len(), 152);
    assert!(second["next_cursor"].is_null());
    assert_eq!(all_attributes["items"][49]["key"], "key047");
    assert_eq!(second["items"][0]["key"], "key048");
    let invalid = get_attributes()
        .query(&[("cursor", "bad")])
        .send()
        .await
        .unwrap();
    assert_eq!(invalid.status(), reqwest::StatusCode::BAD_REQUEST);
    assert_eq!(
        invalid.json::<serde_json::Value>().await.unwrap()["code"],
        "invalid_cursor"
    );
    let other_device =
        format!("{base}/api/v1/devices/00000000-0000-7000-8000-000000000000/attributes");
    let wrong_device = get_attributes_for(&http, cookie, &other_device)
        .query(&[("cursor", cursor)])
        .send()
        .await
        .unwrap();
    assert_eq!(wrong_device.status(), reqwest::StatusCode::BAD_REQUEST);
    assert_eq!(
        get_attributes()
            .query(&[("limit", "201")])
            .send()
            .await
            .unwrap()
            .status(),
        reqwest::StatusCode::BAD_REQUEST
    );
}

fn get_attributes_for(
    http: &reqwest::Client,
    cookie: Option<&str>,
    url: &str,
) -> reqwest::RequestBuilder {
    let request = http.get(url);
    match cookie {
        Some(cookie) => request.header("Cookie", cookie),
        None => request,
    }
}

#[tokio::test]
async fn retained_replace_clear_and_caps_keep_accounting_consistent() {
    let _guard = RETAINED_CAP_TEST_LOCK.lock().await;
    let app = TestApp::builder().retained_limits(1, 5).start().await;
    let device = app.create_device("retained-cap", true).await;
    let status = format!("devices/{}/status", device.id);
    let data = format!("devices/{}/data/x", device.id);
    let (client, mut eventloop, _) =
        common::mqtt_connect(app.mqtt_addr, &device.id, &device.id, &device.token, true).await;

    publish_qos1(&client, &mut eventloop, &status, true, b"1234").await;
    assert_eq!(app.retained_count().await, 1, "status should be retained");
    assert!(
        app.presence
            .is_online(&device.id)
            .expect("presence state should remain healthy"),
        "retaining status must not change MQTT presence"
    );

    // Replacement subtracts the previous payload before applying the byte cap.
    publish_qos1(&client, &mut eventloop, &status, true, b"12345").await;
    assert_eq!(
        app.retained_count().await,
        1,
        "replacing the same topic must not consume another topic slot"
    );

    publish_qos1(&client, &mut eventloop, &status, true, b"").await;
    assert_eq!(
        app.retained_count().await,
        0,
        "clearing retained status removes it"
    );
    publish_qos1(&client, &mut eventloop, &data, true, b"12345").await;
    assert_eq!(
        app.retained_count().await,
        1,
        "clearing must release the topic slot"
    );

    client
        .publish(&status, QoS::AtLeastOnce, true, b"x")
        .await
        .expect("queue over-cap retained publish");
    common::mqtt_wait_closed(&mut eventloop, Duration::from_secs(2)).await;
    assert_eq!(
        app.retained_count().await,
        1,
        "over-cap publish must not replace or add retained data"
    );
}

#[tokio::test]
async fn retained_byte_cap_rejects_oversized_replacement_and_counts_it() {
    let _guard = RETAINED_CAP_TEST_LOCK.lock().await;
    let _ = tracing_subscriber::fmt()
        .with_env_filter("rmqtt::v3=info")
        .with_test_writer()
        .try_init();
    let app = TestApp::builder().retained_limits(2, 5).start().await;
    let rejected_before = Metrics::global().retained_cap_rejected_total.get();
    let device = app.create_device("retained-byte-cap", true).await;
    let status = format!("devices/{}/status", device.id);
    eprintln!("retained byte cap: initial MQTT connect");
    let (client, mut eventloop, _) =
        mqtt_connect(app.mqtt_addr, &device.id, &device.id, &device.token, true).await;
    wait_presence(&app, &device.id, true).await;

    publish_qos1(&client, &mut eventloop, &status, true, b"12345").await;
    client
        .publish(&status, QoS::AtLeastOnce, true, b"123456")
        .await
        .expect("queue retained payload over byte cap");
    mqtt_wait_closed(&mut eventloop, Duration::from_secs(2)).await;
    wait_presence(&app, &device.id, false).await;

    assert_eq!(app.retained_count().await, 1);
    assert!(
        Metrics::global().retained_cap_rejected_total.get() == rejected_before + 1,
        "byte-cap rejection must increment the retained rejection metric"
    );
    drop(client);
    drop(eventloop);

    eprintln!("retained byte cap: MQTT reconnect after rejection");
    let (client, mut eventloop, _) =
        mqtt_connect(app.mqtt_addr, &device.id, &device.id, &device.token, true).await;
    wait_presence(&app, &device.id, true).await;
    assert_eq!(
        mqtt_subscribe(&client, &mut eventloop, &status, QoS::AtLeastOnce).await,
        rumqttc::SubscribeReasonCode::Success(QoS::AtLeastOnce)
    );
    let retained = mqtt_wait_publish(&mut eventloop, Duration::from_secs(2)).await;
    assert_eq!(retained.payload.as_ref(), b"12345");
    assert!(retained.retain);
}

#[tokio::test]
async fn retained_state_is_empty_after_broker_restart() {
    let mut app = TestApp::start().await;
    let device = app.create_device("retained-restart", true).await;
    let status = format!("devices/{}/status", device.id);
    let (client, mut eventloop, _) =
        common::mqtt_connect(app.mqtt_addr, &device.id, &device.id, &device.token, true).await;

    publish_qos1(&client, &mut eventloop, &status, true, b"online").await;
    assert_eq!(
        app.retained_count().await,
        1,
        "publish should populate retained storage"
    );
    drop(eventloop);
    drop(client);

    app.restart_mqtt().await;
    assert_eq!(
        app.retained_count().await,
        0,
        "a fresh broker context must start without retained messages"
    );
}
