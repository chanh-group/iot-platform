//! Throwaway Phase 0 spike: SQLx + SQLite (WAL/FULL) + Axum + RMQTT in one
//! process. Not product code.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context, bail, ensure};
use axum::Router;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use futures_util::{SinkExt, StreamExt};
use rmqtt_spike::{SpikeConfig, SpikeServer};
use rumqttc::{
    AsyncClient, ConnectReturnCode, Event as MqttEvent, EventLoop, MqttOptions, Packet, Publish,
    QoS,
};
use rust_embed::RustEmbed;
use sqlx::SqlitePool;
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::task::JoinHandle;
use tokio::time::timeout;

const DEVICE: &str = "019c0e1e-0000-7000-8000-00000000000a";
const TOKEN: &str = "iot_spike_token_for_device_a_000000000000";

#[derive(RustEmbed)]
#[folder = "assets"]
struct Assets;

pub struct AllInOne {
    pub http_addr: SocketAddr,
    pub mqtt: SpikeServer,
    pub pool: SqlitePool,
    http_task: JoinHandle<()>,
}

impl AllInOne {
    pub async fn start(db_path: &Path) -> anyhow::Result<Self> {
        let options = SqliteConnectOptions::new()
            .filename(db_path)
            .create_if_missing(true)
            .journal_mode(SqliteJournalMode::Wal)
            .synchronous(SqliteSynchronous::Full)
            .foreign_keys(true);
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(options)
            .await?;
        sqlx::migrate!("./migrations").run(&pool).await?;

        let mqtt = SpikeServer::start(SpikeConfig {
            devices: HashMap::from([(DEVICE.to_owned(), TOKEN.to_owned())]),
            ..SpikeConfig::default()
        })
        .await?;

        let app = Router::new().route("/", get(index)).route("/ws", get(ws));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let http_addr = listener.local_addr()?;
        let http_task = tokio::spawn(async move {
            if let Err(error) = axum::serve(listener, app).await {
                eprintln!("axum stopped: {error}");
            }
        });

        Ok(Self {
            http_addr,
            mqtt,
            pool,
            http_task,
        })
    }
}

impl Drop for AllInOne {
    fn drop(&mut self) {
        self.http_task.abort();
    }
}

async fn index() -> Response {
    match Assets::get("index.html") {
        Some(file) => (
            [(header::CONTENT_TYPE, "text/html; charset=utf-8")],
            file.data.to_vec(),
        )
            .into_response(),
        None => (StatusCode::NOT_FOUND, "missing asset").into_response(),
    }
}

async fn ws(upgrade: WebSocketUpgrade) -> impl IntoResponse {
    upgrade.on_upgrade(echo)
}

async fn echo(mut socket: WebSocket) {
    while let Some(Ok(message)) = socket.recv().await {
        match message {
            Message::Text(_) | Message::Binary(_) => {
                if socket.send(message).await.is_err() {
                    break;
                }
            }
            Message::Close(_) => break,
            _ => {}
        }
    }
}

#[derive(Debug)]
pub struct Report {
    pub http_addr: SocketAddr,
    pub mqtt_addr: SocketAddr,
    pub journal_mode: String,
    pub synchronous: i64,
    pub foreign_keys: i64,
    pub migrations: i64,
    pub devices: i64,
    pub ws_echo: String,
    pub mqtt_payload: String,
}

pub async fn self_check() -> anyhow::Result<Report> {
    let path = temp_db_path();
    let app = AllInOne::start(&path).await?;
    let http_addr = app.http_addr;
    let mqtt_addr = app.mqtt.addr();

    let html = http_get(http_addr, "/").await?;
    ensure!(
        html.contains("allinone-spike"),
        "embedded asset missing: {html}"
    );

    let journal_mode: String = sqlx::query_scalar("PRAGMA journal_mode")
        .fetch_one(&app.pool)
        .await?;
    ensure!(
        journal_mode.eq_ignore_ascii_case("wal"),
        "journal_mode={journal_mode}"
    );
    let synchronous: i64 = sqlx::query_scalar("PRAGMA synchronous")
        .fetch_one(&app.pool)
        .await?;
    ensure!(
        synchronous == 2,
        "synchronous={synchronous}, expected FULL(2)"
    );
    let foreign_keys: i64 = sqlx::query_scalar("PRAGMA foreign_keys")
        .fetch_one(&app.pool)
        .await?;
    ensure!(foreign_keys == 1, "foreign_keys={foreign_keys}");
    let migrations: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM _sqlx_migrations")
        .fetch_one(&app.pool)
        .await?;
    ensure!(migrations == 1, "migrations={migrations}");

    sqlx::query("INSERT INTO devices (id, name, created_at) VALUES (?, ?, ?)")
        .bind(DEVICE)
        .bind("spike")
        .bind(1_i64)
        .execute(&app.pool)
        .await?;
    let devices: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM devices")
        .fetch_one(&app.pool)
        .await?;
    ensure!(devices == 1, "devices={devices}");

    let ws_echo = ws_echo(http_addr).await?;
    ensure!(ws_echo == "ping", "ws echo={ws_echo:?}");

    let mqtt_payload = mqtt_roundtrip(&app.mqtt, mqtt_addr).await?;
    ensure!(
        mqtt_payload == "from-server",
        "mqtt payload={mqtt_payload:?}"
    );

    drop(app);
    for path in db_paths(&path) {
        let _ = std::fs::remove_file(path);
    }

    Ok(Report {
        http_addr,
        mqtt_addr,
        journal_mode,
        synchronous,
        foreign_keys,
        migrations,
        devices,
        ws_echo,
        mqtt_payload,
    })
}

fn temp_db_path() -> PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or_default();
    std::env::temp_dir().join(format!("allinone-spike-{}-{nanos}.db", std::process::id()))
}

fn db_paths(path: &Path) -> [PathBuf; 3] {
    let base = path.display().to_string();
    [
        PathBuf::from(&base),
        PathBuf::from(format!("{base}-wal")),
        PathBuf::from(format!("{base}-shm")),
    ]
}

async fn http_get(addr: SocketAddr, path: &str) -> anyhow::Result<String> {
    let mut stream = TcpStream::connect(addr).await?;
    let request = format!("GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n");
    stream.write_all(request.as_bytes()).await?;
    let mut response = Vec::new();
    stream.read_to_end(&mut response).await?;
    let text = String::from_utf8_lossy(&response).into_owned();
    ensure!(
        text.starts_with("HTTP/1.1 200"),
        "unexpected HTTP response: {text}"
    );
    Ok(text)
}

async fn ws_echo(addr: SocketAddr) -> anyhow::Result<String> {
    let (mut socket, _) = tokio_tungstenite::connect_async(format!("ws://{addr}/ws"))
        .await
        .context("websocket connect failed")?;
    socket
        .send(tokio_tungstenite::tungstenite::Message::Text("ping".into()))
        .await?;
    let reply = timeout(Duration::from_secs(5), socket.next())
        .await
        .context("websocket echo timeout")?
        .context("websocket closed")??;
    match reply {
        tokio_tungstenite::tungstenite::Message::Text(text) => Ok(text.as_str().to_owned()),
        other => bail!("unexpected websocket reply: {other:?}"),
    }
}

async fn mqtt_roundtrip(server: &SpikeServer, addr: SocketAddr) -> anyhow::Result<String> {
    let topic = format!("devices/{DEVICE}/commands");
    let mut options = MqttOptions::new(DEVICE, addr.ip().to_string(), addr.port());
    options
        .set_keep_alive(Duration::from_secs(30))
        .set_clean_session(true)
        .set_credentials(DEVICE, TOKEN);
    let (client, mut eventloop) = AsyncClient::new(options, 16);
    wait_connack(&mut eventloop).await?;
    client.subscribe(&topic, QoS::AtLeastOnce).await?;
    wait_suback(&mut eventloop).await?;
    // The broker must be reachable from the same process that serves HTTP.
    server
        .publish_server(DEVICE, &topic, b"from-server")
        .await?;
    let publish = wait_publish(&mut eventloop).await?;
    Ok(String::from_utf8_lossy(publish.payload.as_ref()).into_owned())
}

async fn wait_connack(eventloop: &mut EventLoop) -> anyhow::Result<()> {
    timeout(Duration::from_secs(5), async {
        loop {
            match eventloop.poll().await? {
                MqttEvent::Incoming(Packet::ConnAck(ack))
                    if ack.code == ConnectReturnCode::Success =>
                {
                    return Ok::<(), anyhow::Error>(());
                }
                _ => {}
            }
        }
    })
    .await
    .context("timeout waiting for CONNACK")??;
    Ok(())
}

async fn wait_suback(eventloop: &mut EventLoop) -> anyhow::Result<()> {
    timeout(Duration::from_secs(5), async {
        loop {
            if let MqttEvent::Incoming(Packet::SubAck(_)) = eventloop.poll().await? {
                return Ok::<(), anyhow::Error>(());
            }
        }
    })
    .await
    .context("timeout waiting for SUBACK")??;
    Ok(())
}

async fn wait_publish(eventloop: &mut EventLoop) -> anyhow::Result<Publish> {
    let publish = timeout(Duration::from_secs(5), async {
        loop {
            if let MqttEvent::Incoming(Packet::Publish(publish)) = eventloop.poll().await? {
                return Ok::<Publish, anyhow::Error>(publish);
            }
        }
    })
    .await
    .context("timeout waiting for PUBLISH")??;
    Ok(publish)
}
