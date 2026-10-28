//! Throwaway Phase 0 spike: RMQTT 0.24.0 hooks, ACL, retained and lifecycle.
//!
//! Every finding here feeds `docs/plans/spike-rmqtt.md`. This crate is not
//! product code and must not be merged into `src/`.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::num::NonZeroU16;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use rmqtt::codec::v5::SubscribeAckReason;
use rmqtt::context::ServerContext;
use rmqtt::hook::{Handler, HookResult, Parameter, ReturnType, Type};
use rmqtt::net::Builder;
use rmqtt::retain::{DefaultRetainStorage, RetainStorage};
use rmqtt::server::MqttServer;
use rmqtt::types::{
    AuthResult, CodecPublish, From, Id, Publish, PublishAclResult, QoS, Retain, SubscribeAclResult,
    TopicFilter, TopicName,
};
use tokio::sync::{Mutex, RwLock};
use tokio::task::JoinHandle;

/// Hook event captured for the report. Payloads are kept only in tests.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Event {
    Connect {
        client_id: String,
        username: Option<String>,
        proto_ver: u8,
        clean_session: bool,
    },
    Authenticate {
        client_id: String,
        username: Option<String>,
        password_ok: bool,
        proto_ver: u8,
        clean_session: bool,
    },
    Connected {
        client_id: String,
        username: Option<String>,
        session_id: String,
        created_at: i64,
        session_present: bool,
    },
    Disconnected {
        client_id: String,
        session_id: String,
        created_at: i64,
        reason: String,
    },
    SubscribeAcl {
        client_id: String,
        filter: String,
        allowed: bool,
    },
    PublishAcl {
        client_id: String,
        topic: String,
        qos: u8,
        retain: bool,
        allowed: bool,
        ingest: bool,
    },
    Publish {
        client_id: Option<String>,
        from_type: String,
        topic: String,
        qos: u8,
        retain: bool,
    },
}

#[derive(Clone)]
struct Hooks {
    inner: Arc<HooksInner>,
}

struct HooksInner {
    devices: RwLock<HashMap<String, String>>,
    events: Mutex<Vec<Event>>,
    acl_disconnect: bool,
}

impl Hooks {
    fn new(devices: HashMap<String, String>, acl_disconnect: bool) -> Self {
        Self {
            inner: Arc::new(HooksInner {
                devices: RwLock::new(devices),
                events: Mutex::new(Vec::new()),
                acl_disconnect,
            }),
        }
    }

    async fn record(&self, event: Event) {
        self.inner.events.lock().await.push(event);
    }

    async fn rotate(&self, device_id: &str, token: &str) {
        self.inner
            .devices
            .write()
            .await
            .insert(device_id.to_owned(), token.to_owned());
    }

    async fn token_ok(&self, device_id: &str, token: Option<&[u8]>) -> bool {
        let devices = self.inner.devices.read().await;
        let Some(expected) = devices.get(device_id) else {
            return false;
        };
        token.is_some_and(|token| token == expected.as_bytes())
    }

    async fn events(&self) -> Vec<Event> {
        self.inner.events.lock().await.clone()
    }
}

fn protocol_level(proto_ver: u8) -> String {
    match proto_ver {
        3 => "3 (MQTT 3.1)".to_owned(),
        4 => "4 (MQTT 3.1.1)".to_owned(),
        5 => "5 (MQTT 5)".to_owned(),
        other => format!("{other}"),
    }
}

#[async_trait]
impl Handler for Hooks {
    async fn hook(&self, param: &Parameter, _acc: Option<HookResult>) -> ReturnType {
        match param {
            Parameter::ClientConnect(connect) => {
                let client_id = connect.client_id().to_string();
                log::info!(
                    "hook connect client_id={client_id} username={:?} proto={} clean_session={}",
                    connect.username(),
                    protocol_level(connect.proto_ver()),
                    connect.clean_start()
                );
                self.record(Event::Connect {
                    client_id,
                    username: connect.username().map(ToString::to_string),
                    proto_ver: connect.proto_ver(),
                    clean_session: connect.clean_start(),
                })
                .await;
                (true, None)
            }
            Parameter::ClientAuthenticate(connect) => {
                let client_id = connect.client_id().to_string();
                let username = connect.username().map(ToString::to_string);
                let password_ok = match &username {
                    Some(username) => {
                        self.token_ok(username, connect.password().map(Bytes::as_ref))
                            .await
                    }
                    None => false,
                };
                log::info!(
                    "hook authenticate client_id={client_id} username={username:?} proto={} password_ok={password_ok}",
                    protocol_level(connect.proto_ver())
                );
                self.record(Event::Authenticate {
                    client_id: client_id.clone(),
                    username: username.clone(),
                    password_ok,
                    proto_ver: connect.proto_ver(),
                    clean_session: connect.clean_start(),
                })
                .await;

                let result = if connect.proto_ver() != 4 {
                    AuthResult::NotAuthorized
                } else if username.as_deref() != Some(client_id.as_str()) || !password_ok {
                    AuthResult::BadUsernameOrPassword
                } else {
                    AuthResult::Allow(false, None)
                };
                (true, Some(HookResult::AuthResult(result)))
            }
            Parameter::ClientConnack(_connect, _ack) => (true, None),
            Parameter::ClientConnected(session) => {
                let session_id = session.id.to_string();
                let session_present = session.session_present().await.unwrap_or(false);
                log::info!(
                    "hook connected client_id={} session_id={session_id} session_present={session_present}",
                    session.id.client_id
                );
                self.record(Event::Connected {
                    client_id: session.id.client_id.to_string(),
                    username: session.id.username.as_ref().map(ToString::to_string),
                    session_id,
                    created_at: session.id.create_time,
                    session_present,
                })
                .await;
                (true, None)
            }
            Parameter::ClientDisconnected(session, reason) => {
                let session_id = session.id.to_string();
                let reason = format!("{reason:?}");
                log::info!(
                    "hook disconnected client_id={} session_id={session_id} reason={reason}",
                    session.id.client_id
                );
                self.record(Event::Disconnected {
                    client_id: session.id.client_id.to_string(),
                    session_id,
                    created_at: session.id.create_time,
                    reason,
                })
                .await;
                (true, None)
            }
            Parameter::ClientSubscribeCheckAcl(session, subscribe) => {
                let client_id = session.id.client_id.to_string();
                let filter = subscribe.topic_filter.to_string();
                let allowed = acl_subscribe(&client_id, &filter);
                log::info!(
                    "hook subscribe_acl client_id={client_id} filter={filter} allowed={allowed}"
                );
                self.record(Event::SubscribeAcl {
                    client_id,
                    filter,
                    allowed,
                })
                .await;
                let result = if allowed {
                    SubscribeAclResult::new_success(subscribe.opts.qos(), None)
                } else {
                    SubscribeAclResult::new_failure(SubscribeAckReason::NotAuthorized)
                };
                (true, Some(HookResult::SubscribeAclResult(result)))
            }
            Parameter::MessagePublishCheckAcl(session, publish) => {
                let client_id = session.id.client_id.to_string();
                let topic = publish.topic.to_string();
                let allowed = acl_publish(&client_id, &topic);
                let ingest = allowed && is_ingest(&client_id, &topic, publish.retain);
                log::info!(
                    "hook publish_acl client_id={client_id} topic={topic} qos={} retain={} allowed={allowed} ingest={ingest}",
                    qos_value(publish.qos),
                    publish.retain
                );
                self.record(Event::PublishAcl {
                    ingest,
                    client_id,
                    topic,
                    qos: qos_value(publish.qos),
                    retain: publish.retain,
                    allowed,
                })
                .await;
                let result = if allowed {
                    PublishAclResult::allow()
                } else {
                    PublishAclResult::rejected(self.inner.acl_disconnect, None)
                };
                (true, Some(HookResult::PublishAclResult(result)))
            }
            Parameter::MessagePublish(session, from, publish) => {
                let topic = publish.topic.to_string();
                log::info!(
                    "hook publish client_id={:?} from={:?} topic={topic} qos={} retain={}",
                    session.map(|session| session.id.client_id.to_string()),
                    from.typ(),
                    qos_value(publish.qos),
                    publish.retain
                );
                self.record(Event::Publish {
                    client_id: session.map(|session| session.id.client_id.to_string()),
                    from_type: from.typ().to_string(),
                    topic,
                    qos: qos_value(publish.qos),
                    retain: publish.retain,
                })
                .await;
                (true, None)
            }
            _ => (true, None),
        }
    }
}

fn qos_value(qos: QoS) -> u8 {
    match qos {
        QoS::AtMostOnce => 0,
        QoS::AtLeastOnce => 1,
        QoS::ExactlyOnce => 2,
    }
}

fn split_topic(topic: &str) -> Vec<&str> {
    topic.split('/').collect()
}

/// Minimum MVP ACL from `docs/plans/02-contracts.md` §2.4. Default deny.
pub fn acl_publish(client_id: &str, topic: &str) -> bool {
    if topic.contains('+')
        || topic.contains('#')
        || topic.starts_with("$SYS")
        || client_id.is_empty()
    {
        return false;
    }
    let levels = split_topic(topic);
    if levels.len() > 16 {
        return false;
    }
    let prefix = format!("devices/{client_id}");
    if !topic.starts_with(&prefix) {
        return false;
    }
    let allowed = [
        format!("{prefix}/telemetry"),
        format!("{prefix}/attributes"),
        format!("{prefix}/commands/ack"),
        format!("{prefix}/status"),
    ];
    allowed.iter().any(|t| t == topic) || topic.starts_with(&format!("{prefix}/data/"))
}

/// Subscribe ACL: exact server-owned topics and `data/#` filters under self.
pub fn acl_subscribe(client_id: &str, filter: &str) -> bool {
    if client_id.is_empty() || filter.starts_with("$SYS") || filter.starts_with("$share") {
        return false;
    }
    if filter.contains('#') && filter != format!("devices/{client_id}/data/#") {
        return false;
    }
    if filter.contains('+') && !filter.starts_with(&format!("devices/{client_id}/data/")) {
        return false;
    }
    let levels = split_topic(filter);
    if levels.len() > 16 {
        return false;
    }
    let prefix = format!("devices/{client_id}");
    let exact = [
        format!("{prefix}/telemetry/ack"),
        format!("{prefix}/commands"),
        format!("{prefix}/status"),
    ];
    exact.iter().any(|t| t == filter) || filter.starts_with(&format!("{prefix}/data/"))
}

/// Telemetry/attributes are ingested only when the publish is non-retained.
/// Retained business messages must be routed but not stored as telemetry.
pub fn is_ingest(client_id: &str, topic: &str, retain: bool) -> bool {
    !retain
        && (topic == format!("devices/{client_id}/telemetry")
            || topic == format!("devices/{client_id}/attributes"))
}

#[derive(Debug, Clone)]
pub struct SpikeConfig {
    pub devices: HashMap<String, String>,
    pub session_expiry: Duration,
    pub max_retained_messages: usize,
    pub max_retained_bytes: usize,
    pub acl_disconnect_on_deny: bool,
}

impl Default for SpikeConfig {
    fn default() -> Self {
        Self {
            devices: HashMap::new(),
            session_expiry: Duration::from_secs(2),
            max_retained_messages: 10_000,
            max_retained_bytes: 16 * 1024 * 1024,
            acl_disconnect_on_deny: true,
        }
    }
}

/// In-memory retained storage with explicit byte/topic caps.
///
/// RMQTT 0.24's `DefaultRetainStorage` reports `enable() == false` and its
/// `set`/`get` are no-ops. The reliable built-in is `set_with_timeout` /
/// `get_message`, so this wrapper delegates to those and owns the budget.
pub struct BudgetedRetainStorage {
    inner: DefaultRetainStorage,
    max_messages: usize,
    max_bytes: usize,
    topics: RwLock<HashMap<String, usize>>,
}

impl BudgetedRetainStorage {
    pub fn new(max_messages: usize, max_bytes: usize) -> Self {
        Self {
            inner: DefaultRetainStorage::new(),
            max_messages,
            max_bytes,
            topics: RwLock::new(HashMap::new()),
        }
    }

    async fn bytes(&self) -> usize {
        self.topics.read().await.values().sum()
    }

    async fn topics_len(&self) -> usize {
        self.topics.read().await.len()
    }
}

#[async_trait]
impl RetainStorage for BudgetedRetainStorage {
    fn enable(&self) -> bool {
        true
    }

    async fn set(
        &self,
        topic: &TopicName,
        retain: Retain,
        expiry_interval: Option<Duration>,
    ) -> rmqtt::Result<()> {
        let key = topic.to_string();
        let mut topics = self.topics.write().await;
        let old = topics.get(&key).copied();
        if retain.publish.payload.is_empty() {
            self.inner
                .set_with_timeout(topic, retain, expiry_interval)
                .await?;
            topics.remove(&key);
            return Ok(());
        }
        let new_bytes = retain.publish.payload.len();
        let current: usize = topics.values().sum();
        let next = current - old.unwrap_or(0) + new_bytes;
        if topics.len() + usize::from(old.is_none()) > self.max_messages || next > self.max_bytes {
            return Err(rmqtt::Error::from(std::io::Error::other(
                "retained budget exceeded",
            )));
        }
        self.inner
            .set_with_timeout(topic, retain, expiry_interval)
            .await?;
        topics.insert(key, new_bytes);
        Ok(())
    }

    async fn get(&self, topic_filter: &TopicFilter) -> rmqtt::Result<Vec<(TopicName, Retain)>> {
        self.inner.get_message(topic_filter).await
    }

    async fn count(&self) -> isize {
        self.topics_len().await as isize
    }

    async fn max(&self) -> isize {
        self.max_messages as isize
    }
}

/// Running spike broker. Dropping aborts `run()` and should release the port.
pub struct SpikeServer {
    addr: SocketAddr,
    scx: ServerContext,
    hooks: Hooks,
    storage: Arc<BudgetedRetainStorage>,
    task: JoinHandle<()>,
}

impl SpikeServer {
    pub async fn start(config: SpikeConfig) -> anyhow::Result<Self> {
        Self::start_on("127.0.0.1:0".parse()?, config).await
    }

    pub async fn start_on(addr: SocketAddr, config: SpikeConfig) -> anyhow::Result<Self> {
        let listener = Builder::new()
            .name("spike/rmqtt")
            .laddr(addr)
            .reuseaddr(Some(true))
            .allow_anonymous(false)
            .max_connections(1000)
            .max_packet_size(262_144)
            .max_inflight(NonZeroU16::new(32).expect("32 is non-zero"))
            .max_mqueue_len(256)
            .max_clientid_len(64)
            .max_qos_allowed(QoS::ExactlyOnce)
            .max_topic_levels(16)
            .max_subscriptions(8)
            .session_expiry_interval(config.session_expiry)
            .max_session_expiry_interval(Duration::from_secs(3600))
            .bind()?
            .tcp()?;
        let addr = listener.local_addr()?;

        let scx = ServerContext::new().busy_check_enable(false).build().await;
        let hooks = Hooks::new(config.devices, config.acl_disconnect_on_deny);
        let register = scx.extends.hook_mgr().register();
        for typ in [
            Type::ClientConnect,
            Type::ClientAuthenticate,
            Type::ClientConnected,
            Type::ClientDisconnected,
            Type::ClientSubscribeCheckAcl,
            Type::MessagePublishCheckAcl,
            Type::MessagePublish,
        ] {
            register
                .add_priority(typ, 10, Box::new(hooks.clone()))
                .await;
        }
        register.start().await;

        let storage = Arc::new(BudgetedRetainStorage::new(
            config.max_retained_messages,
            config.max_retained_bytes,
        ));
        // The trait field takes Box<dyn>; keep an Arc for test assertions.
        let storage_for_server = Box::new(StorageHandle(storage.clone()));
        *scx.extends.retain_mut().await = storage_for_server;

        let server_scx = scx.clone();
        let task = tokio::spawn(async move {
            if let Err(error) = MqttServer::new(server_scx)
                .listener(listener)
                .build()
                .run()
                .await
            {
                eprintln!("spike broker stopped: {error}");
            }
        });

        Ok(Self {
            addr,
            scx,
            hooks,
            storage,
            task,
        })
    }

    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    pub fn port(&self) -> u16 {
        self.addr.port()
    }

    pub async fn events(&self) -> Vec<Event> {
        self.hooks.events().await
    }

    pub async fn rotate(&self, device_id: &str, token: &str) {
        self.hooks.rotate(device_id, token).await;
    }

    pub async fn sessions(&self) -> usize {
        self.scx.extends.shared().await.sessions_count()
    }

    pub async fn retained_topics(&self) -> usize {
        self.storage.topics_len().await
    }

    pub async fn retained_bytes(&self) -> usize {
        self.storage.bytes().await
    }

    pub async fn retained_count(&self) -> isize {
        self.scx.extends.retain().await.count().await
    }

    pub async fn publish_server(
        &self,
        client_id: &str,
        topic: &str,
        payload: &[u8],
    ) -> anyhow::Result<()> {
        let id = Id::new(self.scx.node.id(), 0, None, None, client_id.into(), None);
        let from_id = Id::new(self.scx.node.id(), 0, None, None, "$server".into(), None);
        let publish = Publish::new(
            Box::new(CodecPublish {
                dup: false,
                retain: false,
                qos: QoS::AtLeastOnce,
                topic: topic.into(),
                packet_id: None,
                payload: Bytes::copy_from_slice(payload),
                properties: None,
            }),
            None,
            None,
            None,
        );
        self.scx
            .extends
            .shared()
            .await
            .entry(id)
            .publish(From::from_system(from_id), publish)
            .await
            .map_err(|(_, _, reason)| anyhow::anyhow!("publish failed: {reason:?}"))
    }

    pub async fn kick(&self, client_id: &str) -> anyhow::Result<()> {
        let id = Id::new(self.scx.node.id(), 0, None, None, client_id.into(), None);
        let mut entry = self.scx.extends.shared().await.entry(id);
        entry.kick(true, true, true).await?;
        Ok(())
    }

    pub fn abort(self) {
        self.task.abort();
    }
}

impl Drop for SpikeServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

struct StorageHandle(Arc<BudgetedRetainStorage>);

#[async_trait]
impl RetainStorage for StorageHandle {
    fn enable(&self) -> bool {
        self.0.enable()
    }

    async fn set(
        &self,
        topic: &TopicName,
        retain: Retain,
        expiry_interval: Option<Duration>,
    ) -> rmqtt::Result<()> {
        self.0.set(topic, retain, expiry_interval).await
    }

    async fn get(&self, topic_filter: &TopicFilter) -> rmqtt::Result<Vec<(TopicName, Retain)>> {
        self.0.get(topic_filter).await
    }

    async fn count(&self) -> isize {
        self.0.count().await
    }

    async fn max(&self) -> isize {
        self.0.max().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn acl_publish_matrix() {
        let a = "019c0000-0000-7000-8000-00000000000a";
        let b = "019c0000-0000-7000-8000-00000000000b";
        assert!(acl_publish(a, &format!("devices/{a}/telemetry")));
        assert!(acl_publish(a, &format!("devices/{a}/attributes")));
        assert!(acl_publish(a, &format!("devices/{a}/commands/ack")));
        assert!(acl_publish(a, &format!("devices/{a}/status")));
        assert!(acl_publish(a, &format!("devices/{a}/data/x")));
        assert!(!acl_publish(a, &format!("devices/{b}/telemetry")));
        assert!(!acl_publish(a, &format!("devices/{a}/telemetry/ack")));
        assert!(!acl_publish(a, &format!("devices/{a}/commands")));
        assert!(!acl_publish(a, &format!("devices/{a}/data/+")));
        assert!(!acl_publish(a, "$SYS/broker/uptime"));
    }

    #[test]
    fn ingest_only_live_business_topics() {
        let a = "019c0000-0000-7000-8000-00000000000a";
        assert!(is_ingest(a, &format!("devices/{a}/telemetry"), false));
        assert!(is_ingest(a, &format!("devices/{a}/attributes"), false));
        assert!(!is_ingest(a, &format!("devices/{a}/telemetry"), true));
        assert!(!is_ingest(a, &format!("devices/{a}/status"), false));
        assert!(!is_ingest(a, &format!("devices/{a}/data/x"), false));
    }

    #[test]
    fn acl_subscribe_matrix() {
        let a = "019c0000-0000-7000-8000-00000000000a";
        let b = "019c0000-0000-7000-8000-00000000000b";
        assert!(acl_subscribe(a, &format!("devices/{a}/telemetry/ack")));
        assert!(acl_subscribe(a, &format!("devices/{a}/commands")));
        assert!(acl_subscribe(a, &format!("devices/{a}/status")));
        assert!(acl_subscribe(a, &format!("devices/{a}/data/#")));
        assert!(acl_subscribe(a, &format!("devices/{a}/data/room/+")));
        assert!(!acl_subscribe(a, &format!("devices/{b}/commands")));
        assert!(!acl_subscribe(a, "devices/+/telemetry"));
        assert!(!acl_subscribe(a, &format!("devices/{a}/telemetry")));
        assert!(!acl_subscribe(a, &format!("devices/{a}/#")));
        assert!(!acl_subscribe(a, "$SYS/#"));
        assert!(!acl_subscribe(a, "$share/g/t"));
    }
}
