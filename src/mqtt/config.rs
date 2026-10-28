//! Maps validated application configuration to RMQTT's listener builder.

use std::num::NonZeroU16;
use std::time::Duration;

use rmqtt::net::{Builder, Listener};
use rmqtt::types::QoS;

use crate::config::schema::MqttConfig;

/// Builds the product's single MQTT listener from validated configuration.
pub fn listener(config: &MqttConfig) -> anyhow::Result<Listener> {
    let max_inflight = u16::try_from(config.max_inflight_per_client)
        .ok()
        .and_then(NonZeroU16::new)
        .ok_or_else(|| anyhow::anyhow!("mqtt.max_inflight_per_client must fit a non-zero u16"))?;
    let max_connections = usize::try_from(config.max_connections)
        .map_err(|_| anyhow::anyhow!("mqtt.max_connections must fit usize"))?;
    let max_packet_bytes = u32::try_from(config.max_packet_bytes)
        .map_err(|_| anyhow::anyhow!("mqtt.max_packet_bytes must fit u32"))?;
    let max_queued_messages = usize::try_from(config.max_queued_messages_per_client)
        .map_err(|_| anyhow::anyhow!("mqtt.max_queued_messages_per_client must fit usize"))?;
    let max_client_id_len = usize::try_from(config.max_client_id_len)
        .map_err(|_| anyhow::anyhow!("mqtt.max_client_id_len must fit usize"))?;
    let max_topic_levels = usize::try_from(config.max_topic_levels)
        .map_err(|_| anyhow::anyhow!("mqtt.max_topic_levels must fit usize"))?;
    let max_subscriptions = usize::try_from(config.max_subscriptions_per_client)
        .map_err(|_| anyhow::anyhow!("mqtt.max_subscriptions_per_client must fit usize"))?;

    Builder::new()
        .name("devices")
        .laddr(config.listen)
        .reuseaddr(Some(true))
        .allow_anonymous(config.allow_anonymous)
        .max_connections(max_connections)
        .max_packet_size(max_packet_bytes)
        .max_inflight(max_inflight)
        .max_mqueue_len(max_queued_messages)
        .max_clientid_len(max_client_id_len)
        .max_qos_allowed(QoS::ExactlyOnce)
        .max_topic_levels(max_topic_levels)
        .max_subscriptions(max_subscriptions)
        .max_session_expiry_interval(Duration::from_millis(config.max_session_expiry_ms))
        .bind()?
        .tcp()
}
