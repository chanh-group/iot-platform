//! Port for server-originated device delivery and session control.

use async_trait::async_trait;

/// Broker-independent failures while addressing a device session.
#[derive(Debug, thiserror::Error)]
pub enum DeviceChannelError {
    /// The broker could not deliver or control the requested session.
    #[error("device channel unavailable")]
    Unavailable,
}

/// MQTT-facing operations needed by core services.
#[async_trait]
pub trait DeviceChannel: Send + Sync {
    /// Delivers a server-originated message to a device.
    async fn publish(
        &self,
        device_id: &str,
        topic: &str,
        payload: &[u8],
    ) -> Result<(), DeviceChannelError>;

    /// Publishes only to an active session; dispatchers must not use broker offline queues.
    async fn publish_if_online(
        &self,
        device_id: &str,
        topic: &str,
        payload: &[u8],
    ) -> Result<(), DeviceChannelError> {
        if !self.is_online(device_id).await? {
            return Err(DeviceChannelError::Unavailable);
        }
        self.publish(device_id, topic, payload).await
    }

    /// Closes a device's active broker session.
    async fn kick(&self, device_id: &str) -> Result<(), DeviceChannelError>;

    /// Returns whether the device currently has an active session.
    async fn is_online(&self, device_id: &str) -> Result<bool, DeviceChannelError>;
}
