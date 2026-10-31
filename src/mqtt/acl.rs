//! Device ACL hook: maps RMQTT sessions into `core::acl` (`02` §2.4).
//!
//! Deny is the default. A denied publish closes the connection (RMQTT sends
//! no PUBACK); a denied subscribe returns SUBACK failure and keeps it.

use std::sync::Arc;

use async_trait::async_trait;
use rmqtt::codec::v5::SubscribeAckReason;
use rmqtt::hook::{Handler, HookResult, Parameter, ReturnType};
use rmqtt::types::{FromType, PublishAclResult, SubscribeAclResult};
use serde::Deserialize;

use super::presence::ConnectionLimiter;
use super::server_publish::AckHandle;
use crate::core::acl;
use crate::core::attributes;
use crate::core::auth;
use crate::core::command::CommandAckHandle;
use crate::core::events::EventBus;
use crate::core::telemetry::{
    self, IngestHandle, IngestSubmitError, TelemetryError, TelemetryPayload,
};
use crate::storage::sqlite::attributes::AttributeHandle;

/// Publish/subscribe ACL handlers for authenticated device sessions.
#[derive(Clone)]
pub struct AclHooks {
    ingest: Option<(IngestHandle, AttributeHandle, TelemetryPayload, AckHandle)>,
    command_ack: Option<CommandAckHandle>,
    events: EventBus,
    max_topic_levels: usize,
    connections: Arc<ConnectionLimiter>,
}

impl AclHooks {
    /// Stateless hook; the device identity comes from the session.
    #[must_use]
    pub(crate) fn new(connections: Arc<ConnectionLimiter>, max_topic_levels: usize) -> Self {
        Self {
            ingest: None,
            command_ack: None,
            events: EventBus::new(),
            max_topic_levels,
            connections,
        }
    }

    /// Enables non-blocking command ACK submissions while telemetry is disabled.
    #[must_use]
    pub(crate) fn with_command_ack(
        connections: Arc<ConnectionLimiter>,
        command_ack: CommandAckHandle,
        events: EventBus,
        max_topic_levels: usize,
    ) -> Self {
        Self {
            ingest: None,
            command_ack: Some(command_ack),
            events,
            max_topic_levels,
            connections,
        }
    }

    /// Enables telemetry validation and non-blocking queue submission.
    #[must_use]
    pub(crate) fn with_ingest(
        connections: Arc<ConnectionLimiter>,
        ingest: IngestHandle,
        attributes: AttributeHandle,
        limits: TelemetryPayload,
        ack: AckHandle,
        command_ack: CommandAckHandle,
        max_topic_levels: usize,
    ) -> Self {
        let events = ingest.event_bus();
        Self {
            ingest: Some((ingest, attributes, limits, ack)),
            command_ack: Some(command_ack),
            events,
            max_topic_levels,
            connections,
        }
    }

    fn device_enabled(&self, device_id: &str) -> bool {
        self.connections
            .device_can_exchange(device_id)
            .unwrap_or(false)
    }

    fn report_status(&self, device_id: &str, topic: &str, payload: &[u8]) {
        if topic != format!("devices/{device_id}/status") {
            return;
        }
        if let Ok(status) = serde_json::from_slice::<ReportedStatusMessage>(payload) {
            self.events.publish_reported_status(
                device_id,
                status.status.as_str(),
                status.ts.unwrap_or_else(auth::now_ms),
            );
        }
    }
}

#[async_trait]
impl Handler for AclHooks {
    async fn hook(&self, param: &Parameter, _acc: Option<HookResult>) -> ReturnType {
        match param {
            Parameter::MessagePublishCheckAcl(session, publish) => {
                let device_id: &str = &session.id.client_id;
                let topic: &str = &publish.topic;
                let allowed = self.device_enabled(device_id)
                    && acl::can_publish_with_limit(device_id, topic, self.max_topic_levels);
                let telemetry_topic = format!("devices/{device_id}/telemetry");
                let attributes_topic = format!("devices/{device_id}/attributes");
                let command_ack_topic = format!("devices/{device_id}/commands/ack");
                let retained_telemetry =
                    allowed && topic == telemetry_topic && publish.inner.retain;
                let retained_attributes =
                    allowed && topic == attributes_topic && publish.inner.retain;
                let retained_command_ack =
                    allowed && topic == command_ack_topic && publish.inner.retain;
                if !allowed {
                    tracing::warn!(%device_id, %topic, "mqtt publish denied by acl");
                }
                if retained_attributes {
                    crate::observability::metrics::Metrics::global()
                        .attribute_rejected_total
                        .inc();
                    tracing::warn!(%device_id, %topic, "retained attributes publish rejected");
                }
                if allowed
                    && topic == command_ack_topic
                    && !publish.inner.retain
                    && let Some(command_ack) = &self.command_ack
                {
                    command_ack.try_send(device_id, publish.inner.payload.as_ref());
                }
                if allowed
                    && topic == telemetry_topic
                    && let Some((ingest, _, limits, ack)) = &self.ingest
                {
                    let received_at = auth::now_ms();
                    let payload = publish.inner.payload.as_ref();
                    if publish.inner.retain {
                        if let Some(event_id) = telemetry::event_id_from_raw(payload) {
                            ack.send(
                                device_id.to_owned(),
                                Some(event_id),
                                "rejected",
                                received_at,
                                Some("retained_not_allowed"),
                            );
                        }
                    } else {
                        match telemetry::validate(payload, received_at, *limits) {
                            Ok(event) => {
                                let event_id = event.event_id.clone();
                                match ingest.try_submit(device_id.to_owned(), event) {
                                    Ok(result) => ack.send_after_ingest(
                                        device_id.to_owned(),
                                        event_id,
                                        received_at,
                                        result,
                                    ),
                                    Err(IngestSubmitError::Full) => {
                                        tracing::warn!(%device_id, "telemetry ingest queue full");
                                        ack.send(
                                            device_id.to_owned(),
                                            Some(event_id),
                                            "retryable",
                                            received_at,
                                            Some("queue_full"),
                                        );
                                    }
                                    Err(IngestSubmitError::Closed) => ack.send(
                                        device_id.to_owned(),
                                        Some(event_id),
                                        "retryable",
                                        received_at,
                                        Some("storage_unavailable"),
                                    ),
                                }
                            }
                            Err(error) => {
                                crate::observability::metrics::Metrics::global()
                                    .reject_ingest(match error {
                                        telemetry::TelemetryError::PayloadTooLarge => {
                                            crate::observability::metrics::IngestRejectReason::PayloadTooLarge
                                        }
                                        telemetry::TelemetryError::InvalidPayload => {
                                            crate::observability::metrics::IngestRejectReason::InvalidPayload
                                        }
                                        telemetry::TelemetryError::MissingEventId => {
                                            crate::observability::metrics::IngestRejectReason::MissingEventId
                                        }
                                        telemetry::TelemetryError::InvalidEventId => {
                                            crate::observability::metrics::IngestRejectReason::InvalidEventId
                                        }
                                        telemetry::TelemetryError::InvalidTimestamp => {
                                            crate::observability::metrics::IngestRejectReason::InvalidTimestamp
                                        }
                                    });
                                ack.send(
                                    device_id.to_owned(),
                                    telemetry::event_id_from_raw(payload),
                                    "rejected",
                                    received_at,
                                    Some(error.code()),
                                );
                                tracing::debug!(%device_id, ?error, "telemetry payload rejected");
                            }
                        }
                    }
                } else if allowed
                    && topic == attributes_topic
                    && !publish.inner.retain
                    && let Some((_, attributes_handle, limits, _)) = &self.ingest
                {
                    let updated_at = auth::now_ms();
                    match attributes::validate(publish.inner.payload.as_ref(), updated_at, *limits)
                    {
                        Ok(event) => {
                            if let Err(error) =
                                attributes_handle.try_submit(device_id.to_owned(), event)
                            {
                                crate::observability::metrics::Metrics::global()
                                    .attribute_rejected_total
                                    .inc();
                                tracing::warn!(%device_id, ?error, "attribute update queue rejected payload");
                            }
                        }
                        Err(error) => {
                            crate::observability::metrics::Metrics::global()
                                .attribute_rejected_total
                                .inc();
                            tracing::debug!(%device_id, ?error, "attribute payload rejected");
                        }
                    }
                }
                if allowed && topic == format!("devices/{device_id}/status") {
                    self.report_status(device_id, topic, publish.inner.payload.as_ref());
                }
                let result = if !allowed {
                    PublishAclResult::rejected(true, None)
                } else if retained_telemetry {
                    // Keep the session open so the application-level rejection ACK can be delivered.
                    PublishAclResult::rejected(false, None)
                } else if retained_attributes || retained_command_ack {
                    PublishAclResult::rejected(true, None)
                } else {
                    PublishAclResult::allow()
                };
                (true, Some(HookResult::PublishAclResult(result)))
            }
            Parameter::ClientSubscribeCheckAcl(session, subscribe) => {
                let device_id: &str = &session.id.client_id;
                let filter: &str = &subscribe.topic_filter;
                let allowed = self.device_enabled(device_id)
                    && acl::can_subscribe_with_limit(device_id, filter, self.max_topic_levels);
                if !allowed {
                    tracing::warn!(%device_id, %filter, "mqtt subscribe denied by acl");
                }
                let result = if allowed {
                    SubscribeAclResult::new_success(subscribe.opts.qos(), None)
                } else {
                    SubscribeAclResult::new_failure(SubscribeAckReason::NotAuthorized)
                };
                (true, Some(HookResult::SubscribeAclResult(result)))
            }
            Parameter::MessagePublish(_, from, publish)
                if matches!(from.typ(), FromType::LastWill) =>
            {
                let device_id = from.id.client_id.to_string();
                if auth::is_device_id(&device_id) {
                    self.report_status(&device_id, &publish.topic, publish.inner.payload.as_ref());
                }
                (true, None)
            }
            _ => (true, None),
        }
    }
}

#[derive(Deserialize)]
struct ReportedStatusMessage {
    status: ReportedStatusValue,
    ts: Option<i64>,
}

#[derive(Deserialize)]
#[serde(rename_all = "lowercase")]
enum ReportedStatusValue {
    Online,
    Offline,
}

impl ReportedStatusValue {
    const fn as_str(&self) -> &'static str {
        match self {
            Self::Online => "online",
            Self::Offline => "offline",
        }
    }
}

impl TelemetryError {
    fn code(self) -> &'static str {
        match self {
            Self::PayloadTooLarge => "payload_too_large",
            Self::InvalidPayload => "invalid_payload",
            Self::MissingEventId => "missing_event_id",
            Self::InvalidEventId => "invalid_event_id",
            Self::InvalidTimestamp => "invalid_timestamp",
        }
    }
}
