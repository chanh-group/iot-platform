//! Device ACL hook: maps RMQTT sessions into `core::acl` (`02` §2.4).
//!
//! Deny is the default. A denied publish closes the connection (RMQTT sends
//! no PUBACK); a denied subscribe returns SUBACK failure and keeps it.

use async_trait::async_trait;
use rmqtt::codec::v5::SubscribeAckReason;
use rmqtt::hook::{Handler, HookResult, Parameter, ReturnType};
use rmqtt::types::{PublishAclResult, SubscribeAclResult};

use crate::core::acl;

/// Publish/subscribe ACL handlers for authenticated device sessions.
#[derive(Clone, Copy, Default)]
pub struct AclHooks;

impl AclHooks {
    /// Stateless hook; the device identity comes from the session.
    #[must_use]
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl Handler for AclHooks {
    async fn hook(&self, param: &Parameter, _acc: Option<HookResult>) -> ReturnType {
        match param {
            Parameter::MessagePublishCheckAcl(session, publish) => {
                let device_id: &str = &session.id.client_id;
                let topic: &str = &publish.topic;
                let allowed = acl::can_publish(device_id, topic);
                if !allowed {
                    tracing::warn!(%device_id, %topic, "mqtt publish denied by acl");
                }
                let result = if allowed {
                    PublishAclResult::allow()
                } else {
                    PublishAclResult::rejected(true, None)
                };
                (true, Some(HookResult::PublishAclResult(result)))
            }
            Parameter::ClientSubscribeCheckAcl(session, subscribe) => {
                let device_id: &str = &session.id.client_id;
                let filter: &str = &subscribe.topic_filter;
                let allowed = acl::can_subscribe(device_id, filter);
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
            _ => (true, None),
        }
    }
}
