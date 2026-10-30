//! Device authentication hook (`02` §2.1, §2.2).
//!
//! `ClientConnect` cannot set a CONNACK code (RMQTT only reads user properties
//! from it), so protocol gating runs in `ClientAuthenticate`: RMQTT maps
//! `NotAuthorized` to CONNACK `0x05` for level 3 and `0x87` for level 5, and
//! `BadUsernameOrPassword` to `0x04`.

use std::sync::Arc;

use async_trait::async_trait;
use rmqtt::hook::{Handler, HookResult, Parameter, ReturnType};
use rmqtt::types::{AuthResult, ConnectInfo};
use sqlx::SqlitePool;

use crate::core::auth;
use crate::storage::sqlite::devices;

use super::presence::ConnectionLimiter;

/// Authenticates devices: MQTT 3.1.1 only, `client_id = username = device_id`,
/// active credential matching the presented token.
#[derive(Clone)]
pub struct AuthHooks {
    pool: SqlitePool,
    connections: Arc<ConnectionLimiter>,
}

impl AuthHooks {
    /// Hooks read credentials through this pool.
    #[must_use]
    pub(crate) fn new(pool: SqlitePool, connections: Arc<ConnectionLimiter>) -> Self {
        Self { pool, connections }
    }

    async fn authenticate(&self, connect: &ConnectInfo) -> AuthResult {
        if connect.proto_ver() != 4 {
            return AuthResult::NotAuthorized;
        }

        let client_id: &str = connect.client_id();
        let Some(username) = connect.username() else {
            return AuthResult::BadUsernameOrPassword;
        };
        let username: &str = username;
        let Some(password) = connect.password() else {
            return AuthResult::BadUsernameOrPassword;
        };
        let Ok(token) = std::str::from_utf8(password) else {
            return AuthResult::BadUsernameOrPassword;
        };
        // The authenticated principal is the only identity source; a client id
        // or payload that disagrees cannot borrow another device.
        if username != client_id || !auth::is_device_id(username) {
            return AuthResult::BadUsernameOrPassword;
        }

        match devices::active_credential(&self.pool, username).await {
            Ok(Some(credential))
                if credential.device_enabled
                    && auth::token_matches(&credential.token_hash, token) => {}
            // Unknown, disabled, revoked and wrong-token are indistinguishable.
            Ok(_) => return AuthResult::BadUsernameOrPassword,
            Err(error) => {
                tracing::error!(%error, "device credential lookup failed");
                return AuthResult::NotAuthorized;
            }
        };
        match self.connections.try_reserve(username) {
            Ok(true) => {}
            Ok(false) | Err(_) => return AuthResult::NotAuthorized,
        }

        // Reserve before the final read so a concurrent disable either sees this
        // handshake and waits for it to become active, or makes this read fail.
        match devices::active_credential(&self.pool, username).await {
            Ok(Some(current))
                if current.device_enabled && auth::token_matches(&current.token_hash, token) =>
            {
                match self.connections.set_device_enabled(username, true) {
                    Ok(()) => AuthResult::Allow(false, None),
                    Err(_) => {
                        let _ = self.connections.cancel_reservation(username);
                        AuthResult::NotAuthorized
                    }
                }
            }
            Ok(_) => {
                let _ = self.connections.cancel_reservation(username);
                AuthResult::BadUsernameOrPassword
            }
            Err(error) => {
                let _ = self.connections.cancel_reservation(username);
                tracing::error!(%error, "device credential recheck failed");
                AuthResult::NotAuthorized
            }
        }
    }
}

#[async_trait]
impl Handler for AuthHooks {
    async fn hook(&self, param: &Parameter, _acc: Option<HookResult>) -> ReturnType {
        match param {
            Parameter::ClientAuthenticate(connect) => (
                true,
                Some(HookResult::AuthResult(self.authenticate(connect).await)),
            ),
            _ => (true, None),
        }
    }
}
