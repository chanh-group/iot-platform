//! Domain types, errors, auth, and ports. Must not depend on axum/sqlx/rmqtt.

pub mod acl;
pub mod auth;
pub mod device_channel;
pub mod domain_error;
pub mod presence;
