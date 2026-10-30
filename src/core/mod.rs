//! Domain types, errors, auth, and ports. Must not depend on axum/sqlx/rmqtt.

pub mod acl;
pub mod attributes;
pub mod auth;
pub mod command;
pub mod device;
pub mod device_channel;
pub mod domain_error;
pub mod events;
pub mod presence;
pub mod telemetry;
