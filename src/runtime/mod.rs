//! Startup, supervisor, shutdown, and the data dir lock.

pub mod lock;
pub mod shutdown;
pub mod supervisor;

pub const EXIT_RUNTIME: u8 = 1;
pub const EXIT_CONFIG: u8 = 3;
pub const EXIT_MIGRATION: u8 = 4;
pub const EXIT_LOCK: u8 = 5;
