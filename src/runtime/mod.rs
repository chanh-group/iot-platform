//! Startup, supervisor, shutdown, and the data dir lock.

pub mod data_dir_lock;
pub mod retention;
pub mod shutdown;
pub mod supervisor;

/// Exit code when the runtime fails after startup.
pub const EXIT_RUNTIME: u8 = 1;
/// Exit code for invalid configuration.
pub const EXIT_CONFIG: u8 = 3;
/// Exit code when database open or migration fails.
pub const EXIT_MIGRATION: u8 = 4;
/// Exit code when another process holds the data dir lock.
pub const EXIT_LOCK: u8 = 5;
