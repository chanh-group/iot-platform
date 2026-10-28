//! CLI surface per `02` §6.

use std::path::{Path, PathBuf};
use std::process::ExitCode;

use clap::{Parser, Subcommand};

use crate::config::{self, LoadedConfig};
use crate::runtime::lock::DataDirLock;
use crate::storage::sqlite::{migrate, pool};

const EXIT_CONFIG: u8 = 3;
const EXIT_MIGRATION: u8 = 4;
const EXIT_LOCK: u8 = 5;

#[derive(Debug, Parser)]
#[command(name = "iot-server", version, about = "Single-binary MQTT IoT server")]
pub struct Cli {
    /// Path to `conf.toml`
    #[arg(
        long,
        global = true,
        default_value = "./conf.toml",
        value_name = "PATH"
    )]
    pub config: PathBuf,

    #[command(subcommand)]
    pub command: Command,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    /// Validate config and environment without touching the data dir
    Check,
    /// Apply database migrations
    Migrate,
    /// Administrative commands
    Admin {
        #[command(subcommand)]
        command: AdminCommand,
    },
    /// Run the server
    Run,
}

#[derive(Debug, Subcommand)]
pub enum AdminCommand {
    /// Create the single admin user
    Create {
        #[arg(long, default_value = "admin")]
        username: String,
        /// Read the password from this file instead of prompting
        #[arg(long, value_name = "PATH")]
        password_file: Option<PathBuf>,
    },
}

pub fn run() -> ExitCode {
    let cli = Cli::parse();
    match cli.command {
        Command::Check => check(&cli.config),
        Command::Migrate => migrate(&cli.config),
        other => {
            eprintln!("iot-server: {other:?} not implemented yet");
            ExitCode::FAILURE
        }
    }
}

fn check(config_path: &Path) -> ExitCode {
    match config::load(config_path) {
        Ok(_) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("iot-server: {error}");
            ExitCode::from(EXIT_CONFIG)
        }
    }
}

fn migrate(config_path: &Path) -> ExitCode {
    let loaded = match config::load(config_path) {
        Ok(loaded) => loaded,
        Err(error) => {
            eprintln!("iot-server: {error}");
            return ExitCode::from(EXIT_CONFIG);
        }
    };

    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(error) => {
            eprintln!("iot-server: cannot start async runtime: {error}");
            return ExitCode::FAILURE;
        }
    };
    runtime.block_on(migrate_async(loaded))
}

async fn migrate_async(loaded: LoadedConfig) -> ExitCode {
    let _lock = match DataDirLock::acquire(&loaded.data_dir) {
        Ok(lock) => lock,
        Err(error) => {
            eprintln!("iot-server: {error}");
            return ExitCode::from(EXIT_LOCK);
        }
    };

    let pool = match pool::connect(&loaded).await {
        Ok(pool) => pool,
        Err(error) => {
            eprintln!("iot-server: cannot open database: {error}");
            return ExitCode::from(EXIT_MIGRATION);
        }
    };

    let result = migrate::run(&pool).await;
    pool.close().await;
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("iot-server: migration failed: {error}");
            ExitCode::from(EXIT_MIGRATION)
        }
    }
}
