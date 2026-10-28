//! CLI surface per `02` §6.

use std::future::Future;
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use clap::{Parser, Subcommand};

use crate::config::{self, LoadedConfig};
use crate::core::auth;
use crate::runtime::lock::DataDirLock;
use crate::runtime::{EXIT_CONFIG, EXIT_LOCK, EXIT_MIGRATION, supervisor};
use crate::storage::sqlite::users::{self, CreateAdminError};
use crate::storage::sqlite::{migrate, pool};

const EXIT_ADMIN_EXISTS: u8 = 6;

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
        #[arg(long, default_value = "admin", value_parser = parse_username)]
        username: String,
        /// Read the password from this file instead of prompting
        #[arg(long, value_name = "PATH")]
        password_file: Option<PathBuf>,
    },
}

fn parse_username(value: &str) -> Result<String, String> {
    if auth::is_valid_username(value) {
        Ok(value.to_owned())
    } else {
        Err("username must match ^[a-z0-9_.-]{3,32}$".to_owned())
    }
}

pub fn run() -> ExitCode {
    let cli = Cli::parse();
    match cli.command {
        Command::Check => check(&cli.config),
        Command::Migrate => migrate(&cli.config),
        Command::Admin {
            command:
                AdminCommand::Create {
                    username,
                    password_file,
                },
        } => admin_create(&cli.config, &username, password_file.as_deref()),
        Command::Run => run_server(&cli.config),
    }
}

fn run_server(config_path: &Path) -> ExitCode {
    let loaded = match config::load(config_path) {
        Ok(loaded) => loaded,
        Err(error) => {
            eprintln!("iot-server: {error}");
            return ExitCode::from(EXIT_CONFIG);
        }
    };
    block_on(supervisor::run(config_path, loaded))
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
    block_on(migrate_async(loaded))
}

fn block_on<F: Future<Output = ExitCode>>(future: F) -> ExitCode {
    match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime.block_on(future),
        Err(error) => {
            eprintln!("iot-server: cannot start async runtime: {error}");
            ExitCode::FAILURE
        }
    }
}

fn admin_create(config_path: &Path, username: &str, password_file: Option<&Path>) -> ExitCode {
    let loaded = match config::load(config_path) {
        Ok(loaded) => loaded,
        Err(error) => {
            eprintln!("iot-server: {error}");
            return ExitCode::from(EXIT_CONFIG);
        }
    };

    let password = match read_password(password_file) {
        Ok(password) => password,
        Err(error) => {
            eprintln!("iot-server: {error}");
            return ExitCode::FAILURE;
        }
    };
    if let Err(error) = auth::validate_password(&password, loaded.config.auth.password_min_length) {
        eprintln!("iot-server: {error}");
        return ExitCode::FAILURE;
    }
    let password_hash = match auth::hash_password(&password) {
        Ok(hash) => hash,
        Err(error) => {
            eprintln!("iot-server: cannot hash password: {error}");
            return ExitCode::FAILURE;
        }
    };

    block_on(admin_create_async(
        loaded,
        username.to_owned(),
        password_hash,
    ))
}

fn read_password(password_file: Option<&Path>) -> Result<String, String> {
    match password_file {
        Some(path) => {
            let text = std::fs::read_to_string(path)
                .map_err(|error| format!("cannot read {}: {error}", path.display()))?;
            Ok(text.trim_end_matches(['\r', '\n']).to_owned())
        }
        None => {
            let password = rpassword::prompt_password("Password: ")
                .map_err(|error| format!("cannot read password: {error}"))?;
            let confirm = rpassword::prompt_password("Confirm password: ")
                .map_err(|error| format!("cannot read password: {error}"))?;
            if password != confirm {
                return Err("passwords do not match".to_owned());
            }
            Ok(password)
        }
    }
}

async fn admin_create_async(
    loaded: LoadedConfig,
    username: String,
    password_hash: String,
) -> ExitCode {
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
            return ExitCode::FAILURE;
        }
    };

    let result = async {
        let mut tx = pool.begin().await?;
        let user = users::create_admin(&mut tx, &username, &password_hash, auth::now_ms()).await?;
        tx.commit().await?;
        Ok::<_, CreateAdminError>(user)
    }
    .await;
    pool.close().await;

    match result {
        Ok(user) => {
            let username = user.username;
            println!("created admin '{username}'");
            ExitCode::SUCCESS
        }
        Err(CreateAdminError::AlreadyExists) => {
            eprintln!("iot-server: an admin user already exists");
            ExitCode::from(EXIT_ADMIN_EXISTS)
        }
        Err(CreateAdminError::Database(error)) => {
            eprintln!("iot-server: cannot create admin: {error}");
            ExitCode::FAILURE
        }
    }
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
