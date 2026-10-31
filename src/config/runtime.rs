//! `runtime.toml` overlay and atomically published runtime snapshots.

use std::fs::{self, File, OpenOptions};
use std::future::Future;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use arc_swap::ArcSwap;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::Mutex;

use crate::config::schema::{Config, LogLevel};
use crate::config::{ConfigError, validate};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RuntimeFile {
    revision: u64,
    #[serde(default, skip_serializing_if = "RuntimeLogging::is_empty")]
    logging: RuntimeLogging,
    #[serde(default, skip_serializing_if = "RuntimeTelemetry::is_empty")]
    telemetry: RuntimeTelemetry,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
struct RuntimeLogging {
    #[serde(skip_serializing_if = "Option::is_none")]
    level: Option<LogLevel>,
}

impl RuntimeLogging {
    fn is_empty(&self) -> bool {
        self.level.is_none()
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
struct RuntimeTelemetry {
    #[serde(skip_serializing_if = "Option::is_none")]
    retention_days: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    batch_size: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    flush_interval_ms: Option<u64>,
}

impl RuntimeTelemetry {
    fn is_empty(&self) -> bool {
        self.retention_days.is_none()
            && self.batch_size.is_none()
            && self.flush_interval_ms.is_none()
    }
}

/// One immutable, coherent view of effective configuration and its revision.
pub struct RuntimeSnapshot {
    /// Effective configuration after applying the runtime overlay.
    pub config: Config,
    /// Revision stored alongside this exact configuration snapshot.
    pub revision: u64,
    overlay: RuntimeFile,
}

struct RuntimeConfigInner {
    snapshot: ArcSwap<RuntimeSnapshot>,
    update_lock: Arc<Mutex<()>>,
    path: PathBuf,
}

/// Shared lock-free reader and serialized writer for `runtime.toml`.
#[derive(Clone)]
pub struct RuntimeConfigStore(Arc<RuntimeConfigInner>);

impl RuntimeConfigStore {
    pub(crate) fn new(
        path: PathBuf,
        config: Config,
        revision: u64,
        mut overlay: RuntimeFile,
    ) -> Self {
        overlay.revision = revision;
        Self(Arc::new(RuntimeConfigInner {
            snapshot: ArcSwap::from_pointee(RuntimeSnapshot {
                config,
                revision,
                overlay,
            }),
            update_lock: Arc::new(Mutex::new(())),
            path,
        }))
    }

    /// Loads the current config and revision from one atomic snapshot.
    #[must_use]
    pub fn load(&self) -> Arc<RuntimeSnapshot> {
        self.0.snapshot.load_full()
    }

    /// Validates, persists, applies and publishes one hot-config patch.
    pub async fn update(
        &self,
        expected_revision: u64,
        settings: &Value,
    ) -> Result<RuntimeUpdate, RuntimeConfigError> {
        self.update_and_then(
            expected_revision,
            settings,
            |_| Ok(Box::new(|| {}) as Box<dyn FnOnce() + Send>),
            |_, _| std::future::ready(()),
        )
        .await
    }

    /// Keeps updates serialized through post-publication audit and notification work.
    pub async fn update_and_then<F, Fut>(
        &self,
        expected_revision: u64,
        settings: &Value,
        prepare_log_filter: impl FnOnce(LogLevel) -> Result<Box<dyn FnOnce() + Send>, String>,
        after_publish: F,
    ) -> Result<RuntimeUpdate, RuntimeConfigError>
    where
        F: FnOnce(Arc<RuntimeSnapshot>, Vec<String>) -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let guard = Arc::clone(&self.0.update_lock).lock_owned().await;
        let update = self
            .update_locked(expected_revision, settings, None, prepare_log_filter)
            .await?;
        if update.changed_fields.is_empty() {
            drop(guard);
            return Ok(update);
        }
        let snapshot = Arc::clone(&update.snapshot);
        let changed_fields = update.changed_fields.clone();
        let completion = tokio::spawn(async move {
            after_publish(snapshot, changed_fields).await;
            drop(guard);
        });
        // Dropping this handle on HTTP cancellation detaches completion while its task retains the lock.
        if let Err(error) = completion.await {
            tracing::error!(%error, "runtime config post-commit completion task failed");
        }
        Ok(update)
    }

    #[cfg(test)]
    async fn update_with_failure(
        &self,
        expected_revision: u64,
        settings: &Value,
        fail_at: Option<WriteStage>,
        prepare_log_filter: impl FnOnce(LogLevel) -> Result<Box<dyn FnOnce() + Send>, String>,
    ) -> Result<RuntimeUpdate, RuntimeConfigError> {
        let _guard = self.0.update_lock.lock().await;
        self.update_locked(expected_revision, settings, fail_at, prepare_log_filter)
            .await
    }

    async fn update_locked(
        &self,
        expected_revision: u64,
        settings: &Value,
        fail_at: Option<WriteStage>,
        prepare_log_filter: impl FnOnce(LogLevel) -> Result<Box<dyn FnOnce() + Send>, String>,
    ) -> Result<RuntimeUpdate, RuntimeConfigError> {
        let current = self.load();
        if expected_revision != current.revision {
            return Err(RuntimeConfigError::RevisionConflict(current.revision));
        }

        let config_value = serde_json::to_value(&current.config)
            .map_err(|error| RuntimeConfigError::Serialization(error.to_string()))?;
        let mut fields = Vec::new();
        let mut validation_errors = Vec::new();
        collect_settings(
            &config_value,
            settings,
            "",
            &mut fields,
            &mut validation_errors,
        );
        if !validation_errors.is_empty() {
            return Err(RuntimeConfigError::Validation(validation_errors));
        }

        let mut config = current.config.clone();
        let mut overlay = current.overlay.clone();
        let mut restart_required = Vec::new();
        let mut changed_fields = Vec::new();
        for (path, value) in fields {
            match path.as_str() {
                "logging.level" => match serde_json::from_value::<LogLevel>(value) {
                    Ok(level) => {
                        if config.logging.level != level {
                            changed_fields.push(path);
                        }
                        config.logging.level = level;
                        overlay.logging.level = Some(level);
                    }
                    Err(_) => validation_errors.push(
                        "logging.level: expected one of trace, debug, info, warn, error".to_owned(),
                    ),
                },
                "telemetry.retention_days" => set_u64(
                    &mut config.telemetry.retention_days,
                    &mut overlay.telemetry.retention_days,
                    value,
                    &path,
                    &mut changed_fields,
                    &mut validation_errors,
                ),
                "telemetry.batch_size" => set_u64(
                    &mut config.telemetry.batch_size,
                    &mut overlay.telemetry.batch_size,
                    value,
                    &path,
                    &mut changed_fields,
                    &mut validation_errors,
                ),
                "telemetry.flush_interval_ms" => set_u64(
                    &mut config.telemetry.flush_interval_ms,
                    &mut overlay.telemetry.flush_interval_ms,
                    value,
                    &path,
                    &mut changed_fields,
                    &mut validation_errors,
                ),
                _ => restart_required.push(path),
            }
        }
        if !validation_errors.is_empty() {
            return Err(RuntimeConfigError::Validation(validation_errors));
        }
        if !restart_required.is_empty() {
            return Err(RuntimeConfigError::RestartRequired(restart_required));
        }

        let errors = validate(&config);
        if !errors.is_empty() {
            return Err(RuntimeConfigError::Validation(errors));
        }
        if changed_fields.is_empty() {
            return Ok(RuntimeUpdate {
                snapshot: current,
                changed_fields,
                durability_error: None,
            });
        }

        let apply_log_filter = if current.config.logging.level != config.logging.level {
            Some(prepare_log_filter(config.logging.level).map_err(RuntimeConfigError::Apply)?)
        } else {
            None
        };

        let revision = current
            .revision
            .checked_add(1)
            .ok_or(RuntimeConfigError::RevisionExhausted)?;
        overlay.revision = revision;
        let contents = toml::to_string(&overlay).map_err(|error| {
            RuntimeConfigError::Write(RuntimeWriteError {
                stage: WriteStage::Serialize,
                source: io::Error::other(error),
                renamed: false,
            })
        })?;
        let durability_error = match atomic_write(&self.0.path, contents.as_bytes(), fail_at) {
            Ok(()) => None,
            Err(error) if error.renamed => Some(error),
            Err(error) => return Err(RuntimeConfigError::Write(error)),
        };

        if let Some(apply_log_filter) = apply_log_filter {
            apply_log_filter();
        }
        let snapshot = Arc::new(RuntimeSnapshot {
            config,
            revision,
            overlay,
        });
        self.0.snapshot.store(Arc::clone(&snapshot));
        Ok(RuntimeUpdate {
            snapshot,
            changed_fields,
            durability_error,
        })
    }
}

/// Result of a successful in-process update; directory-sync failure is explicit.
pub struct RuntimeUpdate {
    /// The atomically published snapshot.
    pub snapshot: Arc<RuntimeSnapshot>,
    /// Hot config paths whose effective values changed.
    pub changed_fields: Vec<String>,
    /// Present only when rename succeeded but directory durability is uncertain.
    pub durability_error: Option<RuntimeWriteError>,
}

/// Validation, restart-only, revision, and persistence failures for a PATCH.
#[derive(Debug)]
pub enum RuntimeConfigError {
    Validation(Vec<String>),
    RestartRequired(Vec<String>),
    RevisionConflict(u64),
    RevisionExhausted,
    Serialization(String),
    Apply(String),
    Write(RuntimeWriteError),
}

/// The failed persistence stage and whether rename already committed the new file.
#[derive(Debug)]
pub struct RuntimeWriteError {
    pub stage: WriteStage,
    pub source: io::Error,
    pub renamed: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteStage {
    ParentCreate,
    TempCreate,
    Write,
    FileSync,
    Rename,
    CreatedDirectorySync,
    ParentDirectorySync,
    DirectoryOpen,
    DirectorySync,
    Serialize,
}

impl WriteStage {
    #[must_use]
    pub fn name(self) -> &'static str {
        match self {
            Self::ParentCreate => "parent_create",
            Self::TempCreate => "temp_create",
            Self::Write => "write",
            Self::FileSync => "file_sync",
            Self::Rename => "rename",
            Self::CreatedDirectorySync => "created_directory_sync",
            Self::ParentDirectorySync => "parent_directory_sync",
            Self::DirectoryOpen => "directory_open",
            Self::DirectorySync => "directory_sync",
            Self::Serialize => "serialize",
        }
    }
}

fn set_u64(
    target: &mut u64,
    overlay: &mut Option<u64>,
    value: Value,
    path: &str,
    changed_fields: &mut Vec<String>,
    errors: &mut Vec<String>,
) {
    if let Some(value) = value.as_u64() {
        if *target != value {
            changed_fields.push(path.to_owned());
        }
        *target = value;
        *overlay = Some(value);
    } else {
        errors.push(format!("{path}: expected a non-negative integer"));
    }
}

fn collect_settings(
    schema: &Value,
    settings: &Value,
    prefix: &str,
    fields: &mut Vec<(String, Value)>,
    errors: &mut Vec<String>,
) {
    let Some(object) = settings.as_object() else {
        errors.push("settings: expected a non-empty object".to_owned());
        return;
    };
    if object.is_empty() {
        errors.push(format!(
            "{}: object must not be empty",
            if prefix.is_empty() {
                "settings"
            } else {
                prefix
            }
        ));
        return;
    }
    for (key, value) in object {
        let path = if prefix.is_empty() {
            key.clone()
        } else {
            format!("{prefix}.{key}")
        };
        let optional_schema_field = matches!(
            (prefix, key.as_str()),
            ("database", "url" | "url_env") | ("http", "metrics_bearer_token_env")
        );
        let schema_value = schema.as_object().and_then(|schema| schema.get(key));
        match (schema_value, value.as_object()) {
            (Some(section @ Value::Object(_)), Some(_)) => {
                collect_settings(section, value, &path, fields, errors);
            }
            (Some(Value::Object(_)), None) => {
                errors.push(format!("{path}: expected an object"));
            }
            (Some(_), Some(_)) => errors.push(format!("{path}: expected a scalar value")),
            (Some(_), None) => fields.push((path, value.clone())),
            (None, None) if optional_schema_field => fields.push((path, value.clone())),
            (None, Some(_)) | (None, None) => errors.push(format!("{path}: unknown field")),
        }
    }
}

static TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(0);

fn atomic_write(
    path: &Path,
    contents: &[u8],
    fail_at: Option<WriteStage>,
) -> Result<(), RuntimeWriteError> {
    let sequence = TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    atomic_write_with_sequence(path, contents, fail_at, sequence)
}

fn atomic_write_with_sequence(
    path: &Path,
    contents: &[u8],
    fail_at: Option<WriteStage>,
    sequence: u64,
) -> Result<(), RuntimeWriteError> {
    let parent = path
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    create_parent_directories(parent, fail_at)?;

    let filename = path.file_name().unwrap_or_default().to_string_lossy();
    let temp_path = parent.join(format!(
        ".{filename}.{}.{}.tmp",
        std::process::id(),
        sequence
    ));
    let mut temp_created = false;
    let mut renamed = false;
    let result = (|| {
        inject_failure(fail_at, WriteStage::TempCreate, false)?;
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temp_path)
            .map_err(|source| write_error(WriteStage::TempCreate, source, false))?;
        temp_created = true;
        inject_failure(fail_at, WriteStage::Write, false)?;
        file.write_all(contents)
            .map_err(|source| write_error(WriteStage::Write, source, false))?;
        inject_failure(fail_at, WriteStage::FileSync, false)?;
        file.sync_all()
            .map_err(|source| write_error(WriteStage::FileSync, source, false))?;
        drop(file);

        inject_failure(fail_at, WriteStage::Rename, false)?;
        fs::rename(&temp_path, path)
            .map_err(|source| write_error(WriteStage::Rename, source, false))?;
        renamed = true;

        inject_failure(fail_at, WriteStage::DirectoryOpen, true)?;
        let directory = File::open(parent)
            .map_err(|source| write_error(WriteStage::DirectoryOpen, source, true))?;
        inject_failure(fail_at, WriteStage::DirectorySync, true)?;
        directory
            .sync_all()
            .map_err(|source| write_error(WriteStage::DirectorySync, source, true))
    })();

    if result.is_err()
        && temp_created
        && !renamed
        && let Err(error) = fs::remove_file(&temp_path)
        && error.kind() != io::ErrorKind::NotFound
    {
        tracing::warn!(%error, "could not remove runtime config temp file");
    }
    result
}

fn create_parent_directories(
    parent: &Path,
    fail_at: Option<WriteStage>,
) -> Result<(), RuntimeWriteError> {
    inject_failure(fail_at, WriteStage::ParentCreate, false)?;
    let mut missing = Vec::new();
    let mut current = parent.to_path_buf();
    loop {
        match fs::metadata(&current) {
            Ok(metadata) if metadata.is_dir() => break,
            Ok(_) => {
                return Err(write_error(
                    WriteStage::ParentCreate,
                    io::Error::new(
                        io::ErrorKind::NotADirectory,
                        "runtime config parent is not a directory",
                    ),
                    false,
                ));
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                missing.push(current.clone());
            }
            Err(source) => return Err(write_error(WriteStage::ParentCreate, source, false)),
        }
        let next = current
            .parent()
            .filter(|path| !path.as_os_str().is_empty())
            .unwrap_or(Path::new("."));
        if next == current {
            return Err(write_error(
                WriteStage::ParentCreate,
                io::Error::new(
                    io::ErrorKind::NotFound,
                    "no existing runtime config ancestor",
                ),
                false,
            ));
        }
        current = next.to_path_buf();
    }

    for directory in missing.into_iter().rev() {
        match fs::create_dir(&directory) {
            Ok(()) => {}
            Err(error)
                if error.kind() == io::ErrorKind::AlreadyExists
                    && fs::metadata(&directory).is_ok_and(|metadata| metadata.is_dir()) => {}
            Err(source) => return Err(write_error(WriteStage::ParentCreate, source, false)),
        }
        sync_directory(&directory, WriteStage::CreatedDirectorySync, fail_at)?;
    }

    sync_parent_ancestry(parent, fail_at)
}

fn sync_parent_ancestry(
    parent: &Path,
    fail_at: Option<WriteStage>,
) -> Result<(), RuntimeWriteError> {
    let mut ancestors = Vec::new();
    let mut current = parent.to_path_buf();
    loop {
        ancestors.push(current.clone());
        let Some(next) = current.parent() else {
            break;
        };
        let next = if next.as_os_str().is_empty() {
            Path::new(".")
        } else {
            next
        };
        if next == current {
            break;
        }
        current = next.to_path_buf();
    }

    for directory in ancestors.into_iter().rev() {
        sync_directory(&directory, WriteStage::ParentDirectorySync, fail_at)?;
    }
    Ok(())
}

fn sync_directory(
    path: &Path,
    stage: WriteStage,
    fail_at: Option<WriteStage>,
) -> Result<(), RuntimeWriteError> {
    inject_failure(fail_at, stage, false)?;
    File::open(path)
        .and_then(|directory| directory.sync_all())
        .map_err(|source| write_error(stage, source, false))
}

fn inject_failure(
    fail_at: Option<WriteStage>,
    stage: WriteStage,
    renamed: bool,
) -> Result<(), RuntimeWriteError> {
    if fail_at == Some(stage) {
        return Err(write_error(
            stage,
            io::Error::other("injected runtime config write failure"),
            renamed,
        ));
    }
    Ok(())
}

fn write_error(stage: WriteStage, source: io::Error, renamed: bool) -> RuntimeWriteError {
    RuntimeWriteError {
        stage,
        source,
        renamed,
    }
}

/// Missing file is not an error (revision 0). Parse errors fail startup.
pub(crate) fn apply_file(
    path: &Path,
    config: &mut Config,
) -> Result<(u64, RuntimeFile), ConfigError> {
    let text = match fs::read_to_string(path) {
        Ok(text) => text,
        Err(source) if source.kind() == io::ErrorKind::NotFound => {
            return Ok((0, RuntimeFile::default()));
        }
        Err(source) => {
            return Err(ConfigError::Read {
                path: path.to_path_buf(),
                source,
            });
        }
    };
    let overlay: RuntimeFile = toml::from_str(&text).map_err(|source| ConfigError::Parse {
        path: path.to_path_buf(),
        source,
    })?;
    if let Some(level) = overlay.logging.level {
        config.logging.level = level;
    }
    if let Some(value) = overlay.telemetry.retention_days {
        config.telemetry.retention_days = value;
    }
    if let Some(value) = overlay.telemetry.batch_size {
        config.telemetry.batch_size = value;
    }
    if let Some(value) = overlay.telemetry.flush_interval_ms {
        config.telemetry.flush_interval_ms = value;
    }
    Ok((overlay.revision, overlay))
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicBool;

    use serde_json::json;
    use tempfile::TempDir;

    use super::*;

    fn store(path: &Path) -> RuntimeConfigStore {
        let config: Config = toml::from_str("config_version = 1").unwrap();
        RuntimeConfigStore::new(path.to_path_buf(), config, 0, RuntimeFile::default())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn concurrent_snapshot_reads_keep_revision_and_values_coherent() {
        let dir = TempDir::new().unwrap();
        let store = store(&dir.path().join("runtime.toml"));
        let reading = Arc::new(AtomicBool::new(true));
        let reads = Arc::new(AtomicU64::new(0));
        let reader_store = store.clone();
        let reader_flag = Arc::clone(&reading);
        let read_count = Arc::clone(&reads);
        let reader = tokio::spawn(async move {
            while reader_flag.load(Ordering::Acquire) {
                let snapshot = reader_store.load();
                let expected = if snapshot.revision % 2 == 1 {
                    LogLevel::Debug
                } else {
                    LogLevel::Info
                };
                assert_eq!(snapshot.config.logging.level, expected);
                read_count.fetch_add(1, Ordering::Relaxed);
                tokio::task::yield_now().await;
            }
        });

        for expected_revision in 0..40 {
            let level = if expected_revision % 2 == 0 {
                "debug"
            } else {
                "info"
            };
            store
                .update(
                    expected_revision,
                    &serde_json::json!({"logging":{"level":level}}),
                )
                .await
                .unwrap();
        }
        reading.store(false, Ordering::Release);
        reader.await.unwrap();
        assert!(reads.load(Ordering::Relaxed) > 0);
    }

    #[tokio::test]
    async fn filter_preparation_failure_keeps_file_and_snapshot_unchanged() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("runtime.toml");
        let old_file = "revision = 0\n";
        fs::write(&path, old_file).unwrap();
        let store = store(&path);
        let result = store
            .update_and_then(
                0,
                &json!({"logging":{"level":"debug"}}),
                |_| Err("injected filter preparation failure".to_owned()),
                |_, _| std::future::ready(()),
            )
            .await;
        let error = match result {
            Err(error) => error,
            Ok(_) => panic!("expected filter preparation failure"),
        };

        assert!(matches!(error, RuntimeConfigError::Apply(_)));
        assert_eq!(fs::read_to_string(path).unwrap(), old_file);
        assert_eq!(store.load().revision, 0);
        assert_eq!(store.load().config.logging.level, LogLevel::Info);
    }

    #[tokio::test]
    async fn pre_rename_failures_keep_the_file_and_snapshot_and_clean_our_temp() {
        for stage in [
            WriteStage::ParentCreate,
            WriteStage::TempCreate,
            WriteStage::Write,
            WriteStage::FileSync,
            WriteStage::Rename,
        ] {
            let dir = TempDir::new().unwrap();
            let path = dir.path().join("runtime.toml");
            let old_file = "revision = 0\n";
            fs::write(&path, old_file).unwrap();
            let store = store(&path);
            let result = store
                .update_with_failure(
                    0,
                    &json!({"logging":{"level":"debug"}}),
                    Some(stage),
                    |_| Ok(Box::new(|| {}) as Box<dyn FnOnce() + Send>),
                )
                .await;
            let error = match result {
                Err(error) => error,
                Ok(_) => panic!("expected injected write error at {}", stage.name()),
            };
            let RuntimeConfigError::Write(error) = error else {
                panic!("expected injected write error at {}", stage.name());
            };
            assert!(!error.renamed, "{} must fail before rename", stage.name());
            assert_eq!(fs::read_to_string(path).unwrap(), old_file);
            let snapshot = store.load();
            assert_eq!(snapshot.revision, 0);
            assert_eq!(snapshot.config.logging.level, LogLevel::Info);
            assert_eq!(fs::read_dir(dir.path()).unwrap().count(), 1);
        }
    }

    #[tokio::test]
    async fn new_parent_directory_is_synced_before_temp_file_creation() {
        for stage in [
            WriteStage::CreatedDirectorySync,
            WriteStage::ParentDirectorySync,
        ] {
            let dir = TempDir::new().unwrap();
            let parent = dir.path().join("new-parent");
            let path = parent.join("runtime.toml");
            let store = store(&path);
            let result = store
                .update_with_failure(
                    0,
                    &serde_json::json!({"logging":{"level":"debug"}}),
                    Some(stage),
                    |_| Ok(Box::new(|| {}) as Box<dyn FnOnce() + Send>),
                )
                .await;
            let error = match result {
                Err(error) => error,
                Ok(_) => panic!("expected injected parent sync failure"),
            };
            let RuntimeConfigError::Write(error) = error else {
                panic!("expected injected parent sync failure");
            };

            assert_eq!(error.stage, stage);
            assert!(!error.renamed);
            assert!(parent.is_dir());
            assert!(!path.exists());
            assert_eq!(store.load().revision, 0);
            assert_eq!(std::fs::read_dir(&parent).unwrap().count(), 0);
        }

        let dir = TempDir::new().unwrap();
        let parent = dir.path().join("new-parent");
        let path = parent.join("runtime.toml");
        let store = store(&path);
        let update = store
            .update(0, &serde_json::json!({"logging":{"level":"debug"}}))
            .await
            .unwrap();
        assert_eq!(update.snapshot.revision, 1);
        assert_eq!(store.load().revision, 1);
        let file: RuntimeFile = toml::from_str(&fs::read_to_string(path).unwrap()).unwrap();
        assert_eq!(file.revision, 1);
        assert_eq!(file.logging.level, Some(LogLevel::Debug));
    }

    #[tokio::test]
    async fn retry_resyncs_existing_parent_ancestors_after_sync_failure() {
        let dir = TempDir::new().unwrap();
        let data_dir = dir.path().join("data");
        fs::create_dir(&data_dir).unwrap();
        let parent = data_dir.join("nested");
        let path = parent.join("runtime.toml");
        let store = store(&path);
        let settings = serde_json::json!({"logging":{"level":"debug"}});

        for _ in 0..2 {
            let result = store
                .update_with_failure(0, &settings, Some(WriteStage::ParentDirectorySync), |_| {
                    Ok(Box::new(|| {}) as Box<dyn FnOnce() + Send>)
                })
                .await;
            let error = match result {
                Err(error) => error,
                Ok(_) => panic!("parent ancestry sync failure must prevent commit"),
            };
            let RuntimeConfigError::Write(error) = error else {
                panic!("expected injected parent ancestry sync failure");
            };

            assert_eq!(error.stage, WriteStage::ParentDirectorySync);
            assert!(!error.renamed);
            assert!(parent.is_dir());
            assert!(!path.exists());
            assert_eq!(store.load().revision, 0);
        }

        let update = store.update(0, &settings).await.unwrap();
        assert_eq!(update.snapshot.revision, 1);
        assert_eq!(store.load().revision, 1);
        let file: RuntimeFile = toml::from_str(&fs::read_to_string(path).unwrap()).unwrap();
        assert_eq!(file.revision, 1);
        assert_eq!(file.logging.level, Some(LogLevel::Debug));
    }

    #[test]
    fn temp_name_collision_does_not_remove_another_update_file() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("runtime.toml");
        let old_file = "revision = 0\n";
        fs::write(&path, old_file).unwrap();
        let sequence = u64::MAX;
        let temp_path = dir.path().join(format!(
            ".runtime.toml.{}.{}.tmp",
            std::process::id(),
            sequence
        ));
        fs::write(&temp_path, "owned by another update").unwrap();

        let error =
            atomic_write_with_sequence(&path, b"revision = 1\n", None, sequence).unwrap_err();

        assert_eq!(error.stage, WriteStage::TempCreate);
        assert!(!error.renamed);
        assert_eq!(fs::read_to_string(path).unwrap(), old_file);
        assert_eq!(
            fs::read_to_string(temp_path).unwrap(),
            "owned by another update"
        );
    }

    #[tokio::test]
    async fn post_rename_sync_failures_publish_new_snapshot_and_file() {
        for stage in [WriteStage::DirectoryOpen, WriteStage::DirectorySync] {
            let dir = TempDir::new().unwrap();
            let path = dir.path().join("runtime.toml");
            fs::write(&path, "revision = 0\n").unwrap();
            let store = store(&path);
            let update = store
                .update_with_failure(
                    0,
                    &json!({"logging":{"level":"debug"}}),
                    Some(stage),
                    |_| Ok(Box::new(|| {}) as Box<dyn FnOnce() + Send>),
                )
                .await
                .unwrap();
            assert_eq!(update.snapshot.revision, 1);
            assert_eq!(update.snapshot.config.logging.level, LogLevel::Debug);
            assert_eq!(
                update.durability_error.as_ref().map(|error| error.stage),
                Some(stage)
            );
            let disk: RuntimeFile = toml::from_str(&fs::read_to_string(path).unwrap()).unwrap();
            assert_eq!(disk.revision, 1);
            assert_eq!(disk.logging.level, Some(LogLevel::Debug));
        }
    }
}
