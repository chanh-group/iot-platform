//! Exclusive `<data_dir>/.lock`; held until process exit (`02` §5.3).

use std::fs::{File, OpenOptions, TryLockError};
use std::io::{self, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::process;

use thiserror::Error;

/// Failures while creating, opening, or locking `<data_dir>/.lock`.
#[derive(Debug, Error)]
pub enum LockError {
    #[error("cannot create data dir {path}: {source}")]
    DataDir {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error("cannot open lock file {path}: {source}")]
    Open {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error("data dir {path} is locked by another process")]
    Busy { path: PathBuf },
    #[error("cannot lock {path}: {source}")]
    Lock {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error("cannot write pid to {path}: {source}")]
    Pid {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
}

/// Closing the file releases the advisory lock; no explicit unlock needed.
#[must_use = "dropping the lock releases the data dir immediately"]
pub struct DataDirLock {
    _file: File,
}

impl DataDirLock {
    /// Takes the exclusive lock and writes this process id into the file.
    pub fn try_new(data_dir: &Path) -> Result<Self, LockError> {
        std::fs::create_dir_all(data_dir).map_err(|source| LockError::DataDir {
            path: data_dir.to_path_buf(),
            source,
        })?;

        let path = data_dir.join(".lock");
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&path)
            .map_err(|source| LockError::Open {
                path: path.clone(),
                source,
            })?;

        match file.try_lock() {
            Ok(()) => {}
            Err(TryLockError::WouldBlock) => return Err(LockError::Busy { path }),
            Err(TryLockError::Error(source)) => {
                return Err(LockError::Lock { path, source });
            }
        }

        file.set_len(0)
            .and_then(|()| file.seek(SeekFrom::Start(0)).map(|_| ()))
            .map_err(|source| LockError::Pid {
                path: path.clone(),
                source,
            })?;
        writeln!(file, "{}", process::id()).map_err(|source| LockError::Pid { path, source })?;

        Ok(Self { _file: file })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn second_lock_in_same_process_fails_until_dropped() {
        let dir = tempfile::TempDir::new().unwrap();
        let first = DataDirLock::try_new(dir.path()).unwrap();
        let pid = std::fs::read_to_string(dir.path().join(".lock")).unwrap();
        assert_eq!(pid.trim(), std::process::id().to_string());

        assert!(matches!(
            DataDirLock::try_new(dir.path()),
            Err(LockError::Busy { .. })
        ));
        drop(first);
        let _reacquired = DataDirLock::try_new(dir.path()).unwrap();
    }
}
