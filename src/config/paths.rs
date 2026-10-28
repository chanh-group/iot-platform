//! Lexical path resolution (`02` §5.3). Paths need not exist during `check`.

use std::path::{Component, Path, PathBuf};

/// Lexical normalization: no filesystem access, `..` cannot climb above root.
#[must_use]
pub fn normalize(path: &Path) -> PathBuf {
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            Component::Prefix(prefix) => normalized.push(prefix.as_os_str()),
            Component::RootDir => normalized.push(Component::RootDir.as_os_str()),
            Component::CurDir => {}
            Component::ParentDir => {
                if !normalized.pop() && !normalized.has_root() {
                    normalized.push("..");
                }
            }
            Component::Normal(part) => normalized.push(part),
        }
    }
    normalized
}

/// Resolve a config-relative path against the directory containing `conf.toml`.
#[must_use]
pub fn resolve(base: &Path, path: &Path) -> PathBuf {
    normalize(&base.join(path))
}

/// Append the given components to `base`, used by path-containment checks.
#[must_use]
pub fn is_within(candidate: &Path, base: &Path) -> bool {
    candidate.starts_with(base)
}

/// Strip `sqlite://`, resolve against the config dir, and require the file
/// to live inside `data_dir`. Returns the effective absolute URL.
pub fn resolve_sqlite_url(raw: &str, base: &Path, data_dir: &Path) -> Result<String, String> {
    let Some(rest) = raw.strip_prefix("sqlite://") else {
        return Err("database.url: only sqlite:// URLs are supported in MVP".to_owned());
    };
    if rest.is_empty() || rest == ":memory:" {
        return Err("database.url: an on-disk SQLite path is required".to_owned());
    }
    if rest.contains('?') || rest.contains('#') {
        return Err("database.url: query strings and fragments are not allowed".to_owned());
    }

    let path = resolve(base, Path::new(rest));
    if !is_within(&path, data_dir) {
        return Err(format!(
            "database.url: resolved path {} is outside data_dir {}",
            path.display(),
            data_dir.display()
        ));
    }
    Ok(format!("sqlite://{}", path.display()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalize_removes_dot_and_parent_components() {
        assert_eq!(normalize(Path::new("/a/./b/../c")), PathBuf::from("/a/c"));
        assert_eq!(normalize(Path::new("a/b/../../..")), PathBuf::from(".."));
    }

    #[test]
    fn sqlite_url_must_stay_inside_data_dir() {
        let base = Path::new("/srv/iot");
        let data = Path::new("/srv/iot/data");
        assert_eq!(
            resolve_sqlite_url("sqlite://./data/iot.db", base, data).unwrap(),
            "sqlite:///srv/iot/data/iot.db"
        );

        let err = resolve_sqlite_url("sqlite://../iot.db", base, data).unwrap_err();
        assert!(err.contains("outside data_dir"), "{err}");
    }
}
