//! In-RAM device presence with connection generations (`02` §2.10, §3.4).
//!
//! A session id is unique per connection, so storing it as the generation makes
//! a late disconnect from a replaced session harmless: it only clears presence
//! when it still owns the registry entry.

use std::collections::HashMap;
use std::sync::{Mutex, MutexGuard};

#[derive(Debug, thiserror::Error)]
#[error("presence state mutex is poisoned")]
pub struct PresenceError;

/// Online devices keyed by device id, valued by the current session generation.
#[derive(Debug, Default)]
pub struct Presence {
    sessions: Mutex<HashMap<String, String>>,
}

impl Presence {
    /// Empty registry; every device starts offline.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Records the connection generation; returns `true` when it was offline.
    pub fn connect(&self, device_id: &str, generation: &str) -> Result<bool, PresenceError> {
        Ok(self
            .lock()?
            .insert(device_id.to_owned(), generation.to_owned())
            .is_none())
    }

    /// Clears presence only when `generation` is still the current connection.
    ///
    /// Returns `true` when this disconnect was the live one.
    pub fn disconnect(&self, device_id: &str, generation: &str) -> Result<bool, PresenceError> {
        let mut sessions = self.lock()?;
        if sessions
            .get(device_id)
            .is_none_or(|current| current != generation)
        {
            return Ok(false);
        }
        sessions.remove(device_id);
        Ok(true)
    }

    /// Whether the device currently holds a session.
    pub fn is_online(&self, device_id: &str) -> Result<bool, PresenceError> {
        Ok(self.lock()?.contains_key(device_id))
    }

    /// Number of online devices.
    pub fn online_count(&self) -> Result<usize, PresenceError> {
        Ok(self.lock()?.len())
    }

    fn lock(&self) -> Result<MutexGuard<'_, HashMap<String, String>>, PresenceError> {
        self.sessions.lock().map_err(|_| PresenceError)
    }

    #[cfg(test)]
    pub(crate) fn poison_for_test(&self) {
        let result = std::thread::scope(|scope| {
            scope
                .spawn(|| {
                    let _guard = self.sessions.lock().unwrap();
                    panic!("poison presence for test");
                })
                .join()
        });
        assert!(result.is_err());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn connect_disconnect_updates_state() {
        let presence = Presence::new();
        assert!(!presence.is_online("a").unwrap());

        assert!(presence.connect("a", "session-1").unwrap());
        assert!(!presence.connect("a", "session-2").unwrap());
        assert!(presence.is_online("a").unwrap());
        assert_eq!(presence.online_count().unwrap(), 1);

        assert!(presence.disconnect("a", "session-2").unwrap());
        assert!(!presence.is_online("a").unwrap());
        assert_eq!(presence.online_count().unwrap(), 0);
    }

    #[test]
    fn stale_generation_cannot_clear_a_newer_connection() {
        let presence = Presence::new();
        presence.connect("a", "session-1").unwrap();
        presence.connect("a", "session-2").unwrap();

        assert!(
            !presence.disconnect("a", "session-1").unwrap(),
            "disconnect from the replaced session must be ignored"
        );
        assert!(presence.is_online("a").unwrap());

        assert!(presence.disconnect("a", "session-2").unwrap());
        assert!(!presence.is_online("a").unwrap());
    }

    #[test]
    fn poisoned_mutex_returns_errors_without_recovering_state() {
        let presence = std::sync::Arc::new(Presence::new());
        let to_poison = std::sync::Arc::clone(&presence);
        let join = std::thread::spawn(move || {
            let _guard = to_poison.sessions.lock().unwrap();
            panic!("poison presence mutex for test");
        })
        .join();
        assert!(join.is_err());

        assert!(presence.connect("a", "session").is_err());
        assert!(presence.disconnect("a", "session").is_err());
        assert!(presence.is_online("a").is_err());
        assert!(presence.online_count().is_err());
    }
}
