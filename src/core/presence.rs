//! In-RAM device presence with connection generations (`02` §2.10, §3.4).
//!
//! A session id is unique per connection, so storing it as the generation makes
//! a late disconnect from a replaced session harmless: it only clears presence
//! when it still owns the registry entry.

use std::collections::HashMap;
use std::sync::{Mutex, PoisonError};

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
    pub fn connect(&self, device_id: &str, generation: &str) -> bool {
        self.lock()
            .insert(device_id.to_owned(), generation.to_owned())
            .is_none()
    }

    /// Clears presence only when `generation` is still the current connection.
    ///
    /// Returns `true` when this disconnect was the live one.
    pub fn disconnect(&self, device_id: &str, generation: &str) -> bool {
        let mut sessions = self.lock();
        if sessions
            .get(device_id)
            .is_none_or(|current| current != generation)
        {
            return false;
        }
        sessions.remove(device_id);
        true
    }

    /// Whether the device currently holds a session.
    #[must_use]
    pub fn is_online(&self, device_id: &str) -> bool {
        self.lock().contains_key(device_id)
    }

    /// Number of online devices.
    #[must_use]
    pub fn online_count(&self) -> usize {
        self.lock().len()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<String, String>> {
        self.sessions.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn connect_disconnect_updates_state() {
        let presence = Presence::new();
        assert!(!presence.is_online("a"));

        assert!(presence.connect("a", "session-1"));
        assert!(!presence.connect("a", "session-2"));
        assert!(presence.is_online("a"));
        assert_eq!(presence.online_count(), 1);

        assert!(presence.disconnect("a", "session-2"));
        assert!(!presence.is_online("a"));
        assert_eq!(presence.online_count(), 0);
    }

    #[test]
    fn stale_generation_cannot_clear_a_newer_connection() {
        let presence = Presence::new();
        presence.connect("a", "session-1");
        presence.connect("a", "session-2");

        assert!(
            !presence.disconnect("a", "session-1"),
            "disconnect from the replaced session must be ignored"
        );
        assert!(presence.is_online("a"));

        assert!(presence.disconnect("a", "session-2"));
        assert!(!presence.is_online("a"));
    }
}
