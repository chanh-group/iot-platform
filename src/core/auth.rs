//! Admin credentials and session tokens (`02` §1.3, §6).

use std::time::{SystemTime, UNIX_EPOCH};

use argon2::{Argon2, PasswordHash, PasswordHasher, PasswordVerifier};
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use rand::TryRng;
use rand::rngs::{SysError, SysRng};
use sha2::{Digest, Sha256};
use thiserror::Error;

pub const SESSION_TOKEN_BYTES: usize = 32;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum PasswordPolicyError {
    #[error("password must be at least {min_length} characters")]
    TooShort { min_length: u64 },
}

/// `^[a-z0-9_.-]{3,32}$` without pulling in a regex dependency.
pub fn is_valid_username(username: &str) -> bool {
    (3..=32).contains(&username.len())
        && username.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'_' | b'.' | b'-')
        })
}

pub fn validate_password(password: &str, min_length: u64) -> Result<(), PasswordPolicyError> {
    if (password.chars().count() as u64) < min_length {
        return Err(PasswordPolicyError::TooShort { min_length });
    }
    Ok(())
}

/// Argon2id with the crate defaults, encoded as a PHC string.
pub fn hash_password(password: &str) -> Result<String, argon2::password_hash::Error> {
    let hash = Argon2::default().hash_password(password.as_bytes())?;
    Ok(hash.to_string())
}

pub fn verify_password(password_hash: &str, password: &str) -> bool {
    PasswordHash::new(password_hash).is_ok_and(|parsed| {
        Argon2::default()
            .verify_password(password.as_bytes(), &parsed)
            .is_ok()
    })
}

/// 32 random bytes as base64url without padding (43 characters).
pub fn generate_session_token() -> Result<String, SysError> {
    let mut bytes = [0u8; SESSION_TOKEN_BYTES];
    let mut rng = SysRng;
    rng.try_fill_bytes(&mut bytes)?;
    Ok(URL_SAFE_NO_PAD.encode(bytes))
}

pub fn hash_token(token: &str) -> [u8; 32] {
    Sha256::digest(token.as_bytes()).into()
}

pub fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |elapsed| {
            i64::try_from(elapsed.as_millis()).unwrap_or(i64::MAX)
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn username_regex_is_enforced() {
        for valid in ["admin", "a_b.c-d", "user123"] {
            assert!(is_valid_username(valid), "{valid}");
        }
        for invalid in [
            "",
            "ab",
            "Admin",
            "has space",
            "user@example",
            "a".repeat(33).as_str(),
        ] {
            assert!(!is_valid_username(invalid), "{invalid}");
        }
    }

    #[test]
    fn password_policy_uses_characters_not_bytes() {
        assert!(validate_password("mật-khẩu-123", 12).is_ok());
        assert_eq!(
            validate_password("short", 12),
            Err(PasswordPolicyError::TooShort { min_length: 12 })
        );
    }

    #[test]
    fn argon2id_hash_roundtrips_and_uses_a_fresh_salt() {
        let password = "correct horse battery staple";
        let first = hash_password(password).unwrap();
        let second = hash_password(password).unwrap();

        assert!(first.starts_with("$argon2id$"));
        assert_ne!(first, second);
        assert!(verify_password(&first, password));
        assert!(!verify_password(&first, "wrong password"));
        assert!(!verify_password("not-a-phc-string", password));
    }

    #[test]
    fn session_token_is_32_random_bytes_base64url() {
        let token = generate_session_token().unwrap();
        assert_eq!(token.len(), 43);
        assert!(
            token
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_'))
        );
        assert_ne!(token, generate_session_token().unwrap());
        assert_eq!(hash_token(&token).len(), 32);
        assert_eq!(hash_token(&token), hash_token(&token));
    }
}
