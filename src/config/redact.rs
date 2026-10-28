//! `GET /config` serialization with secrets masked.

use serde_json::Value;

use crate::config::schema::Config;

const REDACTED: &str = "<redacted>";

/// Serialize for `GET /config` with DB URL and secret env names masked.
pub fn redact(config: &Config) -> Result<Value, serde_json::Error> {
    let mut value = serde_json::to_value(config)?;

    if let Some(database) = value.get_mut("database").and_then(Value::as_object_mut) {
        for key in ["url", "url_env"] {
            if database.contains_key(key) {
                database.insert(key.to_owned(), Value::String(REDACTED.to_owned()));
            }
        }
    }
    if let Some(http) = value.get_mut("http").and_then(Value::as_object_mut)
        && http.contains_key("metrics_bearer_token_env")
    {
        http.insert(
            "metrics_bearer_token_env".to_owned(),
            Value::String(REDACTED.to_owned()),
        );
    }

    Ok(value)
}
