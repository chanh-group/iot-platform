//! Shared JSON constraints required for backend-identical persistence.

use serde_json::Value;

pub(crate) fn contains_nul(value: &Value) -> bool {
    match value {
        Value::String(value) => value.contains('\0'),
        Value::Array(values) => values.iter().any(contains_nul),
        Value::Object(values) => values
            .iter()
            .any(|(key, value)| key.contains('\0') || contains_nul(value)),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::contains_nul;
    use serde_json::{Value, json};

    #[test]
    fn detects_nul_in_nested_strings_and_object_keys() {
        assert!(contains_nul(&json!({"nested": ["\0"]})));
        let key = String::from("nul\0key");
        let object = Value::Object([(key, json!("value"))].into_iter().collect());
        assert!(contains_nul(&object));
        assert!(!contains_nul(&json!({"nested": ["ok", 1, true]})));
    }
}
