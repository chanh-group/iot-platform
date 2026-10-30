use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum CommandStatus {
    Pending,
    Dispatched,
    Succeeded,
    Failed,
    Expired,
}

impl CommandStatus {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Dispatched => "dispatched",
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
            Self::Expired => "expired",
        }
    }

    #[must_use]
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "pending" => Some(Self::Pending),
            "dispatched" => Some(Self::Dispatched),
            "succeeded" => Some(Self::Succeeded),
            "failed" => Some(Self::Failed),
            "expired" => Some(Self::Expired),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_only_contract_command_states() {
        for (value, expected) in [
            ("pending", CommandStatus::Pending),
            ("dispatched", CommandStatus::Dispatched),
            ("succeeded", CommandStatus::Succeeded),
            ("failed", CommandStatus::Failed),
            ("expired", CommandStatus::Expired),
        ] {
            assert_eq!(CommandStatus::parse(value), Some(expected));
            assert_eq!(expected.as_str(), value);
        }
        assert_eq!(CommandStatus::parse("unknown"), None);
    }
}
