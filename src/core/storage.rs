//! Storage read and post-commit audit ports.

#[derive(Debug, Clone, Copy)]
pub struct OverviewCounts {
    pub devices_total: i64,
    pub devices_disabled: i64,
    pub telemetry_events_24h: i64,
    pub last_received_at: Option<i64>,
    pub commands_pending: i64,
    pub commands_dispatched: i64,
    pub commands_failed_24h: i64,
    pub commands_expired_24h: i64,
}

pub struct ConfigAudit<'a> {
    pub occurred_at: i64,
    pub actor_id: &'a str,
    pub request_id: &'a str,
    pub remote_ip: &'a str,
    pub old_revision: u64,
    pub new_revision: u64,
    pub changed_fields: &'a [String],
}

#[async_trait::async_trait]
pub trait StorageRepository: Send + Sync {
    async fn health_check(&self) -> anyhow::Result<()>;

    async fn overview(&self, from: i64, now: i64) -> anyhow::Result<OverviewCounts>;

    async fn pending_commands(&self) -> anyhow::Result<i64>;

    async fn audit_config_update(&self, event: ConfigAudit<'_>) -> anyhow::Result<()>;
}
