//! Command expiration transitions.

use crate::core::events::EventBus;
use crate::observability::metrics::Metrics;

use super::{CommandRepository, CommandRepositoryError, PendingCommandAcks};

/// Expires a bounded batch and records the count metric.
pub async fn expire_due(
    repository: &dyn CommandRepository,
    now: i64,
    limit: i64,
    pending_acks: &PendingCommandAcks,
    events: &EventBus,
) -> Result<u64, CommandRepositoryError> {
    let Some(pending_ack_ids) = pending_acks.command_ids() else {
        return Ok(0);
    };
    let expired = repository.expire_due(now, limit, &pending_ack_ids).await?;
    for command in &expired {
        events.publish_command(
            &command.id,
            &command.device_id,
            command.status,
            command.completed_at,
            command.error.as_deref(),
        );
    }
    let count = u64::try_from(expired.len()).unwrap_or(u64::MAX);
    Metrics::global().commands_expired_total.add(count);
    Ok(count)
}
