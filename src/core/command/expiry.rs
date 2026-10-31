//! Command expiration transitions.

use crate::observability::metrics::Metrics;

use super::{CommandRepository, CommandRepositoryError, PendingCommandAcks};

/// Expires a bounded batch and records the count metric.
pub async fn expire_due(
    repository: &dyn CommandRepository,
    now: i64,
    limit: i64,
    pending_acks: &PendingCommandAcks,
) -> Result<u64, CommandRepositoryError> {
    let Some(pending_ack_ids) = pending_acks.command_ids() else {
        return Ok(0);
    };
    let expired = repository.expire_due(now, limit, &pending_ack_ids).await?;
    Metrics::global().commands_expired_total.add(expired);
    Ok(expired)
}
