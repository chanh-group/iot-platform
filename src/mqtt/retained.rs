//! Bounded in-memory retained storage (`02` §2.5).

use std::collections::HashMap;
use std::time::Duration;

use async_trait::async_trait;
use rmqtt::retain::{DefaultRetainStorage, RetainStorage};
use rmqtt::types::{Retain, TopicFilter, TopicName};
use tokio::sync::Mutex;

use crate::observability::metrics::Metrics;

#[derive(Default)]
struct Budget {
    topics: HashMap<String, usize>,
    bytes: usize,
}

/// RMQTT retained storage with serialized topic/byte accounting.
pub(crate) struct BudgetedRetainStorage {
    inner: DefaultRetainStorage,
    max_messages: usize,
    max_bytes: usize,
    budget: Mutex<Budget>,
}

impl BudgetedRetainStorage {
    pub(crate) fn new(max_messages: usize, max_bytes: usize) -> Self {
        Self {
            inner: DefaultRetainStorage::new(),
            max_messages,
            max_bytes,
            budget: Mutex::new(Budget::default()),
        }
    }
}

#[async_trait]
impl RetainStorage for BudgetedRetainStorage {
    fn enable(&self) -> bool {
        true
    }

    async fn set(
        &self,
        topic: &TopicName,
        retain: Retain,
        expiry_interval: Option<Duration>,
    ) -> rmqtt::Result<()> {
        let key = topic.to_string();
        let mut budget = self.budget.lock().await;
        let old_bytes = budget.topics.get(&key).copied();

        if retain.publish.payload.is_empty() {
            let next_bytes = budget
                .bytes
                .checked_sub(old_bytes.unwrap_or_default())
                .ok_or_else(|| storage_error("retained byte accounting underflow"))?;
            self.inner
                .set_with_timeout(topic, retain, expiry_interval)
                .await?;
            budget.topics.remove(&key);
            budget.bytes = next_bytes;
            return Ok(());
        }

        let payload_bytes = retain.publish.payload.len();
        let is_new_topic = old_bytes.is_none();
        let next_bytes = budget
            .bytes
            .checked_sub(old_bytes.unwrap_or_default())
            .and_then(|bytes| bytes.checked_add(payload_bytes))
            .ok_or_else(|| storage_error("retained byte accounting overflow"))?;
        let over_message_cap = is_new_topic && budget.topics.len() >= self.max_messages;
        if over_message_cap || next_bytes > self.max_bytes {
            Metrics::global().retained_cap_rejected_total.inc();
            tracing::warn!(
                topic = %key,
                payload_bytes,
                reason = "retained_cap",
                "retained publish rejected by configured cap"
            );
            return Err(storage_error("retained budget exceeded"));
        }

        self.inner
            .set_with_timeout(topic, retain, expiry_interval)
            .await?;
        budget.topics.insert(key, payload_bytes);
        budget.bytes = next_bytes;
        Ok(())
    }

    async fn get(&self, topic_filter: &TopicFilter) -> rmqtt::Result<Vec<(TopicName, Retain)>> {
        self.inner.get_message(topic_filter).await
    }

    async fn count(&self) -> isize {
        isize::try_from(self.budget.lock().await.topics.len()).unwrap_or(isize::MAX)
    }

    async fn max(&self) -> isize {
        isize::try_from(self.max_messages).unwrap_or(isize::MAX)
    }
}

fn storage_error(message: &'static str) -> rmqtt::Error {
    rmqtt::Error::from(std::io::Error::other(message))
}
