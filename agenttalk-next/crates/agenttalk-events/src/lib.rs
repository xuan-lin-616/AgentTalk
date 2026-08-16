use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RuntimeEvent {
    pub event_id: String,
    pub execution_run_id: String,
    pub runtime_id: String,
    pub thread_id: Option<String>,
    pub turn_id: Option<String>,
    pub sequence: u64,
    pub event_type: String,
    pub timestamp_ms: i64,
    pub payload: Value,
}

#[derive(Debug, Error, Eq, PartialEq)]
pub enum EventStoreError {
    #[error("event sequence must be monotonic")]
    NonMonotonic,
}

pub trait EventStore {
    fn append(&mut self, event: RuntimeEvent) -> Result<u64, EventStoreError>;
    fn replay_after(&self, sequence: u64) -> Vec<RuntimeEvent>;
}

#[derive(Default)]
pub struct InMemoryEventStore {
    events: Vec<RuntimeEvent>,
}

impl InMemoryEventStore {
    pub fn len(&self) -> usize {
        self.events.len()
    }

    pub fn is_empty(&self) -> bool {
        self.events.is_empty()
    }
}

impl EventStore for InMemoryEventStore {
    fn append(&mut self, mut event: RuntimeEvent) -> Result<u64, EventStoreError> {
        let next = self
            .events
            .last()
            .map(|item| item.sequence + 1)
            .unwrap_or(1);
        if event.sequence != 0 && event.sequence < next {
            return Err(EventStoreError::NonMonotonic);
        }
        event.sequence = next;
        self.events.push(event);
        Ok(next)
    }

    fn replay_after(&self, sequence: u64) -> Vec<RuntimeEvent> {
        self.events
            .iter()
            .filter(|event| event.sequence > sequence)
            .cloned()
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn event_store_is_append_only_and_replayable() {
        let mut store = InMemoryEventStore::default();
        store
            .append(RuntimeEvent {
                event_id: "e1".into(),
                execution_run_id: "r1".into(),
                runtime_id: "mock".into(),
                thread_id: None,
                turn_id: None,
                sequence: 0,
                event_type: "execution.created".into(),
                timestamp_ms: 1,
                payload: json!({}),
            })
            .unwrap();
        store
            .append(RuntimeEvent {
                event_id: "e2".into(),
                execution_run_id: "r1".into(),
                runtime_id: "mock".into(),
                thread_id: None,
                turn_id: None,
                sequence: 0,
                event_type: "execution.completed".into(),
                timestamp_ms: 2,
                payload: json!({}),
            })
            .unwrap();
        assert_eq!(store.len(), 2);
        assert_eq!(store.replay_after(1).len(), 1);
    }
}
