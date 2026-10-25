use crate::*;
use std::{collections::BTreeMap, sync::Mutex};

/// Bounded process-local validation adapter. No persistence or durable-intent
/// switch. Capacity exhaustion fails a new insert; existing events are never
/// evicted. Duplicate/conflict detection remains available at capacity.
pub struct MemoryAuditRepository {
    events: Mutex<BTreeMap<(AuditScope, String), AuditEvent>>,
    max_events: usize,
}

impl MemoryAuditRepository {
    /// # Errors
    /// Returns [`AuditError::InvalidInput`] when the event capacity is zero.
    pub fn new(max_events: usize) -> Result<Self, AuditError> {
        if max_events == 0 {
            return Err(AuditError::InvalidInput);
        }
        Ok(Self {
            events: Mutex::new(BTreeMap::new()),
            max_events,
        })
    }
}

impl AuditRepository for MemoryAuditRepository {
    async fn insert(&self, event: AuditEvent) -> Result<InsertStatus, BoxError> {
        let mut events = self.events.lock().map_err(|_| AuditError::StorageFailed)?;
        let key = (event.input.scope.clone(), event.input.id.clone());
        if let Some(previous) = events.get(&key) {
            return Ok(if previous.same_content(&event) {
                InsertStatus::Duplicate
            } else {
                InsertStatus::Conflict
            });
        }
        if events.len() >= self.max_events {
            return Err(AuditError::StorageFailed.into());
        }
        events.insert(key, event);
        Ok(InsertStatus::Inserted)
    }

    async fn get(&self, scope: &AuditScope, id: &str) -> Result<Option<AuditEvent>, BoxError> {
        let events = self.events.lock().map_err(|_| AuditError::StorageFailed)?;
        Ok(events.get(&(scope.clone(), id.to_owned())).cloned())
    }

    async fn list(&self, query: &AuditQuery, limit: usize) -> Result<Vec<AuditEvent>, BoxError> {
        let events = self.events.lock().map_err(|_| AuditError::StorageFailed)?;
        let mut rows: Vec<_> = events
            .values()
            .filter(|event| crate::validation::matches_query(event, query))
            .collect();
        rows.sort_unstable_by(|a, b| {
            (b.recorded_at, &b.input.id).cmp(&(a.recorded_at, &a.input.id))
        });
        Ok(rows.into_iter().take(limit).cloned().collect())
    }
}
