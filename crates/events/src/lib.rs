//! Ephemeral fan-out and a separate, explicitly acknowledged local outbox.
mod outbox;
pub use outbox::{LocalOutbox, OutboxPlugin, OutboxRecord};

use lenso_plugin_core::{BoxError, CleanupPhase, Plugin, Setup};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex, Weak},
};
use tokio::sync::mpsc;

#[derive(Debug, thiserror::Error)]
pub enum EventError {
    #[error("event service is closed")]
    Closed,
    #[error("event capacity exhausted; nothing accepted")]
    Full,
    #[error("invalid event configuration or payload")]
    Invalid,
    #[error("outbox I/O failed; acceptance may be unknown, reopen and inspect")]
    Io(#[from] std::io::Error),
    #[error("outbox data is invalid")]
    Corrupt(#[from] serde_json::Error),
}

#[derive(Clone, Copy)]
pub struct EventConfig {
    pub subscriber_capacity: usize,
    pub max_subscribers: usize,
    pub max_event_bytes: usize,
}
impl EventConfig {
    fn validate(self) -> Result<(), EventError> {
        if self.subscriber_capacity == 0
            || self.subscriber_capacity > 1_000_000
            || self.max_subscribers == 0
            || self.max_event_bytes == 0
        {
            return Err(EventError::Invalid);
        }
        Ok(())
    }
}
impl Default for EventConfig {
    fn default() -> Self {
        Self {
            subscriber_capacity: 64,
            max_subscribers: 1024,
            max_event_bytes: 64 * 1024,
        }
    }
}

struct State {
    closed: bool,
    next: u64,
    subscribers: BTreeMap<u64, mpsc::Sender<Arc<[u8]>>>,
}
/// Process-local only: acceptance means queued, never delivered or persisted.
#[derive(Clone)]
pub struct Events {
    config: EventConfig,
    state: Arc<Mutex<State>>,
}
impl Events {
    /// # Errors
    /// Returns `Invalid` for zero capacities or an excessive subscriber queue size.
    pub fn new(config: EventConfig) -> Result<Self, EventError> {
        config.validate()?;
        Ok(Self {
            config,
            state: Arc::new(Mutex::new(State {
                closed: false,
                next: 0,
                subscribers: BTreeMap::new(),
            })),
        })
    }
    /// # Errors
    /// Returns `Closed` after shutdown or `Full` for subscriber/ID exhaustion.
    pub fn subscribe(&self) -> Result<Subscription, EventError> {
        let mut state = self.state.lock().unwrap();
        if state.closed {
            return Err(EventError::Closed);
        }
        if state.subscribers.len() >= self.config.max_subscribers {
            return Err(EventError::Full);
        }
        let id = state.next;
        state.next = id.checked_add(1).ok_or(EventError::Full)?;
        let (sender, receiver) = mpsc::channel(self.config.subscriber_capacity);
        state.subscribers.insert(id, sender);
        Ok(Subscription {
            id,
            state: Arc::downgrade(&self.state),
            receiver,
        })
    }
    /// Atomic fan-out: if any live subscriber is full, none receives the event.
    /// Zero subscribers returns zero; there is no retained event history.
    ///
    /// # Errors
    /// Returns `Invalid` for oversized payloads, `Closed` after shutdown, or `Full`
    /// when a subscriber cannot reserve capacity. No subscriber receives a failed fan-out.
    pub fn publish(&self, event: impl Into<Arc<[u8]>>) -> Result<usize, EventError> {
        let event = event.into();
        if event.len() > self.config.max_event_bytes {
            return Err(EventError::Invalid);
        }
        let mut state = self.state.lock().unwrap();
        if state.closed {
            return Err(EventError::Closed);
        }
        state.subscribers.retain(|_, sender| !sender.is_closed());
        let permits = state
            .subscribers
            .values()
            .map(|sender| {
                sender
                    .clone()
                    .try_reserve_owned()
                    .map_err(|_| EventError::Full)
            })
            .collect::<Result<Vec<_>, _>>()?;
        let count = permits.len();
        for permit in permits {
            permit.send(event.clone());
        }
        Ok(count)
    }
    /// Reject future work, wake receivers and allow already queued events to drain.
    pub fn close(&self) {
        let mut state = self.state.lock().unwrap();
        state.closed = true;
        state.subscribers.clear();
    }
}
/// Drop unregisters immediately. A cancelled recv does not consume an event.
pub struct Subscription {
    id: u64,
    state: Weak<Mutex<State>>,
    receiver: mpsc::Receiver<Arc<[u8]>>,
}
impl Subscription {
    pub async fn recv(&mut self) -> Option<Arc<[u8]>> {
        self.receiver.recv().await
    }
}
impl Drop for Subscription {
    fn drop(&mut self) {
        if let Some(state) = self.state.upgrade() {
            state.lock().unwrap().subscribers.remove(&self.id);
        }
    }
}

pub struct EventsPlugin(pub EventConfig);
impl Plugin for EventsPlugin {
    type Service = Events;
    fn validate(&self) -> Result<(), BoxError> {
        self.0.validate().map_err(Into::into)
    }
    async fn setup(self, setup: &mut Setup) -> Result<Arc<Events>, BoxError> {
        let events = Arc::new(Events::new(self.0)?);
        let cleanup = events.clone();
        setup.on_cleanup(CleanupPhase::Entrances, move || async move {
            cleanup.close();
            Ok(())
        });
        Ok(events)
    }
}
