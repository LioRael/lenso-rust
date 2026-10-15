use crate::{
    CloseReason, Envelope, Event, GapReason, Identity, RealtimeConfig, RealtimeError, Resource,
    SubscriptionId,
};
use serde::Serialize;
use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::{Arc, Mutex, MutexGuard},
};
use tokio::{sync::watch, time::Instant};

pub(crate) fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().expect("realtime state poisoned")
}

pub(crate) struct Queued {
    pub event: Envelope,
    pub bytes: usize,
}
#[derive(Default)]
pub(crate) struct Inbox {
    pub queue: VecDeque<Queued>,
    pub bytes: usize,
    pub closed: bool,
    pub terminal: Option<Envelope>,
}
pub(crate) struct Mailbox {
    pub inbox: Mutex<Inbox>,
    pub changed: watch::Sender<()>,
}
impl Mailbox {
    pub fn new() -> Self {
        Self {
            inbox: Mutex::new(Inbox::default()),
            changed: watch::channel(()).0,
        }
    }
}
pub(crate) struct SubState {
    pub resource: Resource,
    pub valid_until: Option<Instant>,
    pub renewing: bool,
    pub cancelled: watch::Sender<bool>,
}
pub(crate) struct ConnectionState<P> {
    pub identity: Arc<Identity<P>>,
    pub subscriptions: HashMap<SubscriptionId, SubState>,
    pub mailbox: Arc<Mailbox>,
}
pub(crate) struct State<P> {
    pub closed: bool,
    pub next_id: u64,
    pub pending: usize,
    pub connections: HashMap<u64, ConnectionState<P>>,
    pub topics: HashMap<Resource, HashSet<(u64, SubscriptionId)>>,
}
impl<P> Default for State<P> {
    fn default() -> Self {
        Self {
            closed: false,
            next_id: 0,
            pending: 0,
            connections: HashMap::new(),
            topics: HashMap::new(),
        }
    }
}
impl<P> State<P> {
    pub fn next_id(&mut self) -> Result<u64, RealtimeError> {
        self.next_id = self.next_id.checked_add(1).ok_or(RealtimeError::Limit)?;
        Ok(self.next_id)
    }
    pub fn open(&self) -> Result<(), RealtimeError> {
        if self.closed {
            Err(RealtimeError::Closed)
        } else {
            Ok(())
        }
    }
    pub fn live(&self, id: u64) -> Result<&ConnectionState<P>, RealtimeError> {
        self.open()?;
        self.connections.get(&id).ok_or(RealtimeError::Closed)
    }
    pub fn close_connection(&mut self, id: u64, terminal: Option<Envelope>) {
        let Some(connection) = self.connections.remove(&id) else {
            return;
        };
        for (sub_id, sub) in connection.subscriptions {
            sub.cancelled.send_replace(true);
            self.remove_topic(&sub.resource, id, sub_id);
        }
        let mut inbox = lock(&connection.mailbox.inbox);
        inbox.queue.clear();
        inbox.bytes = 0;
        inbox.closed = true;
        inbox.terminal = terminal;
        connection.mailbox.changed.send_replace(());
    }
    fn remove_topic(&mut self, resource: &Resource, connection: u64, id: SubscriptionId) {
        if let Some(listeners) = self.topics.get_mut(resource) {
            listeners.remove(&(connection, id));
            if listeners.is_empty() {
                self.topics.remove(resource);
            }
        }
    }
    pub fn remove_sub(
        &mut self,
        connection_id: u64,
        id: SubscriptionId,
        reason: Option<CloseReason>,
        config: &RealtimeConfig,
    ) {
        let Some(connection) = self.connections.get_mut(&connection_id) else {
            return;
        };
        let Some(sub) = connection.subscriptions.remove(&id) else {
            return;
        };
        sub.cancelled.send_replace(true);
        {
            let mut inbox = lock(&connection.mailbox.inbox);
            inbox
                .queue
                .retain(|item| item.event.subscription() != Some(id));
            inbox.bytes = inbox.queue.iter().map(|item| item.bytes).sum();
        }
        self.remove_topic(&sub.resource, connection_id, id);
        if let Some(reason) = reason {
            self.push(
                connection_id,
                Envelope::new(Event::Closed {
                    subscription: Some(id),
                    reason,
                }),
                config,
            );
        }
    }
    /// False means the connection overflowed (or was already closed).
    pub fn push(&mut self, id: u64, event: Envelope, config: &RealtimeConfig) -> bool {
        let Some(connection) = self.connections.get(&id) else {
            return false;
        };
        let bytes = serialized_size(&event, config.max_payload_bytes + 512);
        let mut inbox = lock(&connection.mailbox.inbox);
        if let Ok(bytes) = bytes {
            if inbox.queue.len() < config.max_buffered_events
                && bytes <= config.max_buffered_bytes.saturating_sub(inbox.bytes)
            {
                inbox.bytes += bytes;
                inbox.queue.push_back(Queued { event, bytes });
                connection.mailbox.changed.send_replace(());
                return true;
            }
        }
        drop(inbox);
        self.close_connection(
            id,
            Some(Envelope::new(Event::Gap {
                reason: GapReason::Overflow,
            })),
        );
        false
    }
    /// No task per lease. Every admission, delivery and read enforces deadlines;
    /// recv also sleeps to the earliest deadline when no envelope is queued.
    pub fn expire(&mut self, config: &RealtimeConfig) {
        let now = Instant::now();
        let mut expired_connections = Vec::new();
        let mut expired_subs = Vec::new();
        for (&id, connection) in &self.connections {
            if connection.identity.expires_at <= now {
                expired_connections.push(id);
            } else {
                for (&sub_id, sub) in &connection.subscriptions {
                    if sub.valid_until.is_some_and(|deadline| deadline <= now) {
                        expired_subs.push((id, sub_id));
                    }
                }
            }
        }
        for id in expired_connections {
            self.close_connection(
                id,
                Some(Envelope::new(Event::Closed {
                    subscription: None,
                    reason: CloseReason::Expired,
                })),
            );
        }
        for (id, sub_id) in expired_subs {
            self.remove_sub(id, sub_id, Some(CloseReason::Expired), config);
        }
    }
}

/// Count serialized UTF-8 bytes without allocating a second payload buffer.
pub(crate) fn serialized_size(
    value: &impl Serialize,
    limit: usize,
) -> Result<usize, RealtimeError> {
    struct Counter {
        bytes: usize,
        limit: usize,
    }
    impl std::io::Write for Counter {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            if bytes.len() > self.limit.saturating_sub(self.bytes) {
                return Err(std::io::Error::other("realtime payload limit"));
            }
            self.bytes += bytes.len();
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut counter = Counter { bytes: 0, limit };
    serde_json::to_writer(&mut counter, value).map_err(|_| RealtimeError::Payload)?;
    Ok(counter.bytes)
}
