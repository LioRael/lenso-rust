//! Bounded, process-local resource subscriptions, independent of any auth/HTTP runtime.
#![doc = include_str!("../README.md")]

mod contracts;
mod plugin;
mod state;
pub use contracts::*;
pub use plugin::RealtimePlugin;

use futures_util::FutureExt;
use serde::Serialize;
use serde_json::Value;
use state::{lock, serialized_size, ConnectionState, Mailbox, State, SubState};
use std::{
    collections::{HashMap, HashSet},
    future::Future,
    panic::AssertUnwindSafe,
    pin::Pin,
    sync::{Arc, Mutex},
};
use tokio::{sync::watch, time::Instant};

type AuthorizationFuture =
    Pin<Box<dyn Future<Output = Result<Option<Lease>, RealtimeError>> + Send>>;
type Authorizer<P> = Arc<dyn Fn(Arc<Identity<P>>, Resource) -> AuthorizationFuture + Send + Sync>;

fn authorizer<P, F, Fut>(authorize: F) -> Authorizer<P>
where
    F: Fn(Arc<Identity<P>>, Resource) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<Option<Lease>, RealtimeError>> + Send + 'static,
{
    Arc::new(move |identity, resource| Box::pin(authorize(identity, resource)))
}

struct Inner<P> {
    config: RealtimeConfig,
    authorize: Authorizer<P>,
    state: Mutex<State<P>>,
}

/// One isolated memory provider and service. Clones share this exact instance.
pub struct MemoryRealtime<P> {
    inner: Arc<Inner<P>>,
}
impl<P> Clone for MemoryRealtime<P> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}
impl<P: Send + Sync + 'static> MemoryRealtime<P> {
    /// No background tasks or external resources are acquired here.
    /// The callback runs only on subscribe and explicit renew. None denies.
    pub fn new<F, Fut>(config: RealtimeConfig, authorize: F) -> Result<Self, RealtimeError>
    where
        F: Fn(Arc<Identity<P>>, Resource) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Option<Lease>, RealtimeError>> + Send + 'static,
    {
        Self::with_authorizer(config, authorizer(authorize))
    }
    fn with_authorizer(
        config: RealtimeConfig,
        authorize: Authorizer<P>,
    ) -> Result<Self, RealtimeError> {
        config.validate()?;
        Ok(Self {
            inner: Arc::new(Inner {
                config,
                authorize,
                state: Mutex::new(State::default()),
            }),
        })
    }
    /// The caller owns verifying this identity. Scope equality is an additional check.
    pub fn connect(&self, identity: Identity<P>) -> Result<Connection<P>, RealtimeError> {
        contracts::part(&identity.scope)?;
        contracts::part(&identity.subject)?;
        if identity.expires_at <= Instant::now() {
            return Err(RealtimeError::Expired);
        }
        let mut state = lock(&self.inner.state);
        state.expire(&self.inner.config);
        state.open()?;
        if state.connections.len() >= self.inner.config.max_connections
            || state
                .connections
                .values()
                .filter(|c| {
                    c.identity.scope == identity.scope && c.identity.subject == identity.subject
                })
                .count()
                >= self.inner.config.max_connections_per_subject
        {
            return Err(RealtimeError::Limit);
        }
        let id = state.next_id()?;
        let mailbox = Arc::new(Mailbox::new());
        let identity = Arc::new(identity);
        state.connections.insert(
            id,
            ConnectionState {
                identity: identity.clone(),
                mailbox: mailbox.clone(),
                subscriptions: HashMap::new(),
            },
        );
        Ok(Connection {
            inner: self.inner.clone(),
            id,
            mailbox,
            _identity: identity,
        })
    }
    /// Ephemeral fan-out to currently authorized subscribers. No history or ACKs.
    /// Publish access must be enforced by the host's business service.
    pub fn publish(
        &self,
        resource: &Resource,
        event_type: &str,
        data: Value,
    ) -> Result<PublishReport, RealtimeError> {
        if event_type.is_empty()
            || event_type.len() > 64
            || !event_type.as_bytes()[0].is_ascii_alphabetic()
            || !event_type
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"._-".contains(&b))
        {
            return Err(RealtimeError::InvalidInput);
        }
        validate_payload(&data)?;
        #[derive(Serialize)]
        struct Update<'a> {
            kind: &'static str,
            #[serde(rename = "type")]
            event_type: &'a str,
            data: &'a Value,
        }
        serialized_size(
            &Update {
                kind: "update",
                event_type,
                data: &data,
            },
            self.inner.config.max_payload_bytes,
        )?;
        let data = Arc::new(data);
        let mut state = lock(&self.inner.state);
        state.expire(&self.inner.config);
        state.open()?;
        let listeners = state.topics.get(resource).cloned().unwrap_or_default();
        let mut enqueued = HashMap::<u64, usize>::new();
        let mut overflowed = HashSet::new();
        for (connection, id) in listeners {
            if !state
                .connections
                .get(&connection)
                .and_then(|c| c.subscriptions.get(&id))
                .is_some_and(|s| s.valid_until.is_some())
            {
                continue;
            }
            let event = Envelope::new(Event::Update {
                subscription: id,
                event_type: event_type.into(),
                data: data.clone(),
            });
            if state.push(connection, event, &self.inner.config) {
                *enqueued.entry(connection).or_default() += 1;
            } else {
                overflowed.insert(connection);
            }
        }
        Ok(PublishReport {
            enqueued: enqueued
                .into_iter()
                .filter(|(id, _)| !overflowed.contains(id))
                .map(|(_, n)| n)
                .sum(),
            overflowed_connections: overflowed.len(),
        })
    }
    /// Removes current leases, including authorizations still in flight.
    /// Future subscriptions must pass the host authorizer again; this is not a ban list.
    pub fn revoke_resource(&self, resource: &Resource) {
        self.remove_resource(resource, CloseReason::Revoked);
    }
    pub fn delete_resource(&self, resource: &Resource) {
        self.remove_resource(resource, CloseReason::Deleted);
    }
    fn remove_resource(&self, resource: &Resource, reason: CloseReason) {
        let mut state = lock(&self.inner.state);
        state.expire(&self.inner.config);
        for (connection, id) in state.topics.get(resource).cloned().unwrap_or_default() {
            state.remove_sub(connection, id, Some(reason), &self.inner.config);
        }
    }
    /// Local fast path for an exact (scope, subject), never a wildcard match.
    pub fn revoke_subject(&self, scope: &str, subject: &str) -> Result<(), RealtimeError> {
        contracts::part(scope)?;
        contracts::part(subject)?;
        let mut state = lock(&self.inner.state);
        state.expire(&self.inner.config);
        let ids: Vec<_> = state
            .connections
            .iter()
            .filter(|(_, c)| c.identity.scope == scope && c.identity.subject == subject)
            .map(|(&id, _)| id)
            .collect();
        for id in ids {
            state.close_connection(
                id,
                Some(Envelope::new(Event::Closed {
                    subscription: None,
                    reason: CloseReason::Revoked,
                })),
            );
        }
        Ok(())
    }
    /// Also reclaims expired idle entries. No tombstones or historical topics remain.
    pub fn stats(&self) -> Stats {
        let mut state = lock(&self.inner.state);
        state.expire(&self.inner.config);
        Stats {
            connections: state.connections.len(),
            subscriptions: state
                .connections
                .values()
                .map(|c| c.subscriptions.len())
                .sum(),
            topics: state.topics.len(),
            pending: state.pending,
        }
    }
    /// Idempotent synchronous close. Pending authorizer futures are signalled and
    /// dropped when their callers next poll (or drop) the subscribe/renew future.
    pub fn close(&self) {
        let mut state = lock(&self.inner.state);
        state.closed = true;
        let ids: Vec<_> = state.connections.keys().copied().collect();
        for id in ids {
            state.close_connection(
                id,
                Some(Envelope::new(Event::Closed {
                    subscription: None,
                    reason: CloseReason::Shutdown,
                })),
            );
        }
    }
}

/// Non-cloneable receiver. Dropping it closes the connection and purges all data.
/// &mut recv enforces one consumer; subscriptions can be renewed independently.
pub struct Connection<P> {
    inner: Arc<Inner<P>>,
    id: u64,
    mailbox: Arc<Mailbox>,
    // Keep arbitrary host principal destructors outside the state lock, even
    // when this connection is revoked or expires with no authorization in flight.
    _identity: Arc<Identity<P>>,
}
impl<P> Drop for Connection<P> {
    fn drop(&mut self) {
        lock(&self.inner.state).close_connection(self.id, None);
    }
}
impl<P: Send + Sync + 'static> Connection<P> {
    /// Reserves all limits before invoking host authorization.
    pub async fn subscribe(&self, resource: Resource) -> Result<Subscription<P>, RealtimeError> {
        let operation = {
            let mut state = lock(&self.inner.state);
            state.expire(&self.inner.config);
            let connection = state.live(self.id)?;
            if connection.identity.scope != resource.scope() {
                return Err(RealtimeError::Denied);
            }
            let identity = connection.identity.clone();
            let config = &self.inner.config;
            let subject_count: usize = state
                .connections
                .values()
                .filter(|c| {
                    c.identity.scope == identity.scope && c.identity.subject == identity.subject
                })
                .map(|c| c.subscriptions.len())
                .sum();
            if connection.subscriptions.len() >= config.max_subscriptions_per_connection
                || subject_count >= config.max_subscriptions_per_subject
                || state.topics.get(&resource).map_or(0, HashSet::len)
                    >= config.max_subscribers_per_topic
                || (!state.topics.contains_key(&resource)
                    && state.topics.len() >= config.max_topics)
                || state.pending >= config.max_pending_operations
            {
                return Err(RealtimeError::Limit);
            }
            let id = SubscriptionId(state.next_id()?);
            let (cancelled, cancel) = watch::channel(false);
            state
                .topics
                .entry(resource.clone())
                .or_default()
                .insert((self.id, id));
            state
                .connections
                .get_mut(&self.id)
                .expect("live connection")
                .subscriptions
                .insert(
                    id,
                    SubState {
                        resource: resource.clone(),
                        valid_until: None,
                        renewing: false,
                        cancelled,
                    },
                );
            state.pending += 1;
            Operation::new(
                self.inner.clone(),
                self.id,
                id,
                identity,
                resource.clone(),
                cancel,
                None,
            )
        };
        let id = operation.id;
        operation.run().await?;
        Ok(Subscription {
            inner: self.inner.clone(),
            connection: self.id,
            id,
            resource,
        })
    }
    /// Cancel-safe: cancelling a waiting recv does not consume an envelope.
    /// Buffered updates are rechecked against deadlines before being returned.
    pub async fn recv(&mut self) -> Option<Envelope> {
        let mut changed = self.mailbox.changed.subscribe();
        loop {
            let deadline = {
                let mut state = lock(&self.inner.state);
                state.expire(&self.inner.config);
                let mut inbox = lock(&self.mailbox.inbox);
                if let Some(queued) = inbox.queue.pop_front() {
                    inbox.bytes -= queued.bytes;
                    return Some(queued.event);
                }
                if let Some(terminal) = inbox.terminal.take() {
                    return Some(terminal);
                }
                if inbox.closed {
                    return None;
                }
                let connection = state.connections.get(&self.id).expect("open connection");
                connection
                    .subscriptions
                    .values()
                    .filter_map(|s| s.valid_until)
                    .fold(connection.identity.expires_at, Instant::min)
            };
            tokio::select! {
                _ = changed.changed() => {},
                _ = tokio::time::sleep_until(deadline) => {},
            }
        }
    }
    /// Explicit disconnect ends the stream without a terminal envelope.
    pub fn close(&self) {
        lock(&self.inner.state).close_connection(self.id, None);
    }
}

/// Exact subscription capability; cannot be constructed or moved across instances.
/// Dropping it unsubscribes. Keep it alive while receiving its updates.
pub struct Subscription<P> {
    inner: Arc<Inner<P>>,
    connection: u64,
    id: SubscriptionId,
    resource: Resource,
}
impl<P> Drop for Subscription<P> {
    fn drop(&mut self) {
        lock(&self.inner.state).remove_sub(
            self.connection,
            self.id,
            Some(CloseReason::Unsubscribed),
            &self.inner.config,
        );
    }
}
impl<P: Send + Sync + 'static> Subscription<P> {
    pub fn id(&self) -> SubscriptionId {
        self.id
    }
    pub fn resource(&self) -> &Resource {
        &self.resource
    }
    pub fn unsubscribe(&self) {
        lock(&self.inner.state).remove_sub(
            self.connection,
            self.id,
            Some(CloseReason::Unsubscribed),
            &self.inner.config,
        );
    }
    /// Reauthorize before the old lease expires. Concurrent renewals are rejected.
    /// Cancellation of an admitted renewal fails closed and removes this lease.
    pub async fn renew(&self) -> Result<(), RealtimeError> {
        let operation = {
            let mut state = lock(&self.inner.state);
            state.expire(&self.inner.config);
            state.live(self.connection)?;
            if state.pending >= self.inner.config.max_pending_operations {
                return Err(RealtimeError::Limit);
            }
            let connection = state
                .connections
                .get_mut(&self.connection)
                .expect("live connection");
            let sub = connection
                .subscriptions
                .get_mut(&self.id)
                .ok_or(RealtimeError::Closed)?;
            if sub.renewing {
                return Err(RealtimeError::Limit);
            }
            let deadline = sub.valid_until.ok_or(RealtimeError::Closed)?;
            sub.renewing = true;
            let operation = Operation::new(
                self.inner.clone(),
                self.connection,
                self.id,
                connection.identity.clone(),
                sub.resource.clone(),
                sub.cancelled.subscribe(),
                Some(deadline),
            );
            state.pending += 1;
            operation
        };
        operation.run().await
    }
}

struct Operation<P> {
    inner: Arc<Inner<P>>,
    connection: u64,
    id: SubscriptionId,
    identity: Arc<Identity<P>>,
    resource: Resource,
    cancel: watch::Receiver<bool>,
    lease_limit: Instant,
    timeout: Instant,
    renewing: bool,
    finished: bool,
}
impl<P> Operation<P> {
    #[allow(clippy::too_many_arguments)]
    fn new(
        inner: Arc<Inner<P>>,
        connection: u64,
        id: SubscriptionId,
        identity: Arc<Identity<P>>,
        resource: Resource,
        cancel: watch::Receiver<bool>,
        previous: Option<Instant>,
    ) -> Self {
        let started = Instant::now();
        let lease_limit = (started + inner.config.authorization_lease).min(identity.expires_at);
        let timeout = (started + inner.config.authorization_timeout)
            .min(lease_limit)
            .min(previous.unwrap_or(lease_limit));
        Self {
            inner,
            connection,
            id,
            identity,
            resource,
            cancel,
            lease_limit,
            timeout,
            renewing: previous.is_some(),
            finished: false,
        }
    }
}
impl<P: Send + Sync + 'static> Operation<P> {
    async fn run(mut self) -> Result<(), RealtimeError> {
        let authorize = self.inner.authorize.clone();
        let identity = self.identity.clone();
        let resource = self.resource.clone();
        // Include callback construction and polling in the unwind boundary.
        let work =
            AssertUnwindSafe(async move { authorize(identity, resource).await }).catch_unwind();
        let result = tokio::select! {
            biased;
            _ = self.cancel.changed() => Err(RealtimeError::Closed),
            _ = tokio::time::sleep_until(self.timeout) => Err(RealtimeError::Expired),
            result = work => match result {
                Ok(Ok(Some(lease))) if lease.valid_until > Instant::now() => Ok(lease.valid_until.min(self.lease_limit)),
                _ => Err(RealtimeError::Denied),
            }
        };
        let mut state = lock(&self.inner.state);
        state.expire(&self.inner.config);
        let result = result.and_then(|deadline| {
            state.live(self.connection)?;
            if deadline <= Instant::now() {
                return Err(RealtimeError::Expired);
            }
            let sub = state
                .connections
                .get_mut(&self.connection)
                .and_then(|c| c.subscriptions.get_mut(&self.id))
                .ok_or(RealtimeError::Closed)?;
            sub.valid_until = Some(deadline);
            sub.renewing = false;
            if !self.renewing
                && !state.push(
                    self.connection,
                    Envelope::new(Event::Ready {
                        subscription: self.id,
                    }),
                    &self.inner.config,
                )
            {
                return Err(RealtimeError::Closed);
            }
            Ok(())
        });
        if result.is_err() {
            state.remove_sub(
                self.connection,
                self.id,
                self.renewing.then_some(CloseReason::Revoked),
                &self.inner.config,
            );
        }
        self.finished = true;
        result
    }
}
impl<P> Drop for Operation<P> {
    fn drop(&mut self) {
        let mut state = lock(&self.inner.state);
        state.pending -= 1;
        if !self.finished {
            state.remove_sub(
                self.connection,
                self.id,
                self.renewing.then_some(CloseReason::Revoked),
                &self.inner.config,
            );
        }
    }
}

fn validate_payload(data: &Value) -> Result<(), RealtimeError> {
    fn visit(value: &Value, depth: usize, nodes: &mut usize) -> Result<(), RealtimeError> {
        *nodes += 1;
        // The provider wrapper contributes root, kind and type nodes in TS.
        if *nodes > 10_000 || depth > 32 {
            return Err(RealtimeError::Payload);
        }
        match value {
            Value::Array(values) => {
                for value in values {
                    visit(value, depth + 1, nodes)?;
                }
            }
            Value::Object(values) => {
                for value in values.values() {
                    visit(value, depth + 1, nodes)?;
                }
            }
            _ => {}
        }
        Ok(())
    }
    visit(data, 1, &mut 3)
}
