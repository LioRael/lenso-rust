use serde::Serialize;
use serde_json::Value;
use std::{sync::Arc, time::Duration};
use tokio::time::Instant;

/// Host-defined isolation domain, resource type and exact identifier.
#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct Resource {
    scope: String,
    kind: String,
    id: String,
}
impl Resource {
    /// Construct an exact resource topic from bounded UTF-8 identifiers.
    ///
    /// # Errors
    /// Returns [`RealtimeError::InvalidInput`] for invalid resource parts.
    pub fn new(
        scope: impl Into<String>,
        kind: impl Into<String>,
        id: impl Into<String>,
    ) -> Result<Self, RealtimeError> {
        let resource = Self {
            scope: scope.into(),
            kind: kind.into(),
            id: id.into(),
        };
        part(&resource.scope)?;
        part(&resource.kind)?;
        part(&resource.id)?;
        Ok(resource)
    }
    pub fn scope(&self) -> &str {
        &self.scope
    }
    pub fn kind(&self) -> &str {
        &self.kind
    }
    pub fn id(&self) -> &str {
        &self.id
    }

    /// Same escaping as TS resourceTopic/encodeURIComponent. Never a pattern.
    pub fn topic(&self) -> String {
        fn encode(value: &str) -> String {
            let mut result = String::new();
            for byte in value.bytes() {
                if byte.is_ascii_alphanumeric() || b"-_.!~*'()".contains(&byte) {
                    result.push(char::from(byte));
                } else {
                    use std::fmt::Write;
                    write!(result, "%{byte:02X}").expect("writing a String");
                }
            }
            result
        }
        format!(
            "resource/{}/{}/{}",
            encode(&self.scope),
            encode(&self.kind),
            encode(&self.id)
        )
    }
}

pub(crate) fn part(value: &str) -> Result<(), RealtimeError> {
    if value.is_empty() || value.len() > 256 {
        Err(RealtimeError::InvalidInput)
    } else {
        Ok(())
    }
}

/// Identity already verified by the host. Principal is never sent in envelopes.
pub struct Identity<P> {
    pub scope: String,
    pub subject: String,
    pub principal: P,
    /// Monotonic deadline; convert host session expiry at the trusted boundary.
    pub expires_at: Instant,
}

/// Callback grant, clamped to the configured lease and identity deadline.
#[derive(Clone, Copy, Debug)]
pub struct Lease {
    pub valid_until: Instant,
}

/// Sanitized errors never include principal, topic, payload or callback causes.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum RealtimeError {
    #[error("Realtime invalid-input")]
    InvalidInput,
    #[error("Realtime denied")]
    Denied,
    #[error("Realtime expired")]
    Expired,
    #[error("Realtime limit")]
    Limit,
    #[error("Realtime closed")]
    Closed,
    #[error("Realtime payload")]
    Payload,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize)]
#[serde(transparent)]
pub struct SubscriptionId(pub(crate) u64);

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum CloseReason {
    Revoked,
    Deleted,
    Expired,
    Shutdown,
    Unsubscribed,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum GapReason {
    Overflow,
}

/// This is a local event model, not the complete TS cursor/SSE protocol.
#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub enum Event {
    Ready {
        subscription: SubscriptionId,
    },
    Update {
        subscription: SubscriptionId,
        #[serde(rename = "type")]
        event_type: String,
        data: Arc<Value>,
    },
    Closed {
        #[serde(skip_serializing_if = "Option::is_none")]
        subscription: Option<SubscriptionId>,
        reason: CloseReason,
    },
    Gap {
        reason: GapReason,
    },
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct Envelope {
    version: u8,
    #[serde(flatten)]
    pub event: Event,
}
impl Envelope {
    pub(crate) fn new(event: Event) -> Self {
        Self { version: 1, event }
    }
    pub(crate) fn subscription(&self) -> Option<SubscriptionId> {
        match self.event {
            Event::Ready { subscription } | Event::Update { subscription, .. } => {
                Some(subscription)
            }
            Event::Closed { subscription, .. } => subscription,
            Event::Gap { .. } => None,
        }
    }
}

/// Bounds apply per MemoryRealtime instance; all counts must be positive.
#[derive(Clone, Debug)]
pub struct RealtimeConfig {
    pub max_connections: usize,
    pub max_connections_per_subject: usize,
    pub max_subscriptions_per_connection: usize,
    pub max_subscriptions_per_subject: usize,
    pub max_subscribers_per_topic: usize,
    pub max_topics: usize,
    pub max_pending_operations: usize,
    /// Serialized provider update, including type/kind metadata, at most 32 KiB.
    pub max_payload_bytes: usize,
    pub max_buffered_events: usize,
    /// Exact serialized envelope byte count, per connection.
    pub max_buffered_bytes: usize,
    /// Starts before calling the authorizer, at most 30 seconds.
    pub authorization_lease: Duration,
    pub authorization_timeout: Duration,
}
impl Default for RealtimeConfig {
    fn default() -> Self {
        Self {
            max_connections: 1000,
            max_connections_per_subject: 8,
            max_subscriptions_per_connection: 16,
            max_subscriptions_per_subject: 64,
            max_subscribers_per_topic: 1000,
            max_topics: 1000,
            max_pending_operations: 64,
            max_payload_bytes: 16384,
            max_buffered_events: 64,
            max_buffered_bytes: 131072,
            authorization_lease: Duration::from_secs(30),
            authorization_timeout: Duration::from_secs(5),
        }
    }
}
impl RealtimeConfig {
    /// Validate capacity, buffering and authorization time limits.
    ///
    /// # Errors
    /// Returns [`RealtimeError::InvalidInput`] for zero, excessive or inconsistent limits.
    pub fn validate(&self) -> Result<(), RealtimeError> {
        let limits = [
            self.max_connections,
            self.max_connections_per_subject,
            self.max_subscriptions_per_connection,
            self.max_subscriptions_per_subject,
            self.max_subscribers_per_topic,
            self.max_topics,
            self.max_pending_operations,
            self.max_payload_bytes,
            self.max_buffered_events,
            self.max_buffered_bytes,
        ];
        if limits.into_iter().any(|n| n == 0 || n > 10_000_000)
            || self.max_payload_bytes > 32768
            || self.max_buffered_bytes < self.max_payload_bytes + 512
            || self.authorization_lease.is_zero()
            || self.authorization_lease > Duration::from_secs(30)
            || self.authorization_timeout.is_zero()
            || self.authorization_timeout > Duration::from_secs(10_000)
        {
            return Err(RealtimeError::InvalidInput);
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Stats {
    pub connections: usize,
    pub subscriptions: usize,
    pub topics: usize,
    pub pending: usize,
}

/// Enqueue counts only; this never acknowledges consumer processing or durability.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PublishReport {
    pub enqueued: usize,
    pub overflowed_connections: usize,
}
