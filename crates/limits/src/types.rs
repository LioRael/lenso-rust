use std::{
    error::Error,
    fmt,
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};

pub const MAX_QUANTITY: u32 = i32::MAX as u32;
pub const MAX_DURATION_MS: u64 = 31_622_400_000;
/// Matches the TypeScript safe clock range, leaving room for window/TTL arithmetic.
pub const MAX_TIMESTAMP_MS: u64 = 9_007_199_254_740_991 - MAX_DURATION_MS * 2;

/// Trusted application labels, not an identity proof or authorization grant.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct LimitScope {
    instance: String,
    tenant: String,
    key: String,
}

impl LimitScope {
    pub fn new(
        instance: impl Into<String>,
        tenant: impl Into<String>,
        key: impl Into<String>,
    ) -> Result<Self, LimitError> {
        let scope = Self {
            instance: instance.into(),
            tenant: tenant.into(),
            key: key.into(),
        };
        for part in [&scope.instance, &scope.tenant, &scope.key] {
            // JS string lengths count UTF-16 units, not UTF-8 bytes.
            if part.is_empty() || part.encode_utf16().count() > 512 {
                return Err(LimitError::InvalidInput);
            }
        }
        Ok(scope)
    }

    pub fn instance(&self) -> &str {
        &self.instance
    }
    pub fn tenant(&self) -> &str {
        &self.tenant
    }
    pub fn key(&self) -> &str {
        &self.key
    }
}

impl fmt::Debug for LimitScope {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("LimitScope([redacted])")
    }
}

#[derive(Clone, Debug)]
pub struct ConsumeInput {
    pub scope: LimitScope,
    pub capacity: u32,
    pub quantity: u32,
    pub period: Duration,
}
impl ConsumeInput {
    pub fn validate(&self) -> Result<(), LimitError> {
        quantity(self.capacity)?;
        quantity(self.quantity)?;
        duration_ms(self.period)?;
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub struct AcquireInput {
    pub scope: LimitScope,
    pub capacity: u32,
    pub quantity: u32,
    pub ttl: Duration,
}
impl AcquireInput {
    pub fn validate(&self) -> Result<(), LimitError> {
        quantity(self.capacity)?;
        quantity(self.quantity)?;
        duration_ms(self.ttl)?;
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum CounterKind {
    Rate,
    Quota,
}

/// Required explicitly; applies only to admission backend failures.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FailurePolicy {
    /// Return `LimitError::BackendFailure` (TS `throw`).
    Error,
    Deny,
    /// Allow visibly degraded admission, without inventing a lease.
    Allow,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DecisionReason {
    Allowed,
    Exhausted,
    TooLarge,
    BackendFailure,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Decision {
    pub allowed: bool,
    /// Unknown on backend failure.
    pub remaining: Option<u32>,
    /// Zero on admission; `None` means no finite estimate or an unknown outcome.
    pub retry_after: Option<Duration>,
    pub reason: DecisionReason,
}

/// Opaque, process-unique identifier. Not a secret, credential, or distributed token.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct LeaseToken(u64);
impl LeaseToken {
    /// Store implementers/callers must issue a fresh token for each acquisition.
    pub fn fresh() -> Result<Self, LimitError> {
        static NEXT: AtomicU64 = AtomicU64::new(1);
        let mut current = NEXT.load(Ordering::Relaxed);
        loop {
            let next = current.checked_add(1).ok_or(LimitError::Internal)?;
            match NEXT.compare_exchange_weak(current, next, Ordering::Relaxed, Ordering::Relaxed) {
                Ok(_) => return Ok(Self(current)),
                Err(updated) => current = updated,
            }
        }
    }
}
impl fmt::Debug for LeaseToken {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("LeaseToken([redacted])")
    }
}

/// An owned snapshot; changing it cannot rewrite store state.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Lease {
    pub scope: LimitScope,
    pub token: LeaseToken,
    pub quantity: u32,
    /// Unix epoch milliseconds, in the store's clock domain.
    pub expires_at_ms: u64,
}

/// A point-in-time provider observation, never a fencing or future-validity guarantee.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LeaseObservation {
    pub lease: Lease,
    /// Remaining time according to the same clock used for acquire/renew.
    pub remaining: Duration,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Acquisition {
    pub decision: Decision,
    /// Always absent on fail-open admission.
    pub lease: Option<Lease>,
}

/// Sanitized errors: no scope, token, or raw provider message is formatted.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LimitError {
    InvalidInput,
    PolicyConflict,
    BackendFailure,
    Closed,
    /// Synchronous close requested cancellation; use `close_async` to drain executions.
    ActiveRuns,
    /// Synchronization or token generation failed. Never converted to fail-open.
    Internal,
}
impl fmt::Display for LimitError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let code = match self {
            Self::InvalidInput => "invalid-input",
            Self::PolicyConflict => "policy-conflict",
            Self::BackendFailure => "backend-failure",
            Self::Closed => "closed",
            Self::ActiveRuns => "active-runs",
            Self::Internal => "internal",
        };
        write!(f, "Limit operation failed: {code}")
    }
}
impl Error for LimitError {}

/// All failed releases from one close attempt, without identifying their scopes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CloseError {
    pub(crate) failures: Vec<LimitError>,
}
impl CloseError {
    pub fn failures(&self) -> &[LimitError] {
        &self.failures
    }
}
impl fmt::Display for CloseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Limit cleanup failed ({} errors)", self.failures.len())
    }
}
impl Error for CloseError {}

/// Synchronous, bounded, non-reentrant storage operations, atomic for each scope.
///
/// This boundary is for process-local stores. Implementations must validate inputs,
/// preserve policy conflicts, and use `BackendFailure` only for unavailable storage.
/// Never perform blocking network/DB I/O or call back into the owning `Limits`.
/// The service supplies fresh tokens; providers must not reuse live tokens.
pub trait LimitStore: Send + Sync + 'static {
    fn consume(&self, kind: CounterKind, input: &ConsumeInput) -> Result<Decision, LimitError>;
    fn acquire(&self, input: &AcquireInput, token: LeaseToken) -> Result<Acquisition, LimitError>;
    fn renew(
        &self,
        scope: &LimitScope,
        token: LeaseToken,
        ttl: Duration,
    ) -> Result<Option<Lease>, LimitError>;
    fn release(&self, scope: &LimitScope, token: LeaseToken) -> Result<(), LimitError>;

    /// Read current lease liveness without renewing it. Required by `with_lease`.
    /// The default fails safely; existing manual-only stores need not implement it.
    fn observe(
        &self,
        _scope: &LimitScope,
        _token: LeaseToken,
    ) -> Result<Option<LeaseObservation>, LimitError> {
        Err(LimitError::BackendFailure)
    }
}

pub(crate) fn quantity(value: u32) -> Result<(), LimitError> {
    if value == 0 || value > MAX_QUANTITY {
        Err(LimitError::InvalidInput)
    } else {
        Ok(())
    }
}

pub(crate) fn duration_ms(duration: Duration) -> Result<u64, LimitError> {
    if duration.is_zero()
        || duration > Duration::from_millis(MAX_DURATION_MS)
        || duration.as_nanos() != duration.as_millis() * 1_000_000
    {
        Err(LimitError::InvalidInput)
    } else {
        Ok(duration.as_millis() as u64)
    }
}
