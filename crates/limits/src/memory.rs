use crate::{
    types::duration_ms, AcquireInput, Acquisition, ConsumeInput, CounterKind, Decision,
    DecisionReason, Lease, LeaseObservation, LeaseToken, LimitError, LimitScope, LimitStore,
    MAX_TIMESTAMP_MS,
};
use std::{
    collections::{hash_map::Entry, HashMap},
    sync::{Arc, Mutex},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

/// Millisecond epoch clock. Called inside the store lock; must be quick and non-reentrant.
pub trait Clock: Send + Sync + 'static {
    /// # Errors
    /// Returns a clock/provider failure when a trustworthy timestamp is unavailable.
    fn now_ms(&self) -> Result<u64, LimitError>;
}
impl<F: Fn() -> u64 + Send + Sync + 'static> Clock for F {
    fn now_ms(&self) -> Result<u64, LimitError> {
        Ok(self())
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct SystemClock;
impl Clock for SystemClock {
    fn now_ms(&self) -> Result<u64, LimitError> {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| LimitError::InvalidInput)?
            .as_millis()
            .try_into()
            .map_err(|_| LimitError::InvalidInput)
    }
}

struct CounterState {
    capacity: u32,
    period_ms: u64,
    window_start: u64,
    used: u32,
    last_now: u64,
}
struct LeaseState {
    capacity: u32,
    last_now: u64,
    leases: HashMap<LeaseToken, Lease>,
}
#[derive(Default)]
struct State {
    counters: HashMap<(CounterKind, LimitScope), CounterState>,
    concurrency: HashMap<LimitScope, LeaseState>,
}

/// State belongs to this object, shared only through its explicit `Arc`.
///
/// Policies remain registered even after windows/leases expire. Callers must bound
/// scope cardinality; this backend does not evict policy or clock history.
pub struct MemoryLimitStore {
    clock: Arc<dyn Clock>,
    state: Mutex<State>,
}
impl Default for MemoryLimitStore {
    fn default() -> Self {
        Self::new()
    }
}
impl MemoryLimitStore {
    pub fn new() -> Self {
        Self::with_clock(SystemClock)
    }
    pub fn with_clock(clock: impl Clock) -> Self {
        Self {
            clock: Arc::new(clock),
            state: Mutex::new(State::default()),
        }
    }
    fn now(&self, previous: u64) -> Result<u64, LimitError> {
        let now = self.clock.now_ms()?;
        if now > MAX_TIMESTAMP_MS {
            return Err(LimitError::InvalidInput);
        }
        Ok(now.max(previous))
    }
}

impl LimitStore for MemoryLimitStore {
    fn consume(&self, kind: CounterKind, input: &ConsumeInput) -> Result<Decision, LimitError> {
        input.validate()?;
        let period_ms = duration_ms(input.period)?;
        let mut state = self.state.lock().map_err(|_| LimitError::Internal)?;
        let entry = state.counters.entry((kind, input.scope.clone()));
        let previous = match &entry {
            Entry::Occupied(entry) => {
                let previous = entry.get();
                if previous.capacity != input.capacity || previous.period_ms != period_ms {
                    return Err(LimitError::PolicyConflict);
                }
                previous.last_now
            }
            Entry::Vacant(_) => 0,
        };
        let now = self.now(previous)?;
        let window_start = now / period_ms * period_ms;
        let counter = entry.or_insert(CounterState {
            capacity: input.capacity,
            period_ms,
            window_start,
            used: 0,
            last_now: now,
        });
        if counter.window_start != window_start {
            counter.used = 0;
            counter.window_start = window_start;
        }
        counter.last_now = now;
        let allowed = input.quantity <= counter.capacity - counter.used;
        if allowed {
            counter.used += input.quantity;
        }
        let too_large = input.quantity > counter.capacity;
        Ok(Decision {
            allowed,
            remaining: Some(counter.capacity - counter.used),
            retry_after: if allowed {
                Some(Duration::ZERO)
            } else if too_large {
                None
            } else {
                Some(Duration::from_millis(window_start + period_ms - now))
            },
            reason: reason(allowed, too_large),
        })
    }

    fn acquire(&self, input: &AcquireInput, token: LeaseToken) -> Result<Acquisition, LimitError> {
        input.validate()?;
        let ttl_ms = duration_ms(input.ttl)?;
        let mut state = self.state.lock().map_err(|_| LimitError::Internal)?;
        let entry = state.concurrency.entry(input.scope.clone());
        let previous = match &entry {
            Entry::Occupied(entry) => {
                if entry.get().capacity != input.capacity {
                    return Err(LimitError::PolicyConflict);
                }
                entry.get().last_now
            }
            Entry::Vacant(_) => 0,
        };
        let now = self.now(previous)?;
        // Validate before pruning/mutating, so errors do not commit partial state.
        if let Entry::Occupied(entry) = &entry {
            if entry
                .get()
                .leases
                .get(&token)
                .is_some_and(|lease| lease.expires_at_ms > now)
            {
                return Err(LimitError::InvalidInput);
            }
        }
        let leases = entry.or_insert(LeaseState {
            capacity: input.capacity,
            last_now: now,
            leases: HashMap::new(),
        });
        leases.last_now = now;
        leases.leases.retain(|_, lease| lease.expires_at_ms > now);
        let used: u32 = leases.leases.values().map(|lease| lease.quantity).sum();
        let allowed = input.quantity <= leases.capacity - used;
        let lease = allowed.then(|| Lease {
            scope: input.scope.clone(),
            token,
            quantity: input.quantity,
            expires_at_ms: now + ttl_ms,
        });
        if let Some(lease) = &lease {
            leases.leases.insert(token, lease.clone());
        }
        let remaining = leases.capacity - used - if allowed { input.quantity } else { 0 };
        let too_large = input.quantity > leases.capacity;
        let retry_after = if allowed {
            Some(Duration::ZERO)
        } else if too_large {
            None
        } else {
            let mut available = remaining;
            let mut expirations: Vec<_> = leases.leases.values().collect();
            expirations.sort_unstable_by_key(|lease| lease.expires_at_ms);
            expirations.into_iter().find_map(|lease| {
                available += lease.quantity;
                (available >= input.quantity)
                    .then(|| Duration::from_millis(lease.expires_at_ms - now))
            })
        };
        Ok(Acquisition {
            decision: Decision {
                allowed,
                remaining: Some(remaining),
                retry_after,
                reason: reason(allowed, too_large),
            },
            lease,
        })
    }

    fn renew(
        &self,
        scope: &LimitScope,
        token: LeaseToken,
        ttl: Duration,
    ) -> Result<Option<Lease>, LimitError> {
        let ttl_ms = duration_ms(ttl)?;
        let mut state = self.state.lock().map_err(|_| LimitError::Internal)?;
        let Some(leases) = state.concurrency.get_mut(scope) else {
            return Ok(None);
        };
        let now = self.now(leases.last_now)?;
        leases.last_now = now;
        leases.leases.retain(|_, lease| lease.expires_at_ms > now);
        Ok(leases.leases.get_mut(&token).map(|lease| {
            lease.expires_at_ms = lease.expires_at_ms.max(now + ttl_ms);
            lease.clone()
        }))
    }

    fn release(&self, scope: &LimitScope, token: LeaseToken) -> Result<(), LimitError> {
        let mut state = self.state.lock().map_err(|_| LimitError::Internal)?;
        let Some(leases) = state.concurrency.get_mut(scope) else {
            return Ok(());
        };
        let now = self.now(leases.last_now)?;
        leases.last_now = now;
        leases
            .leases
            .retain(|key, lease| *key != token && lease.expires_at_ms > now);
        Ok(())
    }

    fn observe(
        &self,
        scope: &LimitScope,
        token: LeaseToken,
    ) -> Result<Option<LeaseObservation>, LimitError> {
        let mut state = self.state.lock().map_err(|_| LimitError::Internal)?;
        let Some(leases) = state.concurrency.get_mut(scope) else {
            return Ok(None);
        };
        let now = self.now(leases.last_now)?;
        leases.last_now = now;
        leases.leases.retain(|_, lease| lease.expires_at_ms > now);
        Ok(leases.leases.get(&token).map(|lease| LeaseObservation {
            lease: lease.clone(),
            remaining: Duration::from_millis(lease.expires_at_ms - now),
        }))
    }
}

fn reason(allowed: bool, too_large: bool) -> DecisionReason {
    if allowed {
        DecisionReason::Allowed
    } else if too_large {
        DecisionReason::TooLarge
    } else {
        DecisionReason::Exhausted
    }
}
