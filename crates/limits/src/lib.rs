//! Fixed epoch windows and weighted leases, usable without a plugin runtime.
#![doc = include_str!("../README.md")]

mod memory;
mod plugin;
mod run;
mod service;
mod types;

pub use memory::{Clock, MemoryLimitStore, SystemClock};
pub use plugin::LimitsPlugin;
pub use run::{
    LeaseCancellation, LeaseExecution, LeaseRunError, LeaseRunFailure, LeaseRunOptions,
    LeaseStopReason,
};
pub use service::Limits;
pub use types::{
    AcquireInput, Acquisition, CloseError, ConsumeInput, CounterKind, Decision, DecisionReason,
    FailurePolicy, Lease, LeaseObservation, LeaseToken, LimitError, LimitScope, LimitStore,
    MAX_DURATION_MS, MAX_QUANTITY, MAX_TIMESTAMP_MS,
};
