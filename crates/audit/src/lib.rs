//! Scoped, append-only audit with caller-supplied authority and storage.
//!
//! The service is usable without the plugin feature. See the crate README for
//! the durability contract, host admission requirements, and parity boundaries.
#![doc = include_str!("../README.md")]

mod contracts;
mod memory;
#[cfg(feature = "plugin")]
mod plugin;
mod service;
mod validation;

pub use contracts::*;
pub use memory::MemoryAuditRepository;
#[cfg(feature = "plugin")]
pub use plugin::AuditPlugin;
pub use service::{AuditIntentReceipt, AuditService};
