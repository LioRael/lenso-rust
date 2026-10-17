//! Authorization over application-owned, trusted facts. This crate does not authenticate inputs.
#![doc = include_str!("../README.md")]
#![forbid(unsafe_code)]

mod access;
mod engine;
mod facts;
#[cfg(feature = "plugin")]
mod plugin;
mod rbac;

pub use access::*;
pub use engine::*;
pub use facts::*;
#[cfg(feature = "plugin")]
pub use plugin::*;
pub use rbac::*;
