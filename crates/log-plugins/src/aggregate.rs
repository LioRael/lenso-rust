use serde::{Deserialize, Serialize};
use std::{error::Error, fmt};

/// A structured log record, independent of transport contracts.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogRecord {
    pub level: String,
    pub message: String,
    pub duration_ms: i64,
}

/// Counts and total duration for a nonempty log batch.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Summary {
    pub count: i64,
    pub error_count: i64,
    pub total_duration_ms: i64,
}

/// A business failure suitable for mapping to a transport error.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AggregateError {
    pub reason: String,
}

impl fmt::Display for AggregateError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.reason)
    }
}

impl Error for AggregateError {}

/// Per-aggregator classification; matching is case-sensitive.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AggregateConfig {
    pub error_level: String,
}

impl Default for AggregateConfig {
    fn default() -> Self {
        Self {
            error_level: "error".into(),
        }
    }
}

/// Aggregates with the batch contract's default classification.
///
/// # Errors
/// Returns [`AggregateError`] for an empty batch or arithmetic overflow.
pub fn aggregate(logs: &[LogRecord]) -> Result<Summary, AggregateError> {
    aggregate_with_cancellation(logs, || false)
}

/// Checks cancellation before each record, after rejecting an empty batch.
///
/// # Errors
/// Returns [`AggregateError`] for an empty batch, cancellation, or arithmetic overflow.
pub fn aggregate_with_cancellation(
    logs: &[LogRecord],
    cancelled: impl FnMut() -> bool,
) -> Result<Summary, AggregateError> {
    aggregate_configured(logs, &AggregateConfig::default(), cancelled)
}

pub(crate) fn aggregate_configured(
    logs: &[LogRecord],
    config: &AggregateConfig,
    mut cancelled: impl FnMut() -> bool,
) -> Result<Summary, AggregateError> {
    if logs.is_empty() {
        return Err(AggregateError {
            reason: "At least one log is required".into(),
        });
    }
    let mut summary = Summary::default();
    for log in logs {
        if cancelled() {
            return Err(AggregateError {
                reason: "Aggregation cancelled".into(),
            });
        }
        summary.count = checked_add(summary.count, 1)?;
        summary.error_count = checked_add(
            summary.error_count,
            i64::from(log.level == config.error_level),
        )?;
        summary.total_duration_ms = checked_add(summary.total_duration_ms, log.duration_ms)?;
    }
    Ok(summary)
}

fn checked_add(left: i64, right: i64) -> Result<i64, AggregateError> {
    left.checked_add(right).ok_or_else(|| AggregateError {
        reason: "Aggregation overflow".into(),
    })
}
