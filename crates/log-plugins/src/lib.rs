//! Native log sources and aggregation, usable without a transport host.

mod aggregate;
mod plugins;
mod source;

pub use aggregate::{
    aggregate, aggregate_with_cancellation, AggregateConfig, AggregateError, LogRecord, Summary,
};
pub use plugins::{
    JsonLinesPlugin, JsonLinesSource, LogAggregationError, LogAggregationPlugin, LogAggregator,
};
pub use source::{JsonLinesConfig, JsonLinesError, LogSource, MemoryLogSource};
