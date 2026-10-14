//! One native business entry, shared by plain Rust and the explicit Runtime export.
use lenso_cache::MemoryCache;
use lenso_log::Logger;
use lenso_log_plugins::{AggregateError, LogAggregator, LogRecord, MemoryLogSource, Summary};
use lenso_plugin_core::{BoxError, Handle, InstanceKey, Plugin, Setup, WorkGate};
use sha2::{Digest, Sha256};
use std::{future::Future, sync::Arc};

pub struct BatchPlugin {
    pub aggregation: Handle<LogAggregator<MemoryLogSource>>,
    pub cache: Handle<MemoryCache<Summary>>,
    pub logger: Handle<Logger>,
}

pub struct BatchService {
    aggregation: Arc<LogAggregator<MemoryLogSource>>,
    cache: Arc<MemoryCache<Summary>>,
    logger: Arc<Logger>,
    gate: WorkGate,
}

#[derive(serde::Serialize)]
struct BatchInput<'a> {
    logs: &'a [LogRecord],
}

impl Plugin for BatchPlugin {
    type Service = BatchService;

    fn dependencies(&self) -> Vec<InstanceKey> {
        vec![self.aggregation.key(), self.cache.key(), self.logger.key()]
    }

    async fn setup(self, setup: &mut Setup) -> Result<Arc<BatchService>, BoxError> {
        Ok(Arc::new(BatchService {
            aggregation: setup.get(&self.aggregation)?,
            cache: setup.get(&self.cache)?,
            logger: setup.get(&self.logger)?,
            gate: setup.work_gate(),
        }))
    }
}

impl BatchService {
    pub async fn aggregate(&self, records: Vec<LogRecord>) -> Result<Summary, BoxError> {
        self.aggregate_with_cancel(records, std::future::pending())
            .await
    }

    pub async fn aggregate_with_cancel(
        &self,
        records: Vec<LogRecord>,
        cancelled: impl Future<Output = ()> + Send,
    ) -> Result<Summary, BoxError> {
        let work = Arc::new(self.gate.enter()?);
        if records.is_empty() {
            return Err(Box::new(AggregateError {
                reason: "At least one log is required".into(),
            }));
        }
        let key = format!(
            "{:x}",
            Sha256::digest(serde_json::to_vec(&BatchInput { logs: &records })?)
        );
        let aggregation = self.aggregation.clone();
        let load_work = work.clone();
        let load = self.cache.get_or_set(
            &key,
            move || async move {
                // A detached shared loader owns accepted work until it actually settles.
                let _work = load_work;
                aggregation.aggregate_records(&records)
            },
            None,
        );
        tokio::pin!(cancelled);
        let output = tokio::select! {
            biased;
            _ = &mut cancelled => return Err(Box::new(AggregateError {
                reason: "Aggregation cancelled".into(),
            })),
            result = load => result?,
        };
        // Diagnostic failure must not change a completed business outcome.
        let _ = self.logger.debug(
            serde_json::json!({"event":"batch_completed","count":output.count}),
            "Batch completed",
        );
        Ok(output)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use lenso_cache::CacheConfig;

    #[tokio::test]
    async fn native_batch_uses_local_cache_and_retained_entry_rejects_shutdown() {
        let (app, handle) = crate::native_app(CacheConfig::default());
        let running = app.start().await.unwrap();
        let service = running.get(&handle).unwrap();
        let records = vec![LogRecord {
            level: "error".into(),
            message: "synthetic".into(),
            duration_ms: 12,
        }];
        let first = service.aggregate(records.clone()).await.unwrap();
        let key = format!(
            "{:x}",
            Sha256::digest(serde_json::to_vec(&BatchInput { logs: &records }).unwrap())
        );
        assert_eq!(service.cache.get(&key).unwrap(), Some(first.clone()));
        assert_eq!(service.aggregate(records.clone()).await.unwrap(), first);
        running.shutdown().await.unwrap();
        let error = service.aggregate(records).await.unwrap_err();
        assert!(error
            .downcast_ref::<lenso_plugin_core::AccessError>()
            .is_some());
    }

    #[tokio::test]
    async fn native_cancelled_waiter_does_not_populate_cache() {
        let (app, handle) = crate::native_app(CacheConfig::default());
        let running = app.start().await.unwrap();
        let service = running.get(&handle).unwrap();
        let records = vec![LogRecord {
            level: "info".into(),
            message: "synthetic".into(),
            duration_ms: 1,
        }];
        let error = service
            .aggregate_with_cancel(records, async {})
            .await
            .unwrap_err();
        assert_eq!(
            error.downcast_ref::<AggregateError>().unwrap().reason,
            "Aggregation cancelled"
        );
        running.shutdown().await.unwrap();
    }
}
