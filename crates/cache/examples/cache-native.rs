//! A native business service caches summaries of a real local batch file.
use lenso_cache::{CacheConfig, CachePlugin, MemoryCache};
use lenso_plugin_core::{App, BoxError, Handle, InstanceKey, Plugin, Setup, WorkGate};
use serde::{Deserialize, Serialize};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct Summary {
    count: usize,
    total: i64,
}

struct BatchService {
    cache: MemoryCache<Summary>,
    gate: WorkGate,
    reads: Arc<AtomicUsize>,
}
impl BatchService {
    async fn summary(&self) -> Result<Summary, BoxError> {
        let work = Arc::new(self.gate.enter()?);
        let loader_work = work.clone();
        let reads = self.reads.clone();
        Ok(self
            .cache
            .get_or_set(
                "batch-1",
                move || async move {
                    // Shared work retains admission even when its initiating caller leaves.
                    let _work = loader_work;
                    reads.fetch_add(1, Ordering::SeqCst);
                    let text = tokio::fs::read_to_string(concat!(
                        env!("CARGO_MANIFEST_DIR"),
                        "/examples/fixtures/batch.txt"
                    ))
                    .await?;
                    let values = text
                        .lines()
                        .map(str::parse::<i64>)
                        .collect::<Result<Vec<_>, _>>()
                        .map_err(std::io::Error::other)?;
                    Ok::<_, std::io::Error>(Summary {
                        count: values.len(),
                        total: values.iter().sum(),
                    })
                },
                None,
            )
            .await?)
    }
    fn invalidate(&self) -> Result<(), BoxError> {
        let _work = self.gate.enter()?;
        Ok(self.cache.invalidate()?)
    }
}
struct BatchPlugin {
    cache: Handle<MemoryCache<Summary>>,
}
impl Plugin for BatchPlugin {
    type Service = BatchService;
    fn dependencies(&self) -> Vec<InstanceKey> {
        vec![self.cache.key()]
    }
    async fn setup(self, setup: &mut Setup) -> Result<Arc<BatchService>, BoxError> {
        Ok(Arc::new(BatchService {
            cache: setup.get(&self.cache)?.scope("batches")?,
            gate: setup.work_gate(),
            reads: Arc::new(AtomicUsize::new(0)),
        }))
    }
}
async fn demonstrate() -> Result<(), BoxError> {
    let mut app = App::new();
    let cache_handle = app.install(
        "summaries",
        CachePlugin::<Summary>::new(CacheConfig::default()),
    );
    let service_handle = app.install(
        "batch-summary",
        BatchPlugin {
            cache: cache_handle.clone(),
        },
    );
    let running = app.start().await?;
    let result = async {
        let service = running.get(&service_handle)?;
        let cache = running.get(&cache_handle)?;
        let (first, second) = tokio::join!(service.summary(), service.summary());
        assert_eq!(first?, Summary { count: 3, total: 16 });
        assert_eq!(second?, Summary { count: 3, total: 16 });
        assert_eq!(service.summary().await?.total, 16);
        assert_eq!(service.reads.load(Ordering::SeqCst), 1);
        service.invalidate()?;
        assert_eq!(service.summary().await?.total, 16);
        assert_eq!(service.reads.load(Ordering::SeqCst), 2);
        println!("4 business requests, 2 actual file reads; cold/coalesced + warm + invalidated; total=16");
        Ok::<_, BoxError>((service, cache))
    }.await;
    let cleanup = running.shutdown().await;
    // Preserve both failures rather than allowing shutdown to hide business errors.
    match (result, cleanup) {
        (Ok((service, cache)), Ok(())) => {
            assert!(service.summary().await.is_err());
            assert!(cache.get("batch-1").is_err());
            Ok(())
        }
        (business, cleanup) => Err(std::io::Error::other(format!(
            "business={:?}; cleanup={:?}",
            business.err(),
            cleanup.err()
        ))
        .into()),
    }
}
#[tokio::main]
async fn main() -> Result<(), BoxError> {
    demonstrate().await
}
#[tokio::test]
async fn native_business_reuses_io_and_core_closes_cache() {
    demonstrate().await.unwrap();
}
