use lenso_cache::{CacheConfig, MemoryCache};
use serde::{Deserialize, Serialize};
use std::{io, time::Duration};

#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct Summary {
    count: usize,
    total: i64,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cache = MemoryCache::<Summary>::new(CacheConfig {
        namespace: "summaries".into(),
        default_ttl: Duration::from_secs(30),
        max_entries: 100,
        max_in_flight: 4,
        ..CacheConfig::default()
    })?;
    let batches = cache.scope("batches")?;
    let summary = batches
        .get_or_set(
            "batch-1",
            || async {
                let values = [3, 5, 8];
                Ok::<_, io::Error>(Summary {
                    count: values.len(),
                    total: values.iter().sum(),
                })
            },
            None,
        )
        .await?;
    println!("{summary:?}");
    assert_eq!(batches.get("batch-1")?, Some(summary));
    batches.invalidate()?;
    assert_eq!(batches.get("batch-1")?, None);
    cache.close().await?;
    Ok(())
}
