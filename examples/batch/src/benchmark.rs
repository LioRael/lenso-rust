//! Bounded native-call harness; no Runtime connection or JSON per operation.
use crate::{native_app, BatchService};
use lenso_cache::CacheConfig;
use lenso_log_plugins::LogRecord;
use lenso_plugin_core::BoxError;
use serde::Deserialize;
use std::{
    io::Read,
    sync::Arc,
    time::{Duration, Instant},
};

pub async fn native_call() -> Result<(), BoxError> {
    let mut bytes = Vec::new();
    std::io::stdin().take(262_145).read_to_end(&mut bytes)?;
    if bytes.len() > 262_144 {
        return Err("native batch input exceeds 262144 bytes".into());
    }
    let input: crate::AggregateInput = serde_json::from_slice(&bytes)?;
    let records = input
        .logs
        .into_iter()
        .map(|log| LogRecord {
            level: log.level,
            message: log.message,
            duration_ms: log.duration_ms,
        })
        .collect();
    let (app, handle) = native_app(CacheConfig {
        namespace: "batch:aggregate".into(),
        ..CacheConfig::default()
    });
    let running = app.start().await?;
    let result = running.get(&handle)?.aggregate(records).await;
    let shutdown = running.shutdown().await;
    let output = result?;
    shutdown?;
    println!("{}", serde_json::to_string(&output)?);
    Ok(())
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Input {
    logs: Vec<LogRecord>,
    requests: usize,
    concurrency: usize,
    warmup: usize,
}

async fn call(
    service: &BatchService,
    records: &[LogRecord],
) -> Result<(f64, serde_json::Value), BoxError> {
    let records = records.to_vec();
    let started = Instant::now();
    let result = service.aggregate(records).await?;
    let elapsed = started.elapsed().as_secs_f64() * 1000.0;
    Ok((elapsed, serde_json::to_value(result)?))
}

pub async fn run() -> Result<(), BoxError> {
    let mut bytes = Vec::new();
    std::io::stdin().take(262_145).read_to_end(&mut bytes)?;
    if bytes.len() > 262_144 {
        return Err("native benchmark input exceeds 262144 bytes".into());
    }
    let input: Input = serde_json::from_slice(&bytes)?;
    if input.logs.is_empty()
        || input.logs.len() > 1000
        || input.requests == 0
        || input.requests > 100_000
        || !(1..=128).contains(&input.concurrency)
        || input.warmup > 10_000
    {
        return Err("invalid bounded native benchmark input".into());
    }
    let (app, handle) = native_app(CacheConfig {
        default_ttl: Duration::ZERO,
        ..CacheConfig::default()
    });
    let running = app.start().await?;
    let result = async {
        let service = running.get(&handle)?;
        let (cold_ms, output) = call(&service, &input.logs).await?;
        for _ in 0..input.warmup {
            call(&service, &input.logs).await?;
        }
        let records = Arc::new(input.logs);
        let started = Instant::now();
        let mut workers = tokio::task::JoinSet::new();
        for worker in 0..input.concurrency.min(input.requests) {
            let service = service.clone();
            let records = records.clone();
            let expected = output.clone();
            workers.spawn(async move {
                let mut samples = Vec::new();
                let mut errors = 0;
                for _ in (worker..input.requests).step_by(input.concurrency) {
                    match call(&service, &records).await {
                        Ok((elapsed, result)) if result == expected => samples.push(elapsed),
                        _ => errors += 1,
                    }
                }
                (samples, errors)
            });
        }
        let mut samples = Vec::with_capacity(input.requests);
        let mut errors = 0;
        while let Some(result) = workers.join_next().await {
            let (worker_samples, worker_errors) = result?;
            samples.extend(worker_samples);
            errors += worker_errors;
        }
        let total_ms = started.elapsed().as_secs_f64() * 1000.0;
        Ok::<_, BoxError>(serde_json::json!({
            "coldMs":cold_ms, "samplesMs":samples, "totalMs":total_ms,
            "errors":errors, "output":output
        }))
    }
    .await;
    let shutdown = running.shutdown().await;
    let output = result?;
    shutdown?;
    println!("{}", serde_json::to_string(&output)?);
    Ok(())
}
