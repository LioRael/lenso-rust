use lenso_log_plugins::{AggregateConfig, JsonLinesConfig, JsonLinesPlugin, LogAggregationPlugin};
use lenso_plugin_core::{App, Instance};
use std::{error::Error, path::PathBuf};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    let data = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("examples/data");
    let api_source = Instance::new(
        "api-source",
        JsonLinesPlugin::new(JsonLinesConfig::new(data.join("api.jsonl"))),
    );
    let jobs_source = Instance::new(
        "jobs-source",
        JsonLinesPlugin::new(JsonLinesConfig::new(data.join("jobs.jsonl"))),
    );
    let api = Instance::new(
        "api-summary",
        LogAggregationPlugin::new(api_source.handle(), AggregateConfig::default()),
    );
    let jobs = Instance::new(
        "jobs-summary",
        LogAggregationPlugin::new(
            jobs_source.handle(),
            AggregateConfig {
                error_level: "warning".into(),
            },
        ),
    );
    let api_handle = api.handle();
    let jobs_handle = jobs.handle();
    let app = App::new()
        .with(api_source)
        .with(jobs_source)
        .with(api)
        .with(jobs);
    let running = app.start().await?;
    let result = (|| {
        println!(
            "api: {}",
            serde_json::to_string(&running.get(&api_handle)?.summarize()?)?
        );
        println!(
            "jobs: {}",
            serde_json::to_string(&running.get(&jobs_handle)?.summarize()?)?
        );
        Ok::<_, Box<dyn Error + Send + Sync>>(())
    })();
    running.shutdown().await?;
    result
}
