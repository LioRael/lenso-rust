use lenso_plugin_core::{App, BoxError, Handle, Instance, InstanceKey, Plugin, Setup};
use lenso_plugin_runtime::RuntimePlugin;
use runtime_sdk::{CallOptions, HostOptions, RuntimeHost};
use std::sync::Arc;
pub mod logs {
    include!("../../../generated/logs.rs");
}
use logs::{AggregateInput, AggregateInputLogsItem, LogsClient};

// The generated client is an ordinary dependency, not a global lookup.
struct Caller {
    runtime: Handle<RuntimeHost>,
    target: String,
}
impl Plugin for Caller {
    type Service = LogsClient;

    fn dependencies(&self) -> Vec<InstanceKey> {
        vec![self.runtime.key()]
    }

    async fn setup(self, setup: &mut Setup) -> Result<Arc<LogsClient>, BoxError> {
        let host = setup.get(&self.runtime)?;
        Ok(Arc::new(LogsClient::new(
            (*host).clone(),
            "demo",
            self.target,
        )))
    }
}

#[tokio::main]
async fn main() -> Result<(), BoxError> {
    let port = std::env::var("LENSO_RUNTIME_PORT")
        .unwrap_or_else(|_| "7331".into())
        .parse()?;
    let host = Instance::new(
        "runtime",
        RuntimePlugin::new(
            HostOptions {
                namespace: "demo".into(),
                service: "rust-client".into(),
                instance: "one".into(),
                port,
                contract: serde_json::json!({"version":"1","methods":{}}),
                max_concurrency: 1,
            },
            5000,
        ),
    );
    let service = std::env::var("LENSO_RUNTIME_SERVICE").unwrap_or_else(|_| "batch".into());
    let caller = Instance::new(
        "caller",
        Caller {
            runtime: host.handle(),
            target: service,
        },
    );
    let caller_handle = caller.handle();
    let app = App::new().with(host).with(caller);
    let running = app.start().await?;
    let client = running.get(&caller_handle)?;
    let result = client
        .aggregate(
            AggregateInput {
                logs: vec![
                    AggregateInputLogsItem {
                        level: "info".into(),
                        message: "synthetic startup".into(),
                        duration_ms: 3,
                    },
                    AggregateInputLogsItem {
                        level: "error".into(),
                        message: "synthetic failure".into(),
                        duration_ms: 7,
                    },
                ],
            },
            CallOptions::default(),
        )
        .await;
    let drained = running.shutdown().await;
    println!("{}", serde_json::to_string(&result?)?);
    drained?;
    Ok(())
}
