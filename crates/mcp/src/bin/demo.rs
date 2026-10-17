//! Dedicated synthetic stdio server, ready for any MCP stdio client.
use lenso_mcp::{
    stdio::McpServer, CancellationToken, McpConfig, McpPlugin, Operation, Policies, RequestContext,
    ToolError,
};
use lenso_plugin_core::{App, Instance};
use serde_json::{json, Value};
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let tools = Instance::new(
        "mcp",
        McpPlugin {
            config: McpConfig::default(),
            operations: vec![
                Operation::new(
                    "notes",
                    "echo",
                    "Echo synthetic text",
                    json!({"type":"object","properties":{"text":{"type":"string"}},"required":["text"]}),
                    |input: Value, _: RequestContext<()>| async move {
                        let text = input
                            .get("text")
                            .and_then(Value::as_str)
                            .ok_or(ToolError::InvalidInput)?;
                        Ok(json!({"echo":text}))
                    },
                ),
                Operation::new(
                    "notes",
                    "wait",
                    "Wait cooperatively for cancellation",
                    json!({"type":"object"}),
                    |_, context: RequestContext<()>| async move {
                        context.cancellation.cancelled().await;
                        Err(ToolError::Cancelled)
                    },
                ),
            ],
            policies: Policies::new(|_, _| async { Ok(true) }, |_, _| async { Ok(true) }),
        },
    );
    let handle = tools.handle();
    let app = App::new().with(tools).start().await?;
    let adapter = app.get(&handle)?;
    let shutdown = CancellationToken::new();
    let signal = shutdown.clone();
    let signals = tokio::spawn(async move {
        #[cfg(unix)]
        if let Ok(mut terminate) =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        {
            tokio::select! { _ = tokio::signal::ctrl_c() => {}, _ = terminate.recv() => {} }
            signal.cancel();
            return;
        }
        let _ = tokio::signal::ctrl_c().await;
        signal.cancel();
    });
    // Only SDK frames go to stdout. Real host logging must use stderr as well.
    let result = McpServer::new((*adapter).clone(), Arc::new(()))
        .run_stdio(shutdown)
        .await;
    signals.abort();
    let cleanup = app.shutdown().await;
    result?;
    cleanup?;
    Ok(())
}
