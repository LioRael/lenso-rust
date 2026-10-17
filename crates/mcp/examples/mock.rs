//! Local protocol projection only: no listener, login, credentials or model calls.
use lenso_mcp::{
    CancellationToken, McpConfig, McpPlugin, Operation, Policies, RequestContext, ToolError,
};
use lenso_plugin_core::{App, Instance};
use serde_json::{json, Value};
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let selected = Instance::new(
        "local-tools",
        McpPlugin {
            config: McpConfig::default(),
            operations: vec![Operation::new(
                "notes",
                "echo",
                "Echo validated synthetic text",
                json!({
                    "type":"object", "properties":{"text":{"type":"string","maxLength":64}},
                    "required":["text"], "additionalProperties":false
                }),
                |input: Value, _request: RequestContext<()>| async move {
                    // Reuse the host's ordinary business validator in real applications.
                    let text = input
                        .get("text")
                        .and_then(Value::as_str)
                        .ok_or(ToolError::InvalidInput)?;
                    if input.as_object().is_none_or(|o| o.len() != 1) || text.chars().count() > 64 {
                        return Err(ToolError::InvalidInput);
                    }
                    Ok(json!({"echo":text}))
                },
            )],
            // Synthetic trusted host identity. Real hosts inject current policy callbacks.
            policies: Policies::new(|_, _| async { Ok(true) }, |_, _| async { Ok(true) }),
        },
    );
    let handle = selected.handle();
    let app = App::new().with(selected).start().await?;
    let result = async {
        let adapter = app.get(&handle)?;
        let identity = Arc::new(());
        let context = || RequestContext {
            identity: identity.clone(),
            request_id: "mock-0".into(),
            cancellation: CancellationToken::new(),
        };
        let page = adapter.list_tools(context(), None).await?;
        println!("{}", json!({"jsonrpc":"2.0","id":0,"result":page}));
        let result = adapter
            .call_tool(&page.tools[0].name, json!({"text":"local mock"}), context())
            .await?;
        println!("{}", json!({"jsonrpc":"2.0","id":"","result":result}));
        adapter.close().await;
        Ok::<_, Box<dyn std::error::Error>>(())
    }
    .await;
    let shutdown = app.shutdown().await;
    result?;
    shutdown?;
    Ok(())
}
