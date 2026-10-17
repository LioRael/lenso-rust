#![cfg(feature = "stdio")]
use lenso_mcp::{stdio::McpServer, *};
use lenso_plugin_core::WorkGate;
use rmcp::{
    model::{CallToolRequest, CallToolRequestParams, ClientRequest, ProtocolVersion},
    service::{NotificationContext, PeerRequestOptions},
    ClientHandler, RoleClient, ServiceExt,
};
use serde_json::{json, Value};
use std::{process::Stdio, sync::Arc, time::Duration};
use tokio::{process::Command, sync::Notify};

#[tokio::test]
async fn official_client_launches_real_stdio_and_calls_tools() {
    let mut child = Command::new(env!("CARGO_BIN_EXE_lenso-mcp-demo"))
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let pipes = (child.stdout.take().unwrap(), child.stdin.take().unwrap());
    let client = tokio::time::timeout(Duration::from_secs(5), ().serve(pipes))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        client.peer_info().unwrap().protocol_version,
        ProtocolVersion::V_2025_11_25
    );
    let page = client.list_tools(None).await.unwrap();
    assert_eq!(page.tools.len(), 2);
    let echo = page
        .tools
        .iter()
        .find(|t| t.title.as_deref() == Some("notes.echo"))
        .unwrap();
    let success = client
        .call_tool(
            CallToolRequestParams::new(echo.name.clone()).with_arguments(
                json!({"text":"actual stdio client"})
                    .as_object()
                    .unwrap()
                    .clone(),
            ),
        )
        .await
        .unwrap();
    assert_eq!(
        success.structured_content,
        Some(json!({"echo":"actual stdio client"}))
    );
    let invalid = client
        .call_tool(CallToolRequestParams::new(echo.name.clone()))
        .await
        .unwrap();
    assert_eq!(invalid.is_error, Some(true));
    assert!(client
        .call_tool(CallToolRequestParams::new("missing"))
        .await
        .is_err());
    let wait = page
        .tools
        .iter()
        .find(|t| t.title.as_deref() == Some("notes.wait"))
        .unwrap();
    let request = client
        .send_cancellable_request(
            ClientRequest::CallToolRequest(CallToolRequest::new(CallToolRequestParams::new(
                wait.name.clone(),
            ))),
            PeerRequestOptions::default(),
        )
        .await
        .unwrap();
    request
        .cancel(Some("local acceptance cancellation".into()))
        .await
        .unwrap();
    let success = client
        .call_tool(
            CallToolRequestParams::new(echo.name.clone())
                .with_arguments(json!({"text":"after cancel"}).as_object().unwrap().clone()),
        )
        .await
        .unwrap();
    assert_eq!(
        success.structured_content,
        Some(json!({"echo":"after cancel"}))
    );
    client.cancel().await.unwrap();
    assert!(tokio::time::timeout(Duration::from_secs(5), child.wait())
        .await
        .unwrap()
        .unwrap()
        .success());
}

struct Observer(Arc<Notify>);
impl ClientHandler for Observer {
    async fn on_tool_list_changed(&self, _: NotificationContext<RoleClient>) {
        self.0.notify_one();
    }
}
fn op(name: &str) -> Operation<()> {
    Operation::new(
        "dynamic",
        name,
        "Synthetic host lifecycle operation",
        json!({"type":"object"}),
        |_, _: RequestContext<()>| async { Ok(json!({"ok":true})) },
    )
}
#[tokio::test]
async fn official_client_observes_dynamic_catalog_and_cancellation_reaches_service() {
    let started = Arc::new(Notify::new());
    let cancelled = Arc::new(Notify::new());
    let finish = Arc::new(Notify::new());
    let (s, c, f) = (started.clone(), cancelled.clone(), finish.clone());
    let slow = Operation::new(
        "dynamic",
        "slow",
        "",
        json!({"type":"object"}),
        move |_, context: RequestContext<()>| {
            let (s, c, f) = (s.clone(), c.clone(), f.clone());
            async move {
                s.notify_one();
                context.cancellation.cancelled().await;
                c.notify_one();
                f.notified().await;
                Ok(Value::Null)
            }
        },
    );
    let adapter = McpAdapter::new(
        McpConfig {
            page_size: 1,
            ..Default::default()
        },
        vec![op("one"), op("two"), slow],
        Policies::new(|_, _| async { Ok(true) }, |_, _| async { Ok(true) }),
        WorkGate::default(),
    )
    .unwrap();
    let server = McpServer::new(adapter.clone(), Arc::new(()));
    let (server_pipe, client_pipe) = tokio::io::duplex(64 * 1024);
    let target = server.clone();
    let serving = tokio::spawn(async move { target.serve(server_pipe).await.unwrap() });
    let changed = Arc::new(Notify::new());
    let client = Observer(changed.clone()).serve(client_pipe).await.unwrap();
    let running = serving.await.unwrap();
    let page = client.list_tools(None).await.unwrap();
    let cursor = page.next_cursor.unwrap();
    let request = client
        .send_cancellable_request(
            ClientRequest::CallToolRequest(CallToolRequest::new(CallToolRequestParams::new(
                operation_tool_name("dynamic", "slow"),
            ))),
            PeerRequestOptions::default(),
        )
        .await
        .unwrap();
    started.notified().await;
    request.cancel(None).await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), cancelled.notified())
        .await
        .unwrap();
    // Replacement is a host lifecycle action; it cannot be invoked through tools/call.
    server
        .replace_operations(vec![op("replacement")])
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), changed.notified())
        .await
        .unwrap();
    let stale: rmcp::model::PaginatedRequestParams =
        serde_json::from_value(json!({"cursor":cursor})).unwrap();
    assert!(client.list_tools(Some(stale)).await.is_err());
    let page = client.list_tools(None).await.unwrap();
    assert_eq!(page.tools.len(), 1);
    assert_eq!(
        page.tools[0].name.as_ref(),
        operation_tool_name("dynamic", "replacement")
    );
    assert!(client
        .call_tool(CallToolRequestParams::new(operation_tool_name(
            "dynamic", "one"
        )))
        .await
        .is_err());
    let result = client
        .call_tool(CallToolRequestParams::new(page.tools[0].name.clone()))
        .await
        .unwrap();
    assert_eq!(result.structured_content, Some(json!({"ok":true})));
    let closing = adapter.clone();
    let mut close = tokio::spawn(async move { closing.close().await });
    assert!(tokio::time::timeout(Duration::from_millis(10), &mut close)
        .await
        .is_err());
    finish.notify_one();
    close.await.unwrap();
    client.cancel().await.unwrap();
    running.cancel().await.unwrap();
}
