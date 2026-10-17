use lenso_mcp::*;
use lenso_plugin_core::{App, Instance, WorkGate};
use serde_json::{json, Value};
use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::sync::Notify;

fn context(identity: &Arc<()>) -> RequestContext<()> {
    RequestContext {
        identity: identity.clone(),
        request_id: "0".into(),
        cancellation: CancellationToken::new(),
    }
}
fn allow() -> Policies<()> {
    Policies::new(|_, _| async { Ok(true) }, |_, _| async { Ok(true) })
}
fn echo(method: &str) -> Operation<()> {
    Operation::new(
        "notes",
        method,
        "Synthetic local echo",
        json!({"type":"object","required":["value"]}),
        |input, _| async move { input.get("value").cloned().ok_or(ToolError::InvalidInput) },
    )
}
fn error_code(result: &CallToolResult) -> String {
    assert_eq!(result.is_error, Some(true));
    serde_json::from_str::<Value>(&result.content[0].text).unwrap()["code"]
        .as_str()
        .unwrap()
        .into()
}
#[tokio::test]
async fn stable_names_and_protocol_results_survive_reordering() {
    // Independently produced using Python hashlib over the exact TS JSON tuple bytes.
    assert_eq!(
        operation_tool_name("notes", "read"),
        "operation_vhT3TdT3uOUXaVcumDGHJXVF1MX1dr-uDri2OgJ1I2c"
    );
    assert_ne!(
        operation_tool_name("a.b", "c"),
        operation_tool_name("a", "b.c")
    );
    let identity = Arc::new(());
    let a = McpAdapter::new(
        McpConfig::default(),
        vec![echo("read"), echo("write")],
        allow(),
        WorkGate::default(),
    )
    .unwrap();
    let b = McpAdapter::new(
        McpConfig::default(),
        vec![echo("write"), echo("read")],
        allow(),
        WorkGate::default(),
    )
    .unwrap();
    let first = a.list_tools(context(&identity), None).await.unwrap();
    let second = b.list_tools(context(&identity), None).await.unwrap();
    assert_eq!(first.tools[0].name, second.tools[1].name);
    // JSON serialization is the exact host protocol boundary: camelCase and structuredContent.
    for value in [json!({"ok":true}), json!([1, 2]), json!(false), Value::Null] {
        let result = a
            .call_tool(
                &first.tools[0].name,
                json!({"value":value}),
                context(&identity),
            )
            .await
            .unwrap();
        let wire = serde_json::to_value(&result).unwrap();
        assert_eq!(
            serde_json::from_str::<Value>(wire["content"][0]["text"].as_str().unwrap()).unwrap(),
            value
        );
        assert_eq!(
            wire["structuredContent"],
            if value.is_object() {
                value
            } else {
                json!({"result":value})
            }
        );
        assert!(wire.get("isError").is_none());
    }
    a.close().await;
    b.close().await;
}
#[tokio::test]
async fn cursors_are_single_use_identity_scoped_and_visibility_checked() {
    let visible = Arc::new(AtomicBool::new(true));
    let policy = visible.clone();
    let config = McpConfig {
        page_size: 1,
        max_cursors: 1,
        ..Default::default()
    };
    let adapter = McpAdapter::new(
        config,
        vec![echo("one"), echo("two"), echo("three")],
        Policies::new(
            move |tool, _| {
                let flag = policy.clone();
                async move { Ok(tool.title != "notes.three" || flag.load(Ordering::SeqCst)) }
            },
            |_, _| async { Ok(true) },
        ),
        WorkGate::default(),
    )
    .unwrap();
    let identity = Arc::new(());
    let cursor = adapter
        .list_tools(context(&identity), None)
        .await
        .unwrap()
        .next_cursor
        .unwrap();
    assert_eq!(
        adapter
            .list_tools(context(&Arc::new(())), Some(cursor.clone()))
            .await
            .unwrap_err(),
        ProtocolError::InvalidParams
    );
    assert_eq!(
        adapter
            .list_tools(context(&identity), None)
            .await
            .unwrap_err(),
        ProtocolError::Busy
    );
    let page = adapter
        .list_tools(context(&identity), Some(cursor.clone()))
        .await
        .unwrap();
    assert_eq!(page.tools[0].title, "notes.two");
    assert_eq!(
        adapter
            .list_tools(context(&identity), Some(cursor))
            .await
            .unwrap_err(),
        ProtocolError::InvalidParams
    );
    let next = page.next_cursor.unwrap();
    visible.store(false, Ordering::SeqCst);
    assert_eq!(
        adapter
            .list_tools(context(&identity), Some(next.clone()))
            .await
            .unwrap_err(),
        ProtocolError::InvalidParams
    );
    visible.store(true, Ordering::SeqCst);
    assert_eq!(
        adapter
            .list_tools(context(&identity), Some(next))
            .await
            .unwrap()
            .tools[0]
            .title,
        "notes.three"
    );
    adapter.close().await;
}
#[tokio::test]
async fn expired_cursor_and_invalid_catalog_reject() {
    let config = McpConfig {
        page_size: 1,
        cursor_ttl: Duration::from_millis(1),
        ..Default::default()
    };
    let adapter = McpAdapter::new(
        config,
        vec![echo("one"), echo("two")],
        allow(),
        WorkGate::default(),
    )
    .unwrap();
    let identity = Arc::new(());
    let cursor = adapter
        .list_tools(context(&identity), None)
        .await
        .unwrap()
        .next_cursor;
    tokio::time::sleep(Duration::from_millis(5)).await;
    assert_eq!(
        adapter
            .list_tools(context(&identity), cursor)
            .await
            .unwrap_err(),
        ProtocolError::InvalidParams
    );
    assert!(McpAdapter::new(
        McpConfig::default(),
        vec![echo("one"), echo("one")],
        allow(),
        WorkGate::default()
    )
    .is_err());
    let bad = Operation::new(
        "x",
        "y",
        "",
        json!({"type":"array"}),
        |_, _: RequestContext<()>| async { Ok(Value::Null) },
    );
    assert!(McpAdapter::new(
        McpConfig::default(),
        vec![bad],
        allow(),
        WorkGate::default()
    )
    .is_err());
    adapter.close().await;
}
#[tokio::test]
async fn permissions_validation_and_byte_limits_return_safe_failures() {
    let identity = Arc::new(());
    let name = operation_tool_name("notes", "read");
    let adapter = McpAdapter::new(
        McpConfig {
            max_input_bytes: 32,
            max_output_bytes: 256,
            ..Default::default()
        },
        vec![echo("read")],
        allow(),
        WorkGate::default(),
    )
    .unwrap();
    assert_eq!(
        adapter
            .call_tool("unknown", json!({}), context(&identity))
            .await
            .unwrap_err(),
        ProtocolError::InvalidParams
    );
    assert_eq!(
        error_code(
            &adapter
                .call_tool(&name, json!({}), context(&identity))
                .await
                .unwrap()
        ),
        "invalid-input"
    );
    assert_eq!(
        error_code(
            &adapter
                .call_tool(&name, json!([]), context(&identity))
                .await
                .unwrap()
        ),
        "invalid-input"
    );
    let result = adapter
        .call_tool(
            &name,
            json!({"value":"secret".repeat(30)}),
            context(&identity),
        )
        .await
        .unwrap();
    assert_eq!(error_code(&result), "input-too-large");
    assert!(!serde_json::to_string(&result).unwrap().contains("secret"));
    adapter.close().await;
    let denied = McpAdapter::new(
        McpConfig::default(),
        vec![Operation::new(
            "x",
            "y",
            "",
            json!({"type":"object"}),
            |_, _: RequestContext<()>| async { panic!("handler must not run") },
        )],
        Policies::new(|_, _| async { Ok(true) }, |_, _| async { Ok(false) }),
        WorkGate::default(),
    )
    .unwrap();
    assert_eq!(
        error_code(
            &denied
                .call_tool(
                    &operation_tool_name("x", "y"),
                    json!({}),
                    context(&identity)
                )
                .await
                .unwrap()
        ),
        "forbidden-operation"
    );
    denied.close().await;
    let large = McpAdapter::new(
        McpConfig {
            max_output_bytes: 256,
            ..Default::default()
        },
        vec![Operation::new(
            "x",
            "big",
            "",
            json!({"type":"object"}),
            |_, _: RequestContext<()>| async { Ok(json!("界".repeat(100))) },
        )],
        allow(),
        WorkGate::default(),
    )
    .unwrap();
    assert_eq!(
        error_code(
            &large
                .call_tool(
                    &operation_tool_name("x", "big"),
                    json!({}),
                    context(&identity)
                )
                .await
                .unwrap()
        ),
        "output-too-large"
    );
    large.close().await;
}

#[tokio::test]
async fn cancellation_keeps_slot_and_app_resources_until_actual_settlement() {
    let started = Arc::new(Notify::new());
    let finish = Arc::new(Notify::new());
    let signalled = Arc::new(Notify::new());
    let (s, f, c) = (started.clone(), finish.clone(), signalled.clone());
    let plugin = Instance::new(
        "mcp",
        McpPlugin {
            config: McpConfig {
                max_concurrent: 1,
                ..Default::default()
            },
            operations: vec![Operation::new(
                "notes",
                "slow",
                "",
                json!({"type":"object"}),
                move |_, context: RequestContext<()>| {
                    let (s, f, c) = (s.clone(), f.clone(), c.clone());
                    async move {
                        s.notify_one();
                        context.cancellation.cancelled().await;
                        c.notify_one();
                        f.notified().await;
                        Ok(json!({"done":true}))
                    }
                },
            )],
            policies: allow(),
        },
    );
    let handle = plugin.handle();
    let app = Arc::new(App::new().with(plugin).start().await.unwrap());
    let adapter = app.get(&handle).unwrap();
    let identity = Arc::new(());
    let request = context(&identity);
    let cancel = request.cancellation.clone();
    let target = adapter.clone();
    let call = tokio::spawn(async move {
        target
            .call_tool(&operation_tool_name("notes", "slow"), json!({}), request)
            .await
            .unwrap()
    });
    started.notified().await;
    cancel.cancel();
    assert_eq!(error_code(&call.await.unwrap()), "request-cancelled");
    signalled.notified().await;
    assert_eq!(
        error_code(
            &adapter
                .call_tool(
                    &operation_tool_name("notes", "slow"),
                    json!({}),
                    context(&identity)
                )
                .await
                .unwrap()
        ),
        "adapter-busy"
    );
    let owner = app.clone();
    let mut shutdown = tokio::spawn(async move { owner.shutdown().await });
    assert!(
        tokio::time::timeout(Duration::from_millis(10), &mut shutdown)
            .await
            .is_err()
    );
    finish.notify_one();
    shutdown.await.unwrap().unwrap();
    assert_eq!(
        error_code(
            &adapter
                .call_tool(
                    &operation_tool_name("notes", "slow"),
                    json!({}),
                    context(&identity)
                )
                .await
                .unwrap()
        ),
        "adapter-closed"
    );
    adapter.close().await;
}
#[tokio::test]
async fn timeout_and_dropped_waiter_still_signal_and_drain() {
    let started = Arc::new(Notify::new());
    let signalled = Arc::new(Notify::new());
    let finish = Arc::new(Notify::new());
    let (s, c, f) = (started.clone(), signalled.clone(), finish.clone());
    let adapter = McpAdapter::new(
        McpConfig {
            max_concurrent: 1,
            request_timeout: Duration::from_millis(20),
            ..Default::default()
        },
        vec![Operation::new(
            "x",
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
        )],
        allow(),
        WorkGate::default(),
    )
    .unwrap();
    let identity = Arc::new(());
    let request = context(&identity);
    let target = adapter.clone();
    let call = tokio::spawn(async move {
        target
            .call_tool(&operation_tool_name("x", "slow"), json!({}), request)
            .await
            .unwrap()
    });
    started.notified().await;
    assert_eq!(error_code(&call.await.unwrap()), "request-timeout");
    signalled.notified().await;
    assert_eq!(
        adapter
            .list_tools(context(&identity), None)
            .await
            .unwrap_err(),
        ProtocolError::Busy
    );
    let target = adapter.clone();
    let mut close = tokio::spawn(async move { target.close().await });
    assert!(tokio::time::timeout(Duration::from_millis(5), &mut close)
        .await
        .is_err());
    finish.notify_one();
    close.await.unwrap();

    let started = Arc::new(Notify::new());
    let finish = Arc::new(Notify::new());
    let (s, f) = (started.clone(), finish.clone());
    let adapter = McpAdapter::new(
        McpConfig::default(),
        vec![Operation::new(
            "x",
            "drop",
            "",
            json!({"type":"object"}),
            move |_, context: RequestContext<()>| {
                let (s, f) = (s.clone(), f.clone());
                async move {
                    s.notify_one();
                    context.cancellation.cancelled().await;
                    f.notified().await;
                    Ok(Value::Null)
                }
            },
        )],
        allow(),
        WorkGate::default(),
    )
    .unwrap();
    let request = context(&identity);
    let target = adapter.clone();
    let waiter = tokio::spawn(async move {
        target
            .call_tool(&operation_tool_name("x", "drop"), json!({}), request)
            .await
    });
    started.notified().await;
    waiter.abort();
    let target = adapter.clone();
    let mut close = tokio::spawn(async move { target.close().await });
    assert!(tokio::time::timeout(Duration::from_millis(5), &mut close)
        .await
        .is_err());
    finish.notify_one();
    close.await.unwrap();
}
