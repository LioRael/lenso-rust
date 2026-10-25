use lenso_mcp::{
    CancellationToken, McpAdapter, McpConfig, Operation, Policies, ProtocolError, RequestContext,
    ToolError,
};
use lenso_plugin_core::WorkGate;
use serde_json::json;
use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        mpsc, Arc, Mutex,
    },
    time::Duration,
};

struct OnDrop(Option<Box<dyn FnOnce() + Send + Sync>>);
impl Drop for OnDrop {
    fn drop(&mut self) {
        if let Some(callback) = self.0.take() {
            callback();
        }
    }
}

// A lock regression must fail the test rather than hang the test runtime.
fn completes(action: impl FnOnce() + Send + 'static) {
    let (send, receive) = mpsc::channel();
    let worker = std::thread::spawn(move || {
        action();
        send.send(()).unwrap();
    });
    receive
        .recv_timeout(Duration::from_secs(5))
        .expect("adapter operation deadlocked or panicked");
    worker.join().unwrap();
}

fn operations() -> Vec<Operation<OnDrop>> {
    ["a", "b"]
        .into_iter()
        .map(|name| {
            Operation::new("test", name, "", json!({"type":"object"}), |_, _| async {
                Ok(json!({}))
            })
        })
        .collect()
}

#[test]
fn cursor_identity_destructors_can_reenter_on_stop_replace_and_expiry() {
    for action in ["stop", "replace", "expire", "expire-invalid-cursor"] {
        completes(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            runtime.block_on(async {
                let adapter = McpAdapter::new(
                    McpConfig {
                        page_size: 1,
                        cursor_ttl: Duration::from_millis(1),
                        ..Default::default()
                    },
                    operations(),
                    Policies::new(|_, _| async { Ok(true) }, |_, _| async { Ok(true) }),
                    WorkGate::default(),
                )
                .unwrap();
                let reentrant = adapter.clone();
                let drops = Arc::new(AtomicUsize::new(0));
                let observed = drops.clone();
                let identity = Arc::new(OnDrop(Some(Box::new(move || {
                    reentrant.stop();
                    observed.fetch_add(1, Ordering::SeqCst);
                }))));
                let page = adapter
                    .list_tools(
                        RequestContext {
                            identity,
                            request_id: "first".into(),
                            cancellation: CancellationToken::new(),
                        },
                        None,
                    )
                    .await
                    .unwrap();
                assert!(page.next_cursor.is_some());
                match action {
                    "stop" => adapter.stop(),
                    "replace" => adapter.replace_operations(operations()).unwrap(),
                    _ => {
                        tokio::time::sleep(Duration::from_millis(5)).await;
                        let cursor = (action == "expire-invalid-cursor").then(|| "invalid".into());
                        let result = adapter
                            .list_tools(
                                RequestContext {
                                    identity: Arc::new(OnDrop(None)),
                                    request_id: "next".into(),
                                    cancellation: CancellationToken::new(),
                                },
                                cursor,
                            )
                            .await;
                        if action == "expire-invalid-cursor" {
                            // The destructor also stops discovery, which can win
                            // the response race over the invalid-cursor result.
                            assert!(matches!(
                                result,
                                Err(ProtocolError::InvalidParams | ProtocolError::Discovery)
                            ));
                        }
                    }
                }
                assert_eq!(drops.load(Ordering::SeqCst), 1, "{action}");
                adapter.close().await;
            });
        });
    }
}

#[test]
fn retired_handler_captures_can_reenter_catalog_replacement() {
    completes(|| {
        let slot = Arc::new(Mutex::new(None::<McpAdapter<()>>));
        let captured = slot.clone();
        let drops = Arc::new(AtomicUsize::new(0));
        let observed = drops.clone();
        let on_drop = OnDrop(Some(Box::new(move || {
            let adapter = captured.lock().unwrap().take().unwrap();
            adapter.replace_operations(Vec::new()).unwrap();
            observed.fetch_add(1, Ordering::SeqCst);
        })));
        let operation = Operation::new(
            "test",
            "captured",
            "",
            json!({"type":"object"}),
            move |_, _: RequestContext<()>| {
                let _capture = &on_drop;
                async { Ok(json!({})) }
            },
        );
        let adapter = McpAdapter::new(
            McpConfig::default(),
            vec![operation],
            Policies::new(|_, _| async { Ok(true) }, |_, _| async { Ok(true) }),
            WorkGate::default(),
        )
        .unwrap();
        *slot.lock().unwrap() = Some(adapter.clone());
        adapter.replace_operations(Vec::new()).unwrap();
        assert_eq!(drops.load(Ordering::SeqCst), 1);
    });
}

#[test]
fn tool_errors_support_standard_error_propagation() {
    fn propagate() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        Err(ToolError::Forbidden)?;
        Ok(())
    }
    let error = propagate().unwrap_err();
    assert_eq!(error.to_string(), "tool access denied");
    assert_eq!(
        error.downcast_ref::<ToolError>(),
        Some(&ToolError::Forbidden)
    );
}
