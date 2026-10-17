#![cfg(feature = "stdio")]
//! Integration consumes the trusted access interface; no token/OAuth implementation.
use lenso_authorization as auth;
use lenso_mcp::{stdio::McpServer, *};
use lenso_plugin_core::WorkGate;
use rmcp::{model::CallToolRequestParams, ServiceExt};
use serde_json::json;
use std::sync::{Arc, Mutex};

struct Evidence;
struct Session {
    active: bool,
    audience: String,
    expires: u64,
}
struct Host {
    expected: Arc<Evidence>,
    session: Mutex<Session>,
}
impl auth::AccessVerifier<Arc<Evidence>> for Host {
    fn verify<'a>(
        &'a self,
        evidence: &'a Arc<Evidence>,
        _: &'a auth::Resource,
        _: &'a auth::Evaluation,
    ) -> auth::EvaluationFuture<'a, Option<auth::AccessIdentity>> {
        Box::pin(async move {
            if !Arc::ptr_eq(evidence, &self.expected) {
                return Ok(None);
            }
            let session = self.session.lock().unwrap();
            Ok(session.active.then(|| auth::AccessIdentity {
                principal: auth::Principal::new("notes", "synthetic-user", "user"),
                audience: session.audience.clone(),
                session_expires_at: Some(session.expires),
                credential: None,
            }))
        })
    }
}
#[tokio::test]
async fn sdk_calls_reverify_host_access_after_discovery_renewal_and_revocation() {
    let identity = Arc::new(Evidence);
    let host = Arc::new(Host {
        expected: identity.clone(),
        session: Mutex::new(Session {
            active: true,
            audience: "local-mcp".into(),
            expires: 200,
        }),
    });
    let mut options = auth::AuthorizationOptions::new(["read"]);
    options.clock = Arc::new(|| Ok(100));
    options
        .policies
        .push(auth::policy(|_, _| Ok(auth::Effect::Allow)));
    let access = Arc::new(
        auth::AuthorizedAccess::new(
            "notes",
            "local-mcp",
            host.clone(),
            auth::Authorization::new(options).unwrap(),
        )
        .unwrap(),
    );
    let adapter = McpAdapter::new(
        McpConfig::default(),
        vec![Operation::new(
            "notes",
            "read",
            "Synthetic protected read",
            json!({"type":"object"}),
            |_, _: RequestContext<Evidence>| async { Ok(json!({"read":true})) },
        )],
        Policies::new(
            |_, _| async { Ok(true) },
            move |_, context: RequestContext<Evidence>| {
                let access = access.clone();
                async move {
                    Ok(access
                        .can(
                            context.identity,
                            "read",
                            auth::Resource::new(
                                "note",
                                "one",
                                auth::Scope::new("personal", "home"),
                            ),
                            auth::Attributes::new(),
                        )
                        .await)
                }
            },
        ),
        WorkGate::default(),
    )
    .unwrap();
    let server = McpServer::new(adapter.clone(), identity);
    let (server_pipe, client_pipe) = tokio::io::duplex(64 * 1024);
    let (read, write) = tokio::io::split(server_pipe);
    let serving = tokio::spawn(server.run_io(read, write, CancellationToken::new()));
    let client = ().serve(client_pipe).await.unwrap();
    let page = client.list_tools(None).await.unwrap();
    let name = page.tools[0].name.clone();
    assert_eq!(
        client
            .call_tool(CallToolRequestParams::new(name.clone()))
            .await
            .unwrap()
            .structured_content,
        Some(json!({"read":true}))
    );
    // Expired evidence cannot be used even though the tool was already discovered.
    host.session.lock().unwrap().expires = 100;
    assert_eq!(
        client
            .call_tool(CallToolRequestParams::new(name.clone()))
            .await
            .unwrap()
            .is_error,
        Some(true)
    );
    // Only the synthetic host renews the session; MCP stores no allow decision.
    host.session.lock().unwrap().expires = 300;
    assert_eq!(
        client
            .call_tool(CallToolRequestParams::new(name.clone()))
            .await
            .unwrap()
            .structured_content,
        Some(json!({"read":true}))
    );
    host.session.lock().unwrap().audience = "wrong-resource".into();
    assert_eq!(
        client
            .call_tool(CallToolRequestParams::new(name.clone()))
            .await
            .unwrap()
            .is_error,
        Some(true)
    );
    host.session.lock().unwrap().audience = "local-mcp".into();
    host.session.lock().unwrap().active = false;
    let refused = client
        .call_tool(CallToolRequestParams::new(name))
        .await
        .unwrap();
    assert_eq!(refused.is_error, Some(true));
    assert!(serde_json::to_string(&refused)
        .unwrap()
        .contains("forbidden-operation"));
    client.cancel().await.unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(2), serving)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    adapter.close().await;
}
