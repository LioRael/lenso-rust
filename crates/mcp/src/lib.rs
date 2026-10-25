//! Borrowed native MCP tool projection. The host owns authentication and transport.
#![doc = include_str!("../README.md")]

use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use futures_util::FutureExt;
use lenso_plugin_core::{BoxError, CleanupPhase, Plugin, Setup, WorkGate};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{
    collections::{HashMap, HashSet},
    future::Future,
    panic::AssertUnwindSafe,
    pin::Pin,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
    time::{Duration, Instant},
};
use tokio::sync::{oneshot, watch};
pub use tokio_util::sync::CancellationToken;

#[cfg(feature = "stdio")]
pub mod stdio;

type BoxFuture<T> = Pin<Box<dyn Future<Output = T> + Send>>;
type Handler<I> =
    dyn Fn(Value, RequestContext<I>) -> BoxFuture<Result<Value, ToolError>> + Send + Sync;
type Policy<I> =
    dyn Fn(Tool, RequestContext<I>) -> BoxFuture<Result<bool, ToolError>> + Send + Sync;

/// Exact TS tuple encoding and SHA-256/base64url name. Order does not affect names.
pub fn operation_tool_name(plugin_id: &str, method: &str) -> String {
    let tuple = serde_json::to_vec(&(plugin_id, method)).expect("string tuple is JSON");
    format!(
        "operation_{}",
        URL_SAFE_NO_PAD.encode(Sha256::digest(tuple))
    )
}

pub struct RequestContext<I> {
    /// Trusted host evidence, never deserialized from business arguments.
    pub identity: Arc<I>,
    pub request_id: String,
    pub cancellation: CancellationToken,
}
impl<I> Clone for RequestContext<I> {
    fn clone(&self) -> Self {
        Self {
            identity: self.identity.clone(),
            request_id: self.request_id.clone(),
            cancellation: self.cancellation.clone(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Tool {
    pub name: String,
    pub title: String,
    pub description: String,
    pub input_schema: Value,
}

/// Explicit selection of a native service callback; no method inference or registry.
/// Callback must apply its shared business validator (including schema constraints).
pub struct Operation<I> {
    tool: Tool,
    handler: Arc<Handler<I>>,
}
impl<I> Operation<I> {
    pub fn new<F, Fut>(
        plugin_id: &str,
        method: &str,
        description: &str,
        input_schema: Value,
        handler: F,
    ) -> Self
    where
        F: Fn(Value, RequestContext<I>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Value, ToolError>> + Send + 'static,
    {
        Self {
            tool: Tool {
                name: operation_tool_name(plugin_id, method),
                title: format!("{plugin_id}.{method}"),
                description: description.into(),
                input_schema,
            },
            handler: Arc::new(move |input, context| Box::pin(handler(input, context))),
        }
    }
}

/// Both callbacks are required. Visibility never substitutes for invocation permission.
pub struct Policies<I> {
    visible: Arc<Policy<I>>,
    authorize: Arc<Policy<I>>,
}
impl<I> Policies<I> {
    pub fn new<V, VF, A, AF>(visible: V, authorize: A) -> Self
    where
        V: Fn(Tool, RequestContext<I>) -> VF + Send + Sync + 'static,
        VF: Future<Output = Result<bool, ToolError>> + Send + 'static,
        A: Fn(Tool, RequestContext<I>) -> AF + Send + Sync + 'static,
        AF: Future<Output = Result<bool, ToolError>> + Send + 'static,
    {
        Self {
            visible: Arc::new(move |t, c| Box::pin(visible(t, c))),
            authorize: Arc::new(move |t, c| Box::pin(authorize(t, c))),
        }
    }
}

#[derive(Clone)]
pub struct McpConfig {
    pub max_input_bytes: usize,
    pub max_output_bytes: usize,
    pub max_catalog_bytes: usize,
    pub max_concurrent: usize,
    pub page_size: usize,
    pub request_timeout: Duration,
    pub cursor_ttl: Duration,
    pub max_cursors: usize,
}
impl Default for McpConfig {
    fn default() -> Self {
        Self {
            max_input_bytes: 256 * 1024,
            max_output_bytes: 1024 * 1024,
            max_catalog_bytes: 256 * 1024,
            max_concurrent: 4,
            page_size: 100,
            request_timeout: Duration::from_secs(30),
            cursor_ttl: Duration::from_secs(300),
            max_cursors: 1024,
        }
    }
}
impl McpConfig {
    fn validate(&self) -> Result<(), ProtocolError> {
        if self.max_input_bytes == 0
            || self.max_output_bytes < 256
            || self.max_catalog_bytes == 0
            || self.max_concurrent == 0
            || self.page_size == 0
            || self.request_timeout.is_zero()
            || self.cursor_ttl.is_zero()
            || self.max_cursors == 0
            || Instant::now().checked_add(self.cursor_ttl).is_none()
            || Instant::now().checked_add(self.request_timeout).is_none()
        {
            return Err(ProtocolError::Configuration);
        }
        Ok(())
    }
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum ProtocolError {
    #[error("Invalid MCP configuration or catalog.")]
    Configuration,
    #[error("Unknown tool or invalid/stale cursor.")]
    InvalidParams,
    #[error("Adapter is closed.")]
    Closed,
    #[error("Adapter is busy.")]
    Busy,
    #[error("Tool discovery failed or was interrupted.")]
    Discovery,
}
impl ProtocolError {
    pub fn code(&self) -> i32 {
        match self {
            Self::InvalidParams => -32602,
            Self::Closed => -32600,
            _ => -32603,
        }
    }
}

/// Public error classifications carry no arbitrary source text, input or credentials.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ToolError {
    InvalidInput,
    InputTooLarge,
    OutputTooLarge,
    Forbidden,
    Failed,
    Cancelled,
    Timeout,
    Busy,
    Closed,
}
impl std::fmt::Display for ToolError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::InvalidInput => "invalid tool input",
            Self::InputTooLarge => "tool input exceeds limit",
            Self::OutputTooLarge => "tool output exceeds limit",
            Self::Forbidden => "tool access denied",
            Self::Failed => "tool execution failed",
            Self::Cancelled => "tool request cancelled",
            Self::Timeout => "tool request timed out",
            Self::Busy => "tool adapter busy",
            Self::Closed => "tool adapter closed",
        })
    }
}
impl std::error::Error for ToolError {}

impl ToolError {
    fn diagnostic(self) -> Value {
        let (code, phase, message) = match self {
            Self::InvalidInput => (
                "invalid-input",
                "input",
                "Input does not satisfy the operation schema.",
            ),
            Self::InputTooLarge => (
                "input-too-large",
                "input",
                "Operation input exceeds the host limit.",
            ),
            Self::OutputTooLarge => (
                "output-too-large",
                "output",
                "Operation output exceeds the host limit.",
            ),
            Self::Forbidden => (
                "forbidden-operation",
                "invoke",
                "Operation is not available to this caller.",
            ),
            Self::Failed => (
                "invocation-failed",
                "invoke",
                "Operation invocation failed.",
            ),
            Self::Cancelled => (
                "request-cancelled",
                "invoke",
                "Request cancelled; external effects may have occurred.",
            ),
            Self::Timeout => (
                "request-timeout",
                "invoke",
                "Request timed out; work may continue and external effects may have occurred.",
            ),
            Self::Busy => (
                "adapter-busy",
                "invoke",
                "Adapter is busy; no request was queued.",
            ),
            Self::Closed => ("adapter-closed", "invoke", "Adapter is closed."),
        };
        json!({"code":code,"phase":phase,"message":message})
    }
}
#[derive(Debug, Serialize, Deserialize)]
pub struct TextContent {
    pub r#type: String,
    pub text: String,
}
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CallToolResult {
    pub content: Vec<TextContent>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub structured_content: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub is_error: Option<bool>,
}
impl CallToolResult {
    fn failure(error: ToolError) -> Self {
        Self {
            content: vec![TextContent {
                r#type: "text".into(),
                text: error.diagnostic().to_string(),
            }],
            structured_content: None,
            is_error: Some(true),
        }
    }
    fn success(value: Value, limit: usize) -> Self {
        let structured = if value.is_object() {
            value.clone()
        } else {
            json!({"result":value})
        };
        let text = value.to_string();
        if text.len() > limit || structured.to_string().len() > limit {
            return Self::failure(ToolError::OutputTooLarge);
        }
        Self {
            content: vec![TextContent {
                r#type: "text".into(),
                text,
            }],
            structured_content: Some(structured),
            is_error: None,
        }
    }
}
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ToolPage {
    pub tools: Vec<Tool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
}
struct Cursor<I> {
    identity: Arc<I>,
    visible: Vec<String>,
    offset: usize,
    expires: Instant,
}
struct State<I> {
    operations: Arc<Vec<Operation<I>>>,
    catalog_version: u64,
    closed: bool,
    active: usize,
    next_cursor: u64,
    cursors: HashMap<String, Cursor<I>>,
}
struct Inner<I> {
    config: McpConfig,
    policies: Policies<I>,
    gate: WorkGate,
    state: Mutex<State<I>>,
    stop: CancellationToken,
    active: watch::Sender<usize>,
    nonce: u64,
}
/// Clones share adapter ownership; closing never shuts down the borrowed application.
pub struct McpAdapter<I>(Arc<Inner<I>>);
impl<I> Clone for McpAdapter<I> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}
static NEXT_ADAPTER: AtomicU64 = AtomicU64::new(1);
struct Admission<I> {
    inner: Arc<Inner<I>>,
}
impl<I> Drop for Admission<I> {
    fn drop(&mut self) {
        let mut state = self.inner.state.lock().unwrap();
        state.active -= 1;
        self.inner.active.send_replace(state.active);
    }
}
impl<I: Send + Sync + 'static> McpAdapter<I> {
    /// Construct an adapter over a validated catalog and the host's admission gate.
    ///
    /// # Errors
    /// Returns [`ProtocolError::Configuration`] for invalid limits, duplicate tool
    /// names, non-object input schemas, or a catalog exceeding its byte budget.
    pub fn new(
        config: McpConfig,
        operations: Vec<Operation<I>>,
        policies: Policies<I>,
        gate: WorkGate,
    ) -> Result<Self, ProtocolError> {
        validate_catalog(&config, &operations)?;
        let (active, _) = watch::channel(0);
        Ok(Self(Arc::new(Inner {
            config,
            policies,
            gate,
            state: Mutex::new(State {
                operations: Arc::new(operations),
                catalog_version: 0,
                closed: false,
                active: 0,
                next_cursor: 0,
                cursors: HashMap::new(),
            }),
            stop: CancellationToken::new(),
            active,
            nonce: NEXT_ADAPTER.fetch_add(1, Ordering::Relaxed),
        })))
    }
    /// Runs work independently of response delivery. A dropped waiter, cancellation or
    /// deadline retains both the adapter slot and app WorkGuard until actual settlement.
    async fn tracked<T, F, Fut>(
        &self,
        context: RequestContext<I>,
        action: F,
    ) -> Result<T, ToolError>
    where
        T: Send + 'static,
        F: FnOnce(RequestContext<I>) -> Fut + Send + 'static,
        Fut: Future<Output = Result<T, ToolError>> + Send + 'static,
    {
        if context.cancellation.is_cancelled() {
            return Err(ToolError::Cancelled);
        }
        let work = {
            let mut state = self.0.state.lock().unwrap();
            if state.closed {
                return Err(ToolError::Closed);
            }
            if state.active >= self.0.config.max_concurrent {
                return Err(ToolError::Busy);
            }
            let work = self.0.gate.enter().map_err(|_| ToolError::Closed)?;
            state.active += 1;
            self.0.active.send_replace(state.active);
            work
        };
        let admission = Admission {
            inner: self.0.clone(),
        };
        let cancelled = context.cancellation.clone();
        let signal = CancellationToken::new();
        let mut bound = context;
        bound.cancellation = signal.clone();
        let (send, mut receive) = oneshot::channel();
        let worker_signal = signal.clone();
        tokio::spawn(async move {
            let _admission = admission;
            let _work = work;
            let result = AssertUnwindSafe(async {
                if worker_signal.is_cancelled() {
                    return Err(ToolError::Cancelled);
                }
                let result = action(bound).await;
                if worker_signal.is_cancelled() {
                    Err(ToolError::Cancelled)
                } else {
                    result
                }
            })
            .catch_unwind()
            .await
            .unwrap_or(Err(ToolError::Failed));
            let _ = send.send(result);
        });
        // Continue forwarding cancellation even when a host drops its response future.
        let stop = self.0.stop.clone();
        let timeout = self.0.config.request_timeout;
        let (reply, wait) = oneshot::channel();
        tokio::spawn(async move {
            let result = tokio::select! { biased;
                _ = cancelled.cancelled() => Err(ToolError::Cancelled),
                _ = stop.cancelled() => Err(ToolError::Cancelled),
                _ = tokio::time::sleep(timeout) => Err(ToolError::Timeout),
                result = &mut receive => result.unwrap_or(Err(ToolError::Failed)),
            };
            signal.cancel();
            let _ = reply.send(result);
        });
        wait.await.unwrap_or(Err(ToolError::Failed))
    }
    /// Invoke one visible, authorized tool while retaining admission until it settles.
    ///
    /// # Errors
    /// Returns [`ProtocolError::InvalidParams`] for an unknown tool name.
    /// Input, policy, execution and lifecycle failures are encoded in the returned
    /// [`CallToolResult`], not raised as protocol errors.
    pub async fn call_tool(
        &self,
        name: &str,
        input: Value,
        context: RequestContext<I>,
    ) -> Result<CallToolResult, ProtocolError> {
        let (handler, tool) = {
            let state = self.0.state.lock().unwrap();
            let operation = state
                .operations
                .iter()
                .find(|o| o.tool.name == name)
                .ok_or(ProtocolError::InvalidParams)?;
            (operation.handler.clone(), operation.tool.clone())
        };
        if input.to_string().len() > self.0.config.max_input_bytes {
            return Ok(CallToolResult::failure(ToolError::InputTooLarge));
        }
        if !input.is_object() {
            return Ok(CallToolResult::failure(ToolError::InvalidInput));
        }
        let inner = self.0.clone();
        let result = self
            .tracked(context, move |context| async move {
                if !(inner.policies.visible)(tool.clone(), context.clone()).await?
                    || !(inner.policies.authorize)(tool, context.clone()).await?
                {
                    return Err(ToolError::Forbidden);
                }
                if context.cancellation.is_cancelled() {
                    return Err(ToolError::Cancelled);
                }
                handler(input, context).await
            })
            .await;
        Ok(match result {
            Ok(value) => CallToolResult::success(value, self.0.config.max_output_bytes),
            Err(error) => CallToolResult::failure(error),
        })
    }
    /// Discover a page of tools using fresh visibility checks for the supplied identity.
    ///
    /// # Errors
    /// Returns [`ProtocolError`] for closed or busy admission, failed or cancelled
    /// discovery, invalid/stale cursors, or pages exceeding the catalog budget.
    pub async fn list_tools(
        &self,
        context: RequestContext<I>,
        cursor: Option<String>,
    ) -> Result<ToolPage, ProtocolError> {
        let inner = self.0.clone();
        self.tracked(context, move |context| async move {
            let (operations, version) = {
                let state = inner.state.lock().unwrap();
                (state.operations.clone(), state.catalog_version)
            };
            let mut visible = Vec::new();
            for operation in operations.iter() {
                if (inner.policies.visible)(operation.tool.clone(), context.clone()).await? {
                    visible.push(operation.tool.clone());
                }
                if context.cancellation.is_cancelled() {
                    return Err(ToolError::Cancelled);
                }
            }
            Ok(make_page(
                &inner,
                &context.identity,
                visible,
                cursor,
                version,
            ))
        })
        .await
        .map_err(|e| match e {
            ToolError::Closed => ProtocolError::Closed,
            ToolError::Busy => ProtocolError::Busy,
            _ => ProtocolError::Discovery,
        })?
    }
    /// Trusted host lifecycle operation. Validate before atomically replacing the
    /// catalog; admitted calls keep their original handler and work guard.
    ///
    /// # Errors
    /// Returns [`ProtocolError::Configuration`] for an invalid catalog or exhausted
    /// catalog revision, or [`ProtocolError::Closed`] after admission is sealed.
    pub fn replace_operations(&self, operations: Vec<Operation<I>>) -> Result<(), ProtocolError> {
        validate_catalog(&self.0.config, &operations)?;
        let mut state = self.0.state.lock().unwrap();
        if state.closed {
            return Err(ProtocolError::Closed);
        }
        state.catalog_version = state
            .catalog_version
            .checked_add(1)
            .ok_or(ProtocolError::Configuration)?;
        let retired_operations = std::mem::replace(&mut state.operations, Arc::new(operations));
        let retired_cursors = std::mem::take(&mut state.cursors);
        drop(state);
        drop(retired_operations);
        drop(retired_cursors);
        Ok(())
    }
    /// Seal admission and signal all work; registered entrance cleanup uses this
    /// before Core drains WorkGuards. It is safe to call repeatedly.
    pub fn stop(&self) {
        let mut state = self.0.state.lock().unwrap();
        state.closed = true;
        let retired = std::mem::take(&mut state.cursors);
        drop(state);
        self.0.stop.cancel();
        drop(retired);
    }
    pub async fn close(&self) {
        self.stop();
        let mut active = self.0.active.subscribe();
        loop {
            if *active.borrow_and_update() == 0 {
                return;
            }
            if active.changed().await.is_err() {
                return;
            }
        }
    }
}
fn validate_catalog<I>(
    config: &McpConfig,
    operations: &[Operation<I>],
) -> Result<(), ProtocolError> {
    config.validate()?;
    let mut names = HashSet::new();
    for o in operations {
        if o.tool.input_schema.get("type").and_then(Value::as_str) != Some("object")
            || !names.insert(&o.tool.name)
        {
            return Err(ProtocolError::Configuration);
        }
    }
    let tools: Vec<_> = operations.iter().map(|o| &o.tool).collect();
    if serde_json::to_vec(&json!({"tools":tools}))
        .map_err(|_| ProtocolError::Configuration)?
        .len()
        > config.max_catalog_bytes
    {
        return Err(ProtocolError::Configuration);
    }
    Ok(())
}
fn make_page<I>(
    inner: &Inner<I>,
    identity: &Arc<I>,
    visible: Vec<Tool>,
    cursor: Option<String>,
    version: u64,
) -> Result<ToolPage, ProtocolError> {
    let now = Instant::now();
    let names: Vec<_> = visible.iter().map(|t| t.name.clone()).collect();
    // Declared before the guard so host identity destructors run after unlock,
    // including when cursor validation returns early.
    let mut retired = Vec::new();
    let mut state = inner.state.lock().unwrap();
    if state.closed {
        return Err(ProtocolError::Closed);
    }
    if state.catalog_version != version {
        return Err(ProtocolError::InvalidParams);
    }
    let expired: Vec<_> = state
        .cursors
        .iter()
        .filter(|(_, c)| c.expires <= now)
        .map(|(key, _)| key.clone())
        .collect();
    for key in expired {
        retired.extend(state.cursors.remove(&key));
    }
    let old = cursor
        .as_ref()
        .map(|key| state.cursors.get(key).ok_or(ProtocolError::InvalidParams))
        .transpose()?;
    let offset = if let Some(old) = old {
        if !Arc::ptr_eq(identity, &old.identity) || old.visible != names {
            return Err(ProtocolError::InvalidParams);
        }
        old.offset
    } else {
        0
    };
    let more = offset.saturating_add(inner.config.page_size) < visible.len();
    if more && state.cursors.len() - usize::from(old.is_some()) >= inner.config.max_cursors {
        return Err(ProtocolError::Busy);
    }
    let next = if more {
        state.next_cursor = state
            .next_cursor
            .checked_add(1)
            .ok_or(ProtocolError::Busy)?;
        Some(format!("{:x}.{:x}", inner.nonce, state.next_cursor))
    } else {
        None
    };
    let page = ToolPage {
        tools: visible
            .into_iter()
            .skip(offset)
            .take(inner.config.page_size)
            .collect(),
        next_cursor: next.clone(),
    };
    if serde_json::to_vec(&page)
        .map_err(|_| ProtocolError::Discovery)?
        .len()
        > inner.config.max_catalog_bytes
    {
        return Err(ProtocolError::Discovery);
    }
    if let Some(cursor) = cursor {
        retired.extend(state.cursors.remove(&cursor));
    }
    if let Some(key) = next {
        state.cursors.insert(
            key,
            Cursor {
                identity: identity.clone(),
                visible: names,
                offset: offset + page.tools.len(),
                expires: now + inner.config.cursor_ttl,
            },
        );
    }
    Ok(page)
}

/// Optional native lifecycle wiring. Host-owned services captured by callbacks
/// remain borrowed; use `Setup::get` in your assembly for exact-instance injection.
pub struct McpPlugin<I> {
    pub config: McpConfig,
    pub operations: Vec<Operation<I>>,
    pub policies: Policies<I>,
}
impl<I: Send + Sync + 'static> Plugin for McpPlugin<I> {
    type Service = McpAdapter<I>;
    fn validate(&self) -> Result<(), BoxError> {
        validate_catalog(&self.config, &self.operations).map_err(Into::into)
    }
    async fn setup(self, setup: &mut Setup) -> Result<Arc<Self::Service>, BoxError> {
        let adapter = Arc::new(McpAdapter::new(
            self.config,
            self.operations,
            self.policies,
            setup.work_gate(),
        )?);
        let cleanup = adapter.clone();
        setup.on_cleanup(CleanupPhase::Entrances, move || async move {
            cleanup.stop();
            Ok(())
        });
        Ok(adapter)
    }
}
