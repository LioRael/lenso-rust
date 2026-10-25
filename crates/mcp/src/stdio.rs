//! Official rmcp stdio bridge. SDK owns negotiation, protocol and cancellation.
use crate::{McpAdapter, Operation, ProtocolError, RequestContext};
use futures_util::Stream;
use rmcp::transport::async_rw::JsonRpcMessageCodec;
use rmcp::{
    model::{
        CallToolRequestParams, CallToolResponse, ClientJsonRpcMessage, ListToolsResult,
        PaginatedRequestParams, ProtocolVersion, ServerCapabilities, ServerConfig,
        ServerJsonRpcMessage,
    },
    service::RequestContext as SdkContext,
    ErrorData, Peer, RoleServer, ServerHandler, ServiceExt,
};
use std::{
    borrow::Cow,
    pin::Pin,
    sync::{Arc, Mutex},
    task::Poll,
};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_util::{
    codec::{FramedRead, FramedWrite},
    sync::CancellationToken,
};

/// One trusted local host identity per stdio session. Not a remote identity adapter.
pub struct McpServer<I> {
    adapter: McpAdapter<I>,
    identity: Arc<I>,
    peer: Arc<Mutex<Option<Peer<RoleServer>>>>,
}
impl<I> Clone for McpServer<I> {
    fn clone(&self) -> Self {
        Self {
            adapter: self.adapter.clone(),
            identity: self.identity.clone(),
            peer: self.peer.clone(),
        }
    }
}
impl<I: Send + Sync + 'static> McpServer<I> {
    pub fn new(adapter: McpAdapter<I>, identity: Arc<I>) -> Self {
        Self {
            adapter,
            identity,
            peer: Arc::new(Mutex::new(None)),
        }
    }
    fn context(&self, context: SdkContext<RoleServer>) -> RequestContext<I> {
        RequestContext {
            identity: self.identity.clone(),
            request_id: context.id.to_string(),
            cancellation: context.ct,
        }
    }
    /// Host lifecycle update, never a tool. Notify only after atomic replacement.
    /// Before initialization no notification is needed. After initialization,
    /// a send failure leaves the valid new catalog installed.
    ///
    /// # Errors
    /// Returns [`StdioError::Catalog`] if replacement is refused, or
    /// [`StdioError::Transport`] if notifying an initialized peer fails.
    pub async fn replace_operations(
        &self,
        operations: Vec<Operation<I>>,
    ) -> Result<(), StdioError> {
        self.adapter
            .replace_operations(operations)
            .map_err(|_| StdioError::Catalog)?;
        let peer = self.peer.lock().unwrap().clone();
        if let Some(peer) = peer {
            peer.notify_tool_list_changed()
                .await
                .map_err(|_| StdioError::Transport)?;
        }
        Ok(())
    }
    /// Run a dedicated local stdio process. Keep every application log on stderr.
    ///
    /// # Errors
    /// Returns the initialization or transport failures described by [`Self::run_io`].
    pub async fn run_stdio(self, shutdown: CancellationToken) -> Result<(), StdioError> {
        self.run_io(tokio::io::stdin(), tokio::io::stdout(), shutdown)
            .await
    }
    /// Same SDK bridge over host-owned pipes; no socket or listener is created.
    ///
    /// # Errors
    /// Returns [`StdioError`] when SDK initialization times out or negotiation or
    /// transport fails. Accepted adapter work is drained before returning.
    pub async fn run_io<R, W>(
        self,
        read: R,
        write: W,
        shutdown: CancellationToken,
    ) -> Result<(), StdioError>
    where
        R: AsyncRead + Send + Unpin + 'static,
        W: AsyncWrite + Send + Unpin + 'static,
    {
        let adapter = self.adapter.clone();
        let _stop_on_drop = StopOnDrop(adapter.clone());
        let mut input = FramedRead::new(
            read,
            JsonRpcMessageCodec::<ClientJsonRpcMessage>::new_with_max_length(1024 * 1024),
        );
        let eof_adapter = adapter.clone();
        // SDK codec parses bounded frames. EOF/error seals native admission before
        // SDK transport drain; no JSON-RPC parsing or dispatch is duplicated here.
        let input =
            futures_util::stream::poll_fn(move |cx| match Pin::new(&mut input).poll_next(cx) {
                Poll::Ready(Some(Ok(message))) => Poll::Ready(Some(message)),
                Poll::Ready(_) => {
                    eof_adapter.stop();
                    Poll::Ready(None)
                }
                Poll::Pending => Poll::Pending,
            });
        let output = FramedWrite::new(write, JsonRpcMessageCodec::<ServerJsonRpcMessage>::new());
        let serving = async {
            match tokio::time::timeout(
                std::time::Duration::from_secs(30),
                self.serve_with_ct((output, input), shutdown.clone()),
            )
            .await
            {
                Ok(Ok(running)) => running
                    .waiting()
                    .await
                    .map(|_| ())
                    .map_err(|_| StdioError::Transport),
                _ => Err(StdioError::Handshake),
            }
        };
        let result = tokio::select! {
            result = serving => result,
            _ = shutdown.cancelled() => { adapter.stop(); Ok(()) },
        };
        adapter.close().await;
        result
    }
}
struct StopOnDrop<I: Send + Sync + 'static>(McpAdapter<I>);
impl<I: Send + Sync + 'static> Drop for StopOnDrop<I> {
    fn drop(&mut self) {
        self.0.stop();
    }
}
#[derive(Debug, thiserror::Error)]
pub enum StdioError {
    #[error("MCP initialization failed or timed out.")]
    Handshake,
    #[error("MCP transport failed.")]
    Transport,
    #[error("MCP catalog update rejected.")]
    Catalog,
}
fn protocol_error(error: ProtocolError) -> ErrorData {
    ErrorData::new(
        rmcp::model::ErrorCode(error.code()),
        error.to_string(),
        None,
    )
}
impl<I: Send + Sync + 'static> ServerHandler for McpServer<I> {
    async fn on_initialized(&self, context: rmcp::service::NotificationContext<RoleServer>) {
        *self.peer.lock().unwrap() = Some(context.peer);
    }
    fn get_info(&self) -> ServerConfig {
        ServerConfig::new(
            ServerCapabilities::builder()
                .enable_tools()
                .enable_tool_list_changed()
                .build(),
        )
        .with_server_info(rmcp::model::Implementation::new(
            "lenso-mcp",
            env!("CARGO_PKG_VERSION"),
        ))
        .with_instructions(
            "Explicit native tools. Cancellation is cooperative and does not roll back effects.",
        )
    }
    fn supported_protocol_versions(&self) -> Cow<'static, [ProtocolVersion]> {
        Cow::Owned(vec![
            ProtocolVersion::V_2025_11_25,
            ProtocolVersion::V_2025_03_26,
            ProtocolVersion::V_2024_11_05,
        ])
    }
    async fn list_tools(
        &self,
        request: Option<PaginatedRequestParams>,
        context: SdkContext<RoleServer>,
    ) -> Result<ListToolsResult, ErrorData> {
        let page = self
            .adapter
            .list_tools(self.context(context), request.and_then(|r| r.cursor))
            .await
            .map_err(protocol_error)?;
        serde_json::from_value(
            serde_json::to_value(page)
                .map_err(|_| ErrorData::internal_error("Tool discovery failed.", None))?,
        )
        .map_err(|_| ErrorData::internal_error("Tool discovery failed.", None))
    }
    async fn call_tool(
        &self,
        request: CallToolRequestParams,
        context: SdkContext<RoleServer>,
    ) -> Result<CallToolResponse, ErrorData> {
        if request.input_responses.is_some() || request.request_state.is_some() {
            return Err(ErrorData::invalid_params(
                "Multi-round-trip calls are unsupported.",
                None,
            ));
        }
        let input = serde_json::Value::Object(request.arguments.unwrap_or_default());
        let result = self
            .adapter
            .call_tool(&request.name, input, self.context(context))
            .await
            .map_err(protocol_error)?;
        let result: rmcp::model::CallToolResult = serde_json::from_value(
            serde_json::to_value(result)
                .map_err(|_| ErrorData::internal_error("Tool result unavailable.", None))?,
        )
        .map_err(|_| ErrorData::internal_error("Tool result unavailable.", None))?;
        Ok(result.into())
    }
}
