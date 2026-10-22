//! Explicit Axum routes backed by exact Lenso service handles.
use axum::{
    body::{to_bytes, Body, Bytes},
    extract::{Request, State},
    http::StatusCode,
    middleware::{self, Next},
    response::{IntoResponse, Response},
    Json, Router,
};
use http_body::{Body as HttpBody, Frame};
use lenso_plugin_core::{
    BoxError, CleanupPhase, Handle, InstanceKey, Plugin, Setup, WorkGate, WorkGuard,
};
use serde::de::DeserializeOwned;
use std::future::Future;
use std::{
    net::SocketAddr,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};
use tokio::time::{timeout, Sleep};

/// Safe transport classifications; private service errors never become response text.
#[derive(Clone, Copy, Debug)]
pub enum HttpError {
    Invalid,
    Unauthorized,
    Forbidden,
    NotFound,
    TooLarge,
    Timeout,
    Closing,
    Internal,
}
impl IntoResponse for HttpError {
    fn into_response(self) -> Response {
        let (status, code) = match self {
            Self::Invalid => (StatusCode::BAD_REQUEST, "invalid_request"),
            Self::Unauthorized => (StatusCode::UNAUTHORIZED, "unauthorized"),
            Self::Forbidden => (StatusCode::FORBIDDEN, "forbidden"),
            Self::NotFound => (StatusCode::NOT_FOUND, "not_found"),
            Self::TooLarge => (StatusCode::PAYLOAD_TOO_LARGE, "body_too_large"),
            Self::Timeout => (StatusCode::REQUEST_TIMEOUT, "request_timeout"),
            Self::Closing => (StatusCode::SERVICE_UNAVAILABLE, "closing"),
            Self::Internal => (StatusCode::INTERNAL_SERVER_ERROR, "internal_error"),
        };
        (status, Json(serde_json::json!({"error": code}))).into_response()
    }
}
/// Applications own semantic validation and resource authorization.
pub trait Validate {
    fn validate(&self) -> Result<(), HttpError>;
}
pub fn json_input<T: DeserializeOwned + Validate>(bytes: &[u8]) -> Result<T, HttpError> {
    let input: T = serde_json::from_slice(bytes).map_err(|_| HttpError::Invalid)?;
    input.validate()?;
    Ok(input)
}
/// Clone the request extension into work explicitly spawned by the application.
/// The final clone prevents resource cleanup until that work really settles.
#[derive(Clone)]
pub struct AcceptedWork {
    _guard: Arc<WorkGuard>,
}
#[derive(Clone)]
pub struct Limits {
    pub body_bytes: usize,
    pub read_timeout: Duration,
    pub stream_idle_timeout: Duration,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            body_bytes: 1024 * 1024,
            read_timeout: Duration::from_secs(15),
            stream_idle_timeout: Duration::from_secs(30),
        }
    }
}
#[derive(Clone)]
struct Boundary {
    gate: WorkGate,
    limits: Limits,
}
async fn dispatch(State(boundary): State<Boundary>, mut request: Request, next: Next) -> Response {
    let work = match boundary.gate.enter() {
        Ok(g) => AcceptedWork {
            _guard: Arc::new(g),
        },
        Err(_) => return HttpError::Closing.into_response(),
    };
    request.extensions_mut().insert(work.clone());
    // Detach accepted execution from the HTTP waiter. Dropping a client future must
    // not interrupt a write or release its dependencies while it is still running.
    let task = tokio::spawn(async move {
        let (parts, body) = request.into_parts();
        let bytes = match timeout(
            boundary.limits.read_timeout,
            to_bytes(body, boundary.limits.body_bytes),
        )
        .await
        {
            Err(_) => return HttpError::Timeout.into_response(),
            Ok(Err(_)) => return HttpError::TooLarge.into_response(),
            Ok(Ok(bytes)) => bytes,
        };
        let response = next
            .run(Request::from_parts(parts, Body::from(bytes)))
            .await;
        let (parts, body) = response.into_parts();
        Response::from_parts(
            parts,
            Body::new(GuardedBody {
                body,
                work: Some(work),
                idle: Box::pin(tokio::time::sleep(boundary.limits.stream_idle_timeout)),
                duration: boundary.limits.stream_idle_timeout,
            }),
        )
    });
    task.await
        .unwrap_or_else(|_| HttpError::Internal.into_response())
}
struct GuardedBody {
    body: Body,
    work: Option<AcceptedWork>,
    idle: Pin<Box<Sleep>>,
    duration: Duration,
}
impl HttpBody for GuardedBody {
    type Data = Bytes;
    type Error = BoxError;
    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, BoxError>>> {
        if self.work.is_none() {
            return Poll::Ready(None);
        }
        if self.idle.as_mut().poll(cx).is_ready() {
            self.body = Body::empty();
            self.work.take();
            return Poll::Ready(Some(Err(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "stream idle timeout",
            )
            .into())));
        }
        match Pin::new(&mut self.body).poll_frame(cx) {
            Poll::Ready(Some(Ok(frame))) => {
                let deadline = tokio::time::Instant::now() + self.duration;
                self.idle.as_mut().reset(deadline);
                Poll::Ready(Some(Ok(frame)))
            }
            Poll::Ready(Some(Err(error))) => {
                self.body = Body::empty();
                self.work.take();
                Poll::Ready(Some(Err(error.into())))
            }
            Poll::Ready(None) => {
                self.work.take();
                Poll::Ready(None)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}
/// Thin plugin: no operation discovery, identity issuer, or second execution engine.
/// `listen = None` gives the router to a host which owns its listener and join.
pub struct WebPlugin<S, F> {
    pub service: Handle<S>,
    pub routes: F,
    pub limits: Limits,
    pub listen: Option<SocketAddr>,
}
pub struct Web {
    pub router: Router,
    pub local_addr: Option<SocketAddr>,
}
impl<S, F> Plugin for WebPlugin<S, F>
where
    S: Send + Sync + 'static,
    F: FnOnce(Arc<S>) -> Router + Send + 'static,
{
    type Service = Web;
    fn dependencies(&self) -> Vec<InstanceKey> {
        vec![self.service.key()]
    }
    fn validate(&self) -> Result<(), BoxError> {
        if self.limits.body_bytes == 0
            || self.limits.read_timeout.is_zero()
            || self.limits.stream_idle_timeout.is_zero()
        {
            return Err("HTTP limits must be positive".into());
        }
        Ok(())
    }
    async fn setup(self, setup: &mut Setup) -> Result<Arc<Web>, BoxError> {
        let router = (self.routes)(setup.get(&self.service)?)
            .fallback(|| async { HttpError::NotFound })
            .layer(middleware::from_fn_with_state(
                Boundary {
                    gate: setup.work_gate(),
                    limits: self.limits,
                },
                dispatch,
            ));
        let mut local_addr = None;
        if let Some(address) = self.listen {
            let listener = tokio::net::TcpListener::bind(address).await?;
            let (stop, stopped) = tokio::sync::oneshot::channel();
            setup.on_cleanup(CleanupPhase::Entrances, move || async move {
                let _ = stop.send(());
                Ok(())
            });
            local_addr = Some(listener.local_addr()?);
            let served = router.clone();
            let task = tokio::spawn(async move {
                axum::serve(listener, served)
                    .with_graceful_shutdown(async {
                        let _ = stopped.await;
                    })
                    .await
            });
            setup.on_cleanup(CleanupPhase::Resources, move || async move {
                task.await??;
                Ok(())
            });
        }
        Ok(Arc::new(Web { router, local_addr }))
    }
}
