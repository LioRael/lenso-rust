//! Demand-driven SSE transport. Hosts own HTTP admission, identity and headers.
use bytes::Bytes;
use futures_util::{stream, Stream, StreamExt};
use lenso_realtime::{Connection, Envelope, Event};
use std::time::Duration;
use tokio::{
    io::{AsyncWrite, AsyncWriteExt},
    time::Instant,
};

pub const CONTENT_TYPE: &str = "text/event-stream; charset=utf-8";
pub const CACHE_CONTROL: &str = "no-cache, no-transform";

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("SSE replay unsupported; obtain fresh application state")]
    ReplayUnsupported,
    #[error("SSE heartbeat must be between one second and one hour")]
    InvalidHeartbeat,
    #[error("SSE encoding failed")]
    Encoding(#[from] serde_json::Error),
}

/// Fixed event names and compact JSON prevent data from injecting SSE fields.
/// Subscription identifiers are local capabilities, never SSE replay IDs.
pub fn encode(envelope: &Envelope) -> Result<Bytes, Error> {
    let name = match envelope.event {
        Event::Ready { .. } => "ready",
        Event::Update { .. } => "update",
        Event::Closed { .. } => "closed",
        Event::Gap { .. } => "gap",
    };
    Ok(Bytes::from(format!(
        "event: {name}\ndata: {}\n\n",
        serde_json::to_string(envelope)?
    )))
}

/// Owns the connection, with no queue or background task. Keep Subscription
/// handles alive in the host while consuming; renewal remains explicitly owned
/// by that host. Drop (including a cancelled writer/body) closes the connection.
pub struct Sse<P> {
    connection: Option<Connection<P>>,
    heartbeat: Duration,
    deadline: Instant,
}
impl<P: Send + Sync + 'static> Sse<P> {
    /// Reject any nonempty Last-Event-ID before sending HTTP success headers.
    /// Failure drops the supplied connection. No replay ID or retry is emitted.
    pub fn new(
        connection: Connection<P>,
        heartbeat: Duration,
        last_event_id: Option<&str>,
    ) -> Result<Self, Error> {
        if last_event_id.is_some_and(|id| !id.is_empty()) {
            return Err(Error::ReplayUnsupported);
        }
        if !(Duration::from_secs(1)..=Duration::from_secs(3600)).contains(&heartbeat) {
            return Err(Error::InvalidHeartbeat);
        }
        Ok(Self {
            connection: Some(connection),
            heartbeat,
            deadline: Instant::now() + heartbeat,
        })
    }

    /// Cancel-safe while waiting. Reads at most one envelope per call. Events
    /// (including expiry/EOF) win over a simultaneous heartbeat. No missed-tick
    /// backlog accumulates when a downstream consumer stops polling.
    pub async fn next_frame(&mut self) -> Option<Result<Bytes, Error>> {
        let connection = self.connection.as_mut()?;
        let frame = tokio::select! {
            biased;
            envelope = connection.recv() => match envelope {
                Some(envelope) => encode(&envelope),
                None => { self.connection.take(); return None; }
            },
            _ = tokio::time::sleep_until(self.deadline) => Ok(Bytes::from_static(b": heartbeat\n\n")),
        };
        self.deadline = Instant::now() + self.heartbeat;
        if frame.is_err() {
            self.connection.take();
        }
        Some(frame)
    }

    /// Compatible with Axum Body::from_stream. The host retains subscriptions
    /// and its accepted-work guard through body EOF/drop; do not admit again.
    pub fn into_stream(self) -> impl Stream<Item = Result<Bytes, Error>> + Send {
        stream::unfold(self, |mut sse| async move {
            sse.next_frame().await.map(|frame| (frame, sse))
        })
        .fuse()
    }

    /// Write raw SSE body bytes to an already established transport. Each write
    /// completes before reading again. HTTP framing/headers belong to the host.
    /// Error or cancellation drops this owned adapter immediately.
    pub async fn write_to<W: AsyncWrite + Unpin>(mut self, writer: &mut W) -> std::io::Result<()> {
        while let Some(frame) = self.next_frame().await {
            let frame = frame.map_err(std::io::Error::other)?;
            writer.write_all(&frame).await?;
            writer.flush().await?;
        }
        Ok(())
    }
}
