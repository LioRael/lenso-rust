use crate::{error::Result, local::Signals, ErrorCode, StorageError};
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};
use tokio::{
    fs::File,
    io::{AsyncRead, AsyncReadExt, ReadBuf},
    sync::{mpsc, oneshot},
};
use tokio_util::task::TaskTracker;

const CHUNK: usize = 64 * 1024;
type Reply = oneshot::Sender<Result<Vec<u8>>>;

/// Pull-driven download. At most one 64 KiB chunk is requested ahead of consumption.
/// Drop closes the reader; caller cancellation and `LocalStorage::close` also close
/// an unpolled body. IO errors contain a downcastable `StorageError` source.
pub struct DownloadBody {
    requests: Option<mpsc::UnboundedSender<Reply>>,
    pending: Option<oneshot::Receiver<Result<Vec<u8>>>>,
    buffer: Vec<u8>,
    offset: usize,
    signals: Signals,
    ended: bool,
}
impl DownloadBody {
    pub(crate) fn new(
        mut file: File,
        mut remaining: u64,
        signals: Signals,
        tracker: &TaskTracker,
    ) -> Self {
        let (requests, mut receive) = mpsc::unbounded_channel::<Reply>();
        let worker_signals = signals.clone();
        tracker.spawn(async move {
            loop {
                let request = tokio::select! {
                    biased;
                    _ = worker_signals.cancelled() => break,
                    request = receive.recv() => match request { Some(request) => request, None => break },
                };
                if remaining == 0 { let _ = request.send(Ok(Vec::new())); break; }
                let mut buffer = vec![0; remaining.min(CHUNK as u64) as usize];
                let result = match file.read(&mut buffer).await {
                    Ok(0) => Err(StorageError::new(ErrorCode::Provider)),
                    Ok(count) => {
                        remaining -= count as u64;
                        buffer.truncate(count);
                        worker_signals.check().map(|()| buffer)
                    }
                    Err(error) => Err(StorageError::io(error)),
                };
                let failed = result.is_err();
                if request.send(result).is_err() || failed { break; }
            }
        });
        Self {
            requests: Some(requests),
            pending: None,
            buffer: Vec::new(),
            offset: 0,
            signals,
            ended: false,
        }
    }
    fn finish(&mut self) {
        self.ended = true;
        self.requests = None;
        self.pending = None;
        self.buffer.clear();
    }
}
impl AsyncRead for DownloadBody {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        output: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let this = self.get_mut();
        if output.remaining() == 0 || this.ended {
            return Poll::Ready(Ok(()));
        }
        if let Err(error) = this.signals.check() {
            this.finish();
            return Poll::Ready(Err(error.into_io()));
        }
        loop {
            if this.offset < this.buffer.len() {
                let count = output.remaining().min(this.buffer.len() - this.offset);
                output.put_slice(&this.buffer[this.offset..this.offset + count]);
                this.offset += count;
                return Poll::Ready(Ok(()));
            }
            if this.ended {
                return Poll::Ready(Ok(()));
            }
            if this.pending.is_none() {
                let (reply, response) = oneshot::channel();
                if this
                    .requests
                    .as_ref()
                    .is_none_or(|sender| sender.send(reply).is_err())
                {
                    this.finish();
                    let error = this
                        .signals
                        .check()
                        .err()
                        .unwrap_or_else(|| StorageError::new(ErrorCode::Provider));
                    return Poll::Ready(Err(error.into_io()));
                }
                this.pending = Some(response);
            }
            match Pin::new(this.pending.as_mut().expect("pending request")).poll(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(response) => {
                    this.pending = None;
                    match response {
                        Ok(Ok(bytes)) if bytes.is_empty() => this.finish(),
                        Ok(Ok(bytes)) => {
                            this.buffer = bytes;
                            this.offset = 0;
                        }
                        response => {
                            let error = match response {
                                Ok(Err(error)) => error,
                                _ => this
                                    .signals
                                    .check()
                                    .err()
                                    .unwrap_or_else(|| StorageError::new(ErrorCode::Provider)),
                            };
                            this.finish();
                            return Poll::Ready(Err(error.into_io()));
                        }
                    }
                }
            }
        }
    }
}
