use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

pub fn poll_once<F: Future>(future: Pin<&mut F>) -> Poll<F::Output> {
    future.poll(&mut Context::from_waker(
        futures_util::task::noop_waker_ref(),
    ))
}
