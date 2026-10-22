use axum::{
    body::{to_bytes, Body},
    http::{Request, StatusCode},
    routing::get,
    Router,
};
use lenso_plugin_core::{App, BoxError, CleanupPhase, Instance, Plugin, Setup};
use lenso_web_axum::{Limits, WebPlugin};
use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::sync::Notify;
use tower::ServiceExt;
struct Business(Arc<AtomicBool>);
impl Plugin for Business {
    type Service = Self;
    async fn setup(self, setup: &mut Setup) -> Result<Arc<Self>, BoxError> {
        let closed = self.0.clone();
        setup.on_cleanup(CleanupPhase::Resources, move || async move {
            closed.store(true, Ordering::SeqCst);
            Ok(())
        });
        Ok(Arc::new(self))
    }
}
#[tokio::test]
async fn response_body_drains_before_resources_and_closing_rejects() {
    let closed = Arc::new(AtomicBool::new(false));
    let service = Instance::new("business", Business(closed.clone()));
    let web = Instance::new(
        "http",
        WebPlugin {
            service: service.handle(),
            routes: |_| Router::new().route("/", get(|| async { "payload" })),
            limits: Limits::default(),
            listen: None,
        },
    );
    let handle = web.handle();
    let app = App::new().with(service).with(web).start().await.unwrap();
    let router = app.get(&handle).unwrap().router.clone();
    let response = router
        .clone()
        .oneshot(Request::new(Body::empty()))
        .await
        .unwrap();
    let shutdown = tokio::spawn({
        let app = app.clone();
        async move { app.shutdown().await }
    });
    while app.work_gate().enter().is_ok() {
        tokio::task::yield_now().await;
    }
    assert!(!closed.load(Ordering::SeqCst));
    assert_eq!(
        router
            .oneshot(Request::new(Body::empty()))
            .await
            .unwrap()
            .status(),
        StatusCode::SERVICE_UNAVAILABLE
    );
    assert_eq!(to_bytes(response.into_body(), 20).await.unwrap(), "payload");
    shutdown.await.unwrap().unwrap();
    assert!(closed.load(Ordering::SeqCst));
}
#[tokio::test]
async fn canceled_waiter_does_not_cancel_accepted_handler() {
    let closed = Arc::new(AtomicBool::new(false));
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let service = Instance::new("business", Business(closed.clone()));
    let web = Instance::new(
        "http",
        WebPlugin {
            service: service.handle(),
            routes: {
                let entered = entered.clone();
                let release = release.clone();
                move |_| {
                    Router::new().route(
                        "/",
                        get(move || {
                            let entered = entered.clone();
                            let release = release.clone();
                            async move {
                                entered.notify_one();
                                release.notified().await;
                                "done"
                            }
                        }),
                    )
                }
            },
            limits: Limits::default(),
            listen: None,
        },
    );
    let handle = web.handle();
    let app = App::new().with(service).with(web).start().await.unwrap();
    let router = app.get(&handle).unwrap().router.clone();
    let caller = tokio::spawn(router.oneshot(Request::new(Body::empty())));
    entered.notified().await;
    caller.abort();
    let _ = caller.await;
    let shutdown = tokio::spawn({
        let app = app.clone();
        async move { app.shutdown().await }
    });
    while app.work_gate().enter().is_ok() {
        tokio::task::yield_now().await;
    }
    assert!(!closed.load(Ordering::SeqCst));
    release.notify_one();
    tokio::time::timeout(Duration::from_secs(2), shutdown)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(closed.load(Ordering::SeqCst));
}

#[tokio::test]
async fn dropping_unpolled_stream_releases_admission() {
    let closed = Arc::new(AtomicBool::new(false));
    let service = Instance::new("business", Business(closed.clone()));
    let web = Instance::new(
        "http",
        WebPlugin {
            service: service.handle(),
            routes: |_| Router::new().route("/", get(|| async { "stream" })),
            limits: Limits::default(),
            listen: None,
        },
    );
    let handle = web.handle();
    let app = App::new().with(service).with(web).start().await.unwrap();
    let response = app
        .get(&handle)
        .unwrap()
        .router
        .clone()
        .oneshot(Request::new(Body::empty()))
        .await
        .unwrap();
    drop(response);
    tokio::time::timeout(Duration::from_secs(2), app.shutdown())
        .await
        .unwrap()
        .unwrap();
    assert!(closed.load(Ordering::SeqCst));
}

struct PendingBody;
impl http_body::Body for PendingBody {
    type Data = axum::body::Bytes;
    type Error = std::convert::Infallible;
    fn poll_frame(
        self: std::pin::Pin<&mut Self>,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Result<http_body::Frame<Self::Data>, Self::Error>>> {
        std::task::Poll::Pending
    }
}
#[tokio::test]
async fn stalled_upload_and_stream_are_bounded() {
    let closed = Arc::new(AtomicBool::new(false));
    let service = Instance::new("business", Business(closed.clone()));
    let web = Instance::new(
        "http",
        WebPlugin {
            service: service.handle(),
            routes: |_| Router::new().route("/", get(|| async { Body::new(PendingBody) })),
            limits: Limits {
                read_timeout: Duration::from_millis(10),
                stream_idle_timeout: Duration::from_millis(10),
                ..Limits::default()
            },
            listen: None,
        },
    );
    let handle = web.handle();
    let app = App::new().with(service).with(web).start().await.unwrap();
    let router = app.get(&handle).unwrap().router.clone();
    let upload = router
        .clone()
        .oneshot(Request::new(Body::new(PendingBody)))
        .await
        .unwrap();
    assert_eq!(upload.status(), StatusCode::REQUEST_TIMEOUT);
    drop(upload);
    let response = router.oneshot(Request::new(Body::empty())).await.unwrap();
    assert!(to_bytes(response.into_body(), 20).await.is_err());
    tokio::time::timeout(Duration::from_secs(2), app.shutdown())
        .await
        .unwrap()
        .unwrap();
}
