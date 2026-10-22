//! In-process HTTP/SSE composition: no listener, network or credentials.
use axum::{
    body::Body,
    extract::State,
    http::{HeaderMap, Request, StatusCode},
    response::{IntoResponse, Response},
    routing::get,
    Router,
};
use futures_util::StreamExt;
use http_body_util::BodyExt;
use lenso_plugin_core::{App, BoxError, CleanupPhase, Instance, Plugin, Setup};
use lenso_realtime::{Identity, Lease, MemoryRealtime, RealtimeConfig, Resource};
use lenso_realtime_sse::{Sse, CACHE_CONTROL, CONTENT_TYPE};
use lenso_web_axum::{HttpError, Limits, WebPlugin};
use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::time::{timeout, Instant};
use tower::ServiceExt;
struct Notes {
    realtime: MemoryRealtime<bool>,
    closed: Arc<AtomicBool>,
}
impl Plugin for Notes {
    type Service = Self;
    async fn setup(self, setup: &mut Setup) -> Result<Arc<Self>, BoxError> {
        let closed = self.closed.clone();
        setup.on_cleanup(CleanupPhase::Resources, move || async move {
            closed.store(true, Ordering::SeqCst);
            Ok(())
        });
        Ok(Arc::new(self))
    }
}
async fn events(
    State(notes): State<Arc<Notes>>,
    headers: HeaderMap,
) -> Result<Response, HttpError> {
    if headers
        .get("x-fixture-reader")
        .and_then(|v| v.to_str().ok())
        != Some("allowed")
    {
        return Err(HttpError::Unauthorized);
    }
    let connection = notes
        .realtime
        .connect(Identity {
            scope: "demo".into(),
            subject: "reader".into(),
            principal: true,
            expires_at: Instant::now() + Duration::from_secs(60),
        })
        .map_err(|_| HttpError::Internal)?;
    let subscription = connection
        .subscribe(Resource::new("demo", "note", "n1").unwrap())
        .await
        .map_err(|_| HttpError::Forbidden)?;
    let sse = Sse::new(
        connection,
        Duration::from_secs(5),
        headers.get("last-event-id").and_then(|v| v.to_str().ok()),
    )
    .map_err(|_| HttpError::Invalid)?;
    let stream = sse.into_stream().map(move |frame| {
        let _keep_subscription = &subscription;
        frame
    });
    Ok((
        [
            ("content-type", CONTENT_TYPE),
            ("cache-control", CACHE_CONTROL),
        ],
        Body::from_stream(stream),
    )
        .into_response())
}
#[tokio::test]
async fn axum_sse_retains_admission_until_body_drop_and_cleans_connections() {
    let closed = Arc::new(AtomicBool::new(false));
    let realtime = MemoryRealtime::new(
        RealtimeConfig::default(),
        |identity: Arc<Identity<bool>>, _| async move {
            Ok(identity.principal.then_some(Lease {
                valid_until: Instant::now() + Duration::from_secs(30),
            }))
        },
    )
    .unwrap();
    let notes = Instance::new(
        "notes",
        Notes {
            realtime: realtime.clone(),
            closed: closed.clone(),
        },
    );
    let web = Instance::new(
        "http",
        WebPlugin {
            service: notes.handle(),
            routes: |notes| {
                Router::new()
                    .route("/notes/n1/events", get(events))
                    .with_state(notes)
            },
            limits: Limits::default(),
            listen: None,
        },
    );
    let handle = web.handle();
    let app = App::new().with(notes).with(web).start().await.unwrap();
    let router = app.get(&handle).unwrap().router.clone();
    let request = || {
        Request::get("/notes/n1/events")
            .header("x-fixture-reader", "allowed")
            .body(Body::empty())
            .unwrap()
    };
    let denied = router
        .clone()
        .oneshot(
            Request::get("/notes/n1/events")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(denied.status(), StatusCode::UNAUTHORIZED);
    drop(denied);
    let mut replay = request();
    replay
        .headers_mut()
        .insert("last-event-id", "unsupported".parse().unwrap());
    let rejected = router.clone().oneshot(replay).await.unwrap();
    assert_eq!(rejected.status(), StatusCode::BAD_REQUEST);
    drop(rejected);
    assert_eq!(realtime.stats().connections, 0);
    let response = router.clone().oneshot(request()).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()["content-type"], CONTENT_TYPE);
    let mut body = response.into_body();
    let ready = timeout(Duration::from_secs(2), body.frame())
        .await
        .unwrap()
        .unwrap()
        .unwrap()
        .into_data()
        .unwrap();
    assert!(ready.starts_with(b"event: ready\n"));
    realtime
        .publish(
            &Resource::new("demo", "note", "n1").unwrap(),
            "note.updated",
            serde_json::json!({"title":"Release checklist","revision":2}),
        )
        .unwrap();
    let update = timeout(Duration::from_secs(2), body.frame())
        .await
        .unwrap()
        .unwrap()
        .unwrap()
        .into_data()
        .unwrap();
    assert!(update.starts_with(b"event: update\n"));
    assert!(std::str::from_utf8(&update)
        .unwrap()
        .contains("Release checklist"));
    let shutdown = tokio::spawn({
        let app = app.clone();
        async move { app.shutdown().await }
    });
    timeout(Duration::from_secs(2), async {
        while app.work_gate().enter().is_ok() {
            tokio::task::yield_now().await
        }
    })
    .await
    .unwrap();
    assert!(!closed.load(Ordering::SeqCst));
    assert!(!shutdown.is_finished());
    let closing = router.oneshot(request()).await.unwrap();
    assert_eq!(closing.status(), StatusCode::SERVICE_UNAVAILABLE);
    drop(closing);
    drop(body);
    timeout(Duration::from_secs(2), shutdown)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(closed.load(Ordering::SeqCst));
    assert_eq!(realtime.stats().connections, 0);
    assert_eq!(realtime.stats().subscriptions, 0);
    realtime.close();
}
