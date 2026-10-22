use lenso_notifications::*;
use lenso_plugin_core::{App, Instance};
use std::sync::{Arc, Mutex};
use std::time::Duration;
#[derive(Default)]
struct Capture {
    calls: Mutex<Vec<(String, Message)>>,
    outcomes: Mutex<Vec<Outcome>>,
}
impl Transport for Capture {
    fn identity(&self) -> &str {
        "local-capture/account-a"
    }
    fn send(&self, k: &str, m: &Message) -> Outcome {
        self.calls.lock().unwrap().push((k.into(), m.clone()));
        self.outcomes
            .lock()
            .unwrap()
            .pop()
            .unwrap_or(Outcome::Accepted("fixture-id".into()))
    }
}
fn scope() -> Scope {
    Scope {
        tenant: "shop".into(),
        owner: "customer-1".into(),
    }
}
fn request() -> Request {
    Request {
        scope: scope(),
        business_key: "order-42".into(),
        to: "customer@example.invalid".into(),
        template: Template {
            name: "order".into(),
            version: 1,
            category: "orders".into(),
            required: false,
            subject: "Order {{id}}".into(),
            text: "Thanks for {{id}}".into(),
        },
        variables: [("id".into(), "42".into())].into(),
    }
}
#[tokio::test]
async fn persistence_conflict_preferences_and_scope() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("n.db");
    provision(&path).await.unwrap();
    let capture = Arc::new(Capture::default());
    let n = Notifications::open(&path, capture.clone()).await.unwrap();
    let r = n.create(request()).await.unwrap();
    assert_eq!(r, n.create(request()).await.unwrap());
    let mut changed = request();
    changed.template.version = 2;
    assert!(matches!(n.create(changed).await, Err(Error::Conflict)));
    let mut other = scope();
    other.tenant = "other".into();
    assert!(matches!(
        n.status(other.clone(), r.id).await,
        Err(Error::Missing)
    ));
    let mut other_request = request();
    other_request.scope = other;
    assert_ne!(n.create(other_request).await.unwrap().id, r.id);
    n.set_preference(scope(), "orders".into(), false)
        .await
        .unwrap();
    let mut disabled = request();
    disabled.business_key = "order-43".into();
    let suppressed = n.create(disabled.clone()).await.unwrap();
    assert_eq!(suppressed.state, State::Suppressed);
    assert_eq!(n.deliver(scope(), suppressed.id).await.unwrap(), suppressed);
    disabled.business_key = "required".into();
    disabled.template.required = true;
    assert_eq!(n.create(disabled).await.unwrap().state, State::Pending);
    let accepted = n.deliver(scope(), r.id).await.unwrap();
    assert_eq!(accepted.state, State::Accepted);
    assert_eq!(n.deliver(scope(), r.id).await.unwrap(), accepted);
    assert_eq!(capture.calls.lock().unwrap().len(), 1);
    n.close().await.unwrap();
    assert!(matches!(n.status(scope(), r.id).await, Err(Error::Closed)));
    let n = Notifications::open(&path, capture).await.unwrap();
    assert_eq!(n.status(scope(), r.id).await.unwrap(), accepted);
    assert_eq!(n.recover(scope(), 0, 2).await.unwrap().len(), 2);
    assert!(matches!(
        n.recover(scope(), 0, 101).await,
        Err(Error::Invalid)
    ));
    n.close().await.unwrap();
}
#[tokio::test]
async fn frozen_retries_and_unknown_never_replayed() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("n.db");
    provision(&path).await.unwrap();
    let capture = Arc::new(Capture::default());
    capture
        .outcomes
        .lock()
        .unwrap()
        .extend([Outcome::Unknown, Outcome::Rejected]);
    let n = Notifications::open(&path, capture.clone()).await.unwrap();
    let r = n.create(request()).await.unwrap();
    assert_eq!(
        n.deliver(scope(), r.id).await.unwrap().state,
        State::Rejected
    );
    let unknown = n.deliver(scope(), r.id).await.unwrap();
    assert_eq!(unknown.state, State::Unknown);
    assert_eq!(unknown.attempts.len(), 2);
    n.close().await.unwrap();
    let n = Notifications::open(&path, capture.clone()).await.unwrap();
    assert_eq!(n.deliver(scope(), r.id).await.unwrap(), unknown);
    let calls = capture.calls.lock().unwrap();
    assert_eq!(calls.len(), 2);
    assert_eq!(calls[0], calls[1]);
}
struct Slow {
    entered: tokio::sync::Notify,
}
impl Transport for Slow {
    fn identity(&self) -> &str {
        "slow-fixture"
    }
    fn send(&self, _: &str, _: &Message) -> Outcome {
        self.entered.notify_one();
        std::thread::sleep(Duration::from_millis(100));
        Outcome::Accepted("slow-id".into())
    }
}
#[tokio::test]
async fn cancelled_waiter_shutdown_drains_owned_delivery() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("n.db");
    provision(&path).await.unwrap();
    let transport = Arc::new(Slow {
        entered: tokio::sync::Notify::new(),
    });
    let instance = Instance::new(
        "notifications",
        NotificationsPlugin::new(&path, transport.clone()),
    );
    let handle = instance.handle();
    let app = App::new().with(instance).start().await.unwrap();
    let n = app.get(&handle).unwrap();
    let r = n.create(request()).await.unwrap();
    let sending = n.clone();
    let waiter = tokio::spawn(async move { sending.deliver(scope(), r.id).await });
    transport.entered.notified().await;
    waiter.abort();
    app.shutdown().await.unwrap();
    assert!(matches!(n.create(request()).await, Err(Error::Closed)));
    let reopened = Notifications::open(&path, transport).await.unwrap();
    assert_eq!(
        reopened.status(scope(), r.id).await.unwrap().state,
        State::Accepted
    );
}
#[tokio::test]
async fn two_connections_claim_once_and_provider_is_pinned() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("n.db");
    provision(&path).await.unwrap();
    let capture = Arc::new(Capture::default());
    let a = Notifications::open(&path, capture.clone()).await.unwrap();
    let b = Notifications::open(&path, capture.clone()).await.unwrap();
    let (r, s) = tokio::join!(a.create(request()), b.create(request()));
    let id = r.unwrap().id;
    assert_eq!(id, s.unwrap().id);
    let (x, y) = tokio::join!(a.deliver(scope(), id), b.deliver(scope(), id));
    x.unwrap();
    y.unwrap();
    assert_eq!(capture.calls.lock().unwrap().len(), 1);
    let mut next = request();
    next.business_key = "next".into();
    let id = a.create(next).await.unwrap().id;
    let other = Notifications::open(
        &path,
        Arc::new(Slow {
            entered: tokio::sync::Notify::new(),
        }),
    )
    .await
    .unwrap();
    assert!(matches!(
        other.deliver(scope(), id).await,
        Err(Error::ProviderChanged)
    ));
}

struct ObserveThenPanic(std::path::PathBuf, std::sync::atomic::AtomicBool);
impl Transport for ObserveThenPanic {
    fn identity(&self) -> &str {
        "panic-fixture"
    }
    fn send(&self, _: &str, _: &Message) -> Outcome {
        let db = rusqlite::Connection::open(&self.0).unwrap();
        let json: String = db
            .query_row("SELECT receipt FROM notifications", [], |r| r.get(0))
            .unwrap();
        let receipt: Receipt = serde_json::from_str(&json).unwrap();
        assert_eq!(receipt.state, State::Unknown);
        assert_eq!(receipt.attempts[0].state, State::Unknown);
        self.1.store(true, std::sync::atomic::Ordering::SeqCst);
        panic!("fixture interruption after durable claim")
    }
}
#[tokio::test]
async fn durable_uncertainty_precedes_transport_and_survives_panic() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("n.db");
    provision(&path).await.unwrap();
    let transport = Arc::new(ObserveThenPanic(
        path.clone(),
        std::sync::atomic::AtomicBool::new(false),
    ));
    let n = Notifications::open(&path, transport.clone()).await.unwrap();
    let id = n.create(request()).await.unwrap().id;
    let receipt = n.deliver(scope(), id).await.unwrap();
    assert_eq!(receipt.state, State::Unknown);
    assert!(transport.1.load(std::sync::atomic::Ordering::SeqCst));
    n.close().await.unwrap();
    let n = Notifications::open(&path, transport).await.unwrap();
    assert_eq!(n.deliver(scope(), id).await.unwrap(), receipt);
}
#[tokio::test]
async fn retries_are_bounded_and_invalid_inputs_fail_before_effects() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("n.db");
    provision(&path).await.unwrap();
    let capture = Arc::new(Capture::default());
    capture
        .outcomes
        .lock()
        .unwrap()
        .extend(vec![Outcome::Rejected; 10]);
    let n = Notifications::open(&path, capture.clone()).await.unwrap();
    let mut invalid = request();
    invalid.variables.clear();
    assert!(matches!(n.create(invalid).await, Err(Error::Invalid)));
    let mut invalid = request();
    invalid.template.text = "x".repeat(65537);
    assert!(matches!(n.create(invalid).await, Err(Error::Invalid)));
    let id = n.create(request()).await.unwrap().id;
    for _ in 0..12 {
        n.deliver(scope(), id).await.unwrap();
    }
    assert_eq!(n.status(scope(), id).await.unwrap().attempts.len(), 10);
    assert_eq!(capture.calls.lock().unwrap().len(), 10);
}
