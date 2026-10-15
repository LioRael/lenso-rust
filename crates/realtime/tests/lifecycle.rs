use lenso_plugin_core::{
    App, BoxError, CleanupPhase, Handle, Instance, InstanceKey, Plugin, Setup,
};
use lenso_realtime::*;
use std::{
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
use tokio::time::Instant;

fn actor() -> Identity<()> {
    Identity {
        scope: "s".into(),
        subject: "a".into(),
        principal: (),
        expires_at: Instant::now() + Duration::from_secs(60),
    }
}
fn note() -> Resource {
    Resource::new("s", "note", "1").unwrap()
}
struct Policy {
    allowed: bool,
    calls: AtomicUsize,
}
struct PolicyPlugin(bool);
impl Plugin for PolicyPlugin {
    type Service = Policy;
    async fn setup(self, _: &mut Setup) -> Result<Arc<Policy>, BoxError> {
        Ok(Arc::new(Policy {
            allowed: self.0,
            calls: AtomicUsize::new(0),
        }))
    }
}
async fn authorize(
    policy: Arc<Policy>,
    _: Arc<Identity<()>>,
    _: Resource,
) -> Result<Option<Lease>, RealtimeError> {
    policy.calls.fetch_add(1, Ordering::SeqCst);
    Ok(policy.allowed.then_some(Lease {
        valid_until: Instant::now() + Duration::from_secs(10),
    }))
}

#[tokio::test]
async fn exact_policy_handles_distinguish_same_type_instances_and_shutdown_closes_retained_services(
) {
    let allow = Instance::new("policy-allow", PolicyPlugin(true));
    let deny = Instance::new("policy-deny", PolicyPlugin(false));
    let allow_handle = allow.handle();
    let deny_handle = deny.handle();
    let a = Instance::new(
        "realtime-allow",
        RealtimePlugin::with_dependency(RealtimeConfig::default(), allow.handle(), authorize),
    );
    let b = Instance::new(
        "realtime-deny",
        RealtimePlugin::with_dependency(RealtimeConfig::default(), deny.handle(), authorize),
    );
    let a_handle = a.handle();
    let b_handle = b.handle();
    // Intentionally list consumers before dependencies.
    let app = App::new()
        .with(a)
        .with(b)
        .with(deny)
        .with(allow)
        .start()
        .await
        .unwrap();
    let a = app.get(&a_handle).unwrap();
    let b = app.get(&b_handle).unwrap();
    let mut ca = a.connect(actor()).unwrap();
    let cb = b.connect(actor()).unwrap();
    let _sub = ca.subscribe(note()).await.unwrap();
    assert!(matches!(
        cb.subscribe(note()).await,
        Err(RealtimeError::Denied)
    ));
    assert_eq!(
        app.get(&allow_handle).unwrap().calls.load(Ordering::SeqCst),
        1
    );
    assert_eq!(
        app.get(&deny_handle).unwrap().calls.load(Ordering::SeqCst),
        1
    );
    app.shutdown().await.unwrap();
    assert!(matches!(
        ca.recv().await.unwrap().event,
        Event::Closed {
            reason: CloseReason::Shutdown,
            ..
        }
    ));
    assert!(ca.recv().await.is_none());
    assert!(matches!(a.connect(actor()), Err(RealtimeError::Closed)));
    assert!(matches!(b.connect(actor()), Err(RealtimeError::Closed)));
}

#[tokio::test]
async fn invalid_configuration_or_missing_exact_dependency_prevents_startup() {
    let called = Arc::new(AtomicBool::new(false));
    let rt = Instance::new(
        "invalid",
        RealtimePlugin::new(
            RealtimeConfig {
                max_topics: 0,
                ..RealtimeConfig::default()
            },
            {
                let called = called.clone();
                move |_: Arc<Identity<()>>, _| {
                    called.store(true, Ordering::SeqCst);
                    async { Ok(None) }
                }
            },
        ),
    );
    assert!(App::new().with(rt).start().await.is_err());
    assert!(!called.load(Ordering::SeqCst));
    let missing = Instance::new("missing", PolicyPlugin(true));
    let rt = Instance::new(
        "realtime",
        RealtimePlugin::with_dependency(RealtimeConfig::default(), missing.handle(), authorize),
    );
    assert!(App::new().with(rt).start().await.is_err());
}

struct FailAfterAcquire {
    realtime: Handle<MemoryRealtime<()>>,
    captured: Arc<Mutex<Option<Arc<MemoryRealtime<()>>>>>,
}
impl Plugin for FailAfterAcquire {
    type Service = ();
    fn dependencies(&self) -> Vec<InstanceKey> {
        vec![self.realtime.key()]
    }
    async fn setup(self, setup: &mut Setup) -> Result<Arc<()>, BoxError> {
        *self.captured.lock().unwrap() = Some(setup.get(&self.realtime)?);
        Err(std::io::Error::other("downstream setup failed").into())
    }
}

#[tokio::test]
async fn downstream_setup_failure_rolls_back_realtime() {
    let rt = Instance::new(
        "realtime",
        RealtimePlugin::new(RealtimeConfig::default(), |_: Arc<Identity<()>>, _| async {
            Ok(None)
        }),
    );
    let captured = Arc::new(Mutex::new(None));
    let failure = Instance::new(
        "failure",
        FailAfterAcquire {
            realtime: rt.handle(),
            captured: captured.clone(),
        },
    );
    assert!(App::new().with(failure).with(rt).start().await.is_err());
    let rt = captured.lock().unwrap().take().unwrap();
    assert!(matches!(rt.connect(actor()), Err(RealtimeError::Closed)));
    assert_eq!(rt.stats(), Stats::default());
}

struct Host {
    realtime: Handle<MemoryRealtime<()>>,
    drained: Arc<AtomicBool>,
}
impl Plugin for Host {
    type Service = (Arc<MemoryRealtime<()>>, lenso_plugin_core::WorkGate);
    fn dependencies(&self) -> Vec<InstanceKey> {
        vec![self.realtime.key()]
    }
    async fn setup(self, setup: &mut Setup) -> Result<Arc<Self::Service>, BoxError> {
        let rt = setup.get(&self.realtime)?;
        setup.on_cleanup(CleanupPhase::Resources, move || async move {
            assert!(self.drained.load(Ordering::SeqCst));
            Ok(())
        });
        Ok(Arc::new((rt, setup.work_gate())))
    }
}

#[tokio::test(start_paused = true)]
async fn entrance_cleanup_wakes_stream_before_global_work_drain() {
    let rt = Instance::new(
        "realtime",
        RealtimePlugin::new(RealtimeConfig::default(), |_: Arc<Identity<()>>, _| async {
            Ok(None)
        }),
    );
    let drained = Arc::new(AtomicBool::new(false));
    let host = Instance::new(
        "host",
        Host {
            realtime: rt.handle(),
            drained: drained.clone(),
        },
    );
    let host_handle = host.handle();
    let app = App::new().with(host).with(rt).start().await.unwrap();
    let host = app.get(&host_handle).unwrap();
    let (started, ready) = tokio::sync::oneshot::channel();
    let waiter = tokio::spawn(async move {
        let _work = host.1.enter().unwrap();
        let mut c = host.0.connect(actor()).unwrap();
        started.send(()).unwrap();
        assert!(matches!(
            c.recv().await.unwrap().event,
            Event::Closed {
                reason: CloseReason::Shutdown,
                ..
            }
        ));
        drained.store(true, Ordering::SeqCst);
    });
    ready.await.unwrap();
    tokio::time::timeout(Duration::from_secs(1), app.shutdown())
        .await
        .unwrap()
        .unwrap();
    waiter.await.unwrap();
}
