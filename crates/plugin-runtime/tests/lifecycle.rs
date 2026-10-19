use lenso_plugin_core::{App, BoxError, CleanupPhase, Handle, InstanceKey, Plugin, Setup};
use lenso_plugin_runtime::{start_runtime_app, RuntimeExport, RuntimePlugin};
use runtime_sdk::{BusinessError, CallOptions, Handlers, HostOptions, RuntimeHost, Target};
use serde_json::{json, Value};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    time::{timeout, Duration},
};

async fn read(peer: &mut TcpStream) -> Value {
    timeout(Duration::from_secs(2), async {
        loop {
            let frame = read_raw(peer).await.unwrap();
            if frame["type"] != "heartbeat" {
                return frame;
            }
        }
    })
    .await
    .unwrap()
}

async fn read_raw(peer: &mut TcpStream) -> std::io::Result<Value> {
    let mut bytes = Vec::new();
    loop {
        let byte = peer.read_u8().await?;
        if byte == b'\n' {
            break;
        }
        bytes.push(byte);
        assert!(bytes.len() < 262144);
    }
    Ok(serde_json::from_slice(&bytes).unwrap())
}

async fn write(peer: &mut TcpStream, frame: Value) {
    let mut bytes = serde_json::to_vec(&frame).unwrap();
    bytes.push(b'\n');
    peer.write_all(&bytes).await.unwrap();
}

async fn registered(listener: &TcpListener) -> TcpStream {
    let (mut peer, _) = listener.accept().await.unwrap();
    assert_eq!(read(&mut peer).await["type"], "hello");
    write(
        &mut peer,
        json!({"type":"welcome","version":1,"generation":"1","heartbeat_ms":1000}),
    )
    .await;
    assert_eq!(read(&mut peer).await["type"], "register");
    write(&mut peer, json!({"type":"registered"})).await;
    peer
}

fn options(listener: &TcpListener, exporting: bool) -> HostOptions {
    HostOptions {
        namespace: "test".into(),
        service: "native".into(),
        instance: "one".into(),
        port: listener.local_addr().unwrap().port(),
        contract: if exporting {
            json!({"version":"1","methods":{"value":{"mode":"unary","input":{"type":"null"},"output":{"type":"integer"},"error":{"type":"null"}}}})
        } else {
            json!({"version":"1","methods":{}})
        },
        max_concurrency: 8,
    }
}

struct Native {
    runtime: Handle<RuntimeHost>,
    cleaned: Arc<AtomicBool>,
}

impl Plugin for Native {
    type Service = u64;

    fn dependencies(&self) -> Vec<InstanceKey> {
        vec![self.runtime.key()]
    }

    async fn setup(self, setup: &mut Setup) -> Result<Arc<u64>, BoxError> {
        setup.on_cleanup(CleanupPhase::Resources, move || async move {
            self.cleaned.store(true, Ordering::SeqCst);
            Ok(())
        });
        let host = setup.get(&self.runtime)?;
        let value = host
            .call(
                &Target {
                    namespace: "test".into(),
                    service: "other".into(),
                    contract_version: "1".into(),
                },
                "value",
                &(),
                CallOptions::default(),
            )
            .await?;
        Ok(Arc::new(value))
    }
}

#[tokio::test]
async fn native_setup_uses_outbound_then_explicit_export_and_entrance_cleanup() {
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let cleaned = Arc::new(AtomicBool::new(false));
    let mut app = App::new();
    let runtime = app.install(
        "runtime",
        RuntimePlugin::new(options(&listener, true), 1000),
    );
    let native = app.install(
        "native",
        Native {
            runtime: runtime.clone(),
            cleaned: cleaned.clone(),
        },
    );
    let export = RuntimeExport::new(runtime, move |running| {
        let service = running.get(&native)?;
        let mut handlers = Handlers::new();
        handlers.unary("value", move |_: (), _| {
            let service = service.clone();
            async move { Ok::<_, BusinessError<()>>(*service) }
        });
        Ok(handlers)
    });
    let start = tokio::spawn(start_runtime_app(app, vec![export]));
    let mut peer = registered(&listener).await;
    let call = read(&mut peer).await;
    assert_eq!(call["type"], "call");
    write(
        &mut peer,
        json!({"type":"result","id":call["id"],"output":42}),
    )
    .await;
    assert_eq!(read(&mut peer).await["type"], "ready");
    assert!(!start.is_finished());
    write(&mut peer, json!({"type":"ready"})).await;
    let running = start.await.unwrap().unwrap();
    write(&mut peer, json!({"type":"invoke","id":"in","method":"value","input":null,"timeout_ms":1000,"trace_id":"trace","caller":{"namespace":"test","service":"caller","instance":"one"}})).await;
    assert_eq!(
        read(&mut peer).await,
        json!({"type":"result","id":"in","output":42})
    );
    let shutdown = tokio::spawn(async move { running.shutdown().await });
    assert_eq!(read(&mut peer).await["type"], "drain");
    assert!(!cleaned.load(Ordering::SeqCst));
    write(&mut peer, json!({"type":"drained"})).await;
    shutdown.await.unwrap().unwrap();
    assert!(cleaned.load(Ordering::SeqCst));
}

#[tokio::test]
async fn outbound_only_app_never_sends_ready() {
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let mut app = App::new();
    app.install(
        "runtime",
        RuntimePlugin::new(options(&listener, false), 1000),
    );
    let start = tokio::spawn(start_runtime_app(app, vec![]));
    let mut peer = registered(&listener).await;
    let running = start.await.unwrap().unwrap();
    let shutdown = tokio::spawn(async move { running.shutdown().await });
    assert_eq!(read(&mut peer).await["type"], "drain");
    write(&mut peer, json!({"type":"drained"})).await;
    shutdown.await.unwrap().unwrap();
}

#[tokio::test]
async fn incomplete_binding_rolls_back_native_resources_without_ready() {
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let cleaned = Arc::new(AtomicBool::new(false));
    let mut app = App::new();
    let runtime = app.install(
        "runtime",
        RuntimePlugin::new(options(&listener, true), 1000),
    );
    app.install(
        "native",
        Native {
            runtime: runtime.clone(),
            cleaned: cleaned.clone(),
        },
    );
    let export = RuntimeExport::new(runtime, |_| Ok(Handlers::new()));
    let start = tokio::spawn(start_runtime_app(app, vec![export]));
    let mut peer = registered(&listener).await;
    let call = read(&mut peer).await;
    assert_eq!(call["type"], "call");
    write(
        &mut peer,
        json!({"type":"result","id":call["id"],"output":42}),
    )
    .await;
    assert_eq!(read(&mut peer).await["type"], "drain");
    write(&mut peer, json!({"type":"drained"})).await;
    assert!(start.await.unwrap().is_err());
    assert!(cleaned.load(Ordering::SeqCst));
    timeout(Duration::from_secs(1), async {
        while let Ok(frame) = read_raw(&mut peer).await {
            assert_eq!(frame["type"], "heartbeat");
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn cancelling_starter_during_native_setup_still_cleans_up() {
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let cleaned = Arc::new(AtomicBool::new(false));
    let mut app = App::new();
    let runtime = app.install(
        "runtime",
        RuntimePlugin::new(options(&listener, false), 1000),
    );
    app.install(
        "native",
        Native {
            runtime,
            cleaned: cleaned.clone(),
        },
    );
    let start = tokio::spawn(start_runtime_app(app, vec![]));
    let mut peer = registered(&listener).await;
    let call = read(&mut peer).await;
    assert_eq!(call["type"], "call");
    start.abort();
    assert!(matches!(start.await, Err(error) if error.is_cancelled()));
    write(
        &mut peer,
        json!({"type":"result","id":call["id"],"output":42}),
    )
    .await;
    assert_eq!(read(&mut peer).await["type"], "drain");
    write(&mut peer, json!({"type":"drained"})).await;
    timeout(Duration::from_secs(1), async {
        while !cleaned.load(Ordering::SeqCst) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn invalid_runtime_configuration_prevents_native_setup_and_connection() {
    struct Probe(Arc<AtomicBool>);
    impl Plugin for Probe {
        type Service = ();
        async fn setup(self, _setup: &mut Setup) -> Result<Arc<()>, BoxError> {
            self.0.store(true, Ordering::SeqCst);
            Ok(Arc::new(()))
        }
    }
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let started = Arc::new(AtomicBool::new(false));
    let mut invalid = options(&listener, false);
    invalid.contract = json!({"version":"1","methods":{"broken":{}}});
    let mut app = App::new();
    app.install("probe", Probe(started.clone()));
    app.install("runtime", RuntimePlugin::new(invalid, 1000));
    assert!(start_runtime_app(app, vec![]).await.is_err());
    assert!(!started.load(Ordering::SeqCst));
    assert!(timeout(Duration::from_millis(25), listener.accept())
        .await
        .is_err());
}

#[tokio::test]
async fn panicking_export_factory_rolls_back_without_publishing_ready() {
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let cleaned = Arc::new(AtomicBool::new(false));
    let mut app = App::new();
    let runtime = app.install(
        "runtime",
        RuntimePlugin::new(options(&listener, true), 1000),
    );
    app.install(
        "native",
        Native {
            runtime: runtime.clone(),
            cleaned: cleaned.clone(),
        },
    );
    let export = RuntimeExport::new(runtime, |_| panic!("export factory panic"));
    let start = tokio::spawn(start_runtime_app(app, vec![export]));
    let mut peer = registered(&listener).await;
    let call = read(&mut peer).await;
    assert_eq!(call["type"], "call");
    write(
        &mut peer,
        json!({"type":"result","id":call["id"],"output":42}),
    )
    .await;
    assert_eq!(read(&mut peer).await["type"], "drain");
    write(&mut peer, json!({"type":"drained"})).await;
    let error = start.await.unwrap().err().unwrap();
    assert!(error.to_string().contains("activation_panicked"));
    assert!(cleaned.load(Ordering::SeqCst));
}

#[tokio::test]
async fn cancelling_after_runtime_delivery_is_queued_cleans_unacknowledged_app() {
    use std::{
        future::Future,
        task::{Context, Wake, Waker},
    };
    use tokio::sync::Semaphore;

    struct DeliveryWake(Arc<Semaphore>);
    impl Wake for DeliveryWake {
        fn wake(self: Arc<Self>) {
            self.0.add_permits(1);
        }
    }
    struct Resource(Arc<Semaphore>);
    impl Plugin for Resource {
        type Service = ();
        async fn setup(self, setup: &mut Setup) -> Result<Arc<()>, BoxError> {
            setup.on_cleanup(CleanupPhase::Resources, move || async move {
                self.0.add_permits(1);
                Ok(())
            });
            Ok(Arc::new(()))
        }
    }
    let cleaned = Arc::new(Semaphore::new(0));
    let delivered = Arc::new(Semaphore::new(0));
    let mut app = App::new();
    app.install("resource", Resource(cleaned.clone()));
    let mut start = Box::pin(start_runtime_app(app, vec![]));
    let waker = Waker::from(Arc::new(DeliveryWake(delivered.clone())));
    assert!(start
        .as_mut()
        .poll(&mut Context::from_waker(&waker))
        .is_pending());
    timeout(Duration::from_secs(2), delivered.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
    // Delivery woke its receiver, but that future has not been polled to take the App.
    drop(start);
    timeout(Duration::from_secs(2), cleaned.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
}

#[test]
fn static_inspection_validates_runtime_options_without_connecting_or_tokio() {
    let options = || HostOptions {
        namespace: "test".into(),
        service: "native".into(),
        instance: "static".into(),
        port: 1,
        contract: json!({"version":"1","methods":{}}),
        max_concurrency: 8,
    };
    let mut app = App::new();
    app.install("runtime", RuntimePlugin::new(options(), 1000));
    assert!(app.inspect().is_valid());
    let mut invalid = App::new();
    invalid.install("runtime", RuntimePlugin::new(options(), 0));
    assert_eq!(
        invalid.inspect().diagnostics[0].code,
        lenso_plugin_core::DiagnosticCode::InvalidConfig
    );
}
