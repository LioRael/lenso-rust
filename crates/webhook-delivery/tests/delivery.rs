use hmac::{Hmac, KeyInit, Mac};
use lenso_plugin_core::{App, Instance};
use lenso_tasks::{EnqueueOptions, JobState, RetryPolicy, TasksPlugin};
use lenso_webhook_delivery::*;
use sha2::Sha256;
use std::time::Duration;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    sync::mpsc,
};

const KEY: &[u8] = b"fixture-only-memory-signing-key-32";
const RAW: &[u8] = b"{ \"type\": \"order.paid\", \"order\": 42 }\n";
struct Received {
    id: String,
    body: Vec<u8>,
}
async fn fixture(
    responses: Vec<(u16, Duration)>,
) -> (u16, mpsc::Receiver<Received>, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let (send, recv) = mpsc::channel(10);
    let task = tokio::spawn(async move {
        for (status, delay) in responses {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut data = Vec::new();
            let header_end = loop {
                let mut chunk = [0; 1024];
                let n = socket.read(&mut chunk).await.unwrap();
                assert!(n > 0);
                data.extend_from_slice(&chunk[..n]);
                if let Some(index) = data.windows(4).position(|w| w == b"\r\n\r\n") {
                    break index + 4;
                }
            };
            let headers = String::from_utf8(data[..header_end].to_vec()).unwrap();
            let header = |name: &str| {
                headers
                    .lines()
                    .find_map(|line| {
                        line.split_once(':')
                            .filter(|(k, _)| k.eq_ignore_ascii_case(name))
                            .map(|(_, v)| v.trim().to_owned())
                    })
                    .unwrap()
            };
            let len: usize = header("Content-Length").parse().unwrap();
            while data.len() < header_end + len {
                let mut chunk = [0; 1024];
                let n = socket.read(&mut chunk).await.unwrap();
                assert!(n > 0);
                data.extend_from_slice(&chunk[..n]);
            }
            let body = data[header_end..header_end + len].to_vec();
            let id = header("Webhook-Event-Id");
            assert_eq!(header("Webhook-Key-Id"), "fixture");
            // Receiver independently verifies exact raw bytes; no JSON round trip.
            let signature = header("Authorization");
            let hex = signature.strip_prefix("LensoWebhook v1=").unwrap();
            let bytes: Vec<u8> = (0..hex.len())
                .step_by(2)
                .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).unwrap())
                .collect();
            let mut mac = Hmac::<Sha256>::new_from_slice(KEY).unwrap();
            mac.update(b"v1\n");
            mac.update(id.as_bytes());
            mac.update(b"\n");
            mac.update(&body);
            mac.verify_slice(&bytes).unwrap();
            send.send(Received { id, body }).await.unwrap();
            tokio::time::sleep(delay).await;
            let response=format!("HTTP/1.1 {status} Fixture\r\nContent-Length: 0\r\nLocation: http://169.254.169.254/latest/meta-data/\r\nConnection: close\r\n\r\n");
            let _ = socket.write_all(response.as_bytes()).await;
        }
    });
    (port, recv, task)
}
fn endpoint(port: u16, timeout: Duration) -> Endpoint {
    Endpoint::local_fixture(port, KEY.to_vec(), timeout).unwrap()
}

#[tokio::test]
async fn order_consumer_tasks_retry_raw_signature_and_restart() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("delivery.db");
    let qp = dir.path().join("tasks.db");
    provision(&path).await.unwrap();
    lenso_tasks::provision(&qp, "orders").await.unwrap();
    let (port, mut received, server) =
        fixture(vec![(500, Duration::ZERO), (204, Duration::ZERO)]).await;
    let instance = Instance::new(
        "webhooks",
        DeliveryPlugin::new(&path, endpoint(port, Duration::from_secs(1))),
    );
    let handle = instance.handle();
    let app = App::new().with(instance).start().await.unwrap();
    let delivery = app.get(&handle).unwrap();
    let initial = delivery
        .create("order-42-paid".into(), RAW.to_vec(), 3)
        .await
        .unwrap();
    assert_eq!(
        initial,
        delivery
            .create("order-42-paid".into(), RAW.to_vec(), 3)
            .await
            .unwrap()
    );
    assert!(matches!(
        delivery
            .create("order-42-paid".into(), b"{}".to_vec(), 3)
            .await,
        Err(Error::Conflict)
    ));
    let task = delivery.task(
        "order-paid",
        RetryPolicy {
            max_attempts: 3,
            delay_ms: 0,
            ..Default::default()
        },
    );
    let queue = Instance::new("tasks", TasksPlugin::new(&qp, "orders", vec![task]));
    let qh = queue.handle();
    let tasks_app = App::new().with(queue).start().await.unwrap();
    let tasks = tasks_app.get(&qh).unwrap();
    let job = tasks
        .enqueue(
            "order-paid",
            serde_json::json!("order-42-paid"),
            EnqueueOptions {
                deduplication_key: Some("order-42-paid".into()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    tasks.run_one().await.unwrap();
    assert_eq!(
        delivery.status("order-42-paid".into()).await.unwrap().state,
        State::Retryable
    );
    tasks.run_one().await.unwrap();
    assert_eq!(
        tasks.get(job).await.unwrap().unwrap().state,
        JobState::Succeeded
    );
    for _ in 0..2 {
        let r = received.recv().await.unwrap();
        assert_eq!(r.id, "order-42-paid");
        assert_eq!(r.body, RAW);
    }
    assert_eq!(
        delivery
            .attempts("order-42-paid".into())
            .await
            .unwrap()
            .iter()
            .map(|a| a.http_status)
            .collect::<Vec<_>>(),
        vec![Some(500), Some(204)]
    );
    server.await.unwrap();
    tasks_app.shutdown().await.unwrap();
    app.shutdown().await.unwrap();
    assert!(matches!(
        delivery.status("order-42-paid".into()).await,
        Err(Error::Closed)
    ));
    let instance = Instance::new(
        "reopened",
        DeliveryPlugin::new(&path, endpoint(port, Duration::from_secs(1))),
    );
    let handle = instance.handle();
    let app = App::new().with(instance).start().await.unwrap();
    let delivery = app.get(&handle).unwrap();
    let receipt = delivery
        .deliver("order-42-paid".into(), None)
        .await
        .unwrap();
    assert_eq!(receipt.state, State::Delivered);
    assert_eq!(receipt.attempts, 2);
    app.shutdown().await.unwrap();
}

#[tokio::test]
async fn timeout_unknown_is_not_automatically_retried_and_replay_is_explicit() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    provision(&path).await.unwrap();
    let (port, mut received, server) = fixture(vec![
        (204, Duration::from_millis(150)),
        (204, Duration::ZERO),
    ])
    .await;
    let i = Instance::new(
        "delivery",
        DeliveryPlugin::new(&path, endpoint(port, Duration::from_millis(50))),
    );
    let h = i.handle();
    let app = App::new().with(i).start().await.unwrap();
    let d = app.get(&h).unwrap();
    d.create("timeout".into(), RAW.to_vec(), 3).await.unwrap();
    assert_eq!(
        d.deliver("timeout".into(), None).await.unwrap().state,
        State::Unknown
    );
    assert_eq!(received.recv().await.unwrap().id, "timeout");
    assert_eq!(d.deliver("timeout".into(), None).await.unwrap().attempts, 1);
    tokio::time::sleep(Duration::from_millis(160)).await;
    d.replay("timeout".into(), ReplayConsent::AcceptPossibleDuplicate)
        .await
        .unwrap();
    assert_eq!(
        d.deliver("timeout".into(), None).await.unwrap().state,
        State::Delivered
    );
    assert_eq!(received.recv().await.unwrap().id, "timeout");
    assert_eq!(
        d.create("timeout".into(), RAW.to_vec(), 3)
            .await
            .unwrap()
            .state,
        State::Delivered
    );
    assert_eq!(
        d.attempts("timeout".into()).await.unwrap()[0].outcome,
        "unknown"
    );
    server.await.unwrap();
    app.shutdown().await.unwrap();
}

#[tokio::test]
async fn shutdown_drains_dropped_waiter_and_marks_inflight_unknown() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    provision(&path).await.unwrap();
    let (port, mut received, server) = fixture(vec![(204, Duration::from_millis(150))]).await;
    let i = Instance::new(
        "delivery",
        DeliveryPlugin::new(&path, endpoint(port, Duration::from_secs(1))),
    );
    let h = i.handle();
    let app = App::new().with(i).start().await.unwrap();
    let d = app.get(&h).unwrap();
    d.create("cancel".into(), RAW.to_vec(), 3).await.unwrap();
    let sender = d.clone();
    let waiter = tokio::spawn(async move { sender.deliver("cancel".into(), None).await });
    received.recv().await.unwrap();
    waiter.abort();
    app.shutdown().await.unwrap();
    server.await.unwrap();
    let db = rusqlite::Connection::open(&path).unwrap();
    assert_eq!(
        db.query_row("SELECT state FROM deliveries", [], |r| r
            .get::<_, String>(0))
            .unwrap(),
        "unknown"
    );
    assert_eq!(
        db.query_row("SELECT outcome FROM attempts", [], |r| r
            .get::<_, String>(0))
            .unwrap(),
        "unknown"
    );
}

#[tokio::test]
async fn interrupted_claim_recovery_pause_and_redirect_boundary() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    provision(&path).await.unwrap();
    let (port, mut received, server) = fixture(vec![
        (500, Duration::ZERO),
        (500, Duration::ZERO),
        (302, Duration::ZERO),
    ])
    .await;
    let i = Instance::new(
        "delivery",
        DeliveryPlugin::new(&path, endpoint(port, Duration::from_secs(1))),
    );
    let h = i.handle();
    let app = App::new().with(i).start().await.unwrap();
    let d = app.get(&h).unwrap();
    d.create("exhausted".into(), RAW.to_vec(), 2).await.unwrap();
    assert_eq!(
        d.deliver("exhausted".into(), None).await.unwrap().state,
        State::Retryable
    );
    assert_eq!(
        d.deliver("exhausted".into(), None).await.unwrap().state,
        State::Paused
    );
    assert_eq!(
        d.deliver("exhausted".into(), None).await.unwrap().attempts,
        2
    );
    for _ in 0..2 {
        assert_eq!(received.recv().await.unwrap().id, "exhausted");
    }
    d.create("redirect".into(), RAW.to_vec(), 3).await.unwrap();
    d.pause("redirect".into()).await.unwrap();
    assert_eq!(
        d.deliver("redirect".into(), None).await.unwrap().attempts,
        0
    );
    d.replay("redirect".into(), ReplayConsent::AcceptPossibleDuplicate)
        .await
        .unwrap();
    assert_eq!(
        d.deliver("redirect".into(), None).await.unwrap().state,
        State::Paused
    );
    received.recv().await.unwrap();
    server.await.unwrap();
    d.create("crashed".into(), RAW.to_vec(), 3).await.unwrap();
    app.shutdown().await.unwrap();
    // Durable pre-send marker left by a killed process, no request is sent by recovery.
    let db = rusqlite::Connection::open(&path).unwrap();
    db.execute(
        "UPDATE deliveries SET state='sending',attempts=1 WHERE event='crashed'",
        [],
    )
    .unwrap();
    db.execute(
        "INSERT INTO attempts VALUES('crashed',1,'sending',NULL)",
        [],
    )
    .unwrap();
    drop(db);
    let i = Instance::new(
        "recovered",
        DeliveryPlugin::new(&path, endpoint(port, Duration::from_secs(1))),
    );
    let h = i.handle();
    let app = App::new().with(i).start().await.unwrap();
    let d = app.get(&h).unwrap();
    assert_eq!(
        d.deliver("crashed".into(), None).await.unwrap().state,
        State::Unknown
    );
    assert_eq!(
        d.attempts("crashed".into()).await.unwrap()[0].outcome,
        "unknown"
    );
    app.shutdown().await.unwrap();
}

#[test]
fn production_endpoint_rejects_unsafe_destinations() {
    for address in [
        "127.0.0.1",
        "10.1.2.3",
        "169.254.169.254",
        "172.16.0.1",
        "192.168.1.1",
        "100.100.100.200",
        "0.0.0.0",
        "224.0.0.1",
        "::1",
        "fc00::1",
        "fe80::1",
        "::ffff:127.0.0.1",
        "2001:db8::1",
    ] {
        assert!(
            Endpoint::https(
                "https://hooks.example.org/events",
                address.parse().unwrap(),
                "key",
                KEY.to_vec(),
                Duration::from_secs(1)
            )
            .is_err(),
            "{address}"
        );
    }
    for url in [
        "http://example.org/events",
        "https://localhost/events",
        "https://user:secret@example.org/events",
        "https://example.org/events?secret=value",
        "https://example.org/events#fragment",
        "https://127.0.0.1/events",
    ] {
        assert!(
            Endpoint::https(
                url,
                "8.8.8.8".parse().unwrap(),
                "key",
                KEY.to_vec(),
                Duration::from_secs(1)
            )
            .is_err(),
            "{url}"
        );
    }
    assert!(Endpoint::https(
        "https://hooks.example.org/events",
        "8.8.8.8".parse().unwrap(),
        "key",
        KEY.to_vec(),
        Duration::from_secs(1)
    )
    .is_ok());
}

#[tokio::test]
async fn tasks_cancellation_retains_unknown_without_resend() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let qp = dir.path().join("tasks");
    provision(&path).await.unwrap();
    lenso_tasks::provision(&qp, "cancel").await.unwrap();
    let (port, mut received, server) = fixture(vec![(204, Duration::from_millis(180))]).await;
    let i = Instance::new(
        "delivery",
        DeliveryPlugin::new(&path, endpoint(port, Duration::from_secs(1))),
    );
    let h = i.handle();
    let app = App::new().with(i).start().await.unwrap();
    let d = app.get(&h).unwrap();
    d.create("cancel-job".into(), RAW.to_vec(), 3)
        .await
        .unwrap();
    let queue = Instance::new(
        "tasks",
        TasksPlugin::new(
            &qp,
            "cancel",
            vec![d.task(
                "deliver",
                RetryPolicy {
                    max_attempts: 3,
                    ..Default::default()
                },
            )],
        )
        .worker_options(lenso_tasks::WorkerOptions {
            lease_ms: 1000,
            heartbeat_ms: 10,
            execution_timeout_ms: 2000,
        }),
    );
    let qh = queue.handle();
    let qa = App::new().with(queue).start().await.unwrap();
    let q = qa.get(&qh).unwrap();
    let job = q
        .enqueue(
            "deliver",
            serde_json::json!("cancel-job"),
            Default::default(),
        )
        .await
        .unwrap();
    let worker = q.clone();
    let run = tokio::spawn(async move { worker.run_one().await });
    received.recv().await.unwrap();
    q.cancel(job).await.unwrap();
    run.await.unwrap().unwrap();
    assert_eq!(
        q.get(job).await.unwrap().unwrap().state,
        JobState::Cancelled
    );
    assert_eq!(
        d.status("cancel-job".into()).await.unwrap().state,
        State::Unknown
    );
    assert_eq!(
        d.deliver("cancel-job".into(), None).await.unwrap().attempts,
        1
    );
    server.await.unwrap();
    qa.shutdown().await.unwrap();
    app.shutdown().await.unwrap();
}

// Run only as a child of killed_process_recovery_after_real_http_acceptance.
#[test]
#[ignore]
fn crash_child() {
    let Some(path) = std::env::var_os("LENSO_WEBHOOK_FIXTURE_PATH") else {
        return;
    };
    let port = std::env::var("LENSO_WEBHOOK_FIXTURE_PORT")
        .unwrap()
        .parse()
        .unwrap();
    tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(async move {
            let i = Instance::new(
                "child",
                DeliveryPlugin::new(path, endpoint(port, Duration::from_secs(30))),
            );
            let h = i.handle();
            let app = App::new().with(i).start().await.unwrap();
            let d = app.get(&h).unwrap();
            d.create("crash-http".into(), RAW.to_vec(), 3)
                .await
                .unwrap();
            d.deliver("crash-http".into(), None).await.unwrap();
            app.shutdown().await.unwrap();
        });
}

#[tokio::test]
async fn killed_process_recovery_after_real_http_acceptance() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    provision(&path).await.unwrap();
    let (port, mut received, server) = fixture(vec![(204, Duration::from_secs(30))]).await;
    let mut child = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "crash_child", "--ignored"])
        .env("LENSO_WEBHOOK_FIXTURE_PATH", &path)
        .env("LENSO_WEBHOOK_FIXTURE_PORT", port.to_string())
        .stdout(std::process::Stdio::null())
        .spawn()
        .unwrap();
    let observed = tokio::time::timeout(Duration::from_secs(10), received.recv()).await;
    child.kill().unwrap();
    child.wait().unwrap();
    assert_eq!(observed.unwrap().unwrap().id, "crash-http");
    server.abort();
    let i = Instance::new(
        "recovered",
        DeliveryPlugin::new(&path, endpoint(port, Duration::from_secs(1))),
    );
    let h = i.handle();
    let app = App::new().with(i).start().await.unwrap();
    let d = app.get(&h).unwrap();
    assert_eq!(
        d.deliver("crash-http".into(), None).await.unwrap().state,
        State::Unknown
    );
    assert_eq!(
        d.attempts("crash-http".into()).await.unwrap(),
        vec![Attempt {
            number: 1,
            outcome: "unknown".into(),
            http_status: None
        }]
    );
    app.shutdown().await.unwrap();
}
