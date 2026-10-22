use lenso_realtime::{
    Connection, Identity, Lease, MemoryRealtime, RealtimeConfig, Resource, Subscription,
};
use lenso_realtime_sse::{Error, Sse};
use serde_json::json;
use std::{sync::Arc, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    time::Instant,
};

async fn fixture(
    config: RealtimeConfig,
    session: u64,
) -> (
    MemoryRealtime<bool>,
    Resource,
    Subscription<bool>,
    Sse<bool>,
) {
    let service = MemoryRealtime::new(config, |identity: Arc<Identity<bool>>, _| async move {
        Ok(identity.principal.then_some(Lease {
            valid_until: Instant::now() + Duration::from_secs(30),
        }))
    })
    .unwrap();
    let note = Resource::new("workspace", "note", "n1").unwrap();
    let connection = connect(&service, session);
    let subscription = connection.subscribe(note.clone()).await.unwrap();
    let sse = Sse::new(connection, Duration::from_secs(5), None).unwrap();
    (service, note, subscription, sse)
}
fn connect(service: &MemoryRealtime<bool>, session: u64) -> Connection<bool> {
    service
        .connect(Identity {
            scope: "workspace".into(),
            subject: "reader".into(),
            principal: true,
            expires_at: Instant::now() + Duration::from_secs(session),
        })
        .unwrap()
}
async fn frame(sse: &mut Sse<bool>) -> String {
    String::from_utf8(sse.next_frame().await.unwrap().unwrap().to_vec()).unwrap()
}
fn data(frame: &str) -> serde_json::Value {
    serde_json::from_str(
        frame
            .lines()
            .find_map(|line| line.strip_prefix("data: "))
            .unwrap(),
    )
    .unwrap()
}

#[tokio::test(start_paused = true)]
async fn encoding_heartbeat_and_cancelled_receive() {
    let (service, note, _subscription, mut sse) = fixture(RealtimeConfig::default(), 60).await;
    assert!(frame(&mut sse).await.starts_with("event: ready\n"));
    let payload = json!({"text": "hello\r\nevent: forged\ndata: x\u{2028}你好"});
    service
        .publish(&note, "note.updated", payload.clone())
        .unwrap();
    let update = frame(&mut sse).await;
    assert_eq!(update.lines().count(), 3);
    assert_eq!(data(&update)["data"], payload);
    assert!(!update.contains("\nid:"));
    assert!(
        tokio::time::timeout(Duration::from_secs(1), sse.next_frame())
            .await
            .is_err()
    );
    assert_eq!(frame(&mut sse).await, ": heartbeat\n\n");
    service.publish(&note, "note.updated", json!(2)).unwrap();
    assert_eq!(data(&frame(&mut sse).await)["data"], 2);
    drop(sse);
    assert_eq!(service.stats().connections, 0);
    assert_eq!(service.stats().subscriptions, 0);
}

#[tokio::test(start_paused = true)]
async fn slow_consumer_gets_one_gap_then_eof_without_prefetch() {
    let config = RealtimeConfig {
        max_buffered_events: 2,
        ..Default::default()
    };
    let (service, note, _subscription, sse) = fixture(config, 60).await;
    let stream = sse.into_stream();
    // No polling: the ready event still occupies one of the two bounded slots.
    assert_eq!(
        service
            .publish(&note, "note.updated", json!(1))
            .unwrap()
            .enqueued,
        1
    );
    assert_eq!(
        service
            .publish(&note, "note.updated", json!(2))
            .unwrap()
            .overflowed_connections,
        1
    );
    for n in 3..1000 {
        assert_eq!(
            service
                .publish(&note, "note.updated", json!(n))
                .unwrap()
                .enqueued,
            0
        );
    }
    futures_util::pin_mut!(stream);
    use futures_util::StreamExt;
    let gap = stream.next().await.unwrap().unwrap();
    assert_eq!(
        data(std::str::from_utf8(&gap).unwrap())["reason"],
        "overflow"
    );
    assert!(gap.starts_with(b"event: gap\n"));
    assert!(stream.next().await.is_none());
    assert!(stream.next().await.is_none());
    assert_eq!(service.stats().connections, 0);
}

#[tokio::test(start_paused = true)]
async fn revocation_and_expiry_purge_queued_updates() {
    for mode in ["resource", "subject", "lease", "session", "shutdown"] {
        let (service, note, _subscription, mut sse) = fixture(
            RealtimeConfig::default(),
            if mode == "session" { 2 } else { 60 },
        )
        .await;
        frame(&mut sse).await;
        service
            .publish(&note, "note.updated", json!("secret"))
            .unwrap();
        match mode {
            "resource" => service.revoke_resource(&note),
            "subject" => service.revoke_subject("workspace", "reader").unwrap(),
            "lease" => tokio::time::advance(Duration::from_secs(30)).await,
            "session" => tokio::time::advance(Duration::from_secs(2)).await,
            _ => service.close(),
        }
        let closed = frame(&mut sse).await;
        assert!(closed.starts_with("event: closed\n"), "{mode}: {closed}");
        assert!(!closed.contains("secret"));
        let expected = match mode {
            "resource" | "subject" => "revoked",
            "shutdown" => "shutdown",
            _ => "expired",
        };
        assert_eq!(data(&closed)["reason"], expected);
        if matches!(mode, "subject" | "session" | "shutdown") {
            assert!(sse.next_frame().await.is_none());
        }
        drop(sse);
        assert_eq!(service.stats().connections, 0);
    }
}

#[tokio::test]
async fn writer_error_and_cancellation_release_resources() {
    let (service, _, _subscription, sse) = fixture(RealtimeConfig::default(), 60).await;
    let (mut writer, reader) = tokio::io::duplex(1);
    drop(reader);
    assert!(sse.write_to(&mut writer).await.is_err());
    assert_eq!(service.stats().connections, 0);
    let (service, note, _subscription, sse) = fixture(
        RealtimeConfig {
            max_buffered_events: 2,
            ..Default::default()
        },
        60,
    )
    .await;
    let (mut writer, _reader) = tokio::io::duplex(1);
    let mut sending = Box::pin(sse.write_to(&mut writer));
    assert!(std::future::Future::poll(
        sending.as_mut(),
        &mut std::task::Context::from_waker(futures_util::task::noop_waker_ref())
    )
    .is_pending());
    // One frame is stalled in the writer; no subsequent receive is performed.
    for n in 0..2 {
        assert_eq!(
            service
                .publish(&note, "note.updated", json!(n))
                .unwrap()
                .enqueued,
            1
        );
    }
    assert_eq!(
        service
            .publish(&note, "note.updated", json!(3))
            .unwrap()
            .overflowed_connections,
        1
    );
    drop(sending);
    assert_eq!(service.stats().connections, 0);
    let (service, _, _subscription, sse) = fixture(RealtimeConfig::default(), 60).await;
    let mut sending = Box::pin(sse.write_to(&mut writer));
    assert!(std::future::Future::poll(
        sending.as_mut(),
        &mut std::task::Context::from_waker(futures_util::task::noop_waker_ref())
    )
    .is_pending());
    drop(sending);
    assert_eq!(service.stats().connections, 0);
    assert_eq!(service.stats().subscriptions, 0);
}

#[tokio::test]
async fn http_fixture_normal_eof_and_resume_rejection() {
    let (service, _, _subscription, sse) = fixture(RealtimeConfig::default(), 60).await;
    let (mut server, mut client) = tokio::io::duplex(32);
    service.close();
    let serve = async {
        server.write_all(b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream; charset=utf-8\r\nCache-Control: no-cache, no-transform\r\nConnection: close\r\n\r\n").await.unwrap();
        sse.write_to(&mut server).await.unwrap();
        server.shutdown().await.unwrap();
    };
    let receive = async {
        let mut response = String::new();
        client.read_to_string(&mut response).await.unwrap();
        response
    };
    let (_, response) = tokio::join!(serve, receive);
    assert!(response.contains("\r\n\r\nevent: closed\n"));
    assert_eq!(
        data(response.split("\r\n\r\n").nth(1).unwrap())["reason"],
        "shutdown"
    );
    let (service, _, _subscription, sse) = fixture(RealtimeConfig::default(), 60).await;
    drop(sse);
    assert!(matches!(
        Sse::new(
            connect(&service, 60),
            Duration::from_secs(5),
            Some("cursor")
        ),
        Err(Error::ReplayUnsupported)
    ));
    assert!(matches!(
        Sse::new(connect(&service, 60), Duration::ZERO, None),
        Err(Error::InvalidHeartbeat)
    ));
    assert_eq!(service.stats().connections, 0);
    let connection = connect(&service, 60);
    connection.close();
    let mut sse = Sse::new(connection, Duration::from_secs(5), Some("")).unwrap();
    assert!(sse.next_frame().await.is_none());
}
