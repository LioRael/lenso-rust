use lenso_notifications::{
    provision, Message, Notifications, Request, Scope, State, Template, Transport,
};
use lenso_notifications_smtp::{Delivery, Smtp};
use std::{
    io::{BufRead, BufReader, Write},
    net::{TcpListener, TcpStream},
    sync::Arc,
    thread,
    time::Duration,
};

fn message() -> Message {
    Message {
        to: "customer@example.invalid".into(),
        subject: "Order 42".into(),
        text: "Thanks for order 42\r\n.dot line".into(),
    }
}
fn line(reader: &mut BufReader<TcpStream>) -> String {
    let mut line = String::new();
    reader.read_line(&mut line).unwrap();
    line
}
fn reply(reader: &mut BufReader<TcpStream>, text: &str) {
    reader.get_mut().write_all(text.as_bytes()).unwrap();
}
#[derive(Clone, Copy)]
enum End {
    Accept,
    AckThenClose,
    StallQuit,
    UnexpectedAck,
    LostAck,
    Reject(u16),
    Stall,
}
fn fixture(end: End) -> (Smtp, thread::JoinHandle<String>) {
    fixture_notify(end, None)
}
fn fixture_notify(
    end: End,
    received: Option<tokio::sync::oneshot::Sender<()>>,
) -> (Smtp, thread::JoinHandle<String>) {
    let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
    let smtp = Smtp::local_fixture(
        listener.local_addr().unwrap().port(),
        Duration::from_millis(150),
    )
    .unwrap();
    let worker = thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(3)))
            .unwrap();
        let mut reader = BufReader::new(stream);
        reply(&mut reader, "220 localhost fixture\r\n");
        assert!(line(&mut reader).starts_with("EHLO "));
        reply(&mut reader, "250 localhost\r\n");
        assert!(line(&mut reader).starts_with("MAIL FROM:<orders@example.invalid>"));
        reply(&mut reader, "250 sender ok\r\n");
        assert!(line(&mut reader).starts_with("RCPT TO:<customer@example.invalid>"));
        reply(&mut reader, "250 recipient ok\r\n");
        assert_eq!(line(&mut reader), "DATA\r\n");
        reply(&mut reader, "354 send content\r\n");
        let mut content = String::new();
        loop {
            let next = line(&mut reader);
            assert!(!next.is_empty());
            if next == ".\r\n" {
                break;
            }
            content.push_str(&next);
        }
        if let Some(received) = received {
            let _ = received.send(());
        }
        match end {
            End::LostAck => return content,
            End::Stall => {
                thread::sleep(Duration::from_millis(400));
                return content;
            }
            End::Reject(code) => reply(&mut reader, &format!("{code} rejected\r\n")),
            End::UnexpectedAck => reply(&mut reader, "354 unexpected intermediate reply\r\n"),
            End::Accept | End::AckThenClose | End::StallQuit => {
                reply(&mut reader, "250 queued\r\n")
            }
        }
        if matches!(end, End::AckThenClose) {
            return content;
        }
        // Lettre may abort a rejected session itself. Successful sessions QUIT.
        let quit = line(&mut reader);
        if !quit.is_empty() {
            assert_eq!(quit, "QUIT\r\n");
            if matches!(end, End::StallQuit) {
                thread::sleep(Duration::from_millis(400));
                return content;
            }
            reply(&mut reader, "221 bye\r\n");
        }
        content
    });
    (smtp, worker)
}

#[test]
fn rejects_injection_before_connecting() {
    let smtp = Smtp::local_fixture(1, Duration::from_millis(10)).unwrap();
    for to in [
        "victim@example.invalid\r\nBcc: attacker@example.invalid",
        "a@example.invalid,b@example.invalid",
        "Name <a@example.invalid>",
    ] {
        let mut m = message();
        m.to = to.into();
        assert_eq!(smtp.deliver("key", &m), Delivery::InvalidMessage);
    }
    let mut m = message();
    m.subject = "Order\r\nBcc: attacker@example.invalid".into();
    assert_eq!(smtp.deliver("key", &m), Delivery::InvalidMessage);
    assert_eq!(
        smtp.deliver("bad\r\nkey", &message()),
        Delivery::InvalidMessage
    );
    assert!(Smtp::local_fixture(1, Duration::ZERO).is_err());
    assert!(Smtp::new(
        "account",
        "smtp.example.invalid",
        "127.0.0.1:465".parse().unwrap(),
        "bad\r\nfrom",
        None,
        Duration::from_secs(1)
    )
    .is_err());
}

#[test]
#[ignore = "requires permitted loopback TCP listener"]
fn smtp_status_ack_and_close_semantics() {
    for (end, expected) in [
        (End::Accept, Delivery::Accepted("key@lenso.invalid".into())),
        (
            End::AckThenClose,
            Delivery::Accepted("key@lenso.invalid".into()),
        ),
        (
            End::StallQuit,
            Delivery::Accepted("key@lenso.invalid".into()),
        ),
        (End::UnexpectedAck, Delivery::Unknown),
        (End::LostAck, Delivery::Unknown),
        (End::Stall, Delivery::Unknown),
        (End::Reject(451), Delivery::TemporaryRejection),
        (End::Reject(550), Delivery::PermanentRejection),
    ] {
        let (smtp, worker) = fixture(end);
        assert_eq!(smtp.deliver("key", &message()), expected);
        let content = worker.join().unwrap();
        assert!(content.contains("To: customer@example.invalid\r\n"));
        assert!(content.contains("Subject: Order 42\r\n"));
        assert!(content.contains("Message-ID: <key@lenso.invalid>\r\n"));
        assert!(content.contains("Thanks for order 42\r\n..dot line"));
    }
}

#[tokio::test]
#[ignore = "requires permitted loopback TCP listener"]
async fn order_notifications_consume_smtp_and_never_replay_unknown() {
    for (end, expected) in [
        (End::Accept, State::Accepted),
        (End::LostAck, State::Unknown),
    ] {
        let (smtp, worker) = fixture(end);
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("notifications.db");
        provision(&path).await.unwrap();
        let transport: Arc<dyn Transport> = Arc::new(smtp);
        let notifications = Notifications::open(&path, transport.clone()).await.unwrap();
        let scope = Scope {
            tenant: "shop".into(),
            owner: "customer-1".into(),
        };
        let receipt = notifications
            .create(Request {
                scope: scope.clone(),
                business_key: "order-42".into(),
                to: message().to,
                template: Template {
                    name: "order".into(),
                    version: 1,
                    category: "orders".into(),
                    required: true,
                    subject: "Order {{id}}".into(),
                    text: "Thanks for order {{id}}".into(),
                },
                variables: [("id".into(), "42".into())].into(),
            })
            .await
            .unwrap();
        let delivered = notifications
            .deliver(scope.clone(), receipt.id)
            .await
            .unwrap();
        assert_eq!(delivered.state, expected);
        let captured = worker.join().unwrap();
        assert!(captured.contains("Subject: Order 42\r\n"));
        assert!(captured.contains("Thanks for order 42"));
        notifications.close().await.unwrap();
        let reopened = Notifications::open(&path, transport).await.unwrap();
        assert_eq!(
            reopened.deliver(scope, receipt.id).await.unwrap(),
            delivered
        );
        assert_eq!(delivered.attempts.len(), 1);
        reopened.close().await.unwrap();
    }
}

#[tokio::test]
#[ignore = "requires permitted loopback TCP listener"]
async fn cancelled_waiter_retains_unknown_until_close_settles() {
    let (tx, rx) = tokio::sync::oneshot::channel();
    let (smtp, worker) = fixture_notify(End::Stall, Some(tx));
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("cancel.db");
    provision(&path).await.unwrap();
    let transport: Arc<dyn Transport> = Arc::new(smtp);
    let n = Notifications::open(&path, transport.clone()).await.unwrap();
    let scope = Scope {
        tenant: "shop".into(),
        owner: "customer".into(),
    };
    let receipt = n
        .create(Request {
            scope: scope.clone(),
            business_key: "cancel-order".into(),
            to: message().to,
            template: Template {
                name: "order".into(),
                version: 1,
                category: "orders".into(),
                required: true,
                subject: "Order 42".into(),
                text: "Thanks for order 42".into(),
            },
            variables: Default::default(),
        })
        .await
        .unwrap();
    let waiting = n.clone();
    let sending_scope = scope.clone();
    let waiter = tokio::spawn(async move { waiting.deliver(sending_scope, receipt.id).await });
    rx.await.unwrap();
    waiter.abort();
    n.close().await.unwrap();
    worker.join().unwrap();
    let reopened = Notifications::open(&path, transport).await.unwrap();
    let result = reopened.deliver(scope, receipt.id).await.unwrap();
    assert_eq!(result.state, State::Unknown);
    assert_eq!(result.attempts.len(), 1);
    reopened.close().await.unwrap();
}

#[test]
#[ignore = "requires permitted loopback TCP listener"]
fn production_tls_never_falls_back_to_plaintext() {
    use std::io::Read;
    let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
    let smtp = Smtp::new(
        "fixture",
        "smtp.example.invalid",
        listener.local_addr().unwrap(),
        "orders@example.invalid",
        None,
        Duration::from_millis(200),
    )
    .unwrap();
    let worker = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        let mut first = [0; 1];
        stream.read_exact(&mut first).unwrap();
        assert_eq!(
            first[0], 0x16,
            "must send a TLS handshake, never EHLO or AUTH"
        );
        stream.write_all(b"220 plaintext is not TLS\r\n").unwrap();
    });
    assert_eq!(smtp.deliver("tls-test", &message()), Delivery::Unknown);
    worker.join().unwrap();
}
