use lenso_notifications::{Message, Outcome, Resend, Transport};
use std::{
    io::{Read, Write},
    net::TcpListener,
    time::{Duration, Instant},
};
fn fixture(
    status: u16,
    body: &'static str,
    delay: Duration,
) -> (Resend, std::thread::JoinHandle<String>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("local HTTP fixture must be permitted");
    let adapter = Resend::local_fixture(
        listener.local_addr().unwrap().port(),
        Duration::from_millis(100),
    )
    .unwrap();
    listener.set_nonblocking(true).unwrap();
    let thread = std::thread::spawn(move || {
        let deadline = Instant::now() + Duration::from_secs(3);
        let mut socket = loop {
            match listener.accept() {
                Ok((s, _)) => break s,
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    assert!(Instant::now() < deadline);
                    std::thread::sleep(Duration::from_millis(2));
                }
                Err(e) => panic!("{e}"),
            }
        };
        socket
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        let mut bytes = vec![];
        loop {
            let mut buf = [0; 1024];
            let count = socket.read(&mut buf).unwrap();
            assert!(count > 0);
            bytes.extend_from_slice(&buf[..count]);
            if let Some(pos) = bytes.windows(4).position(|w| w == b"\r\n\r\n") {
                let headers = String::from_utf8_lossy(&bytes[..pos]);
                let length: usize = headers
                    .lines()
                    .find_map(|line| {
                        line.to_ascii_lowercase()
                            .strip_prefix("content-length:")
                            .map(|s| s.trim().parse().unwrap())
                    })
                    .unwrap();
                if bytes.len() >= pos + 4 + length {
                    break;
                }
            }
        }
        std::thread::sleep(delay);
        let _ = write!(
            socket,
            "HTTP/1.1 {status} Fixture\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        );
        String::from_utf8(bytes).unwrap()
    });
    (adapter, thread)
}
fn message() -> Message {
    Message {
        to: "buyer@example.invalid".into(),
        subject: "Order 42".into(),
        text: "Plain text 确认".into(),
    }
}
#[test]
#[ignore = "requires permitted loopback listener; explicitly run --test http -- --ignored"]
fn real_http_encoding_and_acceptance() {
    let (adapter, server) = fixture(200, r#"{"id":"fixture-email"}"#, Duration::ZERO);
    assert!(
        matches!(adapter.send("stable-key",&message()),Outcome::Accepted(id) if id=="fixture-email")
    );
    let request = server.join().unwrap();
    let (headers, body) = request.split_once("\r\n\r\n").unwrap();
    assert!(headers.starts_with("POST /emails HTTP/1.1"));
    assert!(headers
        .to_ascii_lowercase()
        .contains("idempotency-key: stable-key"));
    assert!(headers.contains("Bearer fixture-token"));
    let json: serde_json::Value = serde_json::from_str(body).unwrap();
    assert_eq!(json["to"][0], message().to);
    assert_eq!(json["text"], message().text);
    assert_eq!(json["from"], "orders@example.invalid");
}
#[test]
#[ignore = "requires permitted loopback listener; explicitly run --test http -- --ignored"]
fn rejection_ambiguity_malformed_and_timeout() {
    for (code, body, rejected) in [
        (422, "{}", true),
        (429, "{}", false),
        (500, "{}", false),
        (200, "not-json", false),
        (302, "{}", false),
    ] {
        let (adapter, server) = fixture(code, body, Duration::ZERO);
        let outcome = adapter.send("key", &message());
        assert_eq!(matches!(outcome, Outcome::Rejected), rejected);
        if !rejected {
            assert!(matches!(outcome, Outcome::Unknown));
        }
        server.join().unwrap();
    }
    let (adapter, server) = fixture(200, r#"{"id":"late"}"#, Duration::from_millis(200));
    let started = Instant::now();
    assert!(matches!(adapter.send("key", &message()), Outcome::Unknown));
    assert!(started.elapsed() < Duration::from_secs(1));
    server.join().unwrap();
}
