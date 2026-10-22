use hmac::{Hmac, KeyInit, Mac};
use lenso_webhook_receiver::*;
use sha2::Sha256;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
const KEY: &[u8] = b"PUBLIC-TEST-FIXTURE-NOT-A-SECRET";
fn config() -> Config {
    Config {
        receiver_id: "tenant_a".into(),
        source: "partner".into(),
        event_types: vec!["order.created".into()],
        keys: vec![SigningKey {
            id: "fixture".into(),
            secret: KEY.to_vec(),
        }],
        max_body_bytes: 4096,
        body_timeout: Duration::from_millis(30),
        tolerance_seconds: 300,
    }
}
fn body() -> Vec<u8> {
    serde_json::from_str::<serde_json::Value>(include_str!("fixture.json")).unwrap()["body"]
        .as_str()
        .unwrap()
        .as_bytes()
        .to_vec()
}
fn headers(b: &[u8], time: u64) -> Headers {
    let mut m = Hmac::<Sha256>::new_from_slice(KEY).unwrap();
    m.update(format!("{time}\nevt_1\n").as_bytes());
    m.update(b);
    Headers {
        event_id: "evt_1".into(),
        timestamp: time.to_string(),
        signature: format!(
            "v1;kid=fixture;mac={}",
            m.finalize()
                .into_bytes()
                .iter()
                .map(|b| format!("{b:02x}"))
                .collect::<String>()
        ),
    }
}
fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
}
fn database() -> tempfile::NamedTempFile {
    let f = tempfile::NamedTempFile::new().unwrap();
    provision(f.path()).unwrap();
    rusqlite::Connection::open(f.path())
        .unwrap()
        .execute_batch("CREATE TABLE orders(id TEXT);")
        .unwrap();
    f
}
fn count(f: &tempfile::NamedTempFile, table: &str) -> i64 {
    rusqlite::Connection::open(f.path())
        .unwrap()
        .query_row(&format!("SELECT count(*) FROM {table}"), [], |r| r.get(0))
        .unwrap()
}
fn apply(tx: &BusinessTransaction<'_>, _: &Event) -> Result<(), Error> {
    tx.execute("INSERT INTO orders VALUES (?1)", ["order_1"])?;
    Ok(())
}
#[tokio::test]
async fn durable_concurrent_duplicate_conflict_namespace() {
    let f = database();
    let a = Receiver::open(f.path(), config()).unwrap();
    let b = Receiver::open(f.path(), config()).unwrap();
    let bytes = body();
    let (x, y) = tokio::join!(
        a.receive(headers(&bytes, now()), bytes.as_slice(), apply),
        b.receive(headers(&bytes, now()), bytes.as_slice(), apply)
    );
    assert!(matches!(
        (x, y),
        (Ok(Outcome::Accepted), Ok(Outcome::Duplicate))
            | (Ok(Outcome::Duplicate), Ok(Outcome::Accepted))
    ));
    assert_eq!(count(&f, "orders"), 1);
    a.close().unwrap();
    let a = Receiver::open(f.path(), config()).unwrap();
    assert_eq!(
        a.receive(headers(&bytes, now()), bytes.as_slice(), apply)
            .await,
        Ok(Outcome::Duplicate)
    );
    let mut changed = bytes.clone();
    changed.push(b' ');
    assert_eq!(
        a.receive(headers(&changed, now()), changed.as_slice(), apply)
            .await,
        Err(Error::Conflict)
    );
    let mut cfg = config();
    cfg.receiver_id = "tenant_b".into();
    let b = Receiver::open(f.path(), cfg).unwrap();
    assert_eq!(
        b.receive(headers(&bytes, now()), bytes.as_slice(), apply)
            .await,
        Ok(Outcome::Accepted)
    );
    assert_eq!(count(&f, "orders"), 2);
}
#[tokio::test]
async fn rollback_and_transaction_capability() {
    for sql in [
        "COMMIT",
        "ROLLBACK",
        "SAVEPOINT escape",
        "DELETE FROM lenso_webhook_inbox",
        "DROP TABLE orders",
        "ATTACH ':memory:' AS other",
        "PRAGMA writable_schema=ON",
    ] {
        let f = database();
        let a = Receiver::open(f.path(), config()).unwrap();
        let bytes = body();
        assert_eq!(
            a.receive(headers(&bytes, now()), bytes.as_slice(), move |tx, e| {
                apply(tx, e)?;
                tx.execute(sql, [])?;
                Ok(())
            })
            .await,
            Err(Error::Apply)
        );
        assert_eq!(count(&f, "orders"), 0);
        assert_eq!(count(&f, "lenso_webhook_inbox"), 0);
        assert_eq!(
            a.receive(headers(&bytes, now()), bytes.as_slice(), apply)
                .await,
            Ok(Outcome::Accepted)
        );
    }
    let f = database();
    let a = Receiver::open(f.path(), config()).unwrap();
    let bytes = body();
    assert_eq!(
        a.receive(headers(&bytes, now()), bytes.as_slice(), |tx, e| {
            apply(tx, e)?;
            Err(Error::Apply)
        })
        .await,
        Err(Error::Apply)
    );
    assert_eq!(count(&f, "orders"), 0);
    assert_eq!(count(&f, "lenso_webhook_inbox"), 0);
}
#[tokio::test]
async fn authentication_limits_and_policy() {
    let f = database();
    let a = Receiver::open(f.path(), config()).unwrap();
    let bytes = body();
    let mut changed = bytes.clone();
    changed[0] = b' ';
    assert_eq!(
        a.receive(headers(&bytes, now()), changed.as_slice(), apply)
            .await,
        Err(Error::Unauthorized)
    );
    assert_eq!(
        a.receive(headers(&bytes, now() - 301), bytes.as_slice(), apply)
            .await,
        Err(Error::Unauthorized)
    );
    assert_eq!(
        a.receive(headers(&bytes, now() + 301), bytes.as_slice(), apply)
            .await,
        Err(Error::Unauthorized)
    );
    let mut h = headers(&bytes, now());
    h.signature = h.signature.replace("fixture", "wrong");
    assert_eq!(
        a.receive(h, bytes.as_slice(), apply).await,
        Err(Error::Unauthorized)
    );
    let mut cfg = config();
    cfg.keys[0].secret = b"wrong".to_vec();
    let bad = Receiver::open(f.path(), cfg).unwrap();
    assert_eq!(
        bad.receive(headers(&bytes, now()), bytes.as_slice(), apply)
            .await,
        Err(Error::Unauthorized)
    );
    let big = vec![0; 4097];
    assert_eq!(
        a.receive(headers(&big, now()), big.as_slice(), apply).await,
        Err(Error::TooLarge)
    );
    let (_writer, reader) = tokio::io::duplex(16);
    assert_eq!(
        a.receive(headers(&bytes, now()), reader, apply).await,
        Err(Error::Timeout)
    );
    let mut cfg = config();
    cfg.source = "other".into();
    let bad = Receiver::open(f.path(), cfg).unwrap();
    assert_eq!(
        bad.receive(headers(&bytes, now()), bytes.as_slice(), apply)
            .await,
        Err(Error::Forbidden)
    );
    let deep = format!(
        "{}{}{}",
        String::from_utf8(bytes.clone())
            .unwrap()
            .replace("{\"order\":\"order_1\"}}", ""),
        "[".repeat(34),
        format_args!("0{}{}", "]".repeat(34), "}")
    );
    assert_eq!(
        a.receive(headers(deep.as_bytes(), now()), deep.as_bytes(), apply)
            .await,
        Err(Error::Invalid)
    );
    assert_eq!(count(&f, "orders"), 0);
}
#[tokio::test]
async fn plugin_retained_service_rejects_after_shutdown() {
    let f = database();
    let mut app = lenso_plugin_core::App::new();
    let h = app.install(
        "receiver",
        ReceiverPlugin {
            path: f.path().into(),
            config: config(),
        },
    );
    let app = app.start().await.unwrap();
    let service = app.get(&h).unwrap();
    app.shutdown().await.unwrap();
    let bytes = body();
    assert_eq!(
        service
            .receive(headers(&bytes, now()), bytes.as_slice(), apply)
            .await,
        Err(Error::Unavailable)
    );
}
#[tokio::test]
async fn swallowed_sql_error_cannot_escape_rollback() {
    let f = database();
    let db = rusqlite::Connection::open(f.path()).unwrap();
    db.execute_batch("CREATE UNIQUE INDEX unique_order ON orders(id);")
        .unwrap();
    let a = Receiver::open(f.path(), config()).unwrap();
    let bytes = body();
    assert_eq!(
        a.receive(headers(&bytes, now()), bytes.as_slice(), |tx, e| {
            apply(tx, e)?;
            let _ = tx.execute("INSERT OR ROLLBACK INTO orders VALUES ('order_1')", []);
            assert_eq!(
                tx.execute("INSERT INTO orders VALUES ('escaped')", []),
                Err(Error::Apply)
            );
            Ok(())
        })
        .await,
        Err(Error::Apply)
    );
    assert_eq!(count(&f, "orders"), 0);
    assert_eq!(count(&f, "lenso_webhook_inbox"), 0);
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelled_waiter_still_drains_transaction() {
    let f = database();
    let mut app = lenso_plugin_core::App::new();
    let h = app.install(
        "receiver",
        ReceiverPlugin {
            path: f.path().into(),
            config: config(),
        },
    );
    let app = app.start().await.unwrap();
    let receiver = app.get(&h).unwrap();
    let (started_rx_tx, started_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let task = tokio::spawn(async move {
        let b = body();
        receiver
            .receive(headers(&b, now()), b.as_slice(), move |tx, e| {
                let _ = started_rx_tx.send(());
                release_rx.recv().unwrap();
                apply(tx, e)
            })
            .await
    });
    started_rx.await.unwrap();
    task.abort();
    let mut shutdown = Box::pin(app.shutdown());
    assert!(
        tokio::time::timeout(Duration::from_millis(20), &mut shutdown)
            .await
            .is_err()
    );
    release_tx.send(()).unwrap();
    shutdown.await.unwrap();
    assert_eq!(count(&f, "orders"), 1);
    assert_eq!(count(&f, "lenso_webhook_inbox"), 1);
}
