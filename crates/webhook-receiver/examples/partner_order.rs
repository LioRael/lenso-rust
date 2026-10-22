//! Only public fixture keys and a disposable database; no listener or outbound I/O.
use hmac::{Hmac, KeyInit, Mac};
use lenso_webhook_receiver::*;
use sha2::Sha256;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
fn headers(body: &[u8]) -> Headers {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
        .to_string();
    let mut mac = Hmac::<Sha256>::new_from_slice(b"PUBLIC-TEST-FIXTURE-NOT-A-SECRET").unwrap();
    mac.update(format!("{timestamp}\nevt_1\n").as_bytes());
    mac.update(body);
    Headers {
        event_id: "evt_1".into(),
        timestamp,
        signature: format!(
            "v1;kid=fixture;mac={}",
            mac.finalize()
                .into_bytes()
                .iter()
                .map(|b| format!("{b:02x}"))
                .collect::<String>()
        ),
    }
}
fn apply(tx: &BusinessTransaction<'_>, event: &Event) -> Result<(), Error> {
    tx.execute(
        "INSERT INTO orders(id) VALUES (?1)",
        [event.data["order"].as_str().ok_or(Error::Apply)?],
    )?;
    Ok(())
}
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let file = tempfile::NamedTempFile::new()?;
    provision(file.path())?;
    let db = rusqlite::Connection::open(file.path())?;
    db.execute_batch("CREATE TABLE orders(id TEXT PRIMARY KEY);")?;
    let fixture: serde_json::Value = serde_json::from_str(include_str!("../tests/fixture.json"))?;
    let bytes = fixture["body"].as_str().unwrap().as_bytes();
    let config = Config {
        receiver_id: "partner-tenant".into(),
        source: "partner".into(),
        event_types: vec!["order.created".into()],
        keys: vec![SigningKey {
            id: "fixture".into(),
            secret: b"PUBLIC-TEST-FIXTURE-NOT-A-SECRET".to_vec(),
        }],
        max_body_bytes: 4096,
        body_timeout: Duration::from_secs(1),
        tolerance_seconds: 300,
    };
    let mut app = lenso_plugin_core::App::new();
    let handle = app.install(
        "partner",
        ReceiverPlugin {
            path: file.path().into(),
            config: config.clone(),
        },
    );
    let app = app.start().await?;
    let receiver = app.get(&handle)?;
    let result = async {
        assert_eq!(
            receiver.receive(headers(bytes), bytes, apply).await?,
            Outcome::Accepted
        );
        assert_eq!(
            receiver.receive(headers(bytes), bytes, apply).await?,
            Outcome::Duplicate
        );
        Ok::<_, Error>(())
    }
    .await;
    let shutdown = app.shutdown().await;
    if result.is_err() || shutdown.is_err() {
        return Err(format!("business: {result:?}; shutdown: {shutdown:?}").into());
    }
    let receiver = Receiver::open(file.path(), config)?;
    assert_eq!(
        receiver.receive(headers(bytes), bytes, apply).await?,
        Outcome::Duplicate
    );
    let count: i64 = db.query_row("SELECT count(*) FROM orders", [], |row| row.get(0))?;
    assert_eq!(count, 1);
    receiver.close()?;
    println!("accepted, duplicate, reopened duplicate; durable orders={count}");
    Ok(())
}
