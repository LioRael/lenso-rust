//! In-memory HTTP fixture: no listener, credentials or external service.
use lenso_realtime::{Identity, Lease, MemoryRealtime, RealtimeConfig, Resource};
use lenso_realtime_sse::{Sse, CACHE_CONTROL, CONTENT_TYPE};
use serde_json::{json, Value};
use std::{sync::Arc, time::Duration};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    time::Instant,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let notes = MemoryRealtime::new(
        RealtimeConfig::default(),
        |identity: Arc<Identity<bool>>, _| async move {
            Ok(identity.principal.then_some(Lease {
                valid_until: Instant::now() + Duration::from_secs(30),
            }))
        },
    )?;
    let note = Resource::new("demo", "note", "n1")?;
    let (server, client) = tokio::io::duplex(128);
    let serve = async {
        let mut server = BufReader::new(server);
        let mut line = String::new();
        server.read_line(&mut line).await?;
        assert_eq!(line, "GET /dashboard/notes/n1/events HTTP/1.1\r\n");
        loop {
            line.clear();
            server.read_line(&mut line).await?;
            if line == "\r\n" {
                break;
            }
        }
        // Fixture identity. Real hosts supply verified identity and keep their
        // existing admission guard alive through body EOF/drop.
        let connection = notes.connect(Identity {
            scope: "demo".into(),
            subject: "dashboard".into(),
            principal: true,
            expires_at: Instant::now() + Duration::from_secs(60),
        })?;
        let subscription = connection.subscribe(note.clone()).await?;
        let body = Sse::new(connection, Duration::from_secs(5), None)?;
        server.get_mut().write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: {CONTENT_TYPE}\r\nCache-Control: {CACHE_CONTROL}\r\nConnection: close\r\n\r\n").as_bytes()).await?;
        let result = body.write_to(server.get_mut()).await;
        drop(subscription);
        result?;
        server.get_mut().shutdown().await?;
        Ok::<_, Box<dyn std::error::Error>>(())
    };
    let dashboard = async {
        let mut client = BufReader::new(client);
        client
            .get_mut()
            .write_all(b"GET /dashboard/notes/n1/events HTTP/1.1\r\nHost: fixture\r\n\r\n")
            .await?;
        let mut line = String::new();
        loop {
            line.clear();
            client.read_line(&mut line).await?;
            if line == "\r\n" {
                break;
            }
        }
        line.clear();
        let mut title = String::new();
        let mut revision = 0;
        let mut ended = false;
        while client.read_line(&mut line).await? != 0 {
            if let Some(json) = line.strip_prefix("data: ") {
                let event: Value = serde_json::from_str(json)?;
                match event["kind"].as_str().unwrap() {
                    "ready" => {
                        // Simulates a publication after the notes edit commits.
                        // Ready itself is not a snapshot consistency guarantee.
                        notes.publish(
                            &note,
                            "note.updated",
                            json!({"title": "Release checklist", "revision": 2}),
                        )?;
                    }
                    "update" => {
                        title = event["data"]["title"].as_str().unwrap().into();
                        revision = event["data"]["revision"].as_u64().unwrap();
                        notes.revoke_subject("demo", "dashboard")?;
                    }
                    "closed" => ended = true,
                    "gap" => panic!("dashboard must fetch fresh state before reconnecting"),
                    _ => unreachable!(),
                }
            }
            line.clear();
        }
        assert_eq!(
            (title.as_str(), revision, ended),
            ("Release checklist", 2, true)
        );
        println!(
            "Dashboard rendered note n1: {title} (revision {revision}); revoked stream reached EOF"
        );
        Ok::<_, Box<dyn std::error::Error>>(())
    };
    let (served, consumed) = tokio::join!(serve, dashboard);
    notes.close();
    served?;
    consumed?;
    assert_eq!(notes.stats().connections, 0);
    Ok(())
}
