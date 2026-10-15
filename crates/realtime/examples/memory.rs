use lenso_realtime::{Event, Identity, Lease, MemoryRealtime, RealtimeConfig, Resource};
use serde_json::json;
use std::{sync::Arc, time::Duration};
use tokio::time::Instant;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // In a host, derive this capability from a verified request/session.
    let realtime = MemoryRealtime::new(
        RealtimeConfig::default(),
        |identity: Arc<Identity<bool>>, _| async move {
            Ok(identity.principal.then_some(Lease {
                valid_until: Instant::now() + Duration::from_secs(10),
            }))
        },
    )?;
    let note = Resource::new("workspace", "note", "n1")?;
    let mut connection = realtime.connect(Identity {
        scope: "workspace".into(),
        subject: "reader".into(),
        principal: true,
        expires_at: Instant::now() + Duration::from_secs(60),
    })?;
    let subscription = connection.subscribe(note.clone()).await?;
    println!(
        "{}",
        serde_json::to_string(&connection.recv().await.unwrap())?
    );
    // Publish only after the host business operation commits. This is not an
    // outbox or snapshot consistency protocol.
    let report = realtime.publish(&note, "note.updated", json!({"revision": 2}))?;
    assert_eq!(report.enqueued, 1);
    println!(
        "{}",
        serde_json::to_string(&connection.recv().await.unwrap())?
    );
    subscription.renew().await?;
    realtime.revoke_resource(&note);
    assert!(matches!(
        connection.recv().await.unwrap().event,
        Event::Closed { .. }
    ));
    realtime.close();
    Ok(())
}
