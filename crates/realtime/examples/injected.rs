use lenso_plugin_core::{App, BoxError, Instance, Plugin, Setup};
use lenso_realtime::{Identity, Lease, RealtimeConfig, RealtimePlugin, Resource};
use std::{sync::Arc, time::Duration};
use tokio::time::Instant;

// An ordinary business/policy service, also usable without plugins.
struct NotesPolicy {
    reader: String,
}
impl NotesPolicy {
    fn may_read(&self, identity: &Identity<()>, resource: &Resource) -> bool {
        identity.subject == self.reader && resource.kind() == "note"
    }
}
struct PolicyPlugin;
impl Plugin for PolicyPlugin {
    type Service = NotesPolicy;
    async fn setup(self, _: &mut Setup) -> Result<Arc<NotesPolicy>, BoxError> {
        Ok(Arc::new(NotesPolicy {
            reader: "reader".into(),
        }))
    }
}

#[tokio::main]
async fn main() -> Result<(), BoxError> {
    let policy = Instance::new("notes-policy", PolicyPlugin);
    let realtime = Instance::new(
        "notes-realtime",
        RealtimePlugin::with_dependency(
            RealtimeConfig::default(),
            policy.handle(),
            |policy, identity: Arc<Identity<()>>, resource| async move {
                Ok(policy.may_read(&identity, &resource).then_some(Lease {
                    valid_until: Instant::now() + Duration::from_secs(10),
                }))
            },
        ),
    );
    let handle = realtime.handle();
    let app = App::new().with(realtime).with(policy).start().await?;
    let service = app.get(&handle)?;
    let mut connection = service.connect(Identity {
        scope: "workspace".into(),
        subject: "reader".into(),
        principal: (),
        expires_at: Instant::now() + Duration::from_secs(60),
    })?;
    let _subscription = connection
        .subscribe(Resource::new("workspace", "note", "n1")?)
        .await?;
    println!(
        "{}",
        serde_json::to_string(&connection.recv().await.unwrap())?
    );
    app.shutdown().await?;
    println!(
        "{}",
        serde_json::to_string(&connection.recv().await.unwrap())?
    );
    assert!(connection.recv().await.is_none());
    Ok(())
}
