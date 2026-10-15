use lenso_plugin_core::{App, BoxError, Handle, Instance, InstanceKey, Plugin, Setup, WorkGate};
use lenso_storage::{
    LocalStorage, LocalStorageConfig, LocalStoragePlugin, ObjectStorage, PutObject,
};
use std::{io::Cursor, sync::Arc};

// Business code depends on the small storage interface and can also be constructed
// directly with a LocalStorage or another ObjectStorage implementation.
struct Archive {
    storage: Arc<dyn ObjectStorage>,
}
impl Archive {
    async fn save(&self, key: &str, bytes: Vec<u8>) -> Result<(), BoxError> {
        self.storage
            .put(PutObject::new(key, Cursor::new(bytes)))
            .await?;
        Ok(())
    }
}
struct ArchiveEntry {
    archive: Archive,
    gate: WorkGate,
}
impl ArchiveEntry {
    async fn save(&self, key: &str, bytes: Vec<u8>) -> Result<(), BoxError> {
        let _work = self.gate.enter()?;
        self.archive.save(key, bytes).await
    }
}
struct ArchivePlugin {
    storage: Handle<LocalStorage>,
}
impl Plugin for ArchivePlugin {
    type Service = ArchiveEntry;
    fn dependencies(&self) -> Vec<InstanceKey> {
        vec![self.storage.key()]
    }
    async fn setup(self, setup: &mut Setup) -> Result<Arc<ArchiveEntry>, BoxError> {
        Ok(Arc::new(ArchiveEntry {
            archive: Archive {
                storage: setup.get(&self.storage)?,
            },
            gate: setup.work_gate(),
        }))
    }
}

#[tokio::main]
async fn main() -> Result<(), BoxError> {
    let temp = tempfile::tempdir()?;
    let root = temp.path().canonicalize()?.join("objects");
    let storage = Instance::new(
        "archive-storage",
        LocalStoragePlugin::new(LocalStorageConfig::new(root)),
    );
    let archive = Instance::new(
        "archive",
        ArchivePlugin {
            storage: storage.handle(),
        },
    );
    let handle = archive.handle();
    let app = App::new().with(archive).with(storage).start().await?;
    app.get(&handle)?
        .save("reports/today.txt", b"daily report".to_vec())
        .await?;
    app.shutdown().await?;
    println!("Exact-instance storage dependency completed and shut down.");
    Ok(())
}
