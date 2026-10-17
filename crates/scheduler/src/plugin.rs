use crate::{store::Store, validate_name, Scheduler, SchedulerError};
use lenso_plugin_core::{BoxError, CleanupPhase, Handle, InstanceKey, Plugin, Setup};
use lenso_tasks::Tasks;
use std::{path::PathBuf, sync::Arc};

/// Opens one existing SQLite scheduler; no implicit timer, worker, schema or network.
pub struct SchedulerPlugin {
    path: PathBuf,
    name: String,
    tasks: Handle<Tasks>,
}
impl SchedulerPlugin {
    pub fn new(path: impl Into<PathBuf>, name: impl Into<String>, tasks: Handle<Tasks>) -> Self {
        Self {
            path: path.into(),
            name: name.into(),
            tasks,
        }
    }
}
impl Plugin for SchedulerPlugin {
    type Service = Scheduler;
    fn dependencies(&self) -> Vec<InstanceKey> {
        vec![self.tasks.key()]
    }
    fn validate(&self) -> Result<(), BoxError> {
        validate_name(&self.name)?;
        if self.path.as_os_str().is_empty() {
            return Err(SchedulerError::InvalidOptions.into());
        }
        Ok(())
    }
    async fn setup(self, setup: &mut Setup) -> Result<Arc<Scheduler>, BoxError> {
        let tasks = setup.get(&self.tasks)?;
        let queue_identity = tasks.identity().to_owned();
        let store = Arc::new(
            tokio::task::spawn_blocking(move || {
                Store::open(&self.path, self.name, &queue_identity)
            })
            .await??,
        );
        let cleanup = store.clone();
        setup.on_cleanup(CleanupPhase::Resources, move || async move {
            tokio::task::spawn_blocking(move || cleanup.close()).await??;
            Ok(())
        });
        Ok(Arc::new(Scheduler {
            store,
            tasks,
            gate: setup.work_gate(),
        }))
    }
}
