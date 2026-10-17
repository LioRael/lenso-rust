use crate::{store::Store, validate_name, Task, TaskError, Tasks, WorkerOptions};
use lenso_plugin_core::{BoxError, CleanupPhase, Plugin, Setup};
use std::{collections::BTreeMap, path::PathBuf, sync::Arc};

/// Owns exactly one SQLite connection. Starts no worker, timer, migration or network I/O.
pub struct TasksPlugin {
    path: PathBuf,
    queue: String,
    tasks: Vec<Task>,
    options: WorkerOptions,
}
impl TasksPlugin {
    pub fn new(path: impl Into<PathBuf>, queue: impl Into<String>, tasks: Vec<Task>) -> Self {
        Self {
            path: path.into(),
            queue: queue.into(),
            tasks,
            options: WorkerOptions::default(),
        }
    }
    pub fn worker_options(mut self, options: WorkerOptions) -> Self {
        self.options = options;
        self
    }
}
impl Plugin for TasksPlugin {
    type Service = Tasks;
    fn validate(&self) -> Result<(), BoxError> {
        validate_name(&self.queue)?;
        self.options.validate()?;
        if self.path.as_os_str().is_empty() || self.tasks.is_empty() {
            return Err(TaskError::InvalidOptions.into());
        }
        let mut names = std::collections::HashSet::new();
        for task in &self.tasks {
            validate_name(&task.name)?;
            task.retry.validate()?;
            if !names.insert(&task.name) {
                return Err(TaskError::InvalidOptions.into());
            }
        }
        Ok(())
    }
    async fn setup(self, setup: &mut Setup) -> Result<Arc<Tasks>, BoxError> {
        let store = Arc::new(
            tokio::task::spawn_blocking(move || Store::open(&self.path, self.queue)).await??,
        );
        let cleanup = store.clone();
        setup.on_cleanup(CleanupPhase::Resources, move || async move {
            tokio::task::spawn_blocking(move || cleanup.close()).await??;
            Ok(())
        });
        Ok(Arc::new(Tasks {
            store,
            tasks: Arc::new(
                self.tasks
                    .into_iter()
                    .map(|task| (task.name.clone(), task))
                    .collect::<BTreeMap<_, _>>(),
            ),
            options: self.options,
            gate: setup.work_gate(),
        }))
    }
}
