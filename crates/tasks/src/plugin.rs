use crate::{store::Store, validate_name, Task, TaskError, Tasks, WorkerOptions};
use lenso_plugin_core::{BoxError, CleanupPhase, Plugin, Setup};
use std::{collections::BTreeMap, path::PathBuf, sync::Arc};

enum Provider {
    Sqlite(PathBuf),
    #[cfg(feature = "postgres")]
    Postgres(Box<crate::PostgresOptions>),
}
/// Owns provider connections acquired at startup. Starts no worker or migration.
pub struct TasksPlugin {
    provider: Provider,
    queue: String,
    tasks: Vec<Task>,
    options: WorkerOptions,
}
impl TasksPlugin {
    pub fn new(path: impl Into<PathBuf>, queue: impl Into<String>, tasks: Vec<Task>) -> Self {
        Self {
            provider: Provider::Sqlite(path.into()),
            queue: queue.into(),
            tasks,
            options: WorkerOptions::default(),
        }
    }
    /// Use PostgreSQL over a Unix socket or trusted local tunnel (NoTls).
    /// Configure connection timeout and session statement_timeout in the Config.
    #[cfg(feature = "postgres")]
    pub fn postgres(
        config: crate::PostgresConfig,
        queue: impl Into<String>,
        tasks: Vec<Task>,
    ) -> Self {
        Self::postgres_with_options(crate::PostgresOptions::new(config), queue, tasks)
    }
    /// PostgreSQL with configurable TLS and bounded connection reuse.
    #[cfg(feature = "postgres")]
    pub fn postgres_with_options(
        config: crate::PostgresOptions,
        queue: impl Into<String>,
        tasks: Vec<Task>,
    ) -> Self {
        Self {
            provider: Provider::Postgres(Box::new(config)),
            queue: queue.into(),
            tasks,
            options: WorkerOptions::default(),
        }
    }
    #[must_use]
    pub fn worker_options(mut self, options: WorkerOptions) -> Self {
        self.options = options;
        self
    }
}
impl Plugin for TasksPlugin {
    type Service = Tasks;
    fn validate(&self) -> Result<(), BoxError> {
        validate_name(&self.queue)?;
        #[cfg(feature = "postgres")]
        if let Provider::Postgres(options) = &self.provider {
            options.validate()?;
        }
        self.options.validate()?;
        if matches!(&self.provider, Provider::Sqlite(path) if path.as_os_str().is_empty())
            || self.tasks.is_empty()
        {
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
            tokio::task::spawn_blocking(move || match self.provider {
                Provider::Sqlite(path) => Store::open(&path, self.queue),
                #[cfg(feature = "postgres")]
                Provider::Postgres(config) => Store::open_postgres(*config, self.queue),
            })
            .await??,
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
