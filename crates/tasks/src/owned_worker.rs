use crate::{TaskError, Tasks};
use lenso_plugin_core::{BoxError, CleanupPhase, Handle, InstanceKey, Plugin, Setup};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::sync::{watch, Notify};

/// Explicit host policy; each lane awaits its accepted run before polling again.
#[derive(Clone, Debug)]
pub struct WorkerConfig {
    pub concurrency: usize,
    pub idle_wait: Duration,
}
impl Default for WorkerConfig {
    fn default() -> Self {
        Self {
            concurrency: 1,
            idle_wait: Duration::from_millis(250),
        }
    }
}

struct Control {
    stopped: Mutex<bool>,
    wake: Notify,
}
impl Control {
    fn stop(&self) {
        *self.stopped.lock().expect("worker control poisoned") = true;
        self.wake.notify_waiters();
    }
}

/// Owned worker lifetime. Stop closes worker admission only; accepted handlers
/// settle normally. Dropping a join waiter does not cancel the worker or cleanup.
pub struct Worker {
    control: Arc<Control>,
    done: watch::Receiver<Option<Result<(), Arc<TaskError>>>>,
}
impl Worker {
    pub fn stop(&self) {
        self.control.stop();
    }
    pub async fn join(&self) -> Result<(), Arc<TaskError>> {
        let mut done = self.done.clone();
        loop {
            if let Some(result) = done.borrow_and_update().clone() {
                return result;
            }
            if done.changed().await.is_err() {
                return Err(Arc::new(TaskError::Runtime));
            }
        }
    }
}

/// Install explicitly after the exact Tasks instance. Core stops admission,
/// drains work, then joins before closing the provider. No additional queue.
pub struct WorkerPlugin {
    tasks: Handle<Tasks>,
    config: WorkerConfig,
}
impl WorkerPlugin {
    pub fn new(tasks: Handle<Tasks>, config: WorkerConfig) -> Self {
        Self { tasks, config }
    }
}
impl Plugin for WorkerPlugin {
    type Service = Worker;
    fn dependencies(&self) -> Vec<InstanceKey> {
        vec![self.tasks.key()]
    }
    fn validate(&self) -> Result<(), BoxError> {
        if !(1..=64).contains(&self.config.concurrency) || self.config.idle_wait.is_zero() {
            return Err(TaskError::InvalidOptions.into());
        }
        Ok(())
    }
    async fn setup(self, setup: &mut Setup) -> Result<Arc<Worker>, BoxError> {
        let tasks = setup.get(&self.tasks)?;
        let control = Arc::new(Control {
            stopped: Mutex::new(false),
            wake: Notify::new(),
        });
        let (complete, done) = watch::channel(None);
        let worker = Arc::new(Worker {
            control: control.clone(),
            done,
        });
        let cleanup = worker.clone();
        setup.on_cleanup(CleanupPhase::Entrances, move || async move {
            cleanup.stop();
            Ok(())
        });
        // Core must stop every entrance before waiting for accepted handlers.
        // This dependency's Resources cleanup runs before Tasks closes its store.
        let cleanup = worker.clone();
        setup.on_cleanup(CleanupPhase::Resources, move || async move {
            cleanup
                .join()
                .await
                .map_err(|e| -> BoxError { Box::new(e) })
        });
        tokio::spawn(async move {
            let mut lanes = tokio::task::JoinSet::new();
            for _ in 0..self.config.concurrency {
                let tasks = tasks.clone();
                let control = control.clone();
                let idle = self.config.idle_wait;
                lanes.spawn(async move {
                    loop {
                        // Register the notification before checking stop, avoiding a lost wake.
                        let wake = control.wake.notified();
                        tokio::pin!(wake);
                        wake.as_mut().enable();
                        let run = {
                            let stopped = control.stopped.lock().expect("worker control poisoned");
                            if *stopped { return Ok(()); }
                            tasks.spawn_one()
                        };
                        let result = match run {
                            Ok(run) => run.await.map_err(|_| TaskError::Runtime)?,
                            Err(TaskError::Closed) => return Ok(()),
                            Err(error) => return Err(error),
                        };
                        match result {
                            Ok(Some(_)) => {},
                            Ok(None) => { tokio::select! { _ = &mut wake => {}, _ = tokio::time::sleep(idle) => {} } },
                            Err(error) => return Err(error),
                        }
                    }
                });
            }
            let mut failure = None;
            while let Some(result) = lanes.join_next().await {
                let result = result.unwrap_or(Err(TaskError::Runtime));
                if let Err(error) = result {
                    if failure.is_none() {
                        failure = Some(Arc::new(error));
                    }
                    control.stop();
                }
            }
            complete.send_replace(Some(failure.map_or(Ok(()), Err)));
        });
        Ok(worker)
    }
}
