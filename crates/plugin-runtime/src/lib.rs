//! Optional Runtime transport integration for native Lenso applications.
//!
//! Install a registered connection as an ordinary typed dependency. Export factories
//! run only after native setup completes, so native services never depend on their
//! own exported handlers.

use lenso_plugin_core::{App, BoxError, CleanupPhase, Handle, Plugin, RunningApp, Setup};
use runtime_sdk::{Handlers, HostOptions, RuntimeError, RuntimeHost};
use std::sync::Arc;

/// A registered, initially unready Runtime connection.
pub struct RuntimePlugin {
    options: HostOptions,
    drain_timeout_ms: u64,
}

impl RuntimePlugin {
    pub fn new(options: HostOptions, drain_timeout_ms: u64) -> Self {
        Self {
            options,
            drain_timeout_ms,
        }
    }
}

impl Plugin for RuntimePlugin {
    type Service = RuntimeHost;

    fn validate(&self) -> Result<(), BoxError> {
        self.options.validate()?;
        if !(1..=60000).contains(&self.drain_timeout_ms) {
            return Err(Box::new(RuntimeError::new("invalid_input")));
        }
        Ok(())
    }

    async fn setup(self, setup: &mut Setup) -> Result<Arc<RuntimeHost>, BoxError> {
        let host = Arc::new(RuntimeHost::connect_registered(self.options).await?);
        let connection = host.clone();
        setup.on_cleanup(CleanupPhase::Entrances, move || async move {
            let result = connection.drain(self.drain_timeout_ms).await;
            connection.close();
            result.map_err(|error| Box::new(error) as BoxError)
        });
        Ok(host)
    }
}

type ExportFactory = Box<dyn FnOnce(&RunningApp) -> Result<Handlers, BoxError> + Send + 'static>;

/// Explicit post-setup binding for one exact installed connection.
pub struct RuntimeExport {
    handle: Handle<RuntimeHost>,
    factory: ExportFactory,
}

impl RuntimeExport {
    pub fn new(
        handle: Handle<RuntimeHost>,
        factory: impl FnOnce(&RunningApp) -> Result<Handlers, BoxError> + Send + 'static,
    ) -> Self {
        Self {
            handle,
            factory: Box::new(factory),
        }
    }
}

/// Startup failure, retaining any failure reported by rollback cleanup.
#[derive(Debug)]
pub struct RuntimeStartError {
    pub cause: BoxError,
    pub rollback: Option<BoxError>,
}

impl std::fmt::Display for RuntimeStartError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "Runtime application startup failed: {}",
            self.cause
        )?;
        if let Some(rollback) = &self.rollback {
            write!(formatter, "; rollback failed: {rollback}")?;
        }
        Ok(())
    }
}

impl std::error::Error for RuntimeStartError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.cause.as_ref())
    }
}

/// Complete native setup, then bind and activate only the explicitly selected handles.
/// An empty export list supports outbound-only applications without sending ready.
pub async fn start_runtime_app(
    app: App,
    exports: Vec<RuntimeExport>,
) -> Result<RunningApp, BoxError> {
    for (index, export) in exports.iter().enumerate() {
        if exports[..index]
            .iter()
            .any(|other| other.handle.key() == export.handle.key())
        {
            return Err(Box::new(RuntimeError::new("invalid_input")));
        }
    }
    let (mut sender, receiver) = tokio::sync::oneshot::channel();
    // Core setup is not cancellation-safe. Own the entire startup in one task;
    // if its caller disappears, finish setup and shut down instead of abandoning it.
    tokio::spawn(async move {
        let result = start_and_activate(app, exports, &mut sender).await;
        if let Err(Ok(running)) = sender.send(result) {
            let _ = running.shutdown().await;
        }
    });
    receiver
        .await
        .map_err(|_| Box::new(RuntimeError::new("connection_lost")) as BoxError)?
}

async fn start_and_activate(
    app: App,
    exports: Vec<RuntimeExport>,
    sender: &mut tokio::sync::oneshot::Sender<Result<RunningApp, BoxError>>,
) -> Result<RunningApp, BoxError> {
    let running = app.start().await?;
    let cancelled = sender.is_closed();
    let activation = async {
        if cancelled {
            return Err(Box::new(RuntimeError::new("cancelled")) as BoxError);
        }
        let mut bindings = Vec::with_capacity(exports.len());
        for export in exports {
            let host = running.get(&export.handle)?;
            let handlers = (export.factory)(&running)?;
            host.validate_handlers(&handlers)?;
            bindings.push((host, handlers));
        }
        for (host, handlers) in bindings {
            host.activate(handlers).await?;
        }
        Ok::<(), BoxError>(())
    };
    let activation = tokio::select! {
        result = activation => result,
        _ = sender.closed() => Err(Box::new(RuntimeError::new("cancelled")) as BoxError),
    };
    if let Err(cause) = activation {
        let rollback = running
            .shutdown()
            .await
            .err()
            .map(|error| Box::new(error) as BoxError);
        return Err(Box::new(RuntimeStartError { cause, rollback }));
    }
    Ok(running)
}
