//! Typed, per-instance native plugin composition on Tokio.
//!
//! Configuration validation and dependency planning finish before setup begins.
//! Services are ordinary `Arc<S>` values, not operations in a global container.
//! See the crate README for a complete example and lifecycle boundaries.
#![doc = include_str!("../README.md")]

use futures_util::FutureExt;
use std::{
    any::{Any, TypeId},
    collections::{HashMap, HashSet},
    error::Error,
    fmt,
    future::Future,
    hash::{Hash, Hasher},
    marker::PhantomData,
    panic::{catch_unwind, AssertUnwindSafe},
    pin::Pin,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};
use tokio::sync::{oneshot, watch, Mutex, Notify};

mod config;
mod inspection;
pub use config::{Config, ConfigError, ConfigFieldSource, ConfigSource};
pub use inspection::{
    CheckStatus, DependencyInspection, Diagnostic, DiagnosticCode, Inspection, InspectionCheck,
    InstanceInspection,
};

/// An application or cleanup error supplied by a plugin.
pub type BoxError = Box<dyn Error + Send + Sync + 'static>;
type Service = Arc<dyn Any + Send + Sync>;
type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;
type Cleanup = Box<dyn FnOnce() -> BoxFuture<'static, Result<(), BoxError>> + Send>;

/// Opaque identity of one reserved instance; cloning preserves identity.
#[derive(Clone)]
pub struct InstanceKey(Arc<u8>);

impl PartialEq for InstanceKey {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}
impl Eq for InstanceKey {}
impl Hash for InstanceKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        Arc::as_ptr(&self.0).hash(state);
    }
}
impl fmt::Debug for InstanceKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("InstanceKey")
            .field(&Arc::as_ptr(&self.0))
            .finish()
    }
}

/// A typed reference to an instance, not ownership of its lifecycle.
#[derive(Debug)]
pub struct Handle<S> {
    key: InstanceKey,
    marker: PhantomData<fn() -> S>,
}
impl<S> Clone for Handle<S> {
    fn clone(&self) -> Self {
        Self {
            key: self.key.clone(),
            marker: PhantomData,
        }
    }
}
impl<S> Handle<S> {
    /// Declare this instance as a dependency.
    pub fn key(&self) -> InstanceKey {
        self.key.clone()
    }
}

/// A native plugin owning configuration and publishing a typed service.
///
/// Setup must register cleanup immediately after acquiring each resource.
/// Implementations may use ordinary `async fn setup` without a helper macro.
pub trait Plugin: Send + 'static {
    /// Ordinary Rust service published by this instance.
    type Service: Send + Sync + 'static;
    /// Check configuration synchronously without acquiring resources.
    fn validate(&self) -> Result<(), BoxError> {
        Ok(())
    }
    /// Required instance identities, including any forward reservations.
    fn dependencies(&self) -> Vec<InstanceKey> {
        Vec::new()
    }
    /// Acquire resources and publish a service.
    fn setup(
        self,
        setup: &mut Setup,
    ) -> impl Future<Output = Result<Arc<Self::Service>, BoxError>> + Send;
}

trait ErasedPlugin: Send {
    fn validate(&self) -> Result<(), BoxError>;
    fn dependencies(&self) -> Vec<InstanceKey>;
    fn setup(self: Box<Self>, setup: &mut Setup) -> BoxFuture<'_, Result<Service, BoxError>>;
}
impl<P: Plugin> ErasedPlugin for P {
    fn validate(&self) -> Result<(), BoxError> {
        Plugin::validate(self)
    }
    fn dependencies(&self) -> Vec<InstanceKey> {
        Plugin::dependencies(self)
    }
    fn setup(self: Box<Self>, setup: &mut Setup) -> BoxFuture<'_, Result<Service, BoxError>> {
        Box::pin(async move { (*self).setup(setup).await.map(|s| s as Service) })
    }
}

/// Entrances are stopped globally before any resources are closed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CleanupPhase {
    /// Stop listeners, schedulers, or other sources of new work.
    Entrances,
    /// Release resources after all entrances have stopped.
    Resources,
}

struct RegisteredCleanup {
    phase: CleanupPhase,
    action: CleanupHandle,
}

type CleanupResult = Result<(), Arc<dyn Error + Send + Sync>>;

struct OnceCleanup {
    action: Mutex<Option<Cleanup>>,
    completion: watch::Sender<Option<CleanupResult>>,
}

/// An optional early disposer for a registered resource.
///
/// Early disposal, rollback and shutdown join the same owned cleanup task.
/// The plugin must ensure early disposal does not invalidate active dependents.
#[derive(Clone)]
pub struct CleanupHandle {
    inner: Arc<OnceCleanup>,
    instance: String,
    phase: CleanupPhase,
}

impl CleanupHandle {
    /// Dispose once and share completion, even when a disposal waiter is cancelled.
    /// A failed early disposal remains visible to later rollback/shutdown.
    pub async fn dispose(&self) -> Result<(), CleanupFailure> {
        let mut completion = self.inner.completion.subscribe();
        let mut action = self.inner.action.lock().await;
        if let Some(action) = action.take() {
            let inner = self.inner.clone();
            tokio::spawn(async move {
                let result = tokio::spawn(async move { action().await }).await;
                let result = match result {
                    Ok(Ok(())) => Ok(()),
                    Ok(Err(e)) => Err(Arc::from(e)),
                    Err(e) => Err(Arc::new(e) as Arc<dyn Error + Send + Sync>),
                };
                inner.completion.send_replace(Some(result));
            });
        }
        drop(action);
        loop {
            if let Some(result) = completion.borrow_and_update().clone() {
                return result.map_err(|source| CleanupFailure {
                    instance: self.instance.clone(),
                    phase: self.phase,
                    source,
                });
            }
            let _ = completion.changed().await;
        }
    }
}

/// Context restricted to the instance's declared dependencies.
pub struct Setup {
    services: HashMap<InstanceKey, Service>,
    dependencies: HashSet<InstanceKey>,
    instance: String,
    cleanups: Vec<RegisteredCleanup>,
    gate: WorkGate,
}
impl Setup {
    /// Admission gate shared by every instance in this application.
    pub fn work_gate(&self) -> WorkGate {
        self.gate.clone()
    }
    /// Name assigned to this particular instance.
    pub fn instance_name(&self) -> &str {
        &self.instance
    }
    /// Get a declared dependency. Undeclared and unavailable handles are errors.
    pub fn get<S: Send + Sync + 'static>(&self, handle: &Handle<S>) -> Result<Arc<S>, AccessError> {
        if !self.dependencies.contains(&handle.key) {
            return Err(AccessError::Undeclared);
        }
        get_service(&self.services, handle)
    }
    /// Register deferred asynchronous cleanup immediately, without awaiting it.
    ///
    /// Within each phase, callbacks run in reverse registration/startup order.
    /// Captured resources must be owned; callbacks are invoked exactly once.
    /// The returned handle allows explicitly coordinated early disposal.
    pub fn on_cleanup<F, Fut>(&mut self, phase: CleanupPhase, action: F) -> CleanupHandle
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = Result<(), BoxError>> + Send + 'static,
    {
        let (completion, _) = watch::channel(None);
        let handle = CleanupHandle {
            inner: Arc::new(OnceCleanup {
                action: Mutex::new(Some(Box::new(move || Box::pin(action())))),
                completion,
            }),
            instance: self.instance.clone(),
            phase,
        };
        self.cleanups.push(RegisteredCleanup {
            phase,
            action: handle.clone(),
        });
        handle
    }
}

/// Typed dependency access failed.
#[derive(Debug, thiserror::Error)]
pub enum AccessError {
    /// Shutdown has sealed admission to this application.
    #[error("application is closing")]
    Closing,
    /// The instance did not declare this dependency.
    #[error("handle is not a declared dependency")]
    Undeclared,
    /// The handle belongs to another app or is not available.
    #[error("handle is unavailable in this app")]
    Unavailable,
    /// Internal service type does not match the typed reservation.
    #[error("service type does not match its handle")]
    TypeMismatch,
}
fn get_service<S: Send + Sync + 'static>(
    services: &HashMap<InstanceKey, Service>,
    handle: &Handle<S>,
) -> Result<Arc<S>, AccessError> {
    services
        .get(&handle.key)
        .cloned()
        .ok_or(AccessError::Unavailable)?
        .downcast::<S>()
        .map_err(|_| AccessError::TypeMismatch)
}

const SEALED: usize = 1 << (usize::BITS - 1);

#[derive(Default)]
struct Admission {
    state: AtomicUsize,
    drained: Notify,
}

/// Shared admission boundary for business work, including retained `Arc` services.
///
/// Enter once at the outer business entry and retain the guard through all awaits.
/// Dependency-local log/cache calls within admitted work must not reenter: shutdown
/// seals new admission while already admitted work is allowed to finish.
#[derive(Clone, Default)]
pub struct WorkGate(Arc<Admission>);

impl WorkGate {
    /// Atomically admit work unless shutdown has sealed the gate.
    pub fn enter(&self) -> Result<WorkGuard, AccessError> {
        let mut state = self.0.state.load(Ordering::Acquire);
        loop {
            if state & SEALED != 0 || state == SEALED - 1 {
                return Err(AccessError::Closing);
            }
            match self.0.state.compare_exchange_weak(
                state,
                state + 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return Ok(WorkGuard { gate: self.clone() }),
                Err(current) => state = current,
            }
        }
    }

    fn seal(&self) {
        self.0.state.fetch_or(SEALED, Ordering::AcqRel);
    }

    async fn drain(&self) {
        while self.0.state.load(Ordering::Acquire) & !SEALED != 0 {
            self.0.drained.notified().await;
        }
    }
}

/// An admitted unit of business work. Dropping it releases the in-flight count.
#[must_use = "retain the guard until business work and its dependency calls finish"]
pub struct WorkGuard {
    gate: WorkGate,
}

impl Drop for WorkGuard {
    fn drop(&mut self) {
        if self.gate.0.state.fetch_sub(1, Ordering::AcqRel) == SEALED | 1 {
            // There is exactly one cleanup owner; a stored permit avoids a lost wakeup.
            self.gate.0.drained.notify_one();
        }
    }
}

struct InstalledInstance {
    key: InstanceKey,
    name: String,
    service_type: TypeId,
    plugin: Option<Box<dyn ErasedPlugin>>,
}

/// An owned plugin instance with a stable handle, ready to add to an app.
pub struct Instance<P: Plugin> {
    name: String,
    plugin: P,
    key: InstanceKey,
}
impl<P: Plugin> Instance<P> {
    /// Create an instance without validating configuration or acquiring resources.
    pub fn new(name: impl Into<String>, plugin: P) -> Self {
        Self {
            name: name.into(),
            plugin,
            key: InstanceKey(Arc::new(0)),
        }
    }
    /// Capture the typed handle before moving this instance into an app.
    pub fn handle(&self) -> Handle<P::Service> {
        Handle {
            key: self.key.clone(),
            marker: PhantomData,
        }
    }
}

/// Builder for one isolated application; errors are checked by [`App::start`].
#[derive(Default)]
pub struct App {
    instances: Vec<InstalledInstance>,
    binding_errors: Vec<Diagnostic>,
}
impl App {
    /// Create an empty application.
    pub fn new() -> Self {
        Self::default()
    }
    /// Create and add an instance, returning its typed handle.
    pub fn install<P: Plugin>(&mut self, name: impl Into<String>, plugin: P) -> Handle<P::Service> {
        let instance = Instance::new(name, plugin);
        let handle = instance.handle();
        self.insert(instance);
        handle
    }
    /// Add an owned instance to this app.
    pub fn with<P: Plugin>(mut self, instance: Instance<P>) -> Self {
        self.insert(instance);
        self
    }
    fn insert<P: Plugin>(&mut self, instance: Instance<P>) {
        self.instances.push(InstalledInstance {
            key: instance.key,
            name: instance.name,
            service_type: TypeId::of::<P::Service>(),
            plugin: Some(Box::new(instance.plugin)),
        });
    }
    /// Reserve a forward dependency. Every reservation must be bound before start.
    pub fn reserve<S: Send + Sync + 'static>(&mut self, name: impl Into<String>) -> Handle<S> {
        let key = InstanceKey(Arc::new(0));
        self.instances.push(InstalledInstance {
            key: key.clone(),
            name: name.into(),
            service_type: TypeId::of::<S>(),
            plugin: None,
        });
        Handle {
            key,
            marker: PhantomData,
        }
    }
    /// Bind exactly once. Foreign or duplicate bindings fail validation at start.
    pub fn bind<P: Plugin>(&mut self, handle: &Handle<P::Service>, plugin: P) {
        match self
            .instances
            .iter_mut()
            .enumerate()
            .find(|(_, i)| i.key == handle.key)
        {
            Some((_, i)) if i.plugin.is_none() && i.service_type == TypeId::of::<P::Service>() => {
                i.plugin = Some(Box::new(plugin));
            }
            Some((index, _)) => self.binding_errors.push(Diagnostic::new(
                DiagnosticCode::InvalidBinding,
                Some(index),
                "duplicate or mismatched binding",
            )),
            None => self.binding_errors.push(Diagnostic::new(
                DiagnosticCode::ForeignBinding,
                None,
                "binding uses a foreign handle",
            )),
        }
    }
    /// Inspect configuration and dependency declarations without setup or a Tokio runtime.
    ///
    /// This borrows the builder; it can still be started afterwards. Plugin validation
    /// and dependency hooks must themselves be free of resource acquisition. Inspection
    /// cannot certify resource availability, setup success, or runtime readiness.
    pub fn inspect(&self) -> Inspection {
        self.inspect_plan().0
    }

    fn plan(&self) -> Result<(Vec<usize>, Vec<Vec<InstanceKey>>), StartError> {
        let (inspection, dependencies) = self.inspect_plan();
        if inspection.is_valid() {
            Ok((inspection.startup_order, dependencies))
        } else {
            Err(StartError::Validation(
                inspection
                    .diagnostics
                    .iter()
                    .map(|diagnostic| {
                        let instance = diagnostic
                            .instance
                            .map(|index| self.instances[index].name.as_str());
                        match instance {
                            Some(name) => format!("{} for {name}", diagnostic.message),
                            None => diagnostic.message.clone(),
                        }
                    })
                    .collect(),
            ))
        }
    }
    /// Validate the complete graph, then set up in dependency order.
    ///
    /// Call from an active Tokio runtime. An owned task finishes startup even if
    /// this waiter is cancelled, and shuts down an app not acknowledged by its
    /// caller. Setup errors and unwind panics roll back registered resources.
    /// Runtime termination and abort-on-panic are outside this guarantee.
    pub async fn start(self) -> Result<RunningApp, StartError> {
        let (delivery, receiver) = oneshot::channel();
        let (acknowledge, acknowledged) = oneshot::channel();
        tokio::spawn(async move {
            let result = self.start_owned().await;
            let running = result.as_ref().ok().cloned();
            let delivered = delivery.send(result).is_ok();
            if let Some(running) = running {
                if !delivered || acknowledged.await.is_err() {
                    let _ = running.shutdown().await;
                }
            }
        });
        let result = receiver.await.map_err(|_| {
            StartError::Validation(vec!["owned startup task stopped before delivery".into()])
        })?;
        // No await between receiving ownership and acknowledging it.
        let _ = acknowledge.send(());
        result
    }

    async fn start_owned(mut self) -> Result<RunningApp, StartError> {
        let (order, dependencies) = self.plan()?;
        let mut services = HashMap::new();
        let mut cleanups = Vec::new();
        let gate = WorkGate::default();
        for index in order {
            let instance = &mut self.instances[index];
            let mut setup = Setup {
                services: services.clone(),
                dependencies: dependencies[index].iter().cloned().collect(),
                instance: instance.name.clone(),
                cleanups: Vec::new(),
                gate: gate.clone(),
            };
            let Some(plugin) = instance.plugin.take() else {
                return Err(StartError::Validation(vec!["unbound instance".into()]));
            };
            // Future construction happens inside this async block so an unwind
            // cannot drop Setup and lose callbacks registered before the panic.
            let result = AssertUnwindSafe(async { plugin.setup(&mut setup).await })
                .catch_unwind()
                .await
                .unwrap_or_else(|_| Err("plugin setup panicked".into()));
            cleanups.extend(setup.cleanups);
            match result {
                Ok(service) => {
                    services.insert(instance.key.clone(), service);
                }
                Err(source) => {
                    let cleanup = run_cleanups(cleanups, &gate).await.err();
                    return Err(StartError::Setup {
                        instance: instance.name.clone(),
                        source,
                        cleanup,
                    });
                }
            }
        }
        let (completion, _) = watch::channel(None);
        Ok(RunningApp {
            inner: Arc::new(Running {
                services,
                cleanups: Mutex::new(Some(cleanups)),
                completion,
                gate,
            }),
        })
    }
}

/// Startup failed before resources were acquired, or setup failed with rollback.
#[derive(Debug, thiserror::Error)]
pub enum StartError {
    /// All discovered builder/configuration/dependency errors.
    #[error("application validation failed: {0:?}")]
    Validation(Vec<String>),
    /// Setup error plus any failures encountered during rollback.
    #[error("setup failed for {instance}: {source}; cleanup: {cleanup:?}")]
    Setup {
        /// Instance that failed setup.
        instance: String,
        /// Original plugin error.
        #[source]
        source: BoxError,
        /// Aggregate rollback failures, if any.
        cleanup: Option<ShutdownError>,
    },
}

/// One cleanup callback failed; subsequent callbacks still run.
#[derive(Clone, Debug, thiserror::Error)]
#[error("cleanup failed for {instance} in {phase:?}: {source}")]
pub struct CleanupFailure {
    /// Instance owning the callback.
    pub instance: String,
    /// Phase of the callback.
    pub phase: CleanupPhase,
    /// Original error, or Tokio task panic error.
    pub source: Arc<dyn Error + Send + Sync>,
}
/// Aggregate of every failed cleanup in execution order.
#[derive(Clone, Debug, thiserror::Error)]
#[error("cleanup failed: {failures:?}")]
pub struct ShutdownError {
    /// Individual failures; never empty.
    pub failures: Vec<CleanupFailure>,
}

async fn run_cleanups(
    cleanups: Vec<RegisteredCleanup>,
    gate: &WorkGate,
) -> Result<(), ShutdownError> {
    gate.seal();
    let mut failures = Vec::new();
    let mut pending: Vec<_> = cleanups.into_iter().rev().map(Some).collect();
    for phase in [CleanupPhase::Entrances, CleanupPhase::Resources] {
        if phase == CleanupPhase::Resources {
            gate.drain().await;
        }
        for entry in &mut pending {
            if entry.as_ref().is_some_and(|c| c.phase == phase) {
                if let Some(c) = entry.take() {
                    if let Err(failure) = c.action.dispose().await {
                        failures.push(failure);
                    }
                }
            }
        }
    }
    if failures.is_empty() {
        Ok(())
    } else {
        Err(ShutdownError { failures })
    }
}

struct Running {
    services: HashMap<InstanceKey, Service>,
    cleanups: Mutex<Option<Vec<RegisteredCleanup>>>,
    completion: watch::Sender<Option<Result<(), ShutdownError>>>,
    gate: WorkGate,
}
/// Shared application lifetime. Dropping this value does **not** run async cleanup.
#[derive(Clone)]
pub struct RunningApp {
    inner: Arc<Running>,
}
impl RunningApp {
    /// Admission gate for business work on this application's services.
    pub fn work_gate(&self) -> WorkGate {
        self.inner.gate.clone()
    }
    /// Get a service while the app is open. Retained services must use the gate
    /// at business entry; merely obtaining an `Arc` does not admit work.
    pub fn get<S: Send + Sync + 'static>(&self, handle: &Handle<S>) -> Result<Arc<S>, AccessError> {
        let _access = self.inner.gate.enter()?;
        get_service(&self.inner.services, handle)
    }
    /// Shut down once and share completion across concurrent/repeated callers.
    ///
    /// Once polled past lifecycle acquisition, cleanup runs in a detached Tokio
    /// task even if this waiter is cancelled. Keep the Tokio runtime alive until
    /// completion; no asynchronous `Drop` behavior is promised.
    pub async fn shutdown(&self) -> Result<(), ShutdownError> {
        let mut completion = self.inner.completion.subscribe();
        let mut cleanups = self.inner.cleanups.lock().await;
        if let Some(cleanups) = cleanups.take() {
            self.inner.gate.seal();
            let inner = self.inner.clone();
            tokio::spawn(async move {
                let result = run_cleanups(cleanups, &inner.gate).await;
                inner.completion.send_replace(Some(result));
            });
        }
        drop(cleanups);
        loop {
            if let Some(result) = completion.borrow_and_update().clone() {
                return result;
            }
            // The sender is retained by self and the cleanup task.
            let _ = completion.changed().await;
        }
    }
}
