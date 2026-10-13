//! Typed, per-instance native plugin composition on Tokio.
//!
//! Configuration validation and dependency planning finish before setup begins.
//! Services are ordinary `Arc<S>` values, not operations in a global container.
//! See the crate README for a complete example and lifecycle boundaries.
#![doc = include_str!("../README.md")]

use std::{
    any::{Any, TypeId},
    collections::{HashMap, HashSet},
    error::Error,
    fmt,
    future::Future,
    hash::{Hash, Hasher},
    marker::PhantomData,
    pin::Pin,
    sync::Arc,
};
use tokio::sync::{watch, Mutex};

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
    instance: String,
    phase: CleanupPhase,
    action: Cleanup,
}

/// Context restricted to the instance's declared dependencies.
pub struct Setup {
    services: HashMap<InstanceKey, Service>,
    dependencies: HashSet<InstanceKey>,
    instance: String,
    cleanups: Vec<RegisteredCleanup>,
}
impl Setup {
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
    pub fn on_cleanup<F, Fut>(&mut self, phase: CleanupPhase, action: F)
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = Result<(), BoxError>> + Send + 'static,
    {
        self.cleanups.push(RegisteredCleanup {
            instance: self.instance.clone(),
            phase,
            action: Box::new(move || Box::pin(action())),
        });
    }
}

/// Typed dependency access failed.
#[derive(Debug, thiserror::Error)]
pub enum AccessError {
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
    binding_errors: Vec<String>,
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
        match self.instances.iter_mut().find(|i| i.key == handle.key) {
            Some(i) if i.plugin.is_none() && i.service_type == TypeId::of::<P::Service>() => {
                i.plugin = Some(Box::new(plugin));
            }
            Some(i) => self
                .binding_errors
                .push(format!("duplicate or mismatched binding: {}", i.name)),
            None => self
                .binding_errors
                .push("binding uses a foreign handle".into()),
        }
    }
    fn plan(&self) -> Result<(Vec<usize>, Vec<Vec<InstanceKey>>), StartError> {
        let mut errors = self.binding_errors.clone();
        let mut names = HashSet::new();
        let mut keys = HashMap::new();
        for (index, i) in self.instances.iter().enumerate() {
            if !names.insert(&i.name) {
                errors.push(format!("duplicate instance name: {}", i.name));
            }
            if keys.insert(i.key.clone(), index).is_some() {
                errors.push(format!("duplicate instance identity: {}", i.name));
            }
        }
        let mut dependencies = Vec::new();
        for i in &self.instances {
            let deps = match &i.plugin {
                Some(plugin) => {
                    if let Err(e) = plugin.validate() {
                        errors.push(format!("invalid config for {}: {e}", i.name));
                    }
                    plugin.dependencies()
                }
                None => {
                    errors.push(format!("unbound instance: {}", i.name));
                    Vec::new()
                }
            };
            for dep in &deps {
                if !keys.contains_key(dep) {
                    errors.push(format!("missing or foreign dependency for {}", i.name));
                }
            }
            dependencies.push(deps);
        }
        let mut order = Vec::new();
        let mut visited = HashSet::new();
        loop {
            let before = order.len();
            for (index, i) in self.instances.iter().enumerate() {
                if !visited.contains(&i.key)
                    && dependencies[index].iter().all(|d| visited.contains(d))
                {
                    visited.insert(i.key.clone());
                    order.push(index);
                }
            }
            if order.len() == before {
                break;
            }
        }
        if order.len() != self.instances.len()
            && dependencies.iter().flatten().all(|d| keys.contains_key(d))
        {
            errors.push("dependency cycle".into());
        }
        if errors.is_empty() {
            Ok((order, dependencies))
        } else {
            Err(StartError::Validation(errors))
        }
    }
    /// Validate the complete graph, then set up in dependency order.
    ///
    /// Call from an active Tokio runtime. Do not cancel startup: only shutdown
    /// waiters have a cancellation guarantee. Returned setup errors roll back
    /// the failing instance's registered resources and all prior instances.
    pub async fn start(mut self) -> Result<RunningApp, StartError> {
        let (order, dependencies) = self.plan()?;
        let mut services = HashMap::new();
        let mut cleanups = Vec::new();
        for index in order {
            let instance = &mut self.instances[index];
            let mut setup = Setup {
                services: services.clone(),
                dependencies: dependencies[index].iter().cloned().collect(),
                instance: instance.name.clone(),
                cleanups: Vec::new(),
            };
            let Some(plugin) = instance.plugin.take() else {
                return Err(StartError::Validation(vec!["unbound instance".into()]));
            };
            let result = plugin.setup(&mut setup).await;
            cleanups.extend(setup.cleanups);
            match result {
                Ok(service) => {
                    services.insert(instance.key.clone(), service);
                }
                Err(source) => {
                    let cleanup = run_cleanups(cleanups).await.err();
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
#[derive(Clone, Debug)]
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

async fn run_cleanups(cleanups: Vec<RegisteredCleanup>) -> Result<(), ShutdownError> {
    let mut failures = Vec::new();
    let mut pending: Vec<_> = cleanups.into_iter().rev().map(Some).collect();
    for phase in [CleanupPhase::Entrances, CleanupPhase::Resources] {
        for entry in &mut pending {
            if entry.as_ref().is_some_and(|c| c.phase == phase) {
                if let Some(c) = entry.take() {
                    // A callback panic must not strand completion or skip later cleanup.
                    let result = tokio::spawn(async move { (c.action)().await }).await;
                    let source = match result {
                        Ok(Ok(())) => continue,
                        Ok(Err(e)) => Arc::from(e),
                        Err(e) => Arc::new(e) as Arc<dyn Error + Send + Sync>,
                    };
                    failures.push(CleanupFailure {
                        instance: c.instance,
                        phase,
                        source,
                    });
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
}
/// Shared application lifetime. Dropping this value does **not** run async cleanup.
#[derive(Clone)]
pub struct RunningApp {
    inner: Arc<Running>,
}
impl RunningApp {
    /// Get a service from this app. Retained services follow their plugin's post-shutdown semantics.
    pub fn get<S: Send + Sync + 'static>(&self, handle: &Handle<S>) -> Result<Arc<S>, AccessError> {
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
            let inner = self.inner.clone();
            tokio::spawn(async move {
                let result = run_cleanups(cleanups).await;
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
