use lenso_plugin_core::{
    AccessError, App, BoxError, CleanupPhase, Handle, Instance, InstanceKey, Plugin, Setup,
    StartError,
};
use std::sync::{Arc, Mutex};
use tokio::sync::Semaphore;

type Events = Arc<Mutex<Vec<String>>>;

#[tokio::test]
async fn instance_handles_precede_composition_and_dependency_order_is_not_insertion_order() {
    let events = Events::default();
    let provider = Instance::new("provider", Probe::new("provider", &events));
    let provider_handle = provider.handle();
    let mut dependent_plugin = Probe::new("dependent", &events);
    dependent_plugin.dependency = Some(provider_handle.clone());
    let dependent = Instance::new("dependent", dependent_plugin);
    let dependent_handle = dependent.handle();
    let app = App::new().with(dependent).with(provider);
    let running = app.start().await.unwrap();
    assert_eq!(
        running.get(&dependent_handle).unwrap().as_str(),
        "dependent"
    );
    assert_eq!(
        *events.lock().unwrap(),
        ["start provider", "start dependent"]
    );
    running.shutdown().await.unwrap();
}

struct Probe {
    name: &'static str,
    dependency: Option<Handle<String>>,
    events: Events,
    invalid: bool,
    fail_setup: bool,
    fail_cleanup: bool,
}
impl Probe {
    fn new(name: &'static str, events: &Events) -> Self {
        Self {
            name,
            dependency: None,
            events: events.clone(),
            invalid: false,
            fail_setup: false,
            fail_cleanup: false,
        }
    }
}
impl Plugin for Probe {
    type Service = String;
    fn validate(&self) -> Result<(), BoxError> {
        if self.invalid {
            Err("bad configuration".into())
        } else {
            Ok(())
        }
    }
    fn dependencies(&self) -> Vec<InstanceKey> {
        self.dependency.iter().map(Handle::key).collect()
    }
    async fn setup(self, setup: &mut Setup) -> Result<Arc<String>, BoxError> {
        if let Some(dependency) = &self.dependency {
            setup.get(dependency)?;
        }
        self.events
            .lock()
            .unwrap()
            .push(format!("start {}", self.name));
        // Register resources first to prove phase order overrides registration order.
        for phase in [CleanupPhase::Resources, CleanupPhase::Entrances] {
            let events = self.events.clone();
            let name = self.name;
            let fail = self.fail_cleanup;
            setup.on_cleanup(phase, move || async move {
                events.lock().unwrap().push(format!("{phase:?} {name}"));
                if fail {
                    Err(format!("cleanup {name} {phase:?}").into())
                } else {
                    Ok(())
                }
            });
        }
        if self.fail_setup {
            Err("partial setup failed".into())
        } else {
            Ok(Arc::new(self.name.into()))
        }
    }
}

#[tokio::test]
async fn partial_setup_failure_rolls_back_failing_and_prior_instances_in_two_phases() {
    let events = Events::default();
    let mut app = App::new();
    let first = app.install("first", Probe::new("first", &events));
    let mut second = Probe::new("second", &events);
    second.dependency = Some(first);
    second.fail_setup = true;
    second.fail_cleanup = true;
    app.install("second", second);
    let Err(StartError::Setup {
        instance,
        source,
        cleanup: Some(cleanup),
    }) = app.start().await
    else {
        panic!("expected setup failure with rollback errors");
    };
    assert_eq!(instance, "second");
    assert_eq!(source.to_string(), "partial setup failed");
    assert_eq!(cleanup.failures.len(), 2);
    assert_eq!(
        *events.lock().unwrap(),
        [
            "start first",
            "start second",
            "Entrances second",
            "Entrances first",
            "Resources second",
            "Resources first",
        ]
    );
}

#[tokio::test]
async fn config_duplicate_names_duplicate_bindings_and_unbound_handles_prevent_all_setup() {
    let events = Events::default();
    let instance = Instance::new("same", Probe::new("first", &events));
    let first = instance.handle();
    let mut app = App::new().with(instance);
    app.bind(&first.clone(), Probe::new("duplicate", &events));
    let mut invalid = Probe::new("invalid", &events);
    invalid.invalid = true;
    app.install("same", invalid);
    app.reserve::<String>("unbound");
    let Err(StartError::Validation(errors)) = app.start().await else {
        panic!("expected validation error")
    };
    assert!(errors.iter().any(|e| e.contains("duplicate instance name")));
    assert!(errors
        .iter()
        .any(|e| e.contains("duplicate or mismatched binding")));
    assert!(errors.iter().any(|e| e.contains("invalid config")));
    assert!(errors.iter().any(|e| e.contains("unbound instance")));
    assert!(events.lock().unwrap().is_empty());
}

#[tokio::test]
async fn omitted_instance_dependency_prevents_all_setup() {
    let events = Events::default();
    let provider = Instance::new("provider", Probe::new("provider", &events));
    let mut dependent = Probe::new("dependent", &events);
    dependent.dependency = Some(provider.handle());
    let app = App::new()
        .with(Instance::new(
            "independent",
            Probe::new("independent", &events),
        ))
        .with(Instance::new("dependent", dependent));
    let Err(StartError::Validation(errors)) = app.start().await else {
        panic!("expected omitted provider validation error");
    };
    assert!(errors
        .iter()
        .any(|e| e.contains("missing or foreign dependency")));
    assert!(events.lock().unwrap().is_empty());
}

#[tokio::test]
async fn missing_foreign_dependencies_and_foreign_bindings_prevent_all_setup() {
    let events = Events::default();
    let mut foreign_app = App::new();
    let foreign = foreign_app.reserve::<String>("foreign");
    let mut app = App::new();
    app.install("valid", Probe::new("valid", &events));
    let mut dependent = Probe::new("dependent", &events);
    dependent.dependency = Some(foreign.clone());
    app.install("dependent", dependent);
    app.bind(&foreign, Probe::new("foreign", &events));
    let Err(StartError::Validation(errors)) = app.start().await else {
        panic!("expected validation error")
    };
    assert!(errors
        .iter()
        .any(|e| e.contains("missing or foreign dependency")));
    assert!(errors
        .iter()
        .any(|e| e.contains("binding uses a foreign handle")));
    assert!(events.lock().unwrap().is_empty());
}

#[tokio::test]
async fn cycles_are_rejected_before_independent_resources_start() {
    let events = Events::default();
    let mut app = App::new();
    app.install("independent", Probe::new("independent", &events));
    let a = app.reserve::<String>("a");
    let b = app.reserve::<String>("b");
    let mut pa = Probe::new("a", &events);
    pa.dependency = Some(b.clone());
    let mut pb = Probe::new("b", &events);
    pb.dependency = Some(a.clone());
    app.bind(&a, pa);
    app.bind(&b, pb);
    let Err(StartError::Validation(errors)) = app.start().await else {
        panic!("expected cycle error")
    };
    assert!(errors.iter().any(|e| e.contains("dependency cycle")));
    assert!(events.lock().unwrap().is_empty());
}

#[tokio::test]
async fn forward_dependencies_start_in_order_and_same_service_type_handles_are_distinct() {
    let events = Events::default();
    let mut app = App::new();
    let dependency = app.reserve::<String>("dependency");
    let mut dependent = Probe::new("dependent", &events);
    dependent.dependency = Some(dependency.clone());
    let dependent = app.install("dependent", dependent);
    app.bind(&dependency, Probe::new("dependency", &events));
    let running = app.start().await.unwrap();
    assert_eq!(running.get(&dependency).unwrap().as_str(), "dependency");
    assert_eq!(running.get(&dependent).unwrap().as_str(), "dependent");
    running.shutdown().await.unwrap();
    assert_eq!(
        *events.lock().unwrap(),
        [
            "start dependency",
            "start dependent",
            "Entrances dependent",
            "Entrances dependency",
            "Resources dependent",
            "Resources dependency",
        ]
    );
}

struct GatedCleanup {
    entered: Arc<Semaphore>,
    release: Arc<Semaphore>,
    events: Events,
}
impl Plugin for GatedCleanup {
    type Service = ();
    async fn setup(self, setup: &mut Setup) -> Result<Arc<()>, BoxError> {
        setup.on_cleanup(CleanupPhase::Resources, move || async move {
            self.entered.add_permits(1);
            self.release.acquire().await.unwrap().forget();
            self.events.lock().unwrap().push("closed".into());
            Err("close failed".into())
        });
        Ok(Arc::new(()))
    }
}

#[tokio::test]
async fn cancelled_shutdown_waiter_does_not_lose_cleanup_and_repeated_callers_share_error() {
    let events = Events::default();
    let entered = Arc::new(Semaphore::new(0));
    let release = Arc::new(Semaphore::new(0));
    let mut app = App::new();
    app.install(
        "gated",
        GatedCleanup {
            entered: entered.clone(),
            release: release.clone(),
            events: events.clone(),
        },
    );
    let running = app.start().await.unwrap();
    let first = tokio::spawn({
        let running = running.clone();
        async move { running.shutdown().await }
    });
    entered.acquire().await.unwrap().forget();
    first.abort();
    assert!(first.await.unwrap_err().is_cancelled());
    let second = tokio::spawn({
        let running = running.clone();
        async move { running.shutdown().await }
    });
    let third = tokio::spawn({
        let running = running.clone();
        async move { running.shutdown().await }
    });
    release.add_permits(1);
    let e2 = second.await.unwrap().unwrap_err();
    let e3 = third.await.unwrap().unwrap_err();
    let repeated = running.shutdown().await.unwrap_err();
    assert!(Arc::ptr_eq(&e2.failures[0].source, &e3.failures[0].source));
    assert!(Arc::ptr_eq(
        &e2.failures[0].source,
        &repeated.failures[0].source
    ));
    assert_eq!(*events.lock().unwrap(), ["closed"]);
}

#[tokio::test]
async fn cleanup_errors_are_aggregated_without_skipping_later_callbacks() {
    let events = Events::default();
    let mut app = App::new();
    let mut a = Probe::new("a", &events);
    a.fail_cleanup = true;
    let a = app.install("a", a);
    let mut b = Probe::new("b", &events);
    b.dependency = Some(a);
    b.fail_cleanup = true;
    app.install("b", b);
    let running = app.start().await.unwrap();
    let error = running.shutdown().await.unwrap_err();
    assert_eq!(
        error
            .failures
            .iter()
            .map(|f| (f.instance.as_str(), f.phase))
            .collect::<Vec<_>>(),
        [
            ("b", CleanupPhase::Entrances),
            ("a", CleanupPhase::Entrances),
            ("b", CleanupPhase::Resources),
            ("a", CleanupPhase::Resources),
        ]
    );
    assert_eq!(events.lock().unwrap().len(), 6);
}

struct Undeclared(Handle<String>);
impl Plugin for Undeclared {
    type Service = ();
    async fn setup(self, setup: &mut Setup) -> Result<Arc<()>, BoxError> {
        assert!(matches!(setup.get(&self.0), Err(AccessError::Undeclared)));
        Ok(Arc::new(()))
    }
}
#[tokio::test]
async fn setup_cannot_get_undeclared_dependencies_and_running_rejects_foreign_handles() {
    let events = Events::default();
    let mut app = App::new();
    let first = app.install("first", Probe::new("first", &events));
    app.install("undeclared", Undeclared(first));
    let foreign = App::new().reserve::<String>("foreign");
    let running = app.start().await.unwrap();
    assert!(matches!(
        running.get(&foreign),
        Err(AccessError::Unavailable)
    ));
    running.shutdown().await.unwrap();
}
