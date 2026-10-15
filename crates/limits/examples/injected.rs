use lenso_limits::{
    ConsumeInput, Decision, FailurePolicy, LimitScope, Limits, LimitsPlugin, MemoryLimitStore,
};
use lenso_plugin_core::{App, BoxError, Handle, Instance, InstanceKey, Plugin, Setup, WorkGate};
use std::{sync::Arc, time::Duration};

struct StorePlugin;
impl Plugin for StorePlugin {
    type Service = MemoryLimitStore;
    async fn setup(self, _: &mut Setup) -> Result<Arc<Self::Service>, BoxError> {
        Ok(Arc::new(MemoryLimitStore::new()))
    }
}

struct ReportsPlugin {
    limits: Handle<Limits>,
}
struct Reports {
    limits: Arc<Limits>,
    gate: WorkGate,
}
impl Reports {
    fn try_run(&self) -> Result<Decision, BoxError> {
        let _work = self.gate.enter()?;
        // Identity/scope must come from trusted business context.
        Ok(self.limits.consume_quota(&ConsumeInput {
            scope: LimitScope::new("reports", "tenant-a", "subject:123")?,
            capacity: 1,
            quantity: 1,
            period: Duration::from_secs(60),
        })?)
    }
}
impl Plugin for ReportsPlugin {
    type Service = Reports;
    fn dependencies(&self) -> Vec<InstanceKey> {
        vec![self.limits.key()]
    }
    async fn setup(self, setup: &mut Setup) -> Result<Arc<Self::Service>, BoxError> {
        Ok(Arc::new(Reports {
            limits: setup.get(&self.limits)?,
            gate: setup.work_gate(),
        }))
    }
}

#[tokio::main]
async fn main() -> Result<(), BoxError> {
    let store = Instance::new("report-store", StorePlugin);
    let limits = Instance::new(
        "report-limits",
        LimitsPlugin::from_handle(store.handle(), FailurePolicy::Deny),
    );
    let reports = Instance::new(
        "reports",
        ReportsPlugin {
            limits: limits.handle(),
        },
    );
    let handle = reports.handle();
    // Insertion order is irrelevant: exact typed handles define the dependency graph.
    let app = App::new()
        .with(reports)
        .with(limits)
        .with(store)
        .start()
        .await?;
    let service = app.get(&handle)?;
    println!("first report: {:?}", service.try_run()?);
    println!("second report: {:?}", service.try_run()?);
    app.shutdown().await?;
    assert!(service.try_run().is_err());
    Ok(())
}
