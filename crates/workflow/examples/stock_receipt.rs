//! A second consumer: validate shipment -> dock signal -> inventory or quarantine.
//! All business schemas and handlers live here, outside the workflow engine.
use lenso_plugin_core::{App, Instance};
use lenso_tasks::{provision, Failure, RetryPolicy, Task, TasksPlugin};
use lenso_workflow::{Definition, Outcome, Signal, Step, Target, WorkflowPlugin};
use serde::Deserialize;
use serde_json::json;
#[derive(Deserialize)]
struct Shipment {
    sku: String,
    packages: Vec<u32>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let db = dir.path().join("stock.sqlite");
    provision(&db, "stock").await?;
    let task = Task::new(
        "stock.count.v1",
        RetryPolicy::default(),
        |input: Shipment, _| async move {
            let units = input
                .packages
                .iter()
                .try_fold(0u32, |sum, n| sum.checked_add(*n))
                .ok_or(Failure::InvalidInput)?;
            if input.sku.is_empty() || input.sku.len() > 64 || units == 0 || units > 10_000 {
                return Err(Failure::InvalidInput);
            }
            Ok(json!({"sku":input.sku,"units":units}))
        },
    );
    let tasks = Instance::new("tasks", TasksPlugin::new(&db, "stock", vec![task]));
    let th = tasks.handle();
    let definition = Definition::new(
        "stock-receipt",
        1,
        vec![
            Step::task("count", 1, "stock.count.v1", Target::Step(1)),
            Step::signal("dock", 1, Target::Step(2), Target::Step(3)),
            Step::transaction("stock", 1, Target::Complete, |tx, context| {
                let sku = context.payload["sku"]
                    .as_str()
                    .ok_or(lenso_tasks::TaskError::InvalidInput)?;
                let units = context.payload["units"]
                    .as_i64()
                    .ok_or(lenso_tasks::TaskError::InvalidInput)?;
                tx.execute(
                    "INSERT INTO shipment_receipt VALUES (?1,?2)",
                    rusqlite::params![context.idempotency_key, "stocked"],
                )?;
                tx.execute("INSERT INTO stock VALUES (?1,?2) ON CONFLICT(sku) DO UPDATE SET units=stock.units+excluded.units",rusqlite::params![sku,units])?;
                Ok(json!({"stocked":units,"sku":sku}))
            }),
            Step::transaction("quarantine", 1, Target::Complete, |tx, context| {
                tx.execute(
                    "INSERT INTO shipment_receipt VALUES (?1,?2)",
                    rusqlite::params![context.idempotency_key, "quarantined"],
                )?;
                Ok(json!({"quarantined":true}))
            }),
        ],
    );
    let flow = Instance::new("workflow", WorkflowPlugin::new(th.clone(), definition));
    let fh = flow.handle();
    let app = App::new().with(tasks).with(flow).start().await?;
    let result=async {
        let tasks=app.get(&th)?; let flow=app.get(&fh)?; flow.provision().await?;
        tasks.sqlite_transaction(|tx| {
            tx.execute("CREATE TABLE stock(sku TEXT PRIMARY KEY,units INTEGER NOT NULL)",[])?;
            tx.execute("CREATE TABLE shipment_receipt(receipt_key TEXT PRIMARY KEY,status TEXT NOT NULL)",[])?; Ok(())
        }).await?;
        for (id,accepted) in [("delivery-1",true),("delivery-2",false)] {
            flow.start(id.into(),json!({"sku":"WIDGET","packages":[3,4]})).await?;
            tasks.run_one().await?; flow.resume(id.into()).await?;
            let signal=Signal { step:"dock".into(),version:1,key:format!("dock-fixture:{id}"),choice:accepted,data:json!({}) };
            assert_eq!(flow.signal(id.into(),1,signal.clone()).await?,Outcome::Applied);
            flow.resume(id.into()).await?;
            assert_eq!(flow.signal(id.into(),1,signal).await?,Outcome::Duplicate);
            assert_eq!(flow.resume(id.into()).await?,Outcome::Terminal);
            println!("{id}: {}",flow.result(id.into()).await?.ok_or("missing receipt")?);
        }
        let units:i64=tasks.sqlite_transaction(|tx|tx.query_row("SELECT units FROM stock WHERE sku='WIDGET'",[],|r|r.get(0))).await?;
        assert_eq!(units,7); println!("stock units after duplicate signals and quarantine: {units}");
        Ok::<_,Box<dyn std::error::Error>>(())
    }.await;
    let shutdown = app.shutdown().await;
    if result.is_err() || shutdown.is_err() {
        return Err(format!("operation: {result:?}; shutdown: {shutdown:?}").into());
    }
    Ok(())
}
