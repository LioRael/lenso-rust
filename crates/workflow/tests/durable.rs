#[path = "../examples/support/file_review.rs"]
mod business;
use lenso_plugin_core::{App, Instance, Plugin, RunningApp};
use lenso_tasks::{provision, Tasks, TasksPlugin};
use lenso_workflow::{Definition, Outcome, Signal, State, Step, Target, Workflow, WorkflowPlugin};
use serde_json::json;
use std::{path::Path, sync::Arc};
async fn open(path: &Path) -> (RunningApp, Arc<Tasks>, Arc<Workflow>) {
    let tasks = Instance::new(
        "tasks",
        TasksPlugin::new(path, "review", vec![business::processing_task()]),
    );
    let th = tasks.handle();
    let flow = Instance::new(
        "workflow",
        WorkflowPlugin::new(th.clone(), business::definition()),
    );
    let fh = flow.handle();
    let app = App::new().with(tasks).with(flow).start().await.unwrap();
    let tasks = app.get(&th).unwrap();
    let flow = app.get(&fh).unwrap();
    (app, tasks, flow)
}
fn signal(choice: bool) -> Signal {
    Signal {
        step: "approval".into(),
        version: 1,
        key: "human-fixture-1".into(),
        choice,
        data: json!({}),
    }
}
async fn init(tasks: &Tasks, flow: &Workflow) {
    flow.provision().await.unwrap();
    business::provision_business(tasks).await.unwrap();
}

#[tokio::test]
async fn restart_checkpoints_duplicate_and_out_of_order_signals() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("review.sqlite");
    let file = dir.path().join("input.txt");
    std::fs::write(&file, "approved document").unwrap();
    provision(&db, "review").await.unwrap();
    let (app, tasks, flow) = open(&db).await;
    init(&tasks, &flow).await;
    let input = json!({"path":file.to_str().unwrap()});
    assert_eq!(
        flow.start("one".into(), input.clone()).await.unwrap(),
        Outcome::Applied
    );
    assert_eq!(
        flow.start("one".into(), input).await.unwrap(),
        Outcome::Duplicate
    );
    assert_eq!(
        flow.start("one".into(), json!({"path":"other"}))
            .await
            .unwrap(),
        Outcome::Conflict
    );
    assert_eq!(
        flow.signal("one".into(), 1, signal(true)).await.unwrap(),
        Outcome::NotReady
    );
    tasks.run_one().await.unwrap().unwrap();
    let job = flow
        .checkpoint("one".into())
        .await
        .unwrap()
        .unwrap()
        .job
        .unwrap();
    app.shutdown().await.unwrap();
    // Restart after Tasks commit but before workflow checkpoint reconciliation.
    let (app, tasks, flow) = open(&db).await;
    assert_eq!(flow.resume("one".into()).await.unwrap(), Outcome::Applied);
    assert_eq!(
        flow.checkpoint("one".into()).await.unwrap().unwrap().state,
        State::Waiting
    );
    assert_eq!(tasks.get(job).await.unwrap().unwrap().attempt, 1);
    std::fs::remove_file(&file).unwrap();
    app.shutdown().await.unwrap();
    let (app, _, flow) = open(&db).await;
    let stale = Signal {
        version: 2,
        ..signal(true)
    };
    assert_eq!(
        flow.signal("one".into(), 1, stale).await.unwrap(),
        Outcome::VersionMismatch
    );
    let (a, b) = tokio::join!(
        flow.signal("one".into(), 1, signal(true)),
        flow.signal("one".into(), 1, signal(true))
    );
    let results = [a.unwrap(), b.unwrap()];
    assert!(results.contains(&Outcome::Applied) && results.contains(&Outcome::Duplicate));
    assert_eq!(
        flow.signal("one".into(), 1, signal(false)).await.unwrap(),
        Outcome::Conflict
    );
    assert!(flow.result("one".into()).await.unwrap().is_none());
    app.shutdown().await.unwrap();
    // Restart after signal commit, before executing the transaction step.
    let (app, tasks, flow) = open(&db).await;
    assert_eq!(flow.resume("one".into()).await.unwrap(), Outcome::Applied);
    assert_eq!(
        flow.result("one".into()).await.unwrap().unwrap()["content"],
        "APPROVED DOCUMENT"
    );
    assert_eq!(flow.cancel("one".into()).await.unwrap(), Outcome::Terminal);
    assert!(tasks.run_one().await.unwrap().is_none());
    app.shutdown().await.unwrap();
    let (app, tasks, flow) = open(&db).await;
    assert_eq!(
        flow.signal("one".into(), 1, signal(true)).await.unwrap(),
        Outcome::Duplicate
    );
    assert_eq!(flow.resume("one".into()).await.unwrap(), Outcome::Terminal);
    assert_eq!(
        flow.result("one".into()).await.unwrap().unwrap()["content"],
        "APPROVED DOCUMENT"
    );
    let count: i64 = tasks
        .sqlite_transaction(|tx| {
            tx.query_row("SELECT count(*) FROM file_review_result", [], |r| r.get(0))
        })
        .await
        .unwrap();
    assert_eq!(count, 1);
    let cp = flow.checkpoint("one".into()).await.unwrap().unwrap();
    assert_eq!(
        cp.completed
            .iter()
            .map(|r| r.step.as_str())
            .collect::<Vec<_>>(),
        ["process", "approval", "publish"]
    );
    assert_eq!(cp.completed[0].job, Some(job));
    app.shutdown().await.unwrap();
}

#[tokio::test]
async fn failure_retry_cancel_reject_and_definition_fence() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("review.sqlite");
    let file = dir.path().join("missing.txt");
    let input = json!({"path":file.to_str().unwrap()});
    provision(&db, "review").await.unwrap();
    // Invalid backward/cyclic dependencies are rejected before startup.
    let owner = Instance::new(
        "owner",
        TasksPlugin::new(&db, "review", vec![business::processing_task()]),
    );
    let invalid = WorkflowPlugin::new(
        owner.handle(),
        Definition::new(
            "cycle",
            1,
            vec![Step::signal("loop", 1, Target::Step(0), Target::Reject)],
        ),
    );
    assert!(invalid.validate().is_err());
    let (app, tasks, flow) = open(&db).await;
    init(&tasks, &flow).await;
    flow.start("retry".into(), input.clone()).await.unwrap();
    tasks.run_one().await.unwrap();
    flow.resume("retry".into()).await.unwrap();
    assert_eq!(
        flow.checkpoint("retry".into())
            .await
            .unwrap()
            .unwrap()
            .state,
        State::Failed
    );
    std::fs::write(&file, "fixed input").unwrap();
    assert_eq!(flow.retry("retry".into()).await.unwrap(), Outcome::Applied);
    tasks.run_one().await.unwrap();
    flow.resume("retry".into()).await.unwrap();
    let cp = flow.checkpoint("retry".into()).await.unwrap().unwrap();
    assert_eq!(
        tasks
            .get(cp.completed[0].job.unwrap())
            .await
            .unwrap()
            .unwrap()
            .attempt,
        2
    );
    assert_eq!(
        flow.signal("retry".into(), 1, signal(false)).await.unwrap(),
        Outcome::Applied
    );
    assert_eq!(
        flow.checkpoint("retry".into())
            .await
            .unwrap()
            .unwrap()
            .state,
        State::Rejected
    );
    assert!(flow.result("retry".into()).await.unwrap().is_none());
    flow.start("cancel".into(), input.clone()).await.unwrap();
    assert_eq!(
        flow.cancel("cancel".into()).await.unwrap(),
        Outcome::Applied
    );
    assert_eq!(
        flow.cancel("cancel".into()).await.unwrap(),
        Outcome::Duplicate
    );
    assert_eq!(
        flow.signal("cancel".into(), 1, signal(true)).await.unwrap(),
        Outcome::Terminal
    );
    assert!(tasks.run_one().await.unwrap().is_none());
    flow.start("cancel-waiting".into(), input.clone())
        .await
        .unwrap();
    tasks.run_one().await.unwrap();
    flow.resume("cancel-waiting".into()).await.unwrap();
    assert_eq!(
        flow.cancel("cancel-waiting".into()).await.unwrap(),
        Outcome::Applied
    );
    assert_eq!(
        flow.signal("cancel-waiting".into(), 1, signal(true))
            .await
            .unwrap(),
        Outcome::Terminal
    );
    // Once approved but before transaction execution, cancellation still fences writes.
    flow.start("cancel-approved".into(), input.clone())
        .await
        .unwrap();
    tasks.run_one().await.unwrap();
    flow.resume("cancel-approved".into()).await.unwrap();
    flow.signal("cancel-approved".into(), 1, signal(true))
        .await
        .unwrap();
    flow.cancel("cancel-approved".into()).await.unwrap();
    assert_eq!(
        flow.resume("cancel-approved".into()).await.unwrap(),
        Outcome::Terminal
    );
    flow.start("version".into(), input.clone()).await.unwrap();
    let mut cp = flow.checkpoint("version".into()).await.unwrap().unwrap();
    cp.definition.steps[0].version = 2;
    let queue = tasks.identity().to_owned();
    let data = serde_json::to_string(&cp).unwrap();
    tasks.sqlite_transaction(move |tx| { tx.execute("UPDATE lenso_workflow_run_v1 SET checkpoint=?1 WHERE queue_identity=?2 AND definition='file-review' AND id='version'",rusqlite::params![data,queue])?; Ok(()) }).await.unwrap();
    assert_eq!(
        flow.resume("version".into()).await.unwrap(),
        Outcome::VersionMismatch
    );
    assert_eq!(
        flow.start("version".into(), input).await.unwrap(),
        Outcome::VersionMismatch
    );
    assert_eq!(
        flow.cancel("version".into()).await.unwrap(),
        Outcome::VersionMismatch
    );
    app.shutdown().await.unwrap();
    assert!(matches!(
        flow.checkpoint("retry".into()).await,
        Err(lenso_tasks::TaskError::Closed)
    ));
}

#[tokio::test]
async fn checkpoint_failure_rolls_back_business_result() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("review.sqlite");
    let file = dir.path().join("input.txt");
    std::fs::write(&file, "atomic").unwrap();
    provision(&db, "review").await.unwrap();
    let (app, tasks, flow) = open(&db).await;
    init(&tasks, &flow).await;
    flow.start("atomic".into(), json!({"path":file.to_str().unwrap()}))
        .await
        .unwrap();
    tasks.run_one().await.unwrap();
    flow.resume("atomic".into()).await.unwrap();
    flow.signal("atomic".into(), 1, signal(true)).await.unwrap();
    tasks.sqlite_transaction(|tx| { tx.execute("CREATE TRIGGER refuse_checkpoint BEFORE UPDATE ON lenso_workflow_run_v1 WHEN json_extract(NEW.checkpoint,'$.state')='Completed' BEGIN SELECT RAISE(ABORT,'fixture'); END",[])?; Ok(()) }).await.unwrap();
    assert!(flow.resume("atomic".into()).await.is_err());
    let cp = flow.checkpoint("atomic".into()).await.unwrap().unwrap();
    assert_eq!(cp.state, State::Running);
    assert_eq!(cp.step, 2);
    assert_eq!(cp.signals.len(), 1);
    let count: i64 = tasks
        .sqlite_transaction(|tx| {
            tx.query_row("SELECT count(*) FROM file_review_result", [], |r| r.get(0))
        })
        .await
        .unwrap();
    assert_eq!(count, 0);
    tasks
        .sqlite_transaction(|tx| {
            tx.execute("DROP TRIGGER refuse_checkpoint", [])?;
            Ok(())
        })
        .await
        .unwrap();
    assert_eq!(
        flow.resume("atomic".into()).await.unwrap(),
        Outcome::Applied
    );
    assert_eq!(
        flow.resume("atomic".into()).await.unwrap(),
        Outcome::Terminal
    );
    app.shutdown().await.unwrap();
}
