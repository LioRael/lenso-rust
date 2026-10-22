#[path = "../examples/support/reports.rs"]
mod reports;
use lenso_plugin_core::{App, Instance};
use lenso_tasks::{provision, EnqueueOptions, JobState, RetryPolicy, Task, TaskError, TasksPlugin};
use reports::{ReportInput, Reports};
use serde_json::json;
use std::{sync::Arc, time::Duration};

#[tokio::test]
async fn report_submission_is_atomic_authorized_and_durable() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("reports.sqlite");
    provision(&path, "reports").await.unwrap();
    provision(&path, "other").await.unwrap();
    let sql = rusqlite::Connection::open(&path).unwrap();
    sql.execute_batch(reports::SCHEMA).unwrap();
    // A real SQLite constraint failure after business INSERT and staged enqueue.
    sql.execute_batch("CREATE TRIGGER reject_mapping BEFORE UPDATE OF job_id ON report_request WHEN NEW.report_id = 'fail' BEGIN SELECT RAISE(ABORT, 'fixture'); END;").unwrap();
    let task = Task::new(
        "report",
        RetryPolicy::default(),
        |v: ReportInput, _| async move { Ok(json!({"id":v.report_id, "title":v.title})) },
    );
    let instance = Instance::new(
        "tasks",
        TasksPlugin::new(&path, "reports", vec![task.clone()]),
    );
    let h = instance.handle();
    let other = Instance::new("other-tasks", TasksPlugin::new(&path, "other", vec![task]));
    let other_h = other.handle();
    let app = App::new().with(instance).with(other).start().await.unwrap();
    let queue = app.get(&h).unwrap();
    let reports = Reports(queue.clone());
    assert!(reports
        .submit("a".into(), "fail".into(), "Title".into())
        .await
        .is_err());
    assert_eq!(
        sql.query_row("SELECT count(*) FROM report_request", [], |r| r
            .get::<_, i64>(0))
            .unwrap(),
        0
    );
    assert!(queue.lookup("report:fail").await.unwrap().is_none());
    let (a, b) = tokio::join!(
        reports.submit("a".into(), "one".into(), "Title".into()),
        reports.submit("a".into(), "one".into(), "Title".into())
    );
    let id = a.unwrap().unwrap();
    assert_eq!(b.unwrap(), Some(id));
    assert_eq!(
        reports
            .submit("b".into(), "one".into(), "Title".into())
            .await
            .unwrap(),
        None
    );
    assert_eq!(
        reports
            .submit("a".into(), "one".into(), "Changed".into())
            .await
            .unwrap(),
        None
    );
    assert_eq!(
        reports.receipt("b".into(), "one".into()).await.unwrap(),
        None
    );
    assert_eq!(
        reports.receipt("a".into(), "one".into()).await.unwrap(),
        Some(id)
    );
    let other_queue = app.get(&other_h).unwrap();
    let other_reports = Reports(other_queue.clone());
    assert_eq!(
        other_reports
            .submit("a".into(), "one".into(), "Title".into())
            .await
            .unwrap(),
        None
    );
    assert_eq!(
        other_reports
            .receipt("a".into(), "one".into())
            .await
            .unwrap(),
        None
    );
    assert!(other_queue.list("report", 0, 100).await.unwrap().is_empty());
    assert_eq!(queue.list("report", 0, 100).await.unwrap().len(), 1);
    assert_eq!(
        queue.run_one().await.unwrap().unwrap().state,
        JobState::Succeeded
    );
    app.shutdown().await.unwrap();
    assert_eq!(
        sql.query_row(
            "SELECT job_id FROM report_request WHERE report_id='one'",
            [],
            |r| r.get::<_, i64>(0)
        )
        .unwrap(),
        id
    );
}

#[tokio::test]
async fn scope_errors_panics_and_transaction_control_rollback() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("scope.sqlite");
    provision(&path, "q").await.unwrap();
    let sql = rusqlite::Connection::open(&path).unwrap();
    sql.execute_batch("CREATE TABLE business (id INTEGER PRIMARY KEY)")
        .unwrap();
    let instance = Instance::new(
        "tasks",
        TasksPlugin::new(
            &path,
            "q",
            vec![Task::new(
                "echo",
                RetryPolicy::default(),
                |v: u32, _| async move { Ok(json!(v)) },
            )],
        ),
    );
    let h = instance.handle();
    let app = App::new().with(instance).start().await.unwrap();
    let queue = app.get(&h).unwrap();
    for command in [
        "COMMIT",
        "-- comment\nROLLBACK",
        "SAVEPOINT nested",
        "PRAGMA user_version=1",
        "ATTACH ':memory:' AS other",
    ] {
        let result = queue
            .sqlite_transaction(move |tx| {
                tx.execute("INSERT INTO business VALUES (1)", [])?;
                let _ = tx.execute(command, []); // Swallowed errors still poison the scope.
                Ok(())
            })
            .await;
        assert!(
            matches!(result, Err(TaskError::TransactionAborted)),
            "{command}: {result:?}"
        );
    }
    assert!(matches!(
        queue
            .sqlite_transaction::<(), _>(|tx| {
                tx.execute("INSERT INTO business VALUES (1)", [])?;
                panic!("business panic")
            })
            .await,
        Err(TaskError::Runtime)
    ));
    assert!(matches!(
        queue
            .sqlite_transaction(|tx| {
                tx.execute("INSERT INTO business VALUES (1)", [])?;
                let _ = tx.enqueue("echo", json!("invalid"), EnqueueOptions::default());
                Ok(())
            })
            .await,
        Err(TaskError::TransactionAborted)
    ));
    assert_eq!(
        sql.query_row("SELECT count(*) FROM business", [], |r| r.get::<_, i64>(0))
            .unwrap(),
        0
    );
    sql.execute_batch("CREATE TRIGGER rollback_business BEFORE INSERT ON business WHEN NEW.id = 9 BEGIN SELECT RAISE(ROLLBACK, 'fixture'); END;").unwrap();
    let result = queue
        .sqlite_transaction(|tx| {
            let _ = tx.execute("INSERT INTO business VALUES (9)", []);
            assert!(matches!(
                tx.execute("INSERT INTO business VALUES (10)", []),
                Err(TaskError::TransactionAborted)
            ));
            Ok(())
        })
        .await;
    assert!(matches!(result, Err(TaskError::RollbackFailed { .. })));
    assert_eq!(
        sql.query_row("SELECT count(*) FROM business", [], |r| r.get::<_, i64>(0))
            .unwrap(),
        0
    );
    // A deferred foreign key fails only at COMMIT. It is surfaced without replay.
    sql.execute_batch("CREATE TABLE parent(id INTEGER PRIMARY KEY); CREATE TABLE deferred_child(id INTEGER REFERENCES parent(id) DEFERRABLE INITIALLY DEFERRED)").unwrap();
    let result = queue
        .sqlite_transaction(|tx| {
            tx.execute("INSERT INTO deferred_child VALUES (123)", [])?;
            tx.enqueue(
                "echo",
                json!(42),
                EnqueueOptions {
                    deduplication_key: Some("unconfirmed".into()),
                    ..Default::default()
                },
            )
        })
        .await;
    assert!(matches!(result, Err(TaskError::CommitUnconfirmed(_))));
    assert_eq!(
        sql.query_row("SELECT count(*) FROM deferred_child", [], |r| r
            .get::<_, i64>(0))
            .unwrap(),
        0
    );
    assert!(queue.lookup("unconfirmed").await.unwrap().is_none());
    // Hook removal and owned connection remain usable after error/panic.
    queue
        .enqueue("echo", json!(1), EnqueueOptions::default())
        .await
        .unwrap();
    queue.run_one().await.unwrap();
    app.shutdown().await.unwrap();
}

#[tokio::test]
async fn dropped_waiter_keeps_same_connection_transaction_admitted() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("drain.sqlite");
    provision(&path, "q").await.unwrap();
    rusqlite::Connection::open(&path)
        .unwrap()
        .execute_batch("CREATE TABLE business(id INTEGER PRIMARY KEY)")
        .unwrap();
    let instance = Instance::new(
        "tasks",
        TasksPlugin::new(
            &path,
            "q",
            vec![Task::new(
                "echo",
                RetryPolicy::default(),
                |v: u32, _| async move { Ok(json!(v)) },
            )],
        ),
    );
    let h = instance.handle();
    let app = App::new().with(instance).start().await.unwrap();
    let queue = app.get(&h).unwrap();
    let (entered, wait) = tokio::sync::oneshot::channel();
    let (release, blocked) = std::sync::mpsc::channel();
    let caller = tokio::spawn({
        let queue = Arc::clone(&queue);
        async move {
            queue
                .sqlite_transaction(move |tx| {
                    tx.execute("INSERT INTO business VALUES (1)", [])?;
                    let staged = tx.enqueue("echo", json!(1), EnqueueOptions::default())?;
                    entered.send(()).unwrap();
                    blocked.recv_timeout(Duration::from_secs(5)).unwrap();
                    Ok(staged)
                })
                .await
        }
    });
    wait.await.unwrap();
    let reader = rusqlite::Connection::open(&path).unwrap();
    assert_eq!(
        reader
            .query_row("SELECT count(*) FROM business", [], |r| r.get::<_, i64>(0))
            .unwrap(),
        0
    );
    assert_eq!(
        reader
            .query_row("SELECT count(*) FROM lenso_task_job", [], |r| r
                .get::<_, i64>(0))
            .unwrap(),
        0
    );
    caller.abort();
    let mut shutdown = tokio::spawn(async move { app.shutdown().await });
    assert!(
        tokio::time::timeout(Duration::from_millis(30), &mut shutdown)
            .await
            .is_err()
    );
    release.send(()).unwrap();
    shutdown.await.unwrap().unwrap();
    assert_eq!(
        reader
            .query_row("SELECT count(*) FROM business", [], |r| r.get::<_, i64>(0))
            .unwrap(),
        1
    );
    assert_eq!(
        reader
            .query_row("SELECT count(*) FROM lenso_task_job", [], |r| r
                .get::<_, i64>(0))
            .unwrap(),
        1
    );
    assert!(matches!(
        queue.sqlite_transaction(|_| Ok(())).await,
        Err(TaskError::Closed)
    ));
}
