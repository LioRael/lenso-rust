use lenso_data_import::*;
use lenso_plugin_core::{App, Instance, RunningApp};
use lenso_storage::{CancellationToken, LocalStorageConfig, LocalStoragePlugin, PutObject};
use lenso_tasks::{provision, RetryPolicy, SqliteTaskTransaction, Task, TaskError, TasksPlugin};
use std::{io::Cursor as Input, path::Path, sync::Arc};

struct Contacts;
impl Sink for Contacts {
    fn validate(&self, row: &Record) -> Result<(), &'static str> {
        if row.values["id"].as_i64().unwrap() < 0 {
            Err("positive-id")
        } else {
            Ok(())
        }
    }
    fn write(&self, tx: &mut SqliteTaskTransaction<'_>, rows: &[Record]) -> Result<(), TaskError> {
        for row in rows {
            tx.execute(
                "INSERT INTO contacts(id,name) VALUES (?1,?2)",
                (
                    row.values["id"].as_i64().unwrap(),
                    row.values["name"].as_str().unwrap(),
                ),
            )?;
        }
        Ok(())
    }
}
fn plan(format: Format, policy: InvalidRows) -> Plan {
    let csv = matches!(format, Format::Csv);
    Plan {
        version: "contacts-v1".into(),
        format,
        invalid_rows: policy,
        max_bytes: 2_000_000,
        max_record_bytes: 32_768,
        batch_rows: 127,
        fields: vec![
            Field {
                source: if csv { "0" } else { "id" }.into(),
                target: "id".into(),
                kind: Kind::Integer,
                required: true,
                max_chars: 0,
            },
            Field {
                source: if csv { "1" } else { "name" }.into(),
                target: "name".into(),
                kind: Kind::Text,
                required: true,
                max_chars: 20000,
            },
        ],
    }
}
async fn start(
    root: &Path,
    plan: Plan,
) -> (RunningApp, Arc<Importer>, Arc<lenso_storage::LocalStorage>) {
    start_with_sink(root, plan, Arc::new(Contacts)).await
}
async fn start_with_sink(
    root: &Path,
    plan: Plan,
    sink: Arc<dyn Sink>,
) -> (RunningApp, Arc<Importer>, Arc<lenso_storage::LocalStorage>) {
    let storage = Instance::new(
        "objects",
        LocalStoragePlugin::new(LocalStorageConfig::new(root.join("objects"))),
    );
    let sh = storage.handle();
    let tasks = Instance::new(
        "tasks",
        TasksPlugin::new(
            root.join("business.sqlite"),
            "contacts",
            vec![Task::new(
                "noop",
                RetryPolicy::default(),
                |_: serde_json::Value, _| async { Ok(serde_json::Value::Null) },
            )],
        ),
    );
    let import = Instance::new(
        "import",
        ImportPlugin {
            storage: sh.clone(),
            tasks: tasks.handle(),
            plan,
            sink,
        },
    );
    let ih = import.handle();
    let app = App::new()
        .with(import)
        .with(storage)
        .with(tasks)
        .start()
        .await
        .unwrap();
    let service = app.get(&ih).unwrap();
    let storage = app.get(&sh).unwrap();
    (app, service, storage)
}
async fn fixture() -> tempfile::TempDir {
    let root = tempfile::tempdir().unwrap();
    provision(root.path().join("business.sqlite"), "contacts")
        .await
        .unwrap();
    let db = rusqlite::Connection::open(root.path().join("business.sqlite")).unwrap();
    db.execute_batch(SCHEMA).unwrap();
    db.execute_batch(
        "CREATE TABLE contacts(id INTEGER PRIMARY KEY, name TEXT NOT NULL CHECK(name <> 'reject'))",
    )
    .unwrap();
    root
}
fn count(root: &Path) -> i64 {
    rusqlite::Connection::open(root.join("business.sqlite"))
        .unwrap()
        .query_row("SELECT COUNT(*) FROM contacts", [], |r| r.get(0))
        .unwrap()
}
#[tokio::test]
async fn thousands_resume_real_business_table() {
    let dir = fixture().await;
    let root = dir.path().canonicalize().unwrap();
    let (app, importer, storage) = start(&root, plan(Format::Csv, InvalidRows::Stop)).await;
    let source = (0..3507)
        .map(|i| format!("{i},\"客户 {i}, \"\"quoted\"\"\nnext line\"\r\n"))
        .collect::<String>();
    storage
        .put(PutObject::new("contacts", Input::new(source)))
        .await
        .unwrap();
    let preview = importer
        .preview("contacts", CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(preview.records.len(), 127);
    assert_eq!(count(&root), 0);
    for _ in 0..8 {
        importer
            .next("run-1", "contacts", CancellationToken::new())
            .await
            .unwrap();
    }
    assert_eq!(count(&root), 1016);
    app.shutdown().await.unwrap();
    assert!(matches!(
        importer
            .next("run-1", "contacts", CancellationToken::new())
            .await,
        Err(Error::Cancelled)
    ));
    drop((importer, storage, app));
    let (app, importer, _) = start(&root, plan(Format::Csv, InvalidRows::Stop)).await;
    loop {
        if importer
            .next("run-1", "contacts", CancellationToken::new())
            .await
            .unwrap()
            .done
        {
            break;
        }
    }
    assert_eq!(count(&root), 3507);
    let done = importer
        .next("run-1", "contacts", CancellationToken::new())
        .await
        .unwrap();
    assert!(done.done && done.records.is_empty());
    let db = rusqlite::Connection::open(root.join("business.sqlite")).unwrap();
    let (sum, name): (i64, String) = db
        .query_row("SELECT SUM(id), MIN(name) FROM contacts", [], |r| {
            Ok((r.get(0)?, r.get(1)?))
        })
        .unwrap();
    assert_eq!(sum, 3506 * 3507 / 2);
    assert!(name.contains("\"quoted\"\nnext line"));
    app.shutdown().await.unwrap();
}

#[tokio::test]
async fn json_errors_identity_cancel_and_atomic_rollback() {
    let dir = fixture().await;
    let root = dir.path().canonicalize().unwrap();
    let (app, importer, storage) = start(&root, plan(Format::Jsonl, InvalidRows::Skip)).await;
    let data = b"{\"id\":1,\"name\":\"A\"}\nnot-json\n{\"id\":-1,\"name\":\"B\"}\n{\"id\":2,\"name\":\"C\"}\n";
    storage
        .put(PutObject::new("input", Input::new(data)))
        .await
        .unwrap();
    let batch = importer
        .next("one", "input", CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(batch.cursor.row, 4);
    assert_eq!(batch.errors.len(), 2);
    assert_eq!(batch.errors[0].row, 2);
    assert_eq!(batch.errors[0].offset, 20);
    assert_eq!(count(&root), 2);
    let cancelled = CancellationToken::new();
    cancelled.cancel();
    assert!(matches!(
        importer.next("cancel", "input", cancelled).await,
        Err(Error::Cancelled)
    ));
    storage.delete("input", Default::default()).await.unwrap();
    storage
        .put(PutObject::new(
            "input",
            Input::new(b"{\"id\":3,\"name\":\"D\"}\n"),
        ))
        .await
        .unwrap();
    assert!(matches!(
        importer
            .next("one", "input", CancellationToken::new())
            .await,
        Err(Error::BindingChanged)
    ));
    storage
        .put(PutObject::new(
            "rollback",
            Input::new(b"{\"id\":3,\"name\":\"D\"}\n{\"id\":4,\"name\":\"reject\"}\n"),
        ))
        .await
        .unwrap();
    assert!(matches!(
        importer
            .next("rollback", "rollback", CancellationToken::new())
            .await,
        Err(Error::Tasks(_))
    ));
    assert_eq!(count(&root), 2);
    let offset: i64 = rusqlite::Connection::open(root.join("business.sqlite"))
        .unwrap()
        .query_row(
            "SELECT offset FROM lenso_import_sessions WHERE id='rollback'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(offset, 0);
    importer.close();
    assert!(matches!(
        importer.preview("input", CancellationToken::new()).await,
        Err(Error::Cancelled)
    ));
    app.shutdown().await.unwrap();
    let mut changed = plan(Format::Jsonl, InvalidRows::Skip);
    changed.version = "v2".into();
    let (app, importer, _) = start(&root, changed).await;
    assert!(matches!(
        importer
            .next("rollback", "rollback", CancellationToken::new())
            .await,
        Err(Error::BindingChanged)
    ));
    app.shutdown().await.unwrap();
}

#[tokio::test]
async fn boundaries_limits_stop_policy_and_concurrent_resume() {
    let dir = fixture().await;
    let root = dir.path().canonicalize().unwrap();
    let (app, importer, storage) = start(&root, plan(Format::Csv, InvalidRows::Stop)).await;
    // A multi-byte code point straddles the 8192-byte buffered read boundary.
    let name = format!("{}界\"tail\nnext", "x".repeat(8187));
    let input = format!("1,\"{}\"\n2,ok\n", name.replace('"', "\"\""));
    storage
        .put(PutObject::new("boundary", Input::new(input)))
        .await
        .unwrap();
    let (a, b) = tokio::join!(
        importer.next("same", "boundary", CancellationToken::new()),
        importer.next("same", "boundary", CancellationToken::new())
    );
    for result in [a, b] {
        assert!(result.is_ok() || matches!(result, Err(Error::StaleCursor)));
    }
    assert_eq!(count(&root), 2);
    let stored: String = rusqlite::Connection::open(root.join("business.sqlite"))
        .unwrap()
        .query_row("SELECT name FROM contacts WHERE id=1", [], |r| r.get(0))
        .unwrap();
    assert_eq!(stored, name);
    for (key, bytes, code) in [
        ("type", b"3,ok\nnope,bad\n".to_vec(), "integer"),
        ("utf8", vec![b'3', b',', 255, b'\n'], "utf8"),
        ("quote", b"3,\"unterminated\n".to_vec(), "csv-framing"),
        (
            "limit",
            format!("3,{}\n", "x".repeat(32768)).into_bytes(),
            "record-limit",
        ),
    ] {
        storage
            .put(PutObject::new(key, Input::new(bytes)))
            .await
            .unwrap();
        match importer.next(key, key, CancellationToken::new()).await {
            Err(Error::Row(e)) => assert_eq!(e.code, code),
            other => panic!("unexpected: {other:?}"),
        }
        assert_eq!(count(&root), 2);
    }
    app.shutdown().await.unwrap();
    let mut limited = plan(Format::Csv, InvalidRows::Skip);
    limited.max_bytes = 1;
    let (app, importer, _) = start(&root, limited).await;
    assert!(matches!(
        importer.preview("boundary", CancellationToken::new()).await,
        Err(Error::Limit)
    ));
    app.shutdown().await.unwrap();
}

struct PausedContacts {
    entered: std::sync::Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
    release: std::sync::Mutex<std::sync::mpsc::Receiver<()>>,
}
impl Sink for PausedContacts {
    fn write(&self, tx: &mut SqliteTaskTransaction<'_>, rows: &[Record]) -> Result<(), TaskError> {
        Contacts.write(tx, rows)?;
        if let Some(entered) = self.entered.lock().unwrap().take() {
            entered.send(()).unwrap();
            self.release.lock().unwrap().recv().unwrap();
        }
        Ok(())
    }
}
#[tokio::test]
async fn dropped_waiter_and_close_drain_dispatched_transaction() {
    let dir = fixture().await;
    let root = dir.path().canonicalize().unwrap();
    let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let sink = Arc::new(PausedContacts {
        entered: std::sync::Mutex::new(Some(entered_tx)),
        release: std::sync::Mutex::new(release_rx),
    });
    let (app, importer, storage) =
        start_with_sink(&root, plan(Format::Csv, InvalidRows::Stop), sink).await;
    storage
        .put(PutObject::new("one", Input::new(b"1,A\n")))
        .await
        .unwrap();
    let running = importer.clone();
    let waiter =
        tokio::spawn(async move { running.next("one", "one", CancellationToken::new()).await });
    entered_rx.await.unwrap();
    waiter.abort();
    assert!(waiter.await.unwrap_err().is_cancelled());
    importer.close();
    release_tx.send(()).unwrap();
    app.shutdown().await.unwrap();
    assert_eq!(count(&root), 1);
    let (app, importer, _) = start(&root, plan(Format::Csv, InvalidRows::Stop)).await;
    let result = importer
        .next("one", "one", CancellationToken::new())
        .await
        .unwrap();
    assert!(result.done && result.records.is_empty());
    assert_eq!(count(&root), 1);
    app.shutdown().await.unwrap();
}
