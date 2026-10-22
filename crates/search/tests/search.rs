use lenso_plugin_core::{App, Instance};
use lenso_search::{provision, Note, Scope, Search, SearchError, SearchPlugin};

fn put(s: &Search, scope: &Scope, id: &str, title: &str, body: &str) {
    s.upsert(scope, Note { id, title, body }).unwrap();
}

#[test]
fn real_fts_partition_pagination_update_delete_and_restart() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("notes.sqlite");
    provision(&path).unwrap();
    let s = Search::open(&path).unwrap();
    let a = Scope::new("notes", "tenant", "alice");
    let others = [Scope::new("notes", "tenant", "bob"), Scope::new("notes", "other", "alice"), Scope::new("other", "tenant", "alice")];
    put(&s, &a, "1", "Body only", "rust systems");
    put(&s, &a, "2", "Rust title", "rust persistence");
    put(&s, &a, "3", "Rust second", "rust SQLite");
    for other in &others { put(&s, other, "secret", "Rust secret", "rust secret"); }
    let first = s.query(&a, "rust", 1, None).unwrap();
    assert_eq!(first.hits[0].id, "2");
    assert!(first.hits[0].summary.contains("rust"));
    let cursor = first.next_cursor.unwrap();
    for other in &others {
        assert_eq!(s.query(other, "rust", 1, Some(&cursor)).unwrap_err(), SearchError::InvalidCursor);
        assert_eq!(s.query(other, "rust", 10, None).unwrap().hits[0].id, "secret");
    }
    let second = s.query(&a, "rust", 1, Some(&cursor)).unwrap();
    assert_eq!(second.hits[0].id, "3");
    let third = s.query(&a, "rust", 1, second.next_cursor.as_deref()).unwrap();
    assert_eq!(third.hits[0].id, "1");
    assert!(third.next_cursor.is_none());
    s.close().unwrap();
    assert_eq!(s.query(&a, "rust", 1, None).unwrap_err(), SearchError::Closed);
    let s = Search::open(&path).unwrap();
    assert_eq!(s.query(&a, "rust", 1, Some(&cursor)).unwrap().hits[0].id, "3");
    put(&s, &a, "2", "Updated", "python");
    assert_eq!(s.query(&a, "rust", 1, Some(&cursor)).unwrap_err(), SearchError::InvalidCursor);
    assert_eq!(s.query(&a, "python", 10, None).unwrap().hits[0].id, "2");
    assert!(s.delete(&a, "3").unwrap());
    assert!(!s.delete(&a, "secret").unwrap());
    s.close().unwrap();
    let s = Search::open(&path).unwrap();
    assert_eq!(s.query(&a, "rust", 10, None).unwrap().hits.iter().map(|h| h.id.as_str()).collect::<Vec<_>>(), ["1"]);
    assert_eq!(s.query(&others[0], "rust", 10, None).unwrap().hits.len(), 1);
}

#[test]
fn parameterized_inputs_and_observable_limits() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("notes.sqlite");
    provision(&path).unwrap();
    let s = Search::open(&path).unwrap();
    let a = Scope::new("notes", "' OR 1=1 --", "alice");
    put(&s, &a, "'; DROP TABLE lenso_search_notes;--", "Rust", "rust one");
    put(&s, &a, "two", "Rust", "rust two");
    let b = Scope::new("notes", "real", "alice");
    put(&s, &b, "hidden", "Rust", "rust hidden");
    assert_eq!(s.query(&a, "rust", 100, None).unwrap().hits.len(), 2);
    assert!(s.query(&a, "rust OR hidden", 10, None).unwrap().hits.is_empty());
    for q in ["", "  ", "\"", "rust\nword"] {
        assert_eq!(s.query(&a, q, 10, None).unwrap_err(), SearchError::InvalidInput);
    }
    assert_eq!(s.query(&a, &"a".repeat(1025), 10, None).unwrap_err(), SearchError::InvalidInput);
    for n in [0,101] { assert_eq!(s.query(&a, "rust", n, None).unwrap_err(), SearchError::InvalidInput); }
    assert_eq!(s.query(&a, "rust", 1, Some("bad")).unwrap_err(), SearchError::InvalidCursor);
    let cursor = s.query(&a, "rust", 1, None).unwrap().next_cursor.unwrap();
    assert_eq!(s.query(&a, "one", 1, Some(&cursor)).unwrap_err(), SearchError::InvalidCursor);
    assert_eq!(s.query(&a, "rust", 2, Some(&cursor)).unwrap_err(), SearchError::InvalidCursor);
    let mut forged: serde_json::Value = serde_json::from_str(&cursor).unwrap();
    forged["offset"] = 10000.into();
    assert_eq!(s.query(&a, "rust", 1, Some(&forged.to_string())).unwrap_err(), SearchError::InvalidCursor);
    let second_connection = Search::open(&path).unwrap();
    put(&second_connection, &a, "third", "Rust", "rust three");
    assert_eq!(s.query(&a, "rust", 1, Some(&cursor)).unwrap_err(), SearchError::InvalidCursor);
}

#[tokio::test]
async fn plugin_lifecycle_and_explicit_provision() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("notes.sqlite");
    assert!(Search::open(&path).is_err());
    assert!(!path.exists());
    provision(&path).unwrap();
    let instance = Instance::new("search", SearchPlugin::new(&path));
    let handle = instance.handle();
    let app = App::new().with(instance).start().await.unwrap();
    let service = app.get(&handle).unwrap();
    let scope = Scope::new("notes", "tenant", "alice");
    put(&service, &scope, "one", "Rust", "rust durable");
    app.shutdown().await.unwrap();
    assert_eq!(service.query(&scope, "rust", 10, None).unwrap_err(), SearchError::Closed);
    assert_eq!(service.delete(&scope, "one").unwrap_err(), SearchError::Closed);
    assert_eq!(Search::open(&path).unwrap().query(&scope, "rust", 10, None).unwrap().hits.len(), 1);
}
