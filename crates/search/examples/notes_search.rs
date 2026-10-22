use lenso_plugin_core::{App, BoxError, Instance};
use lenso_search::{provision, Note, Scope, SearchError, SearchPlugin};

#[tokio::main]
async fn main() -> Result<(), BoxError> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("notes.sqlite");
    provision(&path)?;
    let alice = Scope::new("notes", "demo", "alice");
    let bob = Scope::new("notes", "demo", "bob");
    let instance = Instance::new("notes-search", SearchPlugin::new(&path));
    let handle = instance.handle();
    let app = App::new().with(instance).start().await?;
    let search = app.get(&handle)?;
    let business = (|| -> Result<(), BoxError> {
        for (scope, id, body) in [(&alice,"a","rust local notes"),(&alice,"b","rust second note"),(&bob,"private","rust private note")] {
            search.upsert(scope, Note { id, title:"Rust", body })?;
        }
        let first = search.query(&alice,"rust",1,None)?;
        assert_eq!(first.hits[0].id,"a");
        let next = search.query(&alice,"rust",1,first.next_cursor.as_deref())?;
        assert_eq!(next.hits[0].id,"b");
        println!("Alice's first page: {:?}",first.hits);
        search.upsert(&alice, Note { id:"a", title:"Persistent SQLite", body:"SQLite durable note" })?;
        search.delete(&alice,"b")?;
        assert!(search.query(&alice,"rust",10,None)?.hits.is_empty());
        Ok(())
    })();
    let cleanup = app.shutdown().await;
    if let Err(error) = cleanup { return Err(format!("business={business:?}; cleanup={error}").into()); }
    business?;
    assert_eq!(search.query(&alice,"sqlite",10,None).unwrap_err(), SearchError::Closed);

    let instance = Instance::new("reopened-search", SearchPlugin::new(&path));
    let handle = instance.handle();
    let app = App::new().with(instance).start().await?;
    let business = (|| -> Result<(), BoxError> {
        let search = app.get(&handle)?;
        assert_eq!(search.query(&alice,"sqlite",10,None)?.hits[0].id,"a");
        assert!(search.query(&alice,"rust",10,None)?.hits.is_empty());
        assert_eq!(search.query(&bob,"rust",10,None)?.hits[0].id,"private");
        println!("Reopened: Alice update/delete and Bob isolation verified");
        Ok(())
    })();
    let cleanup = app.shutdown().await;
    if let Err(error) = cleanup { return Err(format!("business={business:?}; cleanup={error}").into()); }
    business
}
