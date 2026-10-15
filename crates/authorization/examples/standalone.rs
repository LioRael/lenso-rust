use lenso_authorization::*;
use serde_json::json;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut options = AuthorizationOptions::new(["write"]);
    options.identity.required = true;
    options.rules.push(Rule::new(
        "owner",
        Outcome::Allow,
        ["write"],
        "note",
        Condition::Predicate(predicate(|facts, _| {
            Ok(facts.principal.as_ref().is_some_and(|p| {
                facts
                    .resource
                    .attributes
                    .get("owner")
                    .and_then(|v| v.as_str())
                    == Some(p.subject_id.as_str())
            }))
        })),
    ));
    let authorization = Authorization::new(options)?;
    // Application-owned row loaded from trusted storage.
    let mut row = ("alice".to_string(), 1u64, "original".to_string());
    let loaded = row.clone();
    let mut resource = Resource::new("note", "one", Scope::new("personal", "home"));
    resource.attributes.insert("owner".into(), json!(loaded.0));
    resource
        .attributes
        .insert("version".into(), json!(loaded.1));
    let request = Request::new(
        Some(Principal::new("notes", "alice", "user")),
        "write",
        resource,
    );
    authorization.enforce(request).await?;

    // A concurrent owner update occurs after authorization.
    row.0 = "bob".into();
    row.1 += 1;
    // The real write must atomically compare owner/version (SQL WHERE, transaction,
    // or a business-owned lock). A prior allow alone is insufficient.
    let written = if row.0 == loaded.0 && row.1 == loaded.1 {
        row.2 = "edited".into();
        row.1 += 1;
        true
    } else {
        false
    };
    assert!(!written);
    println!("Snapshot allowed; conditional write refused the changed owner/version.");
    Ok(())
}
