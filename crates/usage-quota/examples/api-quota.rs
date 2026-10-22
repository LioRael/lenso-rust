use lenso_usage_quota::{Command, Error, UsageQuota};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let quota = UsageQuota::open(directory.path().join("fixture.db"))?;
    quota.execute(
        "create",
        1,
        Command::Create {
            scope: "demo/api/window-1".into(),
            limit: 2,
        },
    )?;
    for request in ["first", "first", "second", "third"] {
        let result = quota.execute(
            request,
            2,
            Command::ApiCall {
                scope: "demo/api/window-1".into(),
                request: request.into(),
                units: 1,
            },
        );
        match result {
            Ok(record) => println!("{request}: committed {} unit(s)", record.units),
            Err(Error::Exhausted) => println!("{request}: quota exhausted"),
            Err(error) => return Err(error.into()),
        }
    }
    assert_eq!(quota.api_calls("demo/api/window-1")?, 2);
    assert_eq!(quota.balance("demo/api/window-1")?.used, 2);
    println!("Two actual fixture rows; duplicate replay consumed no extra quota.");
    quota.close()?;
    Ok(())
}
