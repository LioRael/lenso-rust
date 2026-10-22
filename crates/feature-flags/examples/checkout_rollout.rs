use lenso_feature_flags::{Configuration, Context, FeatureFlagsPlugin, Identity, Snapshot};
use lenso_plugin_core::{App, BoxError};
fn packages(request: &Snapshot, items: &[&str]) -> Vec<String> {
    if request.evaluate("express_checkout", false).value {
        vec![items.join(" + ")]
    } else {
        items.iter().map(|s| s.to_string()).collect()
    }
}
#[tokio::main]
async fn main() -> Result<(), BoxError> {
    let config = Configuration::from_json(include_bytes!("fixture.json"))?;
    let mut app = App::new();
    let handle = app.install("checkout", FeatureFlagsPlugin::new(config.clone()));
    let running = app.start().await?;
    let flags = running.get(&handle)?;
    let business = (|| -> Result<(), BoxError> {
        let _guard = running.work_gate().enter()?;
        let ctx = Context {
            user: Some(Identity {
                realm: "fixture".into(),
                subject: "pilot".into(),
            }),
            organization: None,
        };
        let bundled = (0..1000)
            .filter(|n| {
                let cohort = Context {
                    user: Some(Identity {
                        realm: "fixture".into(),
                        subject: n.to_string(),
                    }),
                    organization: None,
                };
                packages(&flags.snapshot(cohort), &["book", "pen"]).len() == 1
            })
            .count();
        assert!(bundled > 150 && bundled < 350);
        println!("25% fixture rollout: {bundled}/1000 checkouts bundled");
        let request = flags.snapshot(ctx.clone());
        assert_eq!(packages(&request, &["book", "pen"]), vec!["book + pen"]);
        let mut next = config;
        next.version = "disabled".into();
        let flag = next.flags.get_mut("express_checkout").unwrap();
        flag.users.clear();
        flag.rollout = None;
        flags.publish(1, next)?;
        assert_eq!(packages(&request, &["book", "pen"]).len(), 1);
        assert_eq!(
            packages(&flags.snapshot(ctx.clone()), &["book", "pen"]).len(),
            2
        );
        flags.rollback(2, 1)?;
        assert_eq!(packages(&flags.snapshot(ctx), &["book", "pen"]).len(), 1);
        println!("fixture: bundled -> standard -> rollback bundled; in-flight snapshot unchanged");
        Ok(())
    })();
    let shutdown = running.shutdown().await;
    if let Err(error) = &shutdown {
        eprintln!("shutdown failed: {error}");
    }
    business?;
    shutdown?;
    Ok(())
}
