use lenso_feature_flags::*;
fn config() -> Configuration {
    Configuration::from_json(include_bytes!("../examples/fixture.json")).unwrap()
}
fn ctx(s: &str) -> Context {
    Context {
        user: Some(Identity {
            realm: "fixture".into(),
            subject: s.into(),
        }),
        organization: None,
    }
}
#[test]
fn rules_and_defaults() {
    let mut c = config();
    let org = Identity {
        realm: "org".into(),
        subject: "team".into(),
    };
    c.flags
        .get_mut("express_checkout")
        .unwrap()
        .organizations
        .push(Rule {
            identity: org.clone(),
            value: false,
        });
    let flags = FeatureFlags::new(c).unwrap();
    let mut context = ctx("pilot");
    context.organization = Some(org);
    assert_eq!(
        flags
            .snapshot(context.clone())
            .evaluate("express_checkout", false)
            .reason,
        Reason::User
    );
    context.user = None;
    assert_eq!(
        flags
            .snapshot(context)
            .evaluate("express_checkout", true)
            .reason,
        Reason::Organization
    );
    let s = flags.snapshot(Context::default());
    assert_eq!(
        s.evaluate("express_checkout", true).reason,
        Reason::MissingIdentity
    );
    assert!(s.evaluate("missing", true).value);
    assert!(!s.evaluate("missing", false).value);
    assert_eq!(
        flags
            .snapshot(ctx(""))
            .evaluate("express_checkout", false)
            .reason,
        Reason::Failure(FlagError::Invalid)
    );
}
#[test]
fn versions_rollback_and_snapshot() {
    let flags = FeatureFlags::new(config()).unwrap();
    let old = flags.snapshot(ctx("pilot"));
    let mut c = config();
    c.flags.clear();
    assert_eq!(flags.publish(1, c.clone()), Ok(2));
    assert_eq!(flags.publish(1, c), Err(FlagError::Conflict));
    assert!(old.evaluate("express_checkout", false).value);
    assert!(
        !flags
            .snapshot(ctx("pilot"))
            .evaluate("express_checkout", false)
            .value
    );
    assert_eq!(flags.rollback(2, 1), Ok(3));
    assert!(
        flags
            .snapshot(ctx("pilot"))
            .evaluate("express_checkout", false)
            .value
    );
    flags.close().unwrap();
    assert_eq!(
        flags
            .snapshot(ctx("pilot"))
            .evaluate("express_checkout", true)
            .reason,
        Reason::Failure(FlagError::Closed)
    );
    assert!(old.evaluate("express_checkout", false).value);
}
#[test]
fn stable_nested_rollouts() {
    let decisions = |p| {
        let mut c = config();
        let f = c.flags.get_mut("express_checkout").unwrap();
        f.users.clear();
        f.rollout.as_mut().unwrap().basis_points = p;
        let flags = FeatureFlags::new(c).unwrap();
        (0..1000)
            .map(|n| {
                flags
                    .snapshot(ctx(&n.to_string()))
                    .evaluate("express_checkout", false)
                    .value
            })
            .collect::<Vec<_>>()
    };
    let a = decisions(2500);
    let b = decisions(5000);
    assert_eq!(a, decisions(2500));
    assert!(a.iter().zip(b).all(|(a, b)| !a || b));
    assert!(a.iter().any(|v| *v) && a.iter().any(|v| !v));
    assert!(decisions(0).iter().all(|v| !v));
    assert!(decisions(10000).iter().all(|v| *v));
}
#[test]
fn validation_history_and_concurrent_publish() {
    let flags = FeatureFlags::new(config()).unwrap();
    let mut bad = config();
    bad.flags
        .get_mut("express_checkout")
        .unwrap()
        .rollout
        .as_mut()
        .unwrap()
        .basis_points = 10001;
    assert_eq!(flags.publish(1, bad), Err(FlagError::Invalid));
    let threads: Vec<_> = (0..8)
        .map(|_| {
            let f = flags.clone();
            std::thread::spawn(move || f.publish(1, config()))
        })
        .collect();
    assert_eq!(
        threads
            .into_iter()
            .map(|t| t.join().unwrap())
            .filter(Result::is_ok)
            .count(),
        1
    );
    for r in 2..20 {
        flags.publish(r, config()).unwrap();
    }
    assert_eq!(flags.rollback(20, 1), Err(FlagError::NotRetained));
    assert_eq!(flags.rollback(20, 19), Ok(21));
    assert!(Configuration::from_json(b"{\"version\":\"v\",\"flags\":{},\"typo\":true}").is_err());
}
#[tokio::test]
async fn plugin_cleanup() {
    let mut app = lenso_plugin_core::App::new();
    let h = app.install("flags", FeatureFlagsPlugin::new(config()));
    let running = app.start().await.unwrap();
    let flags = running.get(&h).unwrap();
    running.shutdown().await.unwrap();
    assert_eq!(flags.publish(1, config()), Err(FlagError::Closed));
}

#[test]
fn fixed_bucket_and_organization_cohort() {
    let mut c = config();
    let flag = c.flags.get_mut("express_checkout").unwrap();
    flag.users.clear();
    // Independent reference implementation gives bucket 8782 for fixture/42.
    flag.rollout.as_mut().unwrap().basis_points = 8782;
    let flags = FeatureFlags::new(c.clone()).unwrap();
    assert!(
        !flags
            .snapshot(ctx("42"))
            .evaluate("express_checkout", false)
            .value
    );
    c.flags
        .get_mut("express_checkout")
        .unwrap()
        .rollout
        .as_mut()
        .unwrap()
        .basis_points = 8783;
    flags.publish(1, c.clone()).unwrap();
    assert!(
        flags
            .snapshot(ctx("42"))
            .evaluate("express_checkout", false)
            .value
    );
    let rollout = c
        .flags
        .get_mut("express_checkout")
        .unwrap()
        .rollout
        .as_mut()
        .unwrap();
    rollout.cohort = Cohort::Organization;
    rollout.basis_points = 5000;
    flags.publish(2, c).unwrap();
    let org = Some(Identity {
        realm: "fixture".into(),
        subject: "team".into(),
    });
    let mut a = ctx("alice");
    a.organization = org.clone();
    let mut b = ctx("bob");
    b.organization = org;
    assert_eq!(
        flags.snapshot(a).evaluate("express_checkout", false),
        flags.snapshot(b).evaluate("express_checkout", false)
    );
    assert_eq!(
        flags
            .snapshot(ctx("alice"))
            .evaluate("express_checkout", false)
            .reason,
        Reason::MissingIdentity
    );
}

#[test]
fn duplicate_rules_and_oversize_inputs_rejected() {
    let mut c = config();
    let f = c.flags.get_mut("express_checkout").unwrap();
    f.users.push(f.users[0].clone());
    assert!(matches!(FeatureFlags::new(c), Err(FlagError::Invalid)));
    assert!(Configuration::from_json(&vec![b' '; 1024 * 1024 + 1]).is_err());
    let mut c = config();
    c.version = "x".repeat(257);
    assert!(c.validate().is_err());
}
