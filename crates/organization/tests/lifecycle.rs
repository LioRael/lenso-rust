use lenso_authorization::*;
use lenso_organization::{
    Command, Error, Organization, OrganizationPlugin, Outcome, Role, Subject, ACTIONS,
};
use lenso_plugin_core::{App, RunningApp};
use std::{
    path::Path,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::{SystemTime, UNIX_EPOCH},
};

struct Sessions {
    revoked: AtomicBool,
}
impl AccessVerifier<String> for Sessions {
    fn verify<'a>(
        &'a self,
        token: &'a String,
        _: &'a Resource,
        _: &'a Evaluation,
    ) -> EvaluationFuture<'a, Option<AccessIdentity>> {
        Box::pin(async move {
            if self.revoked.load(Ordering::SeqCst) {
                return Ok(None);
            }
            let (realm, id) = match token.as_str() {
                "TEST_ALICE" => ("saas", "alice"),
                "TEST_BOB" => ("saas", "bob"),
                "TEST_MALLORY" => ("saas", "mallory"),
                "TEST_OTHER_REALM" => ("other", "bob"),
                _ => return Ok(None),
            };
            Ok(Some(AccessIdentity {
                principal: Principal::new(realm, id, "user"),
                audience: "fixture".into(),
                session_expires_at: None,
                credential: None,
            }))
        })
    }
}
fn sessions() -> Arc<Sessions> {
    Arc::new(Sessions {
        revoked: AtomicBool::new(false),
    })
}
async fn start(path: &Path, sessions: Arc<Sessions>) -> (RunningApp, Arc<Organization<String>>) {
    start_policy(path, sessions, policy(|_, _| Ok(Effect::Allow))).await
}
async fn start_policy(
    path: &Path,
    sessions: Arc<Sessions>,
    policy: Arc<dyn Policy>,
) -> (RunningApp, Arc<Organization<String>>) {
    let mut app = App::new();
    let mut options = AuthorizationOptions::new(ACTIONS);
    options.identity.required = true;
    options.policies.push(policy);
    let auth = app.install("host", AuthorizationPlugin::new(options).unwrap());
    let org = app.install(
        "org",
        OrganizationPlugin::new(path, "saas", "fixture", auth, sessions),
    );
    let run = app.start().await.unwrap();
    let service = run.get(&org).unwrap();
    (run, service)
}
fn user(id: &str) -> Subject {
    Subject::user("saas", id)
}
fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}
async fn create(s: &Arc<Organization<String>>, id: &str) {
    s.execute(
        "TEST_ALICE".into(),
        id,
        Command::Create {
            name: "Acme".into(),
        },
    )
    .await
    .unwrap();
}
async fn invite(s: &Arc<Organization<String>>, expiry: u64) -> (String, String) {
    let Outcome::Invited {
        invitation, token, ..
    } = s
        .execute(
            "TEST_ALICE".into(),
            "acme",
            Command::Invite {
                target: user("bob"),
                role: Role::Member,
                expires_at: expiry,
            },
        )
        .await
        .unwrap()
    else {
        panic!()
    };
    (invitation.id, token)
}
fn denied(result: Result<Outcome, Error>) {
    assert!(matches!(result, Err(Error::Denied)));
}

#[tokio::test]
async fn invitation_is_targeted_single_use_persistent_and_removal_revokes_access() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("org.sqlite");
    let (app, s) = start(&path, sessions()).await;
    create(&s, "acme").await;
    create(&s, "other-team").await;
    let (id, token) = invite(&s, now() + 60_000).await;
    let stored: String = rusqlite::Connection::open(&path)
        .unwrap()
        .query_row(
            "SELECT state FROM lenso_organizations_v1 WHERE id='acme'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert!(!stored.contains(&token));
    for (actor, org, t) in [
        ("TEST_BOB", "other-team", token.clone()),
        ("TEST_MALLORY", "acme", token.clone()),
        ("TEST_OTHER_REALM", "acme", token.clone()),
        ("TEST_BOB", "acme", "0".repeat(64)),
    ] {
        denied(
            s.execute(
                actor.into(),
                org,
                Command::Accept {
                    invitation_id: id.clone(),
                    token: t,
                },
            )
            .await,
        );
    }
    s.execute(
        "TEST_BOB".into(),
        "acme",
        Command::Accept {
            invitation_id: id.clone(),
            token: token.clone(),
        },
    )
    .await
    .unwrap();
    denied(
        s.execute(
            "TEST_BOB".into(),
            "acme",
            Command::Accept {
                invitation_id: id,
                token,
            },
        )
        .await,
    );
    app.shutdown().await.unwrap();
    assert!(matches!(
        s.execute("TEST_ALICE".into(), "acme", Command::Read).await,
        Err(Error::Closed)
    ));
    let (app, s) = start(&path, sessions()).await;
    let Outcome::Snapshot(snapshot) = s
        .execute("TEST_BOB".into(), "acme", Command::Read)
        .await
        .unwrap()
    else {
        panic!()
    };
    assert_eq!(snapshot.members.len(), 2);
    assert!(snapshot.invitations.is_empty());
    s.execute(
        "TEST_ALICE".into(),
        "acme",
        Command::Remove {
            target: user("bob"),
        },
    )
    .await
    .unwrap();
    denied(s.execute("TEST_BOB".into(), "acme", Command::Read).await);
    denied(
        s.execute("TEST_MALLORY".into(), "acme", Command::Read)
            .await,
    );
    denied(
        s.execute("TEST_MALLORY".into(), "unknown", Command::Read)
            .await,
    );
    app.shutdown().await.unwrap();
}

#[tokio::test]
async fn cross_connection_accept_and_last_owner_races() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("org.sqlite");
    let (a, s) = start(&path, sessions()).await;
    let (b, t) = start(&path, sessions()).await;
    create(&s, "acme").await;
    let (id, token) = invite(&s, now() + 60_000).await;
    let (x, y) = tokio::join!(
        s.execute(
            "TEST_BOB".into(),
            "acme",
            Command::Accept {
                invitation_id: id.clone(),
                token: token.clone()
            }
        ),
        t.execute(
            "TEST_BOB".into(),
            "acme",
            Command::Accept {
                invitation_id: id,
                token
            }
        )
    );
    assert_ne!(x.is_ok(), y.is_ok());
    s.execute(
        "TEST_ALICE".into(),
        "acme",
        Command::GrantOwner {
            target: user("bob"),
        },
    )
    .await
    .unwrap();
    let (x, y) = tokio::join!(
        s.execute("TEST_ALICE".into(), "acme", Command::Leave),
        t.execute("TEST_BOB".into(), "acme", Command::Leave)
    );
    assert_ne!(x.is_ok(), y.is_ok());
    assert!(matches!(x, Err(Error::LastOwner)) || matches!(y, Err(Error::LastOwner)));
    a.shutdown().await.unwrap();
    b.shutdown().await.unwrap();
}

#[tokio::test]
async fn role_boundaries_transfer_revocation_and_expiry() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("org.sqlite");
    let verifier = sessions();
    let (app, s) = start(&path, verifier.clone()).await;
    create(&s, "acme").await;
    assert!(matches!(
        s.execute("TEST_ALICE".into(), "acme", Command::Leave).await,
        Err(Error::LastOwner)
    ));
    assert!(matches!(
        s.execute(
            "TEST_ALICE".into(),
            "acme",
            Command::ChangeRole {
                target: user("alice"),
                role: Role::Member
            }
        )
        .await,
        Err(Error::LastOwner)
    ));
    assert!(matches!(
        s.execute(
            "TEST_ALICE".into(),
            "acme",
            Command::AddMember {
                target: user("bob"),
                role: Role::Owner
            }
        )
        .await,
        Err(Error::Invalid)
    ));
    let (id, token) = invite(&s, now() + 60_000).await;
    s.execute(
        "TEST_ALICE".into(),
        "acme",
        Command::Revoke {
            invitation_id: id.clone(),
        },
    )
    .await
    .unwrap();
    denied(
        s.execute(
            "TEST_BOB".into(),
            "acme",
            Command::Accept {
                invitation_id: id,
                token,
            },
        )
        .await,
    );
    let (id, token) = invite(&s, now() + 200).await;
    tokio::time::sleep(std::time::Duration::from_millis(220)).await;
    denied(
        s.execute(
            "TEST_BOB".into(),
            "acme",
            Command::Accept {
                invitation_id: id,
                token,
            },
        )
        .await,
    );
    s.execute("TEST_ALICE".into(), "acme", Command::Expire)
        .await
        .unwrap();
    s.execute(
        "TEST_ALICE".into(),
        "acme",
        Command::AddMember {
            target: user("bob"),
            role: Role::Admin,
        },
    )
    .await
    .unwrap();
    denied(
        s.execute(
            "TEST_BOB".into(),
            "acme",
            Command::GrantOwner {
                target: user("bob"),
            },
        )
        .await,
    );
    s.execute(
        "TEST_ALICE".into(),
        "acme",
        Command::TransferOwnership {
            target: user("bob"),
        },
    )
    .await
    .unwrap();
    denied(
        s.execute(
            "TEST_ALICE".into(),
            "acme",
            Command::Rename {
                name: "stolen".into(),
            },
        )
        .await,
    );
    s.execute(
        "TEST_BOB".into(),
        "acme",
        Command::Rename {
            name: "Bob's SaaS".into(),
        },
    )
    .await
    .unwrap();
    verifier.revoked.store(true, Ordering::SeqCst);
    denied(s.execute("TEST_BOB".into(), "acme", Command::Read).await);
    app.shutdown().await.unwrap();
}

struct DelayedPolicy {
    action: &'static str,
    entered: tokio::sync::Notify,
    release: tokio::sync::Notify,
    once: AtomicBool,
    saw_no_membership: AtomicBool,
}
impl DelayedPolicy {
    fn new(action: &'static str) -> Arc<Self> {
        Arc::new(Self {
            action,
            entered: tokio::sync::Notify::new(),
            release: tokio::sync::Notify::new(),
            once: AtomicBool::new(false),
            saw_no_membership: AtomicBool::new(false),
        })
    }
}
impl Policy for DelayedPolicy {
    fn evaluate<'a>(
        &'a self,
        request: &'a Request,
        _: &'a Evaluation,
    ) -> EvaluationFuture<'a, Effect> {
        Box::pin(async move {
            if request.action == self.action {
                if !self.once.swap(true, Ordering::SeqCst) {
                    self.entered.notify_one();
                    self.release.notified().await;
                } else if request
                    .resource
                    .attributes
                    .get("membership_role")
                    .is_some_and(|r| r.is_null())
                {
                    self.saw_no_membership.store(true, Ordering::SeqCst);
                }
            }
            Ok(Effect::Allow)
        })
    }
}

#[tokio::test]
async fn stale_read_retries_with_fresh_membership_policy() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("org.sqlite");
    let policy = DelayedPolicy::new("organization.read");
    let (a, s) = start_policy(&path, sessions(), policy.clone()).await;
    let (b, t) = start(&path, sessions()).await;
    create(&s, "acme").await;
    s.execute(
        "TEST_ALICE".into(),
        "acme",
        Command::AddMember {
            target: user("bob"),
            role: Role::Member,
        },
    )
    .await
    .unwrap();
    s.execute(
        "TEST_ALICE".into(),
        "acme",
        Command::GrantOwner {
            target: user("bob"),
        },
    )
    .await
    .unwrap();
    let reader = s.clone();
    let read = tokio::spawn(async move {
        reader
            .execute("TEST_ALICE".into(), "acme", Command::Read)
            .await
    });
    policy.entered.notified().await;
    t.execute(
        "TEST_BOB".into(),
        "acme",
        Command::Remove {
            target: user("alice"),
        },
    )
    .await
    .unwrap();
    policy.release.notify_one();
    denied(read.await.unwrap());
    assert!(policy.saw_no_membership.load(Ordering::SeqCst));
    a.shutdown().await.unwrap();
    b.shutdown().await.unwrap();
}

#[tokio::test]
async fn cancelled_caller_does_not_abandon_accepted_work_or_shutdown_drain() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("org.sqlite");
    let policy = DelayedPolicy::new("organization.rename");
    let (app, s) = start_policy(&path, sessions(), policy.clone()).await;
    create(&s, "acme").await;
    let service = s.clone();
    let caller = tokio::spawn(async move {
        service
            .execute(
                "TEST_ALICE".into(),
                "acme",
                Command::Rename {
                    name: "Committed after waiter cancellation".into(),
                },
            )
            .await
    });
    policy.entered.notified().await;
    caller.abort();
    assert!(matches!(caller.await, Err(error) if error.is_cancelled()));
    let mut shutdown = tokio::spawn(async move { app.shutdown().await });
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(20), &mut shutdown)
            .await
            .is_err()
    );
    policy.release.notify_one();
    shutdown.await.unwrap().unwrap();
    let (app, s) = start(&path, sessions()).await;
    let Outcome::Snapshot(snapshot) = s
        .execute("TEST_ALICE".into(), "acme", Command::Read)
        .await
        .unwrap()
    else {
        panic!()
    };
    assert_eq!(snapshot.name, "Committed after waiter cancellation");
    app.shutdown().await.unwrap();
}

#[tokio::test]
async fn rejected_storage_write_never_reports_success_or_changes_version() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("org.sqlite");
    let (app, s) = start(&path, sessions()).await;
    create(&s, "acme").await;
    let db = rusqlite::Connection::open(&path).unwrap();
    db.execute_batch("CREATE TRIGGER reject_fixture_write BEFORE UPDATE ON lenso_organizations_v1 BEGIN SELECT RAISE(ABORT, 'TEST_ONLY_WRITE_FAILURE'); END;").unwrap();
    assert!(matches!(
        s.execute(
            "TEST_ALICE".into(),
            "acme",
            Command::Rename {
                name: "must not persist".into()
            }
        )
        .await,
        Err(Error::Storage)
    ));
    let Outcome::Snapshot(snapshot) = s
        .execute("TEST_ALICE".into(), "acme", Command::Read)
        .await
        .unwrap()
    else {
        panic!()
    };
    assert_eq!(snapshot.version, 1);
    assert_eq!(snapshot.name, "Acme");
    app.shutdown().await.unwrap();
}
