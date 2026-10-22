use lenso_authorization::{
    policy, AccessIdentity, AccessVerifier, AuthorizationOptions, AuthorizationPlugin, Effect,
    Evaluation, EvaluationFuture, Principal, Resource,
};
use lenso_organization::{Command, OrganizationPlugin, Outcome, Role, Subject, ACTIONS};
use lenso_plugin_core::{App, BoxError};
use std::{
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

// Fixture session lookup, deliberately not a production authenticator.
struct FixtureSessions;
impl AccessVerifier<String> for FixtureSessions {
    fn verify<'a>(
        &'a self,
        evidence: &'a String,
        _: &'a Resource,
        _: &'a Evaluation,
    ) -> EvaluationFuture<'a, Option<AccessIdentity>> {
        Box::pin(async move {
            let subject = match evidence.as_str() {
                "TEST_ONLY_ALICE_SESSION" => "alice",
                "TEST_ONLY_BOB_SESSION" => "bob",
                _ => return Ok(None),
            };
            Ok(Some(AccessIdentity {
                principal: Principal::new("fixture", subject, "user"),
                audience: "saas-fixture".into(),
                session_expires_at: None,
                credential: None,
            }))
        })
    }
}
#[tokio::main]
async fn main() -> Result<(), BoxError> {
    let temp = tempfile::tempdir()?;
    let path = temp.path().join("team.sqlite");
    let mut app = App::new();
    let mut options = AuthorizationOptions::new(ACTIONS);
    options.identity.required = true;
    // Explicit host policy for this isolated SaaS fixture only. The service also
    // enforces its server-owned membership and owner invariants on every operation.
    options.policies.push(policy(|r, _| {
        Ok(
            if r.resource.attributes.get("realm").and_then(|v| v.as_str()) == Some("fixture") {
                Effect::Allow
            } else {
                Effect::Deny
            },
        )
    }));
    let auth = app.install("host-policy", AuthorizationPlugin::new(options)?);
    let handle = app.install(
        "organizations",
        OrganizationPlugin::new(
            &path,
            "fixture",
            "saas-fixture",
            auth,
            Arc::new(FixtureSessions),
        ),
    );
    let running = app.start().await?;
    let organizations = running.get(&handle)?;
    let business: Result<(), BoxError> = async {
        organizations.execute("TEST_ONLY_ALICE_SESSION".into(), "acme", Command::Create { name: "Acme workspace".into() }).await?;
        let expiry = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() as u64 + 60_000;
        let Outcome::Invited { invitation, token, .. } = organizations.execute("TEST_ONLY_ALICE_SESSION".into(), "acme", Command::Invite { target: Subject::user("fixture", "bob"), role: Role::Member, expires_at: expiry }).await? else { unreachable!() };
        // Mock notification consumes the one-time token locally; no mail/network.
        let mock_notification = (invitation.id, token);
        organizations.execute("TEST_ONLY_BOB_SESSION".into(), "acme", Command::Accept { invitation_id: mock_notification.0, token: mock_notification.1 }).await?;
        let Outcome::Snapshot(team) = organizations.execute("TEST_ONLY_BOB_SESSION".into(), "acme", Command::Read).await? else { unreachable!() };
        assert_eq!(team.members.len(), 2);
        organizations.execute("TEST_ONLY_ALICE_SESSION".into(), "acme", Command::Remove { target: Subject::user("fixture", "bob") }).await?;
        assert!(organizations.execute("TEST_ONLY_BOB_SESSION".into(), "acme", Command::Read).await.is_err());
        println!("SaaS fixture: invited, accepted, read 2 members, removed; access now denied. No email sent.");
        Ok(())
    }.await;
    let shutdown = running.shutdown().await;
    if let Err(error) = business {
        if let Err(cleanup) = shutdown {
            eprintln!("Cleanup also failed: {cleanup}");
        }
        return Err(error);
    }
    shutdown?;
    Ok(())
}
