use lenso_authorization::*;
use lenso_plugin_core::{App, BoxError, Plugin, Setup};
use std::sync::Arc;

struct Roles(RoleGraph);
impl Plugin for Roles {
    type Service = MemoryRoleStore;
    async fn setup(self, _: &mut Setup) -> Result<Arc<MemoryRoleStore>, BoxError> {
        Ok(Arc::new(MemoryRoleStore::new(self.0, ["read"])?))
    }
}

#[tokio::main]
async fn main() -> Result<(), BoxError> {
    let scope = Scope::new("personal", "home");
    let principal = Principal::new("notes", "alice", "user");
    let mut app = App::new();
    let roles = app.install(
        "notes-roles",
        Roles(RoleGraph {
            roles: vec![Role {
                id: "reader".into(),
                scope: scope.clone(),
                permissions: vec![Permission::new("read", "note", scope.clone())],
                inherits: vec![],
            }],
            bindings: vec![Binding {
                id: "alice-reader".into(),
                principal: principal.clone(),
                role_id: "reader".into(),
                scope: scope.clone(),
                expires_at: None,
            }],
        }),
    );
    let authorization = app.install(
        "notes-authorization",
        AuthorizationPlugin::setup(vec![roles.key()], move |setup| {
            Box::pin(async move {
                let roles = setup.get(&roles)?;
                let mut options = AuthorizationOptions::new(["read"]);
                options.identity.required = true;
                options
                    .policies
                    .push(Arc::new(Rbac::from_source(["read"], roles)?));
                Ok(Arc::new(Authorization::new(options)?))
            })
        }),
    );
    let running = app.start().await?;
    let service = running.get(&authorization)?;
    {
        // Hold admission at the outer business entry through authorization and use.
        let _work = running.work_gate().enter()?;
        service
            .enforce(Request::new(
                Some(principal),
                "read",
                Resource::new("note", "one", scope),
            ))
            .await?;
        println!("Exact notes-roles instance granted read.");
    }
    running.shutdown().await?;
    Ok(())
}
