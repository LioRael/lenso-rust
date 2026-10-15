#![allow(dead_code)]
use lenso_authorization::*;
use serde_json::json;
use std::sync::Arc;

pub fn scope() -> Scope {
    Scope::new("personal", "home")
}
pub fn principal() -> Principal {
    Principal::new("notes", "alice", "user")
}
pub fn request() -> Request {
    let mut resource = Resource::new("note", "one", scope());
    resource.attributes.insert("state".into(), json!("open"));
    Request::new(Some(principal()), "read", resource)
}
pub fn options() -> AuthorizationOptions {
    let mut options = AuthorizationOptions::new(["read", "write"]);
    options.clock = Arc::new(|| Ok(100));
    options
}
pub fn rule(effect: Outcome, when: Condition) -> Rule {
    Rule::new(format!("{effect:?}"), effect, ["read"], "note", when)
}
pub fn allow_options() -> AuthorizationOptions {
    let mut options = options();
    options.policies.push(policy(|_, _| Ok(Effect::Allow)));
    options
}
pub fn evaluation() -> Evaluation {
    Evaluation {
        now: 100,
        cancellation: Cancellation::default(),
    }
}
pub fn graph() -> RoleGraph {
    RoleGraph {
        roles: vec![
            Role {
                id: "reader".into(),
                scope: scope(),
                permissions: vec![Permission::new("read", "note", scope())],
                inherits: vec![],
            },
            Role {
                id: "member".into(),
                scope: scope(),
                permissions: vec![],
                inherits: vec!["reader".into()],
            },
        ],
        bindings: vec![Binding {
            id: "alice-member".into(),
            principal: principal(),
            role_id: "member".into(),
            scope: scope(),
            expires_at: Some(200),
        }],
    }
}
