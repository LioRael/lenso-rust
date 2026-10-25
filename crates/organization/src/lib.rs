//! Persistent organization relationships; authentication remains host-owned.
#![forbid(unsafe_code)]

use lenso_authorization::{
    AccessVerifier, Authorization, Cancellation, Evaluation, Principal, Request, Resource, Scope,
};
use lenso_plugin_core::{BoxError, CleanupPhase, Handle, InstanceKey, Plugin, Setup, WorkGate};
use rusqlite::{params, Connection, OptionalExtension};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    path::PathBuf,
    sync::{Arc, Mutex},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum Error {
    #[error("Organization access denied")]
    Denied,
    #[error("Invalid organization input")]
    Invalid,
    #[error("Organization conflict")]
    Conflict,
    #[error("The last owner must remain")]
    LastOwner,
    #[error("Organization capacity exceeded")]
    Capacity,
    #[error("Organization storage unavailable")]
    Storage,
    #[error("Organization service closed")]
    Closed,
}
type Result<T> = std::result::Result<T, Error>;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Subject {
    pub realm: String,
    pub id: String,
    pub kind: String,
}
impl Subject {
    pub fn user(realm: impl Into<String>, id: impl Into<String>) -> Self {
        Self {
            realm: realm.into(),
            id: id.into(),
            kind: "user".into(),
        }
    }
    fn valid(&self) -> bool {
        valid(&self.realm) && valid(&self.id) && valid(&self.kind)
    }
}
impl From<&Principal> for Subject {
    fn from(p: &Principal) -> Self {
        Self {
            realm: p.realm_id.clone(),
            id: p.subject_id.clone(),
            kind: p.kind.clone(),
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Role {
    Owner,
    Admin,
    Member,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Member {
    pub subject: Subject,
    pub role: Role,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum InvitationStatus {
    Pending,
    Accepted,
    Revoked,
    Expired,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Invitation {
    pub id: String,
    pub target: Subject,
    pub role: Role,
    pub expires_at: u64,
    pub status: InvitationStatus,
}
#[derive(Clone, Serialize, Deserialize)]
struct StoredInvitation {
    invitation: Invitation,
    digest: Vec<u8>,
}
#[derive(Clone, Serialize, Deserialize)]
struct State {
    name: String,
    members: Vec<Member>,
    invitations: Vec<StoredInvitation>,
}
#[derive(Clone, Debug)]
pub struct Snapshot {
    pub id: String,
    pub name: String,
    pub version: i64,
    pub members: Vec<Member>,
    /// Invitation metadata is visible only to owners. Digests are never returned.
    pub invitations: Vec<Invitation>,
}

/// Commands are untrusted input. Identity is obtained exclusively from AccessVerifier.
pub enum Command {
    Create {
        name: String,
    },
    Read,
    Rename {
        name: String,
    },
    AddMember {
        target: Subject,
        role: Role,
    },
    ChangeRole {
        target: Subject,
        role: Role,
    },
    GrantOwner {
        target: Subject,
    },
    TransferOwnership {
        target: Subject,
    },
    Remove {
        target: Subject,
    },
    Leave,
    Invite {
        target: Subject,
        role: Role,
        expires_at: u64,
    },
    Accept {
        invitation_id: String,
        token: String,
    },
    Revoke {
        invitation_id: String,
    },
    Expire,
}
impl Command {
    fn target(&self) -> Option<&Subject> {
        match self {
            Self::AddMember { target, .. }
            | Self::ChangeRole { target, .. }
            | Self::Invite { target, .. }
            | Self::GrantOwner { target }
            | Self::TransferOwnership { target }
            | Self::Remove { target } => Some(target),
            _ => None,
        }
    }
    pub fn action(&self) -> &'static str {
        match self {
            Self::Create { .. } => "organization.create",
            Self::Read => "organization.read",
            Self::Rename { .. } => "organization.rename",
            Self::AddMember { .. } => "organization.add",
            Self::ChangeRole { .. } => "organization.role",
            Self::GrantOwner { .. } => "organization.grant_owner",
            Self::TransferOwnership { .. } => "organization.transfer",
            Self::Remove { .. } => "organization.remove",
            Self::Leave => "organization.leave",
            Self::Invite { .. } => "organization.invite",
            Self::Accept { .. } => "organization.accept",
            Self::Revoke { .. } => "organization.revoke",
            Self::Expire => "organization.expire",
        }
    }
    fn validate(&self) -> Result<()> {
        match self {
            Self::Create { name } | Self::Rename { name } if !valid(name) => Err(Error::Invalid),
            Self::AddMember { target, role }
            | Self::ChangeRole { target, role }
            | Self::Invite { target, role, .. }
                if !target.valid() || *role == Role::Owner =>
            {
                Err(Error::Invalid)
            }
            Self::GrantOwner { target }
            | Self::TransferOwnership { target }
            | Self::Remove { target }
                if !target.valid() =>
            {
                Err(Error::Invalid)
            }
            Self::Accept {
                invitation_id,
                token,
            } if !valid(invitation_id) || token.len() != 64 => Err(Error::Denied),
            Self::Revoke { invitation_id } if !valid(invitation_id) => Err(Error::Invalid),
            _ => Ok(()),
        }
    }
}
pub const ACTIONS: [&str; 13] = [
    "organization.create",
    "organization.read",
    "organization.rename",
    "organization.add",
    "organization.role",
    "organization.grant_owner",
    "organization.transfer",
    "organization.remove",
    "organization.leave",
    "organization.invite",
    "organization.accept",
    "organization.revoke",
    "organization.expire",
];

/// Deliberately does not implement Debug: bearer tokens must not enter logs.
pub enum Outcome {
    Snapshot(Snapshot),
    Changed {
        version: i64,
    },
    Invited {
        version: i64,
        invitation: Invitation,
        token: String,
    },
}

type Database = Arc<Mutex<Option<Connection>>>;
pub struct Organization<E> {
    db: Database,
    gate: WorkGate,
    authorization: Arc<Authorization>,
    verifier: Arc<dyn AccessVerifier<E>>,
    realm: String,
    audience: String,
    serial: tokio::sync::Mutex<()>,
}
pub struct OrganizationPlugin<E> {
    path: PathBuf,
    realm: String,
    audience: String,
    authorization: Handle<Authorization>,
    verifier: Arc<dyn AccessVerifier<E>>,
}
impl<E> OrganizationPlugin<E> {
    pub fn new(
        path: impl Into<PathBuf>,
        realm: impl Into<String>,
        audience: impl Into<String>,
        authorization: Handle<Authorization>,
        verifier: Arc<dyn AccessVerifier<E>>,
    ) -> Self {
        Self {
            path: path.into(),
            realm: realm.into(),
            audience: audience.into(),
            authorization,
            verifier,
        }
    }
}
impl<E: Send + Sync + 'static> Plugin for OrganizationPlugin<E> {
    type Service = Organization<E>;
    fn dependencies(&self) -> Vec<InstanceKey> {
        vec![self.authorization.key()]
    }
    fn validate(&self) -> std::result::Result<(), BoxError> {
        if self.path.as_os_str().is_empty() || !valid(&self.realm) || !valid(&self.audience) {
            return Err(Error::Invalid.into());
        }
        Ok(())
    }
    async fn setup(self, setup: &mut Setup) -> std::result::Result<Arc<Self::Service>, BoxError> {
        let authorization = setup.get(&self.authorization)?;
        let db = Arc::new(Mutex::new(Some(
            tokio::task::spawn_blocking(move || -> Result<Connection> {
                let db = Connection::open(self.path).map_err(|_| Error::Storage)?;
                db.busy_timeout(Duration::from_secs(2))
                    .map_err(|_| Error::Storage)?;
                Ok(db)
            })
            .await??,
        )));
        let cleanup = db.clone();
        setup.on_cleanup(CleanupPhase::Resources, move || async move {
            tokio::task::spawn_blocking(move || -> Result<()> {
                let conn = cleanup.lock().map_err(|_| Error::Storage)?.take();
                if let Some(conn) = conn {
                    conn.close().map_err(|_| Error::Storage)?;
                }
                Ok(())
            })
            .await??;
            Ok(())
        });
        let initialize = db.clone();
        tokio::task::spawn_blocking(move || -> Result<()> {
            let guard = initialize.lock().map_err(|_| Error::Storage)?;
            guard.as_ref().ok_or(Error::Closed)?.execute_batch("CREATE TABLE IF NOT EXISTS lenso_organizations_v1 (realm TEXT NOT NULL, id TEXT NOT NULL, version INTEGER NOT NULL, state TEXT NOT NULL, PRIMARY KEY(realm,id));").map_err(|_| Error::Storage)
        }).await??;
        Ok(Arc::new(Organization {
            db,
            gate: setup.work_gate(),
            authorization,
            verifier: self.verifier,
            realm: self.realm,
            audience: self.audience,
            serial: tokio::sync::Mutex::new(()),
        }))
    }
}

impl<E: Send + Sync + 'static> Organization<E> {
    /// Every retry verifies host evidence again and evaluates the current relation snapshot.
    /// Accepted work is owned by a task so caller cancellation cannot abandon a DB write.
    ///
    /// # Errors
    /// Returns validation, access-denial, conflict, capacity, last-owner,
    /// closed-service or storage errors. An uncertain mutation must be reconciled
    /// by reading current state; a failure does not authorize blind replay.
    pub async fn execute(
        self: &Arc<Self>,
        evidence: E,
        id: impl Into<String>,
        command: Command,
    ) -> Result<Outcome> {
        let work = self.gate.enter().map_err(|_| Error::Closed)?;
        let id = id.into();
        if !valid(&id) {
            return Err(Error::Invalid);
        }
        command.validate()?;
        if command
            .target()
            .is_some_and(|target| target.realm != self.realm)
        {
            return Err(Error::Invalid);
        }
        let service = self.clone();
        tokio::spawn(async move {
            let _work = work;
            let _serial = service.serial.lock().await;
            service.run(evidence, id, command).await
        })
        .await
        .map_err(|_| Error::Storage)?
    }
    async fn run(&self, evidence: E, id: String, command: Command) -> Result<Outcome> {
        for _ in 0..8 {
            let db = self.db.clone();
            let realm = self.realm.clone();
            let key = id.clone();
            let loaded = tokio::task::spawn_blocking(move || -> Result<Option<(i64, State)>> {
                let guard = db.lock().map_err(|_| Error::Storage)?;
                let row: Option<(i64, String)> = guard
                    .as_ref()
                    .ok_or(Error::Closed)?
                    .query_row(
                        "SELECT version,state FROM lenso_organizations_v1 WHERE realm=? AND id=?",
                        params![realm, key],
                        |r| Ok((r.get(0)?, r.get(1)?)),
                    )
                    .optional()
                    .map_err(|_| Error::Storage)?;
                row.map(|(v, s)| Ok((v, serde_json::from_str(&s).map_err(|_| Error::Storage)?)))
                    .transpose()
            })
            .await
            .map_err(|_| Error::Storage)??;
            let version = loaded.as_ref().map_or(0, |(v, _)| *v);
            let mut resource = Resource::new("organization", &id, Scope::new("organization", &id));
            resource
                .attributes
                .insert("realm".into(), self.realm.clone().into());
            resource.attributes.insert("version".into(), version.into());
            let evaluation = Evaluation {
                now: now()?,
                cancellation: Cancellation::default(),
            };
            let identity = tokio::time::timeout(
                Duration::from_secs(2),
                self.verifier.verify(&evidence, &resource, &evaluation),
            )
            .await
            .map_err(|_| Error::Denied)?
            .map_err(|_| Error::Denied)?
            .ok_or(Error::Denied)?;
            if identity.principal.realm_id != self.realm
                || identity.audience != self.audience
                || identity
                    .session_expires_at
                    .is_some_and(|t| t <= evaluation.now)
            {
                return Err(Error::Denied);
            }
            let actor = Subject::from(&identity.principal);
            let role = loaded.as_ref().and_then(|(_, s)| {
                s.members
                    .iter()
                    .find(|m| m.subject == actor)
                    .map(|m| m.role)
            });
            // Host policy gets fresh server-owned membership facts, never a client role.
            resource.attributes.insert(
                "membership_role".into(),
                role.map(|r| format!("{r:?}")).into(),
            );
            let mut request = Request::new(Some(identity.principal), command.action(), resource);
            if let Some(target) = command.target() {
                request.context.insert(
                    "target".into(),
                    serde_json::to_value(target).map_err(|_| Error::Invalid)?,
                );
            }
            match &command {
                Command::AddMember { role, .. }
                | Command::ChangeRole { role, .. }
                | Command::Invite { role, .. } => {
                    request
                        .context
                        .insert("requested_role".into(), format!("{role:?}").into());
                }
                Command::GrantOwner { .. } | Command::TransferOwnership { .. } => {
                    request
                        .context
                        .insert("requested_role".into(), "Owner".into());
                }
                Command::Accept { invitation_id, .. } | Command::Revoke { invitation_id } => {
                    request
                        .context
                        .insert("invitation_id".into(), invitation_id.clone().into());
                    if let Some(invite) = loaded.as_ref().and_then(|(_, s)| {
                        s.invitations
                            .iter()
                            .find(|i| i.invitation.id == *invitation_id)
                    }) {
                        request.context.insert(
                            "invitation".into(),
                            serde_json::to_value(&invite.invitation).map_err(|_| Error::Storage)?,
                        );
                    }
                }
                _ => {}
            }
            request.audience = Some(identity.audience);
            let expires = identity
                .session_expires_at
                .into_iter()
                .chain(identity.credential.as_ref().and_then(|c| c.expires_at))
                .min();
            request.credential = identity.credential;
            self.authorization
                .enforce(request)
                .await
                .map_err(|_| Error::Denied)?;
            if expires.is_some_and(|t| now().map_or(true, |n| n >= t)) {
                return Err(Error::Denied);
            }
            let permitted = match command {
                Command::Create { .. } => loaded.is_none(),
                Command::Accept { .. } => loaded.is_some(),
                Command::Read | Command::Leave => role.is_some(),
                _ => role == Some(Role::Owner),
            };
            if !permitted {
                return Err(Error::Denied);
            }
            let mut state = loaded.map(|(_, s)| s).unwrap_or(State {
                name: String::new(),
                members: vec![],
                invitations: vec![],
            });
            let outcome = apply(&mut state, &actor, &id, version, &command, now()?)?;
            let db = self.db.clone();
            let realm = self.realm.clone();
            let key = id.clone();
            let read = matches!(command, Command::Read);
            let commit_deadline = match &command {
                Command::Invite { expires_at, .. } => Some(*expires_at),
                Command::Accept { invitation_id, .. } => state
                    .invitations
                    .iter()
                    .find(|i| i.invitation.id == *invitation_id)
                    .map(|i| i.invitation.expires_at),
                _ => None,
            };
            let stored = serde_json::to_string(&state).map_err(|_| Error::Storage)?;
            let committed = tokio::task::spawn_blocking(move || -> Result<bool> {
                let mut guard = db.lock().map_err(|_| Error::Storage)?;
                let tx = guard.as_mut().ok_or(Error::Closed)?.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate).map_err(|_| Error::Storage)?;
                let current: Option<i64> = tx.query_row("SELECT version FROM lenso_organizations_v1 WHERE realm=? AND id=?", params![realm,key], |r| r.get(0)).optional().map_err(|_| Error::Storage)?;
                if current.unwrap_or(0) != version { return Ok(false); }
                if expires.is_some_and(|t| now().map_or(true, |n| n >= t)) { return Err(Error::Denied); }
                if commit_deadline.is_some_and(|t| now().map_or(true, |n| n >= t)) { return Err(Error::Denied); }
                if !read {
                    tx.execute("INSERT INTO lenso_organizations_v1 (realm,id,version,state) VALUES (?,?,?,?) ON CONFLICT(realm,id) DO UPDATE SET version=excluded.version,state=excluded.state", params![realm,key,version.checked_add(1).ok_or(Error::Conflict)?,stored]).map_err(|_| Error::Storage)?;
                }
                tx.commit().map_err(|_| Error::Storage)?;
                Ok(true)
            }).await.map_err(|_| Error::Storage)??;
            if committed {
                return Ok(outcome);
            }
        }
        Err(Error::Conflict)
    }
}

fn valid(s: &str) -> bool {
    !s.trim().is_empty() && s.len() <= 256
}
fn now() -> Result<u64> {
    u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| Error::Storage)?
            .as_millis(),
    )
    .map_err(|_| Error::Storage)
}
fn random() -> String {
    rand::random::<[u8; 32]>()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

fn apply(
    s: &mut State,
    actor: &Subject,
    id: &str,
    version: i64,
    c: &Command,
    time: u64,
) -> Result<Outcome> {
    let next = version.checked_add(1).ok_or(Error::Conflict)?;
    let mut issued = None;
    match c {
        Command::Create { name } => {
            s.name = name.clone();
            s.members.push(Member {
                subject: actor.clone(),
                role: Role::Owner,
            });
        }
        Command::Read => {
            return Ok(Outcome::Snapshot(Snapshot {
                id: id.into(),
                name: s.name.clone(),
                version,
                members: s.members.clone(),
                invitations: if s
                    .members
                    .iter()
                    .any(|m| m.subject == *actor && m.role == Role::Owner)
                {
                    s.invitations
                        .iter()
                        .map(|i| {
                            let mut public = i.invitation.clone();
                            if public.status == InvitationStatus::Pending
                                && public.expires_at <= time
                            {
                                public.status = InvitationStatus::Expired;
                            }
                            public
                        })
                        .collect()
                } else {
                    vec![]
                },
            }))
        }
        Command::Rename { name } => s.name = name.clone(),
        Command::AddMember { target, role } => add(s, target, *role)?,
        Command::ChangeRole { target, role } => member(s, target)?.role = *role,
        Command::GrantOwner { target } => member(s, target)?.role = Role::Owner,
        Command::TransferOwnership { target } => {
            if target == actor {
                return Err(Error::Invalid);
            }
            member(s, target)?.role = Role::Owner;
            member(s, actor)?.role = Role::Member;
        }
        Command::Remove { target } => {
            member(s, target)?;
            s.members.retain(|m| &m.subject != target);
        }
        Command::Leave => s.members.retain(|m| &m.subject != actor),
        Command::Invite {
            target,
            role,
            expires_at,
        } => {
            if *expires_at <= time || *expires_at - time > 30 * 24 * 60 * 60 * 1000 {
                return Err(Error::Invalid);
            }
            if s.members.iter().any(|m| m.subject == *target) {
                return Err(Error::Conflict);
            }
            let token = random();
            let invitation = Invitation {
                id: random(),
                target: target.clone(),
                role: *role,
                expires_at: *expires_at,
                status: InvitationStatus::Pending,
            };
            s.invitations.push(StoredInvitation {
                invitation: invitation.clone(),
                digest: Sha256::digest(token.as_bytes()).to_vec(),
            });
            issued = Some((invitation, token));
        }
        Command::Accept {
            invitation_id,
            token,
        } => {
            let i = s
                .invitations
                .iter_mut()
                .find(|i| i.invitation.id == *invitation_id)
                .ok_or(Error::Denied)?;
            let digest = Sha256::digest(token.as_bytes());
            if i.invitation.target != *actor
                || i.invitation.status != InvitationStatus::Pending
                || i.invitation.expires_at <= time
                || i.digest.len() != digest.len()
                || i.digest
                    .iter()
                    .zip(digest)
                    .fold(0u8, |diff, (a, b)| diff | (a ^ b))
                    != 0
            {
                return Err(Error::Denied);
            }
            i.invitation.status = InvitationStatus::Accepted;
            let role = i.invitation.role;
            add(s, actor, role)?;
        }
        Command::Revoke { invitation_id } => {
            let i = s
                .invitations
                .iter_mut()
                .find(|i| i.invitation.id == *invitation_id)
                .ok_or(Error::Denied)?;
            if i.invitation.status != InvitationStatus::Pending || i.invitation.expires_at <= time {
                return Err(Error::Conflict);
            }
            i.invitation.status = InvitationStatus::Revoked;
        }
        Command::Expire => {
            for i in &mut s.invitations {
                if i.invitation.status == InvitationStatus::Pending
                    && i.invitation.expires_at <= time
                {
                    i.invitation.status = InvitationStatus::Expired;
                }
            }
        }
    }
    if !s.members.iter().any(|m| m.role == Role::Owner) {
        return Err(Error::LastOwner);
    }
    if s.members.len() > 1000 || s.invitations.len() > 1000 {
        return Err(Error::Capacity);
    }
    Ok(match issued {
        Some((invitation, token)) => Outcome::Invited {
            version: next,
            invitation,
            token,
        },
        None => Outcome::Changed { version: next },
    })
}
fn member<'a>(s: &'a mut State, target: &Subject) -> Result<&'a mut Member> {
    s.members
        .iter_mut()
        .find(|m| &m.subject == target)
        .ok_or(Error::Denied)
}
fn add(s: &mut State, target: &Subject, role: Role) -> Result<()> {
    if s.members.iter().any(|m| &m.subject == target) {
        return Err(Error::Conflict);
    }
    s.members.push(Member {
        subject: target.clone(),
        role,
    });
    Ok(())
}
