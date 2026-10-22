//! Versioned boolean feature flags. Feature selection is never authorization.
#![doc = include_str!("../README.md")]

use lenso_plugin_core::{BoxError, CleanupPhase, Plugin, Setup};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, VecDeque},
    sync::{Arc, Mutex},
};

/// Host-supplied identity; realm avoids collisions across identity providers.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Identity {
    pub realm: String,
    pub subject: String,
}
/// Capture once from the host's verified request context.
#[derive(Clone, Debug, Default)]
pub struct Context {
    pub user: Option<Identity>,
    pub organization: Option<Identity>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Rule {
    pub identity: Identity,
    pub value: bool,
}
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Cohort {
    User,
    Organization,
}
/// Basis points in 0..=10000. Salt is stable across config revisions.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Rollout {
    pub basis_points: u16,
    pub salt: String,
    pub cohort: Cohort,
}
/// User overrides precede organization overrides, then rollout, then default.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Flag {
    pub default: bool,
    #[serde(default)]
    pub users: Vec<Rule>,
    #[serde(default)]
    pub organizations: Vec<Rule>,
    pub rollout: Option<Rollout>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Configuration {
    pub version: String,
    pub flags: BTreeMap<String, Flag>,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum FlagError {
    #[error("invalid feature configuration or context")]
    Invalid,
    #[error("feature revision conflict")]
    Conflict,
    #[error("feature revision not retained")]
    NotRetained,
    #[error("feature service closed")]
    Closed,
    #[error("feature synchronization unavailable")]
    Unavailable,
    #[error("feature revision exhausted")]
    Exhausted,
}
fn valid(s: &str) -> bool {
    !s.is_empty() && s.len() <= 256 && !s.chars().any(char::is_control)
}
fn identity(i: &Identity) -> bool {
    valid(&i.realm) && valid(&i.subject)
}
impl Configuration {
    /// Bounded fixture parsing; errors never echo config contents.
    pub fn from_json(bytes: &[u8]) -> Result<Self, FlagError> {
        if bytes.len() > 1024 * 1024 {
            return Err(FlagError::Invalid);
        }
        let value: Self = serde_json::from_slice(bytes).map_err(|_| FlagError::Invalid)?;
        value.validate()?;
        Ok(value)
    }
    pub fn validate(&self) -> Result<(), FlagError> {
        if !valid(&self.version) || self.flags.len() > 256 {
            return Err(FlagError::Invalid);
        }
        for (key, flag) in &self.flags {
            if !valid(key) {
                return Err(FlagError::Invalid);
            }
            for rules in [&flag.users, &flag.organizations] {
                let mut seen = std::collections::BTreeSet::new();
                if rules.len() > 256
                    || rules
                        .iter()
                        .any(|r| !identity(&r.identity) || !seen.insert(&r.identity))
                {
                    return Err(FlagError::Invalid);
                }
            }
            if flag
                .rollout
                .as_ref()
                .is_some_and(|r| r.basis_points > 10000 || !valid(&r.salt))
            {
                return Err(FlagError::Invalid);
            }
        }
        Ok(())
    }
}
struct State {
    revision: u64,
    current: Arc<Configuration>,
    history: VecDeque<(u64, Arc<Configuration>)>,
    closed: bool,
}
/// Each instance is isolated. Publishing is synchronous and atomic; no I/O or workers.
#[derive(Clone)]
pub struct FeatureFlags {
    state: Arc<Mutex<State>>,
}
impl FeatureFlags {
    pub fn new(config: Configuration) -> Result<Self, FlagError> {
        config.validate()?;
        Ok(Self {
            state: Arc::new(Mutex::new(State {
                revision: 1,
                current: Arc::new(config),
                history: VecDeque::new(),
                closed: false,
            })),
        })
    }
    /// CAS guards against concurrent administrators and ABA after rollback.
    pub fn publish(&self, expected: u64, config: Configuration) -> Result<u64, FlagError> {
        config.validate()?;
        let mut state = self.state.lock().map_err(|_| FlagError::Unavailable)?;
        Self::replace(&mut state, expected, Arc::new(config))
    }
    fn replace(
        state: &mut State,
        expected: u64,
        config: Arc<Configuration>,
    ) -> Result<u64, FlagError> {
        if state.closed {
            return Err(FlagError::Closed);
        }
        if state.revision != expected {
            return Err(FlagError::Conflict);
        }
        let next = expected.checked_add(1).ok_or(FlagError::Exhausted)?;
        state
            .history
            .push_back((state.revision, state.current.clone()));
        if state.history.len() > 16 {
            state.history.pop_front();
        }
        state.current = config;
        state.revision = next;
        Ok(next)
    }
    /// Restore a retained revision as a NEW revision; old requests keep their snapshot.
    pub fn rollback(&self, expected: u64, target: u64) -> Result<u64, FlagError> {
        let mut state = self.state.lock().map_err(|_| FlagError::Unavailable)?;
        if state.closed {
            return Err(FlagError::Closed);
        }
        if state.revision != expected {
            return Err(FlagError::Conflict);
        }
        let config = state
            .history
            .iter()
            .find(|(r, _)| *r == target)
            .map(|(_, c)| c.clone())
            .ok_or(FlagError::NotRetained)?;
        Self::replace(&mut state, expected, config)
    }
    /// Capture configuration and identity once per admitted request. Even failure is pinned.
    pub fn snapshot(&self, context: Context) -> Snapshot {
        let captured = (|| {
            if context.user.as_ref().is_some_and(|i| !identity(i))
                || context.organization.as_ref().is_some_and(|i| !identity(i))
            {
                return Err(FlagError::Invalid);
            }
            let state = self.state.lock().map_err(|_| FlagError::Unavailable)?;
            if state.closed {
                return Err(FlagError::Closed);
            }
            Ok((state.revision, state.current.clone()))
        })();
        Snapshot { context, captured }
    }
    pub fn close(&self) -> Result<(), FlagError> {
        let mut state = self.state.lock().map_err(|_| FlagError::Unavailable)?;
        state.closed = true;
        state.history.clear();
        Ok(())
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Reason {
    User,
    Organization,
    Rollout,
    ConfigDefault,
    MissingIdentity,
    MissingFlag,
    Failure(FlagError),
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Decision {
    pub value: bool,
    pub revision: Option<u64>,
    pub reason: Reason,
}
/// Immutable request configuration AND context, safe to share across concurrent request tasks.
#[derive(Clone)]
pub struct Snapshot {
    context: Context,
    captured: Result<(u64, Arc<Configuration>), FlagError>,
}
impl Snapshot {
    pub fn version(&self) -> Option<&str> {
        self.captured.as_ref().ok().map(|(_, c)| c.version.as_str())
    }
    pub fn revision(&self) -> Option<u64> {
        self.captured.as_ref().ok().map(|(r, _)| *r)
    }
    /// Caller MUST choose the fallback. Rollout nonmembers use config default.
    pub fn evaluate(&self, key: &str, failure_default: bool) -> Decision {
        let (revision, config) = match &self.captured {
            Ok(c) => c,
            Err(e) => {
                return Decision {
                    value: failure_default,
                    revision: None,
                    reason: Reason::Failure(*e),
                }
            }
        };
        let result = |value, reason| Decision {
            value,
            revision: Some(*revision),
            reason,
        };
        let Some(flag) = config.flags.get(key) else {
            return result(failure_default, Reason::MissingFlag);
        };
        for (rules, id, reason) in [
            (&flag.users, &self.context.user, Reason::User),
            (
                &flag.organizations,
                &self.context.organization,
                Reason::Organization,
            ),
        ] {
            if let Some(rule) = rules.iter().find(|r| Some(&r.identity) == id.as_ref()) {
                return result(rule.value, reason);
            }
        }
        if let Some(rollout) = &flag.rollout {
            let id = match rollout.cohort {
                Cohort::User => &self.context.user,
                Cohort::Organization => &self.context.organization,
            };
            let Some(id) = id else {
                return result(failure_default, Reason::MissingIdentity);
            };
            let bucket = bucket(key, &rollout.salt, rollout.cohort, id);
            return result(
                if bucket < rollout.basis_points {
                    true
                } else {
                    flag.default
                },
                Reason::Rollout,
            );
        }
        result(flag.default, Reason::ConfigDefault)
    }
}
// FNV-1a 64, length-prefixed UTF-8 fields, u64 little-endian lengths. NOT a security hash.
fn bucket(key: &str, salt: &str, cohort: Cohort, id: &Identity) -> u16 {
    let mut hash = 0xcbf29ce484222325u64;
    let kind = match cohort {
        Cohort::User => "user",
        Cohort::Organization => "organization",
    };
    for field in ["lenso-flags-v1", key, salt, kind, &id.realm, &id.subject] {
        for byte in (field.len() as u64)
            .to_le_bytes()
            .into_iter()
            .chain(field.bytes())
        {
            hash = (hash ^ u64::from(byte)).wrapping_mul(0x100000001b3);
        }
    }
    (hash % 10000) as u16
}
pub struct FeatureFlagsPlugin {
    config: Configuration,
}
impl FeatureFlagsPlugin {
    pub fn new(config: Configuration) -> Self {
        Self { config }
    }
}
impl Plugin for FeatureFlagsPlugin {
    type Service = FeatureFlags;
    fn validate(&self) -> Result<(), BoxError> {
        self.config.validate().map_err(Into::into)
    }
    async fn setup(self, setup: &mut Setup) -> Result<Arc<Self::Service>, BoxError> {
        let service = Arc::new(FeatureFlags::new(self.config)?);
        let cleanup = service.clone();
        setup.on_cleanup(CleanupPhase::Resources, move || async move {
            cleanup.close().map_err(Into::into)
        });
        Ok(service)
    }
}
