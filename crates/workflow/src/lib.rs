//! Bounded native workflows over the Tasks-owned SQLite transaction.
//! Business handlers, schemas and task definitions belong to consumers.
use lenso_plugin_core::{BoxError, Handle, InstanceKey, Plugin, Setup};
use lenso_tasks::{EnqueueOptions, JobState, SqliteTaskTransaction, TaskError, Tasks};
use rusqlite::params;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{collections::HashSet, sync::Arc};

const SCHEMA: &str = "CREATE TABLE IF NOT EXISTS lenso_workflow_run_v1 (
 queue_identity TEXT NOT NULL, definition TEXT NOT NULL, id TEXT NOT NULL,
 checkpoint TEXT NOT NULL, PRIMARY KEY(queue_identity,definition,id))";
const MAX_STEPS: usize = 16;
const MAX_PAYLOAD: usize = 16_384;

/// Forward-only edges make dependencies explicit and bound execution to 16 steps.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Target {
    Step(usize),
    Complete,
    Reject,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum StepKind {
    Task { task: String, next: Target },
    Signal { yes: Target, no: Target },
    Transaction { next: Target },
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StepVersion {
    pub name: String,
    pub version: u32,
    pub kind: StepKind,
}
/// Persisted topology as well as versions fence changed deployments.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DefinitionVersion {
    pub name: String,
    pub version: u32,
    pub steps: Vec<StepVersion>,
}

/// This callback may only perform same-database business SQL. No network, file
/// writes, nested Tasks calls or other external effects; rollback cannot undo them.
/// The result and next checkpoint commit atomically. Bump the step version when
/// changing handler semantics. An uncertain commit must be reconciled by receipt.
type TransactionHandler = dyn Fn(&mut SqliteTaskTransaction<'_>, &StepContext<'_>) -> Result<Value, TaskError>
    + Send
    + Sync;
pub struct Step {
    version: StepVersion,
    handler: Option<Arc<TransactionHandler>>,
}
impl Step {
    /// Registered Tasks must be read-only/pure in this SQLite-only scope. Tasks
    /// owns retry/lease/worker lifecycle; output becomes the next step's payload.
    pub fn task(
        name: impl Into<String>,
        version: u32,
        task: impl Into<String>,
        next: Target,
    ) -> Self {
        Self {
            version: StepVersion {
                name: name.into(),
                version,
                kind: StepKind::Task {
                    task: task.into(),
                    next,
                },
            },
            handler: None,
        }
    }
    pub fn signal(name: impl Into<String>, version: u32, yes: Target, no: Target) -> Self {
        Self {
            version: StepVersion {
                name: name.into(),
                version,
                kind: StepKind::Signal { yes, no },
            },
            handler: None,
        }
    }
    pub fn transaction<F>(name: impl Into<String>, version: u32, next: Target, handler: F) -> Self
    where
        F: Fn(&mut SqliteTaskTransaction<'_>, &StepContext<'_>) -> Result<Value, TaskError>
            + Send
            + Sync
            + 'static,
    {
        Self {
            version: StepVersion {
                name: name.into(),
                version,
                kind: StepKind::Transaction { next },
            },
            handler: Some(Arc::new(handler)),
        }
    }
}
pub struct Definition {
    version: DefinitionVersion,
    steps: Vec<Step>,
}
impl Definition {
    pub fn new(name: impl Into<String>, version: u32, steps: Vec<Step>) -> Self {
        Self {
            version: DefinitionVersion {
                name: name.into(),
                version,
                steps: steps.iter().map(|s| s.version.clone()).collect(),
            },
            steps,
        }
    }
    fn validate(&self) -> Result<(), TaskError> {
        valid(&self.version.name, 64)?;
        if self.version.version == 0 || self.steps.is_empty() || self.steps.len() > MAX_STEPS {
            return Err(TaskError::InvalidOptions);
        }
        let mut names = HashSet::new();
        for (index, step) in self.steps.iter().enumerate() {
            valid(&step.version.name, 64)?;
            if step.version.version == 0 || !names.insert(&step.version.name) {
                return Err(TaskError::InvalidOptions);
            }
            let targets = match &step.version.kind {
                StepKind::Task { task, next } => {
                    valid(task, 256)?;
                    vec![next]
                }
                StepKind::Signal { yes, no } => vec![yes, no],
                StepKind::Transaction { next } => vec![next],
            };
            for target in targets {
                if matches!(target,Target::Step(n) if *n <= index || *n >= self.steps.len()) {
                    return Err(TaskError::InvalidOptions);
                }
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum State {
    Running,
    Failed,
    Waiting,
    Completed,
    Rejected,
    Cancelled,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Signal {
    pub step: String,
    pub version: u32,
    pub key: String,
    pub choice: bool,
    pub data: Value,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StepReceipt {
    pub step: String,
    pub version: u32,
    pub job: Option<i64>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Checkpoint {
    pub definition: DefinitionVersion,
    pub step: usize,
    pub state: State,
    pub job: Option<i64>,
    pub input: Value,
    pub payload: Value,
    pub signals: Vec<Signal>,
    pub completed: Vec<StepReceipt>,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    Applied,
    Duplicate,
    NotReady,
    Conflict,
    VersionMismatch,
    Terminal,
    Missing,
}
/// Stable business identity and data for a same-database transaction step.
pub struct StepContext<'a> {
    pub queue_identity: &'a str,
    pub workflow: &'a str,
    pub id: &'a str,
    pub step: &'a str,
    pub idempotency_key: &'a str,
    pub payload: &'a Value,
    pub signals: &'a [Signal],
}

pub struct WorkflowPlugin {
    tasks: Handle<Tasks>,
    definition: Definition,
}
impl WorkflowPlugin {
    pub fn new(tasks: Handle<Tasks>, definition: Definition) -> Self {
        Self { tasks, definition }
    }
}
impl Plugin for WorkflowPlugin {
    type Service = Workflow;
    fn dependencies(&self) -> Vec<InstanceKey> {
        vec![self.tasks.key()]
    }
    fn validate(&self) -> Result<(), BoxError> {
        Ok(self.definition.validate()?)
    }
    async fn setup(self, setup: &mut Setup) -> Result<Arc<Workflow>, BoxError> {
        Ok(Arc::new(Workflow {
            tasks: setup.get(&self.tasks)?,
            definition: Arc::new(self.definition),
        }))
    }
}
/// Owns no provider/worker. Tasks admits each operation and drains accepted work.
/// Multi-call operations can stop with Closed; persisted state remains resumable.
pub struct Workflow {
    tasks: Arc<Tasks>,
    definition: Arc<Definition>,
}
fn valid(s: &str, max: usize) -> Result<(), TaskError> {
    if s.is_empty() || s.len() > max || s.chars().any(char::is_control) {
        Err(TaskError::InvalidInput)
    } else {
        Ok(())
    }
}
fn payload(value: &Value) -> Result<(), TaskError> {
    fn depth(v: &Value, n: u8) -> bool {
        n > 0
            && match v {
                Value::Array(a) => a.iter().all(|v| depth(v, n - 1)),
                Value::Object(o) => o.values().all(|v| depth(v, n - 1)),
                _ => true,
            }
    }
    if depth(value, 32) && serde_json::to_vec(value).is_ok_and(|v| v.len() <= MAX_PAYLOAD) {
        Ok(())
    } else {
        Err(TaskError::InvalidInput)
    }
}
fn read(
    tx: &mut SqliteTaskTransaction<'_>,
    queue: &str,
    name: &str,
    id: &str,
) -> Result<Option<Checkpoint>, TaskError> {
    let text: Option<String> = tx.query_row("SELECT (SELECT checkpoint FROM lenso_workflow_run_v1 WHERE queue_identity=?1 AND definition=?2 AND id=?3)",params![queue,name,id],|r|r.get(0))?;
    text.map(|s| serde_json::from_str(&s).map_err(|_| TaskError::InvalidInput))
        .transpose()
}
fn save(
    tx: &mut SqliteTaskTransaction<'_>,
    queue: &str,
    id: &str,
    cp: &Checkpoint,
) -> Result<(), TaskError> {
    let data = serde_json::to_string(cp).map_err(|_| TaskError::InvalidInput)?;
    tx.execute("INSERT INTO lenso_workflow_run_v1 VALUES (?1,?2,?3,?4) ON CONFLICT(queue_identity,definition,id) DO UPDATE SET checkpoint=excluded.checkpoint",params![queue,cp.definition.name,id,data])?;
    Ok(())
}
fn receipt(cp: &mut Checkpoint) {
    let step = &cp.definition.steps[cp.step];
    cp.completed.push(StepReceipt {
        step: step.name.clone(),
        version: step.version,
        job: cp.job,
    });
}
fn enter(
    tx: &mut SqliteTaskTransaction<'_>,
    cp: &mut Checkpoint,
    target: &Target,
) -> Result<(), TaskError> {
    cp.job = None;
    cp.state = match target {
        Target::Complete => State::Completed,
        Target::Reject => State::Rejected,
        Target::Step(index) => {
            cp.step = *index;
            match &cp.definition.steps[*index].kind {
                StepKind::Task { task, .. } => {
                    // Checkpoint and enqueue commit together. No separate dedup key
                    // is needed: the serialized business ID is the receipt boundary.
                    cp.job =
                        Some(tx.enqueue(task, cp.payload.clone(), EnqueueOptions::default())?);
                    State::Running
                }
                StepKind::Signal { .. } => State::Waiting,
                StepKind::Transaction { .. } => State::Running,
            }
        }
    };
    Ok(())
}
impl Workflow {
    pub fn definition(&self) -> &DefinitionVersion {
        &self.definition.version
    }
    /// Explicit schema provisioning on the existing Tasks-owned connection.
    ///
    /// # Errors
    /// Returns Tasks admission, non-SQLite provider, SQL or transaction failures.
    pub async fn provision(&self) -> Result<(), TaskError> {
        self.tasks
            .sqlite_transaction(|tx| {
                tx.execute(SCHEMA, [])?;
                Ok(())
            })
            .await
    }
    /// # Errors
    /// Returns invalid ID/payload or Tasks transaction failures. Conflicting
    /// definitions and repeated starts are reported as [`Outcome`] values.
    pub async fn start(&self, id: String, input: Value) -> Result<Outcome, TaskError> {
        valid(&id, 128)?;
        payload(&input)?;
        let queue = self.tasks.identity().to_owned();
        let definition = self.definition.clone();
        self.tasks
            .sqlite_transaction(move |tx| {
                if let Some(cp) = read(tx, &queue, &definition.version.name, &id)? {
                    return Ok(if cp.definition != definition.version {
                        Outcome::VersionMismatch
                    } else if cp.input == input {
                        Outcome::Duplicate
                    } else {
                        Outcome::Conflict
                    });
                }
                let mut cp = Checkpoint {
                    definition: definition.version.clone(),
                    step: 0,
                    state: State::Running,
                    job: None,
                    input: input.clone(),
                    payload: input,
                    signals: vec![],
                    completed: vec![],
                };
                enter(tx, &mut cp, &Target::Step(0))?;
                save(tx, &queue, &id, &cp)?;
                Ok(Outcome::Applied)
            })
            .await
    }
    /// Receipt query after an unconfirmed COMMIT; missing is only a snapshot.
    /// # Errors
    /// Returns invalid ID/checkpoint encoding or Tasks transaction failures.
    pub async fn checkpoint(&self, id: String) -> Result<Option<Checkpoint>, TaskError> {
        valid(&id, 128)?;
        let queue = self.tasks.identity().to_owned();
        let name = self.definition.version.name.clone();
        self.tasks
            .sqlite_transaction(move |tx| read(tx, &queue, &name, &id))
            .await
    }
    /// Advance one step: reconcile a Tasks receipt or execute one same-DB
    /// transaction callback. Signals wait for signal(). No scheduler is created.
    /// # Errors
    /// Returns invalid input/checkpoint, transaction-handler or Tasks failures.
    /// Earlier steps may have committed; reconcile via the persisted checkpoint.
    pub async fn resume(&self, id: String) -> Result<Outcome, TaskError> {
        let Some(cp) = self.checkpoint(id.clone()).await? else {
            return Ok(Outcome::Missing);
        };
        if cp.definition != self.definition.version {
            return Ok(Outcome::VersionMismatch);
        }
        if !matches!(cp.state, State::Running | State::Failed | State::Waiting) {
            return Ok(Outcome::Terminal);
        }
        let step = self
            .definition
            .steps
            .get(cp.step)
            .ok_or(TaskError::InvalidInput)?;
        let job = match &step.version.kind {
            StepKind::Task { .. } => Some(
                self.tasks
                    .get(cp.job.ok_or(TaskError::InvalidInput)?)
                    .await?
                    .ok_or(TaskError::InvalidInput)?,
            ),
            StepKind::Signal { .. } => return Ok(Outcome::NotReady),
            StepKind::Transaction { .. } => None,
        };
        let queue = self.tasks.identity().to_owned();
        let definition = self.definition.clone();
        self.tasks
            .sqlite_transaction(move |tx| {
                let Some(mut current) = read(tx, &queue, &definition.version.name, &id)? else {
                    return Ok(Outcome::Missing);
                };
                if current != cp {
                    return Ok(Outcome::Conflict);
                }
                let step = &definition.steps[cp.step];
                let next = match &step.version.kind {
                    StepKind::Task { next, .. } => {
                        let job = job.ok_or(TaskError::InvalidInput)?;
                        match job.state {
                            JobState::Succeeded => {
                                current.payload = job.result.ok_or(TaskError::InvalidInput)?;
                                payload(&current.payload)?;
                            }
                            JobState::Failed => {
                                current.state = State::Failed;
                                save(tx, &queue, &id, &current)?;
                                return Ok(Outcome::Applied);
                            }
                            JobState::Cancelled => {
                                current.state = State::Cancelled;
                                save(tx, &queue, &id, &current)?;
                                return Ok(Outcome::Applied);
                            }
                            _ => return Ok(Outcome::NotReady),
                        }
                        next
                    }
                    StepKind::Transaction { next } => {
                        let key = serde_json::to_string(&(
                            &queue,
                            &definition.version.name,
                            &id,
                            &step.version.name,
                            step.version.version,
                        ))
                        .map_err(|_| TaskError::InvalidInput)?;
                        let context = StepContext {
                            queue_identity: &queue,
                            workflow: &definition.version.name,
                            id: &id,
                            step: &step.version.name,
                            idempotency_key: &key,
                            payload: &current.payload,
                            signals: &current.signals,
                        };
                        current.payload =
                            step.handler.as_ref().ok_or(TaskError::InvalidInput)?(tx, &context)?;
                        payload(&current.payload)?;
                        next
                    }
                    StepKind::Signal { .. } => return Ok(Outcome::NotReady),
                };
                receipt(&mut current);
                enter(tx, &mut current, next)?;
                save(tx, &queue, &id, &current)?;
                Ok(Outcome::Applied)
            })
            .await
    }
    /// Only explicitly failed Tasks qualify. Concurrent cancellation can leave a
    /// harmless read queued, but cannot advance the cancelled checkpoint.
    /// # Errors
    /// Returns invalid ID/checkpoint or Tasks failures. Job reopening and checkpoint
    /// progress can precede a later error; inspect state before retrying again.
    pub async fn retry(&self, id: String) -> Result<Outcome, TaskError> {
        let Some(cp) = self.checkpoint(id).await? else {
            return Ok(Outcome::Missing);
        };
        if cp.definition != self.definition.version {
            return Ok(Outcome::VersionMismatch);
        }
        if cp.state != State::Failed {
            return Ok(Outcome::NotReady);
        }
        Ok(
            if self
                .tasks
                .retry(cp.job.ok_or(TaskError::InvalidInput)?)
                .await?
            {
                Outcome::Applied
            } else {
                Outcome::NotReady
            },
        )
    }
    /// One immutable receipt per signal step. Out-of-order signals are not
    /// buffered. Hosts authenticate/authorize senders; keys are not credentials.
    /// # Errors
    /// Returns invalid signal/ID/payload or Tasks transaction failures. Duplicate,
    /// conflicting and out-of-order signals are represented by [`Outcome`].
    pub async fn signal(
        &self,
        id: String,
        flow_version: u32,
        signal: Signal,
    ) -> Result<Outcome, TaskError> {
        valid(&id, 128)?;
        valid(&signal.key, 128)?;
        valid(&signal.step, 64)?;
        payload(&signal.data)?;
        let queue = self.tasks.identity().to_owned();
        let definition = self.definition.clone();
        self.tasks
            .sqlite_transaction(move |tx| {
                let Some(mut cp) = read(tx, &queue, &definition.version.name, &id)? else {
                    return Ok(Outcome::Missing);
                };
                if cp.definition != definition.version || flow_version != definition.version.version
                {
                    return Ok(Outcome::VersionMismatch);
                }
                let Some(expected) = definition
                    .version
                    .steps
                    .iter()
                    .find(|s| s.name == signal.step)
                else {
                    return Ok(Outcome::Conflict);
                };
                if expected.version != signal.version {
                    return Ok(Outcome::VersionMismatch);
                }
                if let Some(prior) = cp.signals.iter().find(|s| s.step == signal.step) {
                    return Ok(if prior == &signal {
                        Outcome::Duplicate
                    } else {
                        Outcome::Conflict
                    });
                }
                if matches!(
                    cp.state,
                    State::Completed | State::Rejected | State::Cancelled
                ) {
                    return Ok(Outcome::Terminal);
                }
                if cp.state != State::Waiting
                    || definition.steps[cp.step].version.name != signal.step
                {
                    return Ok(Outcome::NotReady);
                }
                let StepKind::Signal { yes, no } = &expected.kind else {
                    return Ok(Outcome::Conflict);
                };
                let next = if signal.choice { yes } else { no };
                cp.signals.push(signal);
                receipt(&mut cp);
                enter(tx, &mut cp, next)?;
                save(tx, &queue, &id, &cp)?;
                Ok(Outcome::Applied)
            })
            .await
    }
    /// Cancel checkpoint first, then Tasks; repeat to repair a crash between calls.
    /// # Errors
    /// Returns invalid ID/checkpoint or Tasks failures. Cancellation never rolls
    /// back previously committed business steps.
    pub async fn cancel(&self, id: String) -> Result<Outcome, TaskError> {
        valid(&id, 128)?;
        let queue = self.tasks.identity().to_owned();
        let definition = self.definition.clone();
        let (outcome, job) = self
            .tasks
            .sqlite_transaction(move |tx| {
                let Some(mut cp) = read(tx, &queue, &definition.version.name, &id)? else {
                    return Ok((Outcome::Missing, None));
                };
                if cp.definition != definition.version {
                    return Ok((Outcome::VersionMismatch, None));
                }
                if matches!(cp.state, State::Completed | State::Rejected) {
                    return Ok((Outcome::Terminal, None));
                }
                let outcome = if cp.state == State::Cancelled {
                    Outcome::Duplicate
                } else {
                    Outcome::Applied
                };
                cp.state = State::Cancelled;
                save(tx, &queue, &id, &cp)?;
                Ok((outcome, cp.job))
            })
            .await?;
        if let Some(job) = job {
            self.tasks.cancel(job).await?;
        }
        Ok(outcome)
    }
    /// Completed transaction/task output is the generic workflow result receipt.
    /// # Errors
    /// Returns the checkpoint-read failures from [`Self::checkpoint`].
    pub async fn result(&self, id: String) -> Result<Option<Value>, TaskError> {
        Ok(self
            .checkpoint(id)
            .await?
            .filter(|cp| cp.state == State::Completed)
            .map(|cp| cp.payload))
    }
}
