use crate::{validation, *};
use std::{
    fmt,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::{SystemTime, UNIX_EPOCH},
};

/// In-process, instance-bound receipt. Private fields prevent forging; cloning
/// preserves its identity. It retains only its own intent, not a service history.
/// It is not serializable and grants no permission to perform a business effect.
#[derive(Clone)]
pub struct AuditIntentReceipt {
    owner: Arc<()>,
    intent: Arc<AuditEvent>,
}

impl AuditIntentReceipt {
    pub fn intent_id(&self) -> &str {
        &self.intent.input.id
    }
}

impl fmt::Debug for AuditIntentReceipt {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AuditIntentReceipt")
            .field("intent_id", &self.intent_id())
            .finish_non_exhaustive()
    }
}

/// Ordinary async service over exact injected instances. No task spawning,
/// transport, DI, permissions container, or global admission gate is created.
pub struct AuditService<R: AuditRepository, A: AuditAuthority> {
    repository: Arc<R>,
    authority: Arc<A>,
    config: AuditConfig,
    reporter: Option<Arc<dyn AuditReporter>>,
    clock: Arc<dyn Fn() -> u64 + Send + Sync>,
    receipt_owner: Arc<()>,
    closed: AtomicBool,
}

impl<R: AuditRepository, A: AuditAuthority> AuditService<R, A> {
    pub fn new(
        repository: Arc<R>,
        authority: Arc<A>,
        config: AuditConfig,
    ) -> Result<Self, AuditError> {
        config.validate()?;
        Ok(Self {
            repository,
            authority,
            config,
            reporter: None,
            clock: Arc::new(|| {
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .ok()
                    .and_then(|time| u64::try_from(time.as_millis()).ok())
                    .unwrap_or(u64::MAX)
            }),
            receipt_owner: Arc::new(()),
            closed: AtomicBool::new(false),
        })
    }

    pub fn with_reporter(mut self, reporter: Arc<dyn AuditReporter>) -> Self {
        self.reporter = Some(reporter);
        self
    }

    /// Trusted clock returning Unix milliseconds, checked before persistence.
    pub fn with_clock(mut self, clock: impl Fn() -> u64 + Send + Sync + 'static) -> Self {
        self.clock = Arc::new(clock);
        self
    }

    #[cfg(feature = "plugin")]
    pub(crate) fn close(&self) {
        self.closed.store(true, Ordering::Release);
    }

    fn ensure_open(&self) -> Result<(), AuditError> {
        if self.closed.load(Ordering::Acquire) {
            Err(AuditError::Closed)
        } else {
            Ok(())
        }
    }

    async fn identity(
        &self,
        principal: &A::Principal,
        scope: &AuditScope,
        operation: AuditOperation,
    ) -> Result<AuditActor, AuditError> {
        let actor = self
            .authority
            .resolve(principal, scope, operation)
            .await
            .map_err(|_| AuditError::Unauthorized)?;
        validation::actor(&actor).map_err(|_| AuditError::Unauthorized)?;
        Ok(actor)
    }

    async fn diagnostic(&self, mode: AuditMode, stage: AuditStage) -> Result<(), AuditError> {
        if let Some(reporter) = &self.reporter {
            reporter
                .report(AuditDiagnostic { mode, stage })
                .await
                .map_err(|_| AuditError::DiagnosticsFailed)?;
        }
        Ok(())
    }

    async fn make(
        &self,
        input: AuditInput,
        principal: &A::Principal,
    ) -> Result<AuditEvent, AuditError> {
        validation::input(&input, &self.config.summary_policy)?;
        let actor = self
            .identity(principal, &input.scope, AuditOperation::Append)
            .await?;
        let recorded_at = (self.clock)();
        validation::time(recorded_at)?;
        Ok(AuditEvent {
            input,
            recorded_at,
            actor,
        })
    }

    async fn load(&self, scope: &AuditScope, id: &str) -> Result<Option<AuditEvent>, AuditError> {
        let event = self
            .repository
            .get(scope, id)
            .await
            .map_err(|_| AuditError::StorageFailed)?;
        if event
            .as_ref()
            .is_some_and(|v| v.input.scope != *scope || v.input.id != id)
        {
            return Err(AuditError::StorageFailed);
        }
        Ok(event)
    }

    async fn persist(&self, event: AuditEvent) -> Result<AppendResult, AuditError> {
        if let Some(relation) = &event.input.relation {
            let previous = self
                .load(&event.input.scope, &relation.event_id)
                .await?
                .ok_or(AuditError::RelationMissing)?;
            if relation.kind == RelationKind::Outcome
                && (previous.input.result != AuditResult::Intent
                    || event.input.result == AuditResult::Intent
                    || previous.input.action != event.input.action
                    || previous.input.target != event.input.target)
            {
                return Err(AuditError::InvalidInput);
            }
        }
        match self
            .repository
            .insert(event.clone())
            .await
            .map_err(|_| AuditError::StorageFailed)?
        {
            InsertStatus::Conflict => Err(AuditError::DuplicateConflict),
            InsertStatus::Inserted => Ok(AppendResult {
                status: AppendStatus::Inserted,
                event,
            }),
            InsertStatus::Duplicate => {
                let stored = self
                    .load(&event.input.scope, &event.input.id)
                    .await?
                    .ok_or(AuditError::StorageFailed)?;
                // Fail closed if an adapter misclassifies a conflict as a duplicate.
                if !stored.same_content(&event) {
                    return Err(AuditError::StorageFailed);
                }
                Ok(AppendResult {
                    status: AppendStatus::Duplicate,
                    event: stored,
                })
            }
        }
    }

    pub async fn append(
        &self,
        input: AuditInput,
        principal: &A::Principal,
    ) -> Result<AppendResult, AuditError> {
        self.ensure_open()?;
        self.persist(self.make(input, principal).await?).await
    }

    /// Only storage failure becomes Unconfirmed. Validation, authorization,
    /// conflicts, relations, and reporter failures remain errors.
    pub async fn append_best_effort(
        &self,
        input: AuditInput,
        principal: &A::Principal,
    ) -> Result<BestEffortResult, AuditError> {
        self.ensure_open()?;
        if self.reporter.is_none() {
            return Err(AuditError::DiagnosticsRequired);
        }
        let event = self.make(input, principal).await?;
        match self.persist(event).await {
            Ok(result) => Ok(BestEffortResult::Confirmed(Box::new(result))),
            Err(AuditError::StorageFailed) => {
                self.diagnostic(AuditMode::BestEffort, AuditStage::Append)
                    .await?;
                Ok(BestEffortResult::Unconfirmed)
            }
            Err(error) => Err(error),
        }
    }

    pub async fn get(
        &self,
        scope: &AuditScope,
        id: &str,
        principal: &A::Principal,
    ) -> Result<Option<AuditEvent>, AuditError> {
        self.ensure_open()?;
        validation::scope(scope)?;
        validation::uuid(id)?;
        self.identity(principal, scope, AuditOperation::Query)
            .await?;
        self.load(scope, id).await
    }

    pub async fn query(
        &self,
        query: AuditQuery,
        principal: &A::Principal,
    ) -> Result<AuditPage, AuditError> {
        self.ensure_open()?;
        let limit = validation::query(&query, self.config.max_page_size)?;
        self.identity(principal, &query.scope, AuditOperation::Query)
            .await?;
        let mut rows = self
            .repository
            .list(&query, limit + 1)
            .await
            .map_err(|_| AuditError::StorageFailed)?;
        if rows.len() > limit + 1
            || rows
                .iter()
                .any(|event| !validation::matches_query(event, &query))
            || rows.windows(2).any(|pair| {
                (pair[0].recorded_at, &pair[0].input.id) <= (pair[1].recorded_at, &pair[1].input.id)
            })
        {
            return Err(AuditError::StorageFailed);
        }
        let more = rows.len() > limit;
        rows.truncate(limit);
        let next_cursor = if more {
            rows.last().map(AuditEvent::cursor)
        } else {
            None
        };
        Ok(AuditPage {
            events: rows,
            next_cursor,
        })
    }

    /// Ready means this call newly persisted the intent. A duplicate, failed,
    /// cancelled, or lost acknowledgement never grants permission for the effect.
    pub async fn prepare(
        &self,
        input: AuditInput,
        principal: &A::Principal,
    ) -> Result<PrepareResult, AuditError> {
        self.ensure_open()?;
        if !self.repository.durable_intents() {
            return Err(AuditError::StrictUnavailable);
        }
        let event = self.make(input, principal).await?;
        if event.input.result != AuditResult::Intent || event.input.relation.is_some() {
            return Err(AuditError::InvalidInput);
        }
        let result = match self.persist(event).await {
            Ok(result) => result,
            Err(AuditError::StorageFailed) => {
                self.diagnostic(AuditMode::Strict, AuditStage::Intent)
                    .await?;
                return Err(AuditError::StorageFailed);
            }
            Err(error) => return Err(error),
        };
        match result.status {
            AppendStatus::Duplicate => Ok(PrepareResult::AlreadyRecorded {
                intent_id: result.event.input.id,
            }),
            AppendStatus::Inserted => Ok(PrepareResult::Ready(AuditIntentReceipt {
                owner: self.receipt_owner.clone(),
                intent: Arc::new(result.event),
            })),
        }
    }

    /// Records with the trusted pre-effect identity snapshot, even after principal
    /// revocation. Retry the same outcome ID/content to resolve a lost response.
    /// Different IDs can record additional outcomes, just like explicit append.
    pub async fn complete(
        &self,
        receipt: &AuditIntentReceipt,
        outcome: AuditOutcome,
    ) -> Result<AuditEvent, AuditError> {
        if !Arc::ptr_eq(&receipt.owner, &self.receipt_owner) {
            return Err(AuditError::InvalidReceipt);
        }
        let result = self.complete_inner(&receipt.intent, outcome).await;
        match result {
            Ok(event) => Ok(event),
            // Provider lifecycles have ended; do not touch even the reporter.
            Err(AuditError::Closed) => Err(AuditError::Closed),
            Err(_) => {
                let diagnostics_failed = self
                    .diagnostic(AuditMode::Strict, AuditStage::Outcome)
                    .await
                    .is_err();
                Err(AuditError::OutcomeUnknown {
                    intent_id: receipt.intent_id().to_owned(),
                    diagnostics_failed,
                })
            }
        }
    }

    async fn complete_inner(
        &self,
        intent: &AuditEvent,
        outcome: AuditOutcome,
    ) -> Result<AuditEvent, AuditError> {
        self.ensure_open()?;
        let input = AuditInput {
            id: outcome.id,
            occurred_at: outcome.occurred_at,
            scope: intent.input.scope.clone(),
            action: intent.input.action.clone(),
            target: intent.input.target.clone(),
            result: outcome.result.into(),
            reason_code: outcome.reason_code,
            correlation_id: intent.input.correlation_id.clone(),
            summary: outcome.summary,
            relation: Some(AuditRelation {
                kind: RelationKind::Outcome,
                event_id: intent.input.id.clone(),
            }),
        };
        validation::input(&input, &self.config.summary_policy)?;
        let recorded_at = (self.clock)();
        validation::time(recorded_at)?;
        Ok(self
            .persist(AuditEvent {
                input,
                recorded_at,
                actor: intent.actor.clone(),
            })
            .await?
            .event)
    }
}
