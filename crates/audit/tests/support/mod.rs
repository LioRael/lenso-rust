#![allow(dead_code)]
use lenso_audit::*;
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc, Mutex,
};

pub fn scope() -> AuditScope {
    AuditScope {
        tenant_id: Some("tenant-a".into()),
        scope_id: "owner:alice".into(),
    }
}

pub fn id(n: u64) -> String {
    format!("00000000-0000-0000-0000-{n:012x}")
}

pub fn input(n: u64) -> AuditInput {
    AuditInput {
        id: id(n),
        occurred_at: 100,
        scope: scope(),
        action: "note.remove".into(),
        target: AuditTarget {
            kind: "note".into(),
            id: "note-1".into(),
        },
        result: AuditResult::Success,
        reason_code: "removed".into(),
        correlation_id: Some("request-1".into()),
        summary: Summary::from([("removed".into(), SummaryValue::Boolean(true))]),
        relation: None,
    }
}

pub fn intent(n: u64) -> AuditInput {
    AuditInput {
        result: AuditResult::Intent,
        reason_code: "requested".into(),
        summary: Summary::new(),
        ..input(n)
    }
}

pub fn outcome(n: u64) -> AuditOutcome {
    AuditOutcome {
        id: id(n),
        occurred_at: 300,
        result: OutcomeResult::Success,
        reason_code: "done".into(),
        summary: Summary::new(),
    }
}

pub fn config() -> AuditConfig {
    AuditConfig {
        summary_policy: SummaryPolicy::from([(
            "note.remove".into(),
            [
                ("removed".into(), SummaryRule::Boolean),
                ("count".into(), SummaryRule::Integer { min: 0, max: 10 }),
                (
                    "state".into(),
                    SummaryRule::Enum(vec!["ready".into(), "missing".into()]),
                ),
            ]
            .into(),
        )]),
        ..AuditConfig::default()
    }
}

#[derive(Default)]
pub struct Authority {
    pub revoked: AtomicBool,
    pub calls: Mutex<Vec<(AuditScope, AuditOperation)>>,
}
impl AuditAuthority for Authority {
    type Principal = str;
    async fn resolve(
        &self,
        principal: &str,
        requested: &AuditScope,
        operation: AuditOperation,
    ) -> Result<AuditActor, BoxError> {
        self.calls
            .lock()
            .unwrap()
            .push((requested.clone(), operation));
        if self.revoked.load(Ordering::SeqCst) || principal != "alice" || *requested != scope() {
            return Err("credential=must-not-escape".into());
        }
        Ok(AuditActor::Subject {
            kind: SubjectKind::User,
            realm_id: "https://issuer.example".into(),
            subject_id: "auth0|alice".into(),
        })
    }
}

pub struct TestRepository {
    pub memory: MemoryAuditRepository,
    pub durable: bool,
    pub fail_writes: AtomicBool,
    pub fail_after_commit: AtomicBool,
    pub calls: AtomicUsize,
}
impl TestRepository {
    pub fn new(durable: bool) -> Self {
        Self {
            memory: MemoryAuditRepository::new(100).unwrap(),
            durable,
            fail_writes: AtomicBool::new(false),
            fail_after_commit: AtomicBool::new(false),
            calls: AtomicUsize::new(0),
        }
    }
}
impl AuditRepository for TestRepository {
    fn durable_intents(&self) -> bool {
        self.durable
    }
    async fn insert(&self, event: AuditEvent) -> Result<InsertStatus, BoxError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if self.fail_writes.load(Ordering::SeqCst) {
            return Err("SQL password=must-not-escape".into());
        }
        let result = self.memory.insert(event).await?;
        if self.fail_after_commit.load(Ordering::SeqCst) {
            return Err("commit response lost".into());
        }
        Ok(result)
    }
    async fn get(&self, scope: &AuditScope, id: &str) -> Result<Option<AuditEvent>, BoxError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.memory.get(scope, id).await
    }
    async fn list(&self, query: &AuditQuery, limit: usize) -> Result<Vec<AuditEvent>, BoxError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.memory.list(query, limit).await
    }
}

#[derive(Default)]
pub struct Reporter {
    pub entries: Mutex<Vec<AuditDiagnostic>>,
    pub fail: AtomicBool,
}
impl AuditReporter for Reporter {
    fn report(&self, diagnostic: AuditDiagnostic) -> ReportFuture<'_> {
        Box::pin(async move {
            self.entries.lock().unwrap().push(diagnostic);
            if self.fail.load(Ordering::SeqCst) {
                return Err("logger token=must-not-escape".into());
            }
            Ok(())
        })
    }
}

pub struct Fixture {
    pub repository: Arc<TestRepository>,
    pub authority: Arc<Authority>,
    pub reporter: Arc<Reporter>,
    pub service: Arc<AuditService<TestRepository, Authority>>,
}
impl Fixture {
    pub fn new(durable: bool) -> Self {
        let repository = Arc::new(TestRepository::new(durable));
        let authority = Arc::new(Authority::default());
        let reporter = Arc::new(Reporter::default());
        let service = Arc::new(
            AuditService::new(repository.clone(), authority.clone(), config())
                .unwrap()
                .with_clock(|| 200)
                .with_reporter(reporter.clone()),
        );
        Self {
            repository,
            authority,
            reporter,
            service,
        }
    }
}
