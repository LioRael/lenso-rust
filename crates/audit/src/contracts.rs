use std::{collections::BTreeMap, error::Error, fmt, future::Future, pin::Pin};

/// Adapter errors are discarded at the service boundary, including their source chain.
pub type BoxError = Box<dyn Error + Send + Sync + 'static>;
pub type ReportFuture<'a> = Pin<Box<dyn Future<Output = Result<(), BoxError>> + Send + 'a>>;

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "sqlite", derive(serde::Serialize, serde::Deserialize))]
pub struct AuditScope {
    pub tenant_id: Option<String>,
    pub scope_id: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "sqlite", derive(serde::Serialize, serde::Deserialize))]
pub enum SubjectKind {
    User,
    Guest,
    Service,
}

/// Identity snapshot supplied by a trusted authority, never by event input.
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "sqlite", derive(serde::Serialize, serde::Deserialize))]
pub enum AuditActor {
    Subject {
        kind: SubjectKind,
        realm_id: String,
        subject_id: String,
    },
    System {
        system_id: String,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "sqlite", derive(serde::Serialize, serde::Deserialize))]
pub enum AuditResult {
    Intent,
    Success,
    Failure,
    Denied,
    Unknown,
}

/// An outcome cannot itself be an intent.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OutcomeResult {
    Success,
    Failure,
    Denied,
    Unknown,
}

impl From<OutcomeResult> for AuditResult {
    fn from(value: OutcomeResult) -> Self {
        match value {
            OutcomeResult::Success => Self::Success,
            OutcomeResult::Failure => Self::Failure,
            OutcomeResult::Denied => Self::Denied,
            OutcomeResult::Unknown => Self::Unknown,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "sqlite", derive(serde::Serialize, serde::Deserialize))]
pub enum SummaryValue {
    Boolean(bool),
    Integer(i64),
    Enum(String),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SummaryRule {
    Boolean,
    Integer { min: i64, max: i64 },
    Enum(Vec<String>),
}

pub type Summary = BTreeMap<String, SummaryValue>;
pub type SummaryPolicy = BTreeMap<String, BTreeMap<String, SummaryRule>>;

#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "sqlite", derive(serde::Serialize, serde::Deserialize))]
pub struct AuditTarget {
    pub kind: String,
    pub id: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "sqlite", derive(serde::Serialize, serde::Deserialize))]
pub enum RelationKind {
    Correction,
    Outcome,
}

#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "sqlite", derive(serde::Serialize, serde::Deserialize))]
pub struct AuditRelation {
    pub kind: RelationKind,
    pub event_id: String,
}

/// Times are nonnegative Unix milliseconds; IDs are canonical lowercase UUIDs.
/// Empty summary is explicit, so arbitrary JSON cannot enter through this API.
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "sqlite", derive(serde::Serialize, serde::Deserialize))]
pub struct AuditInput {
    pub id: String,
    pub occurred_at: u64,
    pub scope: AuditScope,
    pub action: String,
    pub target: AuditTarget,
    pub result: AuditResult,
    pub reason_code: String,
    pub correlation_id: Option<String>,
    pub summary: Summary,
    pub relation: Option<AuditRelation>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "sqlite", derive(serde::Serialize, serde::Deserialize))]
pub struct AuditEvent {
    pub input: AuditInput,
    pub recorded_at: u64,
    pub actor: AuditActor,
}

impl AuditEvent {
    /// Recorded time is storage metadata, not idempotency content.
    pub fn same_content(&self, other: &Self) -> bool {
        self.input == other.input && self.actor == other.actor
    }

    pub fn cursor(&self) -> AuditCursor {
        AuditCursor {
            recorded_at: self.recorded_at,
            id: self.input.id.clone(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct AuditCursor {
    pub recorded_at: u64,
    pub id: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AuditQuery {
    pub scope: AuditScope,
    pub limit: Option<usize>,
    pub cursor: Option<AuditCursor>,
    pub action: Option<String>,
    pub target: Option<AuditTarget>,
    pub result: Option<AuditResult>,
    pub correlation_id: Option<String>,
    pub recorded_from: Option<u64>,
    pub recorded_to: Option<u64>,
}

impl AuditQuery {
    pub fn new(scope: AuditScope) -> Self {
        Self {
            scope,
            limit: None,
            cursor: None,
            action: None,
            target: None,
            result: None,
            correlation_id: None,
            recorded_from: None,
            recorded_to: None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AuditPage {
    pub events: Vec<AuditEvent>,
    pub next_cursor: Option<AuditCursor>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum InsertStatus {
    Inserted,
    Duplicate,
    Conflict,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AppendStatus {
    Inserted,
    Duplicate,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AppendResult {
    pub status: AppendStatus,
    pub event: AuditEvent,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BestEffortResult {
    Confirmed(Box<AppendResult>),
    /// The write may have committed; reconcile or retry with identical content.
    Unconfirmed,
}

#[derive(Debug)]
pub enum PrepareResult {
    Ready(crate::AuditIntentReceipt),
    /// This never grants permission to repeat the external effect.
    AlreadyRecorded {
        intent_id: String,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AuditOutcome {
    pub id: String,
    pub occurred_at: u64,
    pub result: OutcomeResult,
    pub reason_code: String,
    pub summary: Summary,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AuditOperation {
    Append,
    Query,
}

/// Trusted storage boundary. Implementations must be append-only and atomically
/// classify a (tenant, scope, id) insert. Duplicate compares all content except
/// recorded_at. Never replace existing content. No global lookup is exposed.
pub trait AuditRepository: Send + Sync + 'static {
    /// Deployment attestation of committed, durable acknowledgements. Default
    /// false. A true value is NOT inferred or verified by AuditService.
    fn durable_intents(&self) -> bool {
        false
    }

    fn insert(
        &self,
        event: AuditEvent,
    ) -> impl Future<Output = Result<InsertStatus, BoxError>> + Send;
    fn get(
        &self,
        scope: &AuditScope,
        id: &str,
    ) -> impl Future<Output = Result<Option<AuditEvent>, BoxError>> + Send;

    /// Return at most `limit` rows in descending (recorded_at, id) order, applying
    /// all filters and an exclusive cursor. `limit` overrides query.limit and
    /// may be max_page_size + 1 to detect a following page.
    fn list(
        &self,
        query: &AuditQuery,
        limit: usize,
    ) -> impl Future<Output = Result<Vec<AuditEvent>, BoxError>> + Send;
}

/// Authenticate/revalidate the principal, authorize the exact scope and operation,
/// then return an identity snapshot. This is not a business-effect permission.
pub trait AuditAuthority: Send + Sync + 'static {
    type Principal: Sync + ?Sized;

    fn resolve(
        &self,
        principal: &Self::Principal,
        scope: &AuditScope,
        operation: AuditOperation,
    ) -> impl Future<Output = Result<AuditActor, BoxError>> + Send;
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AuditMode {
    Strict,
    BestEffort,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AuditStage {
    Intent,
    Append,
    Outcome,
}

/// Only fixed, low-cardinality fields cross the diagnostic boundary.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AuditDiagnostic {
    pub mode: AuditMode,
    pub stage: AuditStage,
}

impl AuditDiagnostic {
    pub fn code(&self) -> &'static str {
        "storage-failed"
    }
}

/// Explicit reporter boundary; no logging/metrics runtime is installed implicitly.
pub trait AuditReporter: Send + Sync + 'static {
    fn report(&self, diagnostic: AuditDiagnostic) -> ReportFuture<'_>;
}

/// Default plugin type parameter; absence remains distinct from a silent reporter.
pub struct NoReporter;

impl AuditReporter for NoReporter {
    fn report(&self, _: AuditDiagnostic) -> ReportFuture<'_> {
        Box::pin(async { Ok(()) })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AuditConfig {
    pub summary_policy: SummaryPolicy,
    pub max_page_size: usize,
}

impl Default for AuditConfig {
    fn default() -> Self {
        Self {
            summary_policy: SummaryPolicy::new(),
            max_page_size: 100,
        }
    }
}

impl AuditConfig {
    /// Pure validation, also called before plugin setup acquires any resources.
    pub fn validate(&self) -> Result<(), AuditError> {
        crate::validation::config(self)
    }
}

/// Safe, fixed error codes. No underlying provider error or source is retained.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AuditError {
    InvalidInput,
    Unauthorized,
    DuplicateConflict,
    RelationMissing,
    StorageFailed,
    StrictUnavailable,
    InvalidReceipt,
    DiagnosticsFailed,
    DiagnosticsRequired,
    Closed,
    /// An external effect may have occurred; no rollback or retry is implied.
    OutcomeUnknown {
        intent_id: String,
        diagnostics_failed: bool,
    },
}

impl AuditError {
    pub fn code(&self) -> &'static str {
        match self {
            Self::InvalidInput => "invalid-input",
            Self::Unauthorized => "unauthorized",
            Self::DuplicateConflict => "duplicate-conflict",
            Self::RelationMissing => "relation-missing",
            Self::StorageFailed => "storage-failed",
            Self::StrictUnavailable => "strict-unavailable",
            Self::InvalidReceipt => "invalid-receipt",
            Self::DiagnosticsFailed => "diagnostics-failed",
            Self::DiagnosticsRequired => "diagnostics-required",
            Self::Closed => "closed",
            Self::OutcomeUnknown { .. } => "outcome-unknown",
        }
    }
}

impl fmt::Display for AuditError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Audit {}", self.code())
    }
}

impl Error for AuditError {}
