mod support;
use lenso_audit::*;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use support::*;
use tokio::sync::Semaphore;

// Protocol fault injection only. The production memory adapter never attests durability.
struct CommitPauseRepository {
    memory: MemoryAuditRepository,
    pause: AtomicBool,
    committed: Semaphore,
    response: Semaphore,
}
impl AuditRepository for CommitPauseRepository {
    fn durable_intents(&self) -> bool {
        true
    }
    async fn insert(&self, event: AuditEvent) -> Result<InsertStatus, BoxError> {
        let result = self.memory.insert(event).await?;
        if self.pause.load(Ordering::SeqCst) {
            self.committed.add_permits(1);
            self.response.acquire().await.unwrap().forget();
        }
        Ok(result)
    }
    async fn get(&self, scope: &AuditScope, id: &str) -> Result<Option<AuditEvent>, BoxError> {
        self.memory.get(scope, id).await
    }
    async fn list(&self, query: &AuditQuery, limit: usize) -> Result<Vec<AuditEvent>, BoxError> {
        self.memory.list(query, limit).await
    }
}

#[tokio::test]
async fn cancelling_after_commit_cannot_issue_a_new_ready_receipt_on_retry() {
    let repo = Arc::new(CommitPauseRepository {
        memory: MemoryAuditRepository::new(10).unwrap(),
        pause: AtomicBool::new(true),
        committed: Semaphore::new(0),
        response: Semaphore::new(0),
    });
    let audit = Arc::new(
        AuditService::new(repo.clone(), Arc::new(Authority::default()), config())
            .unwrap()
            .with_clock(|| 200),
    );
    let task = tokio::spawn({
        let audit = audit.clone();
        async move { audit.prepare(intent(1), "alice").await }
    });
    repo.committed.acquire().await.unwrap().forget();
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    repo.pause.store(false, Ordering::SeqCst);
    assert!(
        matches!(audit.prepare(intent(1), "alice").await.unwrap(), PrepareResult::AlreadyRecorded { intent_id } if intent_id == id(1))
    );
    assert_eq!(
        repo.memory
            .list(&AuditQuery::new(scope()), 10)
            .await
            .unwrap()
            .len(),
        1
    );
}
