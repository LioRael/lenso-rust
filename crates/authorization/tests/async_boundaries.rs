mod support;
use lenso_authorization::*;
use serde_json::json;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Mutex,
};
use support::*;
use tokio::sync::Semaphore;

struct Paused {
    entered: Arc<Semaphore>,
    release: Arc<Semaphore>,
    captured: Arc<Mutex<Option<Evaluation>>>,
    completed: Arc<AtomicUsize>,
}
impl Policy for Paused {
    fn evaluate<'a>(
        &'a self,
        facts: &'a Request,
        evaluation: &'a Evaluation,
    ) -> EvaluationFuture<'a, Effect> {
        Box::pin(async move {
            *self.captured.lock().unwrap() = Some(evaluation.clone());
            self.entered.add_permits(1);
            self.release.acquire().await.unwrap().forget();
            self.completed.fetch_add(1, Ordering::SeqCst);
            Ok(
                if facts.resource.attributes.get("state") == Some(&json!("open")) {
                    Effect::Allow
                } else {
                    Effect::Abstain
                },
            )
        })
    }
}
fn paused() -> Arc<Paused> {
    Arc::new(Paused {
        entered: Arc::new(Semaphore::new(0)),
        release: Arc::new(Semaphore::new(0)),
        captured: Arc::new(Mutex::new(None)),
        completed: Arc::new(AtomicUsize::new(0)),
    })
}

#[tokio::test]
async fn ownership_fixes_facts_before_first_poll_and_across_awaits() {
    let paused = paused();
    let clock_calls = Arc::new(AtomicUsize::new(0));
    let counter = clock_calls.clone();
    let mut config = options();
    config.clock = Arc::new(move || {
        counter.fetch_add(1, Ordering::SeqCst);
        Ok(100)
    });
    config.policies.push(paused.clone());
    let engine = Authorization::new(config).unwrap();
    let mut original = request();
    original
        .resource
        .attributes
        .insert("state".into(), json!("closed"));
    let future = engine.check(original.clone());
    original
        .resource
        .attributes
        .insert("state".into(), json!("open"));
    tokio::pin!(future);
    tokio::select! { _ = &mut future => panic!("must pause"), _ = paused.entered.acquire() => () }
    original.resource.scope.id = "changed-again".into();
    paused.release.add_permits(1);
    assert_eq!(future.await.code, ReasonCode::DefaultDeny);
    assert_eq!(clock_calls.load(Ordering::SeqCst), 1);
    assert!(paused
        .captured
        .lock()
        .unwrap()
        .as_ref()
        .unwrap()
        .cancellation
        .is_cancelled());
}

#[tokio::test(start_paused = true)]
async fn deadline_drops_pending_work_and_late_success_cannot_change_denial() {
    let paused = paused();
    let mut config = options();
    config.timeout = std::time::Duration::from_millis(5);
    config.policies.push(paused.clone());
    let engine = Authorization::new(config).unwrap();
    let decision = engine.check(request()).await;
    assert_eq!(decision.code, ReasonCode::Timeout);
    assert!(paused
        .captured
        .lock()
        .unwrap()
        .as_ref()
        .unwrap()
        .cancellation
        .is_cancelled());
    paused.release.add_permits(1);
    tokio::task::yield_now().await;
    assert_eq!(paused.completed.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn cancellation_before_during_and_same_poll_never_allows() {
    let paused = paused();
    let mut config = options();
    config.policies.push(paused.clone());
    let engine = Authorization::new(config).unwrap();
    let cancel = Cancellation::default();
    cancel.cancel();
    assert_eq!(
        engine.check_with_cancellation(request(), cancel).await.code,
        ReasonCode::Cancelled
    );
    assert!(paused.captured.lock().unwrap().is_none());
    let cancel = Cancellation::default();
    let task = tokio::spawn({
        let engine = engine.clone();
        let cancel = cancel.clone();
        async move { engine.check_with_cancellation(request(), cancel).await }
    });
    paused.entered.acquire().await.unwrap().forget();
    cancel.cancel();
    assert_eq!(task.await.unwrap().code, ReasonCode::Cancelled);
    assert!(paused
        .captured
        .lock()
        .unwrap()
        .as_ref()
        .unwrap()
        .cancellation
        .is_cancelled());
    let cancel = Cancellation::default();
    let captured = cancel.clone();
    let mut config = options();
    config.policies.push(policy(move |_, _| {
        captured.cancel();
        Ok(Effect::Allow)
    }));
    assert_eq!(
        Authorization::new(config)
            .unwrap()
            .check_with_cancellation(request(), cancel)
            .await
            .code,
        ReasonCode::Cancelled
    );
}

#[tokio::test]
async fn abandoning_check_cancels_only_that_evaluation() {
    let paused = paused();
    let mut config = options();
    config.policies.push(paused.clone());
    let engine = Authorization::new(config).unwrap();
    let task = tokio::spawn({
        let engine = engine.clone();
        async move { engine.check(request()).await }
    });
    paused.entered.acquire().await.unwrap().forget();
    let first = paused.captured.lock().unwrap().clone().unwrap();
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    assert!(first.cancellation.is_cancelled());
    paused.release.add_permits(1);
    assert!(engine.can(request()).await);
    assert_eq!(paused.completed.load(Ordering::SeqCst), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_checks_keep_principal_resource_and_context_separate() {
    struct Yielding;
    impl Policy for Yielding {
        fn evaluate<'a>(
            &'a self,
            facts: &'a Request,
            _: &'a Evaluation,
        ) -> EvaluationFuture<'a, Effect> {
            Box::pin(async move {
                tokio::task::yield_now().await;
                Ok(
                    if facts.principal.as_ref().unwrap().subject_id == facts.resource.id
                        && facts.context.get("approved") == Some(&json!(true))
                    {
                        Effect::Allow
                    } else {
                        Effect::Abstain
                    },
                )
            })
        }
    }
    let mut config = options();
    config.policies.push(Arc::new(Yielding));
    let engine = Authorization::new(config).unwrap();
    let mut tasks = vec![];
    for index in 0..32 {
        let engine = engine.clone();
        tasks.push(tokio::spawn(async move {
            let mut facts = request();
            facts.principal.as_mut().unwrap().subject_id = index.to_string();
            facts.resource.id = index.to_string();
            facts
                .context
                .insert("approved".into(), json!(index % 2 == 0));
            assert_eq!(engine.can(facts).await, index % 2 == 0);
        }));
    }
    for task in tasks {
        task.await.unwrap();
    }
}

#[tokio::test]
async fn snapshot_allow_does_not_authorize_a_later_changed_row() {
    let mut config = options();
    config.policies.push(policy(|facts, _| {
        Ok(
            if facts
                .resource
                .attributes
                .get("owner")
                .and_then(|v| v.as_str())
                == facts.principal.as_ref().map(|p| p.subject_id.as_str())
            {
                Effect::Allow
            } else {
                Effect::Abstain
            },
        )
    }));
    let engine = Authorization::new(config).unwrap();
    let row = Arc::new(Mutex::new((
        "alice".to_string(),
        1u64,
        "original".to_string(),
    )));
    let loaded = row.lock().unwrap().clone();
    let mut facts = request();
    facts
        .resource
        .attributes
        .insert("owner".into(), json!(loaded.0));
    assert!(engine.can(facts.clone()).await);
    let other = row.clone();
    tokio::spawn(async move {
        let mut row = other.lock().unwrap();
        row.0 = "bob".into();
        row.1 += 1;
    })
    .await
    .unwrap();
    let written = {
        let mut row = row.lock().unwrap();
        if row.0 == loaded.0 && row.1 == loaded.1 {
            row.2 = "edited".into();
            true
        } else {
            false
        }
    };
    assert!(!written);
    facts
        .resource
        .attributes
        .insert("owner".into(), json!(row.lock().unwrap().0));
    assert!(!engine.can(facts).await);
    assert_eq!(row.lock().unwrap().2, "original");
}

#[tokio::test]
async fn synchronous_callback_cannot_return_allow_after_deadline() {
    let mut config = options();
    config.timeout = std::time::Duration::from_millis(1);
    config.policies.push(policy(|_, _| {
        std::thread::sleep(std::time::Duration::from_millis(5));
        Ok(Effect::Allow)
    }));
    assert_eq!(
        Authorization::new(config)
            .unwrap()
            .check(request())
            .await
            .code,
        ReasonCode::Timeout
    );
}
