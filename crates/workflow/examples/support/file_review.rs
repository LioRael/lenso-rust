use lenso_tasks::{Failure, RetryPolicy, Task, TaskError, Tasks};
use lenso_workflow::{Definition, Step, Target};
use serde::Deserialize;
use serde_json::json;
use std::io::Read;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Input {
    path: String,
}
pub fn processing_task() -> Task {
    Task::new(
        "file-review.process.v1",
        RetryPolicy::default(),
        |input: Input, _| async move {
            tokio::task::spawn_blocking(move || {
                let file = std::fs::File::open(input.path).map_err(|_| Failure::HandlerFailed)?;
                if !file
                    .metadata()
                    .map_err(|_| Failure::HandlerFailed)?
                    .is_file()
                {
                    return Err(Failure::InvalidInput);
                }
                let mut text = String::new();
                file.take(8193)
                    .read_to_string(&mut text)
                    .map_err(|_| Failure::InvalidInput)?;
                if text.len() > 8192 {
                    return Err(Failure::InvalidInput);
                }
                let result = json!({"content":text.to_uppercase()});
                if result.to_string().len() > 16_384 {
                    return Err(Failure::InvalidResult);
                }
                Ok(result)
            })
            .await
            .map_err(|_| Failure::HandlerFailed)?
        },
    )
}
pub async fn provision_business(tasks: &Tasks) -> Result<(), TaskError> {
    tasks.sqlite_transaction(|tx| {
        tx.execute("CREATE TABLE IF NOT EXISTS file_review_result (receipt_key TEXT PRIMARY KEY, content TEXT NOT NULL)",[])?;
        Ok(())
    }).await
}
pub fn definition() -> Definition {
    Definition::new(
        "file-review",
        1,
        vec![
            Step::task("process", 1, "file-review.process.v1", Target::Step(1)),
            Step::signal("approval", 1, Target::Step(2), Target::Reject),
            Step::transaction("publish", 1, Target::Complete, |tx, context| {
                let content = context.payload["content"]
                    .as_str()
                    .ok_or(TaskError::InvalidInput)?;
                tx.execute(
                    "INSERT INTO file_review_result VALUES (?1,?2)",
                    rusqlite::params![context.idempotency_key, content],
                )?;
                Ok(json!({"content":content,"receipt_key":context.idempotency_key}))
            }),
        ],
    )
}
