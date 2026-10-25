use crate::{prepare_enqueue, store::sqlite, EnqueueOptions, Task, TaskError, Tasks};
use rusqlite::{
    hooks::{AuthAction, AuthContext, Authorization},
    Params, Row, Transaction, TransactionBehavior,
};
use serde_json::Value;
use std::{
    collections::BTreeMap,
    panic::{catch_unwind, AssertUnwindSafe},
};

/// Scoped, exclusive borrow of the Tasks-owned SQLite transaction. Values can
/// escape; the connection/transaction cannot. SQL is trusted application code.
///
/// ```compile_fail
/// # async fn cannot_escape(tasks: &lenso_tasks::Tasks) {
/// let borrowed_scope = tasks.sqlite_transaction(|tx| Ok(tx)).await;
/// # }
/// ```
pub struct SqliteTaskTransaction<'a> {
    transaction: &'a Transaction<'a>,
    queue: &'a str,
    tasks: &'a BTreeMap<String, Task>,
    failed: bool,
}
impl SqliteTaskTransaction<'_> {
    /// Single parameterized business statement; transaction control is denied.
    ///
    /// # Errors
    /// Returns SQL errors or `TransactionAborted` after an earlier failure.
    /// Any statement error prevents the outer transaction from committing.
    pub fn execute(&mut self, sql: &str, params: impl Params) -> Result<usize, TaskError> {
        if self.failed {
            return Err(TaskError::TransactionAborted);
        }
        let result = self
            .transaction
            .execute(sql, params)
            .map_err(TaskError::from);
        self.record(result)
    }
    /// # Errors
    /// Returns SQL, missing-row or mapping errors, or `TransactionAborted` after
    /// an earlier failure. A query error also prevents outer commit.
    pub fn query_row<T>(
        &mut self,
        sql: &str,
        params: impl Params,
        map: impl FnOnce(&Row<'_>) -> rusqlite::Result<T>,
    ) -> Result<T, TaskError> {
        if self.failed {
            return Err(TaskError::TransactionAborted);
        }
        let result = self
            .transaction
            .query_row(sql, params, map)
            .map_err(TaskError::from);
        self.record(result)
    }
    /// Staged ID, confirmed only when the outer sqlite_transaction returns Ok.
    ///
    /// # Errors
    /// Returns validation, unknown-task, deduplication or SQL errors. Any failure
    /// marks the entire transaction aborted, even if the caller catches it.
    pub fn enqueue(
        &mut self,
        task: &str,
        input: Value,
        options: EnqueueOptions,
    ) -> Result<i64, TaskError> {
        if self.failed {
            return Err(TaskError::TransactionAborted);
        }
        let result = prepare_enqueue(self.tasks, task, &input, &options).and_then(|definition| {
            sqlite::enqueue_in_transaction(
                self.transaction,
                self.queue,
                &definition,
                &input,
                &options,
            )
        });
        self.record(result)
    }
    fn record<T>(&mut self, result: Result<T, TaskError>) -> Result<T, TaskError> {
        if result.is_err() {
            self.failed = true;
        }
        result
    }
}
impl Tasks {
    /// One synchronous callback on the blocking pool, using this provider's one
    /// startup-owned connection. No nested/foreign transaction or automatic replay.
    /// Do not call back into Tasks or perform external effects from the callback.
    ///
    /// # Errors
    /// Returns admission/provider errors, `SqliteRequired` for other backends,
    /// callback failures, or rollback/commit failures. `CommitUnconfirmed` requires
    /// reconciliation; do not automatically repeat external business effects.
    pub async fn sqlite_transaction<R, F>(&self, operation: F) -> Result<R, TaskError>
    where
        R: Send + 'static,
        F: FnOnce(&mut SqliteTaskTransaction<'_>) -> Result<R, TaskError> + Send + 'static,
    {
        let tasks = self.tasks.clone();
        self.admitted(move |db| {
            let db = db.sqlite().ok_or(TaskError::SqliteRequired)?;
            db.transaction(&tasks, operation)
        })
        .await
    }
}
impl sqlite::SqliteConnection {
    pub(crate) fn transaction<R>(
        &mut self,
        tasks: &BTreeMap<String, Task>,
        operation: impl FnOnce(&mut SqliteTaskTransaction<'_>) -> Result<R, TaskError>,
    ) -> Result<R, TaskError> {
        if !self.connection.is_autocommit() {
            return Err(TaskError::TransactionAborted);
        }
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        transaction.authorizer(Some(|context: AuthContext<'_>| match context.action {
            AuthAction::Transaction { .. }
            | AuthAction::Savepoint { .. }
            | AuthAction::Attach { .. }
            | AuthAction::Detach { .. }
            | AuthAction::Pragma { .. } => Authorization::Deny,
            _ => Authorization::Allow,
        }))?;
        let mut scope = SqliteTaskTransaction {
            transaction: &transaction,
            queue: &self.queue,
            tasks,
            failed: false,
        };
        let result = catch_unwind(AssertUnwindSafe(|| operation(&mut scope)))
            .unwrap_or(Err(TaskError::Runtime));
        let failed = scope.failed;
        transaction.authorizer(None::<fn(AuthContext<'_>) -> Authorization>)?;
        let result = result.and_then(|value| {
            if failed {
                Err(TaskError::TransactionAborted)
            } else {
                Ok(value)
            }
        });
        match result {
            Ok(value) => {
                transaction.commit().map_err(TaskError::CommitUnconfirmed)?;
                Ok(value)
            }
            Err(source) => match transaction.rollback() {
                Ok(()) => Err(source),
                Err(rollback) => Err(TaskError::RollbackFailed {
                    source: Box::new(source),
                    rollback,
                }),
            },
        }
    }
}
