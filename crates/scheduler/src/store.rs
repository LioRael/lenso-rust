use crate::{
    calendar::{Calendar, MINUTE},
    DeliveryError, MissedPolicy, Occurrence, Schedule, ScheduleStatus, SchedulerError, TickOptions,
};
use rusqlite::{params, Connection, OpenFlags, OptionalExtension, TransactionBehavior};
use serde_json::Value;
use std::{path::Path, sync::Mutex, time::Duration};

pub(crate) struct Store {
    inner: Mutex<Option<Database>>,
}
pub(crate) struct Database {
    connection: Connection,
    name: String,
    identity: String,
}
pub(crate) struct Delivery {
    pub schedule_id: i64,
    pub fire_at: i64,
    pub key: String,
    pub task: String,
    pub input: Value,
}
fn configure(connection: &Connection) -> Result<(), SchedulerError> {
    connection.busy_timeout(Duration::from_secs(5))?;
    connection.pragma_update(None, "foreign_keys", true)?;
    Ok(())
}
pub(crate) fn provision(path: &Path, name: &str, queue: &str) -> Result<(), SchedulerError> {
    let mut connection = Connection::open(path)?;
    configure(&connection)?;
    connection.pragma_update(None, "journal_mode", "WAL")?;
    let tx = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
    tx.execute_batch(include_str!("../migrations/0001_scheduler.sql"))?;
    tx.execute("INSERT INTO lenso_scheduler(name, queue_identity) VALUES (?1, ?2) ON CONFLICT(name) DO NOTHING", params![name, queue])?;
    let existing: String = tx.query_row(
        "SELECT queue_identity FROM lenso_scheduler WHERE name = ?1 AND version = 1",
        [name],
        |r| r.get(0),
    )?;
    if existing != queue {
        return Err(SchedulerError::QueueMismatch);
    }
    tx.commit()?;
    Ok(())
}
impl Store {
    pub fn open(path: &Path, name: String, queue: &str) -> Result<Self, SchedulerError> {
        let connection = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_WRITE)?;
        configure(&connection)?;
        let (identity, binding): (String, String) = connection.query_row(
            "SELECT identity, queue_identity FROM lenso_scheduler WHERE name = ?1 AND version = 1",
            [&name],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )?;
        if binding != queue {
            return Err(SchedulerError::QueueMismatch);
        }
        connection
            .prepare("SELECT id, scheduler, key, definition, cursor FROM lenso_schedule LIMIT 0")?;
        connection.prepare("SELECT schedule_id, fire_at, job_id, attempts, last_error FROM lenso_schedule_fire LIMIT 0")?;
        Ok(Self {
            inner: Mutex::new(Some(Database {
                connection,
                name,
                identity,
            })),
        })
    }
    pub fn with<T>(
        &self,
        operation: impl FnOnce(&mut Database) -> Result<T, SchedulerError>,
    ) -> Result<T, SchedulerError> {
        let mut inner = self.inner.lock().map_err(|_| SchedulerError::Runtime)?;
        operation(inner.as_mut().ok_or(SchedulerError::Closed)?)
    }
    pub fn close(&self) -> Result<(), SchedulerError> {
        let mut inner = self.inner.lock().map_err(|_| SchedulerError::Runtime)?;
        if let Some(db) = inner.take() {
            if let Err((connection, error)) = db.connection.close() {
                *inner = Some(Database {
                    connection,
                    name: db.name,
                    identity: db.identity,
                });
                return Err(error.into());
            }
        }
        Ok(())
    }
}
fn definition(raw: &str) -> Result<Schedule, SchedulerError> {
    serde_json::from_str(raw).map_err(|_| SchedulerError::Runtime)
}
fn status(row: &rusqlite::Row<'_>) -> rusqlite::Result<ScheduleStatus> {
    Ok(ScheduleStatus {
        id: row.get(0)?,
        key: row.get(1)?,
        cursor: row.get(2)?,
    })
}
impl Database {
    pub fn create(&mut self, schedule: Schedule) -> Result<ScheduleStatus, SchedulerError> {
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let existing: Option<(ScheduleStatus, String)> = tx.query_row("SELECT id, key, cursor, definition FROM lenso_schedule WHERE scheduler = ?1 AND key = ?2", params![self.name, schedule.key], |r| Ok((status(r)?, r.get(3)?))).optional()?;
        if let Some((status, raw)) = existing {
            if definition(&raw)? != schedule {
                return Err(SchedulerError::ScheduleConflict);
            }
            return Ok(status);
        }
        let cursor = (schedule.start_after / MINUTE + 1) * MINUTE;
        let raw = serde_json::to_string(&schedule).map_err(|_| SchedulerError::Runtime)?;
        tx.execute("INSERT INTO lenso_schedule(scheduler, key, definition, cursor) VALUES (?1, ?2, ?3, ?4)", params![self.name, schedule.key, raw, cursor])?;
        let id = tx.last_insert_rowid();
        tx.commit()?;
        Ok(ScheduleStatus {
            id,
            key: schedule.key,
            cursor,
        })
    }
    pub fn get(&self, key: &str) -> Result<Option<ScheduleStatus>, SchedulerError> {
        Ok(self
            .connection
            .query_row(
                "SELECT id, key, cursor FROM lenso_schedule WHERE scheduler = ?1 AND key = ?2",
                params![self.name, key],
                status,
            )
            .optional()?)
    }
    pub fn stage(&mut self, now: i64, options: TickOptions) -> Result<u32, SchedulerError> {
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let plans = {
            let mut query = tx.prepare("SELECT id, definition, cursor FROM lenso_schedule WHERE scheduler = ?1 AND cursor <= ?2 ORDER BY cursor, id LIMIT ?3")?;
            let rows = query.query_map(params![self.name, now, options.max_schedules], |r| {
                Ok((
                    r.get::<_, i64>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, i64>(2)?,
                ))
            })?;
            rows.collect::<Result<Vec<_>, _>>()?
        };
        let mut staged = 0;
        for (id, raw, mut cursor) in plans {
            let plan = definition(&raw)?;
            let calendar = Calendar::parse(&plan.cron, &plan.timezone)?;
            if let MissedPolicy::Skip { grace_ms } = plan.missed {
                let earliest = now.saturating_sub(i64::from(grace_ms)).max(0);
                // Ceiling preserves the exact allowed boundary; no truncation of lateness.
                cursor = cursor.max(((earliest + MINUTE - 1) / MINUTE) * MINUTE);
            }
            for _ in 0..options.minutes_per_schedule {
                if cursor > now {
                    break;
                }
                if calendar.matches(cursor)? {
                    staged += tx.execute("INSERT INTO lenso_schedule_fire(schedule_id, fire_at) VALUES (?1, ?2) ON CONFLICT(schedule_id, fire_at) DO NOTHING", params![id, cursor])? as u32;
                }
                cursor += MINUTE;
            }
            tx.execute(
                "UPDATE lenso_schedule SET cursor = ?1 WHERE id = ?2",
                params![cursor, id],
            )?;
        }
        tx.commit()?;
        Ok(staged)
    }
    pub fn pending(&self, limit: u32) -> Result<Vec<Delivery>, SchedulerError> {
        let mut query = self.connection.prepare("SELECT f.schedule_id, f.fire_at, s.definition FROM lenso_schedule_fire f JOIN lenso_schedule s ON s.id = f.schedule_id WHERE s.scheduler = ?1 AND f.job_id IS NULL ORDER BY f.attempts, f.fire_at, f.schedule_id LIMIT ?2")?;
        let rows = query.query_map(params![self.name, limit], |r| {
            Ok((
                r.get::<_, i64>(0)?,
                r.get::<_, i64>(1)?,
                r.get::<_, String>(2)?,
            ))
        })?;
        rows.map(|row| {
            let (schedule_id, fire_at, raw) = row?;
            let plan = definition(&raw)?;
            Ok(Delivery {
                schedule_id,
                fire_at,
                key: format!(
                    "lenso:scheduler:v1:{}:{schedule_id}:{fire_at}",
                    self.identity
                ),
                task: plan.task,
                input: plan.input,
            })
        })
        .collect()
    }
    pub fn acknowledge(
        &self,
        id: i64,
        fire_at: i64,
        result: Result<i64, DeliveryError>,
    ) -> Result<(), SchedulerError> {
        let (job, error) = match result {
            Ok(job) => (Some(job), None),
            Err(error) => (None, Some(error.code())),
        };
        // An unsuccessful concurrent delivery must never undo another process' acknowledgement.
        self.connection.execute("UPDATE lenso_schedule_fire SET job_id = ?1, last_error = ?2, attempts = MIN(attempts + 1, 2147483647) WHERE schedule_id = ?3 AND fire_at = ?4 AND job_id IS NULL AND schedule_id IN (SELECT id FROM lenso_schedule WHERE scheduler = ?5)", params![job, error, id, fire_at, self.name])?;
        Ok(())
    }
    pub fn occurrences(
        &self,
        id: i64,
        after: i64,
        limit: u32,
    ) -> Result<Vec<Occurrence>, SchedulerError> {
        let mut query = self.connection.prepare("SELECT f.fire_at, f.job_id, f.last_error FROM lenso_schedule_fire f JOIN lenso_schedule s ON s.id = f.schedule_id WHERE s.scheduler = ?1 AND f.schedule_id = ?2 AND f.fire_at > ?3 ORDER BY f.fire_at LIMIT ?4")?;
        let rows = query.query_map(params![self.name, id, after, limit], |r| {
            Ok((
                r.get::<_, i64>(0)?,
                r.get::<_, Option<i64>>(1)?,
                r.get::<_, Option<String>>(2)?,
            ))
        })?;
        rows.map(|row| {
            let (fire_at, job_id, last_error) = row?;
            Ok(Occurrence {
                schedule_id: id,
                fire_at,
                job_id,
                last_error: last_error.map(|e| DeliveryError::parse(&e)).transpose()?,
            })
        })
        .collect()
    }
    pub fn more(&self, now: i64) -> Result<bool, SchedulerError> {
        Ok(self.connection.query_row("SELECT EXISTS(SELECT 1 FROM lenso_schedule WHERE scheduler = ?1 AND cursor <= ?2) OR EXISTS(SELECT 1 FROM lenso_schedule_fire f JOIN lenso_schedule s ON s.id = f.schedule_id WHERE s.scheduler = ?1 AND f.job_id IS NULL)", params![self.name, now], |r| r.get(0))?)
    }
}
