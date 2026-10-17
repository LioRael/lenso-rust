CREATE TABLE IF NOT EXISTS lenso_scheduler (
    name TEXT PRIMARY KEY,
    identity TEXT NOT NULL UNIQUE DEFAULT (lower(hex(randomblob(16)))),
    queue_identity TEXT NOT NULL,
    version INTEGER NOT NULL DEFAULT 1 CHECK (version = 1)
);
CREATE TABLE IF NOT EXISTS lenso_schedule (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    scheduler TEXT NOT NULL REFERENCES lenso_scheduler(name),
    key TEXT NOT NULL,
    definition TEXT NOT NULL,
    cursor INTEGER NOT NULL CHECK (cursor >= 0 AND cursor % 60000 = 0),
    UNIQUE (scheduler, key)
);
CREATE INDEX IF NOT EXISTS lenso_schedule_cursor ON lenso_schedule(scheduler, cursor, id);
CREATE TABLE IF NOT EXISTS lenso_schedule_fire (
    schedule_id INTEGER NOT NULL REFERENCES lenso_schedule(id),
    fire_at INTEGER NOT NULL CHECK (fire_at >= 0 AND fire_at % 60000 = 0),
    job_id INTEGER,
    attempts INTEGER NOT NULL DEFAULT 0,
    last_error TEXT CHECK (last_error IN ('unknown-task', 'invalid-input', 'conflict', 'queue-unavailable')),
    PRIMARY KEY (schedule_id, fire_at)
);
CREATE INDEX IF NOT EXISTS lenso_schedule_pending ON lenso_schedule_fire(attempts, fire_at, schedule_id) WHERE job_id IS NULL;
