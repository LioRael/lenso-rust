CREATE TABLE IF NOT EXISTS lenso_task_queue (
    name TEXT PRIMARY KEY NOT NULL,
    identity TEXT NOT NULL UNIQUE DEFAULT (lower(hex(randomblob(16))))
);
CREATE TABLE IF NOT EXISTS lenso_task_job (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    queue TEXT NOT NULL REFERENCES lenso_task_queue(name),
    task TEXT NOT NULL,
    input TEXT NOT NULL,
    dedup_key TEXT,
    state TEXT NOT NULL DEFAULT 'pending' CHECK (state IN ('pending','running','succeeded','failed','cancelled')),
    attempt INTEGER NOT NULL DEFAULT 0,
    max_attempts INTEGER NOT NULL CHECK (max_attempts > 0),
    retry_delay_ms INTEGER NOT NULL,
    retry_max_ms INTEGER NOT NULL,
    retry_backoff INTEGER NOT NULL,
    run_at INTEGER NOT NULL,
    cancel_requested INTEGER NOT NULL DEFAULT 0,
    token TEXT,
    lease_until INTEGER,
    expires_at INTEGER,
    result TEXT,
    last_error TEXT,
    UNIQUE(queue, dedup_key)
);
CREATE INDEX IF NOT EXISTS lenso_task_due ON lenso_task_job(queue, state, run_at, id);
CREATE INDEX IF NOT EXISTS lenso_task_lease ON lenso_task_job(queue, state, lease_until);
