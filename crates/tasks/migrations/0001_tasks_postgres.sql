CREATE TABLE IF NOT EXISTS lenso_task_queue (
    name TEXT PRIMARY KEY,
    identity TEXT NOT NULL UNIQUE DEFAULT gen_random_uuid()::text
);
CREATE TABLE IF NOT EXISTS lenso_task_job (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    queue TEXT NOT NULL REFERENCES lenso_task_queue(name),
    task TEXT NOT NULL,
    input TEXT NOT NULL,
    dedup_key TEXT,
    state TEXT NOT NULL DEFAULT 'pending' CHECK (state IN ('pending','running','succeeded','failed','cancelled')),
    attempt BIGINT NOT NULL DEFAULT 0 CHECK (attempt BETWEEN 0 AND 4294967295),
    max_attempts BIGINT NOT NULL CHECK (max_attempts BETWEEN 1 AND 4294967295),
    retry_delay_ms BIGINT NOT NULL CHECK (retry_delay_ms BETWEEN 0 AND 4294967295),
    retry_max_ms BIGINT NOT NULL CHECK (retry_max_ms BETWEEN retry_delay_ms AND 4294967295),
    retry_backoff BOOLEAN NOT NULL,
    run_at BIGINT NOT NULL,
    cancel_requested BOOLEAN NOT NULL DEFAULT false,
    token TEXT,
    lease_until BIGINT,
    expires_at BIGINT,
    result TEXT,
    last_error TEXT,
    UNIQUE(queue, dedup_key)
);
CREATE INDEX IF NOT EXISTS lenso_task_due ON lenso_task_job(queue, state, run_at, id);
CREATE INDEX IF NOT EXISTS lenso_task_lease ON lenso_task_job(queue, state, lease_until);
