CREATE TABLE IF NOT EXISTS uq_clock(id INTEGER PRIMARY KEY CHECK(id=1), last_at INTEGER NOT NULL);
INSERT OR IGNORE INTO uq_clock VALUES(1, 0);
CREATE TABLE IF NOT EXISTS uq_quotas(
 scope TEXT PRIMARY KEY, capacity INTEGER NOT NULL CHECK(capacity>=0),
 used INTEGER NOT NULL DEFAULT 0 CHECK(used>=0),
 held INTEGER NOT NULL DEFAULT 0 CHECK(held>=0),
 CHECK(used<=capacity AND held<=capacity-used)
);
CREATE TABLE IF NOT EXISTS uq_reservations(
 id TEXT PRIMARY KEY, scope TEXT NOT NULL REFERENCES uq_quotas(scope),
 remaining INTEGER NOT NULL CHECK(remaining>=0), expires_at INTEGER NOT NULL CHECK(expires_at>=0)
);
CREATE INDEX IF NOT EXISTS uq_reservation_expiry ON uq_reservations(expires_at) WHERE remaining>0;
CREATE TABLE IF NOT EXISTS uq_operations(seq INTEGER PRIMARY KEY AUTOINCREMENT, operation TEXT UNIQUE NOT NULL, record TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS uq_expirations(reservation TEXT PRIMARY KEY REFERENCES uq_reservations(id), expired_at INTEGER NOT NULL, units INTEGER NOT NULL CHECK(units>0));
CREATE TABLE IF NOT EXISTS uq_api_calls(scope TEXT NOT NULL REFERENCES uq_quotas(scope), request TEXT NOT NULL, units INTEGER NOT NULL CHECK(units>0), PRIMARY KEY(scope, request));
