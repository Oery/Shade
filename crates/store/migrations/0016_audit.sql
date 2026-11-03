-- Observer records are best effort; tool_operations remains the durable ledger.
CREATE TABLE audit_events (
    id INTEGER PRIMARY KEY,
    thread_id TEXT NOT NULL REFERENCES threads (id),
    at INTEGER NOT NULL,
    kind TEXT NOT NULL,
    summary TEXT NOT NULL
) STRICT;

CREATE INDEX audit_events_thread ON audit_events (thread_id, at);
ALTER TABLE usage ADD COLUMN estimated_usd REAL;
