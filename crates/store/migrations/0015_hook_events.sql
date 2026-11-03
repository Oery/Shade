-- Webhook events accepted for a run. A row is written before the sender is
-- answered `202`; the run starts later, within the background capacity
-- shared with scheduled runs, under a fresh root with the hook's grant.

CREATE TABLE hook_events (
    id TEXT PRIMARY KEY,
    hook TEXT NOT NULL,
    -- The provider's delivery ID, from the hook's configured header. A
    -- repeated ID gets the earlier acceptance. NULL when none was sent.
    delivery_id TEXT,
    -- The run's input: the hook's template with the request body.
    input TEXT NOT NULL,
    received_at INTEGER NOT NULL DEFAULT (unixepoch()),
    -- 'queued', 'running', 'completed', 'failed', 'limit_exceeded', or 'interrupted'.
    status TEXT NOT NULL DEFAULT 'queued',
    root_id TEXT REFERENCES threads (id),
    started_at INTEGER,
    finished_at INTEGER,
    error TEXT,
    UNIQUE (hook, delivery_id)
) STRICT;

CREATE INDEX hook_events_active ON hook_events (status) WHERE status IN ('queued', 'running');
CREATE INDEX hook_events_root ON hook_events (root_id);
