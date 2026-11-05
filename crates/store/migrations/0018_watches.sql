-- Watches on MCP resources, and what they fired.
--
-- A watch's last_condition is its baseline: NULL until the first successful
-- observation, and cleared when what it watches changes. Recording an
-- observation and queuing what it fired is one transaction.

CREATE TABLE watches (
    id TEXT PRIMARY KEY,
    title TEXT NOT NULL,
    server TEXT NOT NULL,
    uri TEXT NOT NULL,
    -- A typed condition, as JSON.
    condition TEXT NOT NULL,
    -- 'on_change' or 'every_match'.
    mode TEXT NOT NULL,
    cooldown_seconds INTEGER NOT NULL,
    -- Oldest acceptable observation. NULL when the resource has no freshness envelope.
    max_age_seconds INTEGER,
    prompt TEXT NOT NULL,
    -- JSON array of capabilities delegated to each run.
    caps TEXT NOT NULL,
    -- 'run' starts a fresh root; 'conversation' posts into creator_root, once.
    destination TEXT NOT NULL,
    creator_root TEXT REFERENCES threads (id),
    version INTEGER NOT NULL DEFAULT 1,
    enabled INTEGER NOT NULL DEFAULT 1,
    last_condition INTEGER,
    last_value TEXT,
    last_error TEXT,
    -- Consecutive failures, and whether this episode was reported.
    failures INTEGER NOT NULL DEFAULT 0,
    alerted INTEGER NOT NULL DEFAULT 0,
    last_fired_at INTEGER,
    created_at INTEGER NOT NULL DEFAULT (unixepoch()),
    -- Deleted watches keep their row for their runs' history.
    deleted_at INTEGER
) STRICT;

CREATE INDEX watches_source ON watches (server, uri) WHERE enabled = 1 AND deleted_at IS NULL;

CREATE TABLE watch_runs (
    id TEXT PRIMARY KEY,
    watch_id TEXT NOT NULL REFERENCES watches (id),
    -- The definition version that fired it.
    version INTEGER NOT NULL,
    -- The input, with the value captured when it fired.
    input TEXT NOT NULL,
    fired_at INTEGER NOT NULL,
    -- 'queued', 'running', 'delivered', 'completed', 'failed', 'limit_exceeded', or 'interrupted'.
    status TEXT NOT NULL DEFAULT 'queued',
    root_id TEXT REFERENCES threads (id),
    started_at INTEGER,
    finished_at INTEGER,
    error TEXT
) STRICT;

CREATE INDEX watch_runs_active ON watch_runs (status) WHERE status IN ('queued', 'running');
CREATE INDEX watch_runs_root ON watch_runs (root_id);
