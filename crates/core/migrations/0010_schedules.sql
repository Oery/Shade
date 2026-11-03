-- Scheduled jobs and their runs.
--
-- Claiming an occurrence records a queued run and moves the job's next_at
-- in one transaction; a one-shot job is disabled in that same transaction.
-- A queued run gets its root, grant, budget and inbox input when it starts,
-- so waiting for capacity costs nothing and loses nothing.

CREATE TABLE schedules (
    id TEXT PRIMARY KEY,
    title TEXT NOT NULL,
    -- 'cron' or 'once'.
    kind TEXT NOT NULL,
    -- The cron expression, or the one-shot instant as given.
    spec TEXT NOT NULL,
    -- IANA zone the cron expression is read in, and times are shown in.
    timezone TEXT NOT NULL,
    prompt TEXT NOT NULL,
    -- JSON array of capabilities delegated to each run.
    caps TEXT NOT NULL,
    -- The chat root that created the job, when one did.
    creator_root TEXT REFERENCES threads (id),
    version INTEGER NOT NULL DEFAULT 1,
    enabled INTEGER NOT NULL DEFAULT 1,
    -- The next occurrence in unix seconds. NULL once nothing is left.
    next_at INTEGER,
    created_at INTEGER NOT NULL DEFAULT (unixepoch()),
    -- Deleted jobs keep their row for their runs' history.
    deleted_at INTEGER
) STRICT;

CREATE INDEX schedules_due ON schedules (next_at) WHERE enabled = 1 AND deleted_at IS NULL;

CREATE TABLE runs (
    id TEXT PRIMARY KEY,
    schedule_id TEXT NOT NULL REFERENCES schedules (id),
    -- The definition version that produced it.
    version INTEGER NOT NULL,
    -- The occurrence, in unix seconds. Later missed occurrences fold into it.
    scheduled_for INTEGER NOT NULL,
    -- 'queued', 'running', 'completed', 'failed', 'limit_exceeded', or 'interrupted'.
    status TEXT NOT NULL DEFAULT 'queued',
    -- Set when the run starts.
    root_id TEXT REFERENCES threads (id),
    claimed_at INTEGER NOT NULL DEFAULT (unixepoch()),
    started_at INTEGER,
    finished_at INTEGER,
    error TEXT,
    UNIQUE (schedule_id, scheduled_for)
) STRICT;

CREATE INDEX runs_active ON runs (status) WHERE status IN ('queued', 'running');
CREATE INDEX runs_root ON runs (root_id);
