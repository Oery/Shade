-- Where a job's runs may post, whether the job lives in a file, and changes
-- waiting for the owner's approval.

-- JSON array: `dm`, names from [discord.channels], or channel IDs.
ALTER TABLE schedules ADD COLUMN channels TEXT NOT NULL DEFAULT '["dm"]';
-- 1 when the definition lives in jobs/<id>.toml, which wins at startup.
ALTER TABLE schedules ADD COLUMN file INTEGER NOT NULL DEFAULT 0;

-- A create or edit by owner chat that leaves a job holding caps beyond
-- [jobs] agent_caps. It applies only when the owner approves it, within a
-- day, and only if the job is still at base_version.
CREATE TABLE pending_changes (
    code TEXT PRIMARY KEY,
    job_id TEXT NOT NULL,
    -- NULL for a new job.
    base_version INTEGER,
    -- JSON of the job's whole new definition.
    definition TEXT NOT NULL,
    -- What the owner is shown when it applies.
    receipt TEXT NOT NULL,
    created_at INTEGER NOT NULL DEFAULT (unixepoch())
) STRICT;
