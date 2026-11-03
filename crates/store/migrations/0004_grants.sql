-- Grants: the authority of each root. Budgets: the work one chat turn or one
-- background run may do, shared by every thread spawned during it.

CREATE TABLE grants (
    root_id TEXT PRIMARY KEY REFERENCES threads (id),
    -- 'chat', 'schedule', 'watch', or 'webhook'.
    origin TEXT NOT NULL,
    -- The adapter for chat, the job ID, or the hook name.
    source TEXT NOT NULL,
    -- JSON array of delegated capabilities. NULL follows the origin's current policy.
    caps TEXT
) STRICT;

CREATE TABLE budgets (
    id INTEGER PRIMARY KEY,
    root_id TEXT NOT NULL REFERENCES threads (id),
    started_at INTEGER NOT NULL DEFAULT (unixepoch()),
    completions INTEGER NOT NULL DEFAULT 0,
    tool_calls INTEGER NOT NULL DEFAULT 0,
    -- The limit that stopped this budget. Once set, nothing more is admitted.
    exceeded TEXT
) STRICT;

-- A root moves to a new budget each turn; children keep the one they were spawned in.
ALTER TABLE threads ADD COLUMN budget_id INTEGER REFERENCES budgets (id);

-- Every root so far came from terminal chat.
INSERT INTO grants (root_id, origin, source) SELECT id, 'chat', 'terminal' FROM threads WHERE parent_id IS NULL;
