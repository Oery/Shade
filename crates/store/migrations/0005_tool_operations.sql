-- Shade-owned tool calls whose results arrive after the call. A row is written
-- before the work starts and its result before the result is delivered, so a
-- restart can tell a captured result from an unknown outcome. The call
-- itself, arguments included, is in the thread's history.

CREATE TABLE tool_operations (
    thread_id TEXT NOT NULL REFERENCES threads (id),
    call_id TEXT NOT NULL,
    tool TEXT NOT NULL,
    -- 'running', 'done' (result captured), or 'unknown' (interrupted before that).
    state TEXT NOT NULL,
    result TEXT,
    started_at INTEGER NOT NULL DEFAULT (unixepoch()),
    finished_at INTEGER,
    PRIMARY KEY (thread_id, call_id)
) STRICT, WITHOUT ROWID;
