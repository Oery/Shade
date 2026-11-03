-- Infinity conversation store: threads, history, compaction summaries, and
-- processed input IDs (written in the same transaction as history).

CREATE TABLE threads (
    id TEXT PRIMARY KEY,
    -- NULL for roots. Children always name an existing parent.
    parent_id TEXT REFERENCES threads (id),
    root_id TEXT NOT NULL REFERENCES threads (id),
    -- How many leading parent messages the child inherits.
    spawn_order INTEGER,
    spawn_tool_call_id TEXT,
    closed INTEGER NOT NULL DEFAULT 0,
    subscription_event INTEGER NOT NULL DEFAULT 0,
    compaction INTEGER NOT NULL DEFAULT 0,
    fresh_context INTEGER NOT NULL DEFAULT 0,
    created_at INTEGER NOT NULL DEFAULT (unixepoch())
) STRICT;

CREATE INDEX threads_root ON threads (root_id);

-- `ord` counts from zero per thread. Message IDs are not unique: Infinity
-- gives every item of one completion the same ID.
CREATE TABLE messages (
    thread_id TEXT NOT NULL REFERENCES threads (id),
    ord INTEGER NOT NULL,
    message_id TEXT NOT NULL,
    body TEXT NOT NULL,
    PRIMARY KEY (thread_id, ord)
) STRICT, WITHOUT ROWID;

-- The latest summary is the one inserted last, not the one with the highest order.
CREATE TABLE compaction_summaries (
    id INTEGER PRIMARY KEY,
    thread_id TEXT NOT NULL REFERENCES threads (id),
    summary TEXT NOT NULL,
    up_to_order INTEGER NOT NULL
) STRICT;

CREATE INDEX compaction_summaries_thread ON compaction_summaries (thread_id, id);

CREATE TABLE processed_ids (
    thread_id TEXT NOT NULL REFERENCES threads (id),
    id TEXT NOT NULL,
    PRIMARY KEY (thread_id, id)
) STRICT, WITHOUT ROWID;
