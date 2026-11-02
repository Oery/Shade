-- Infinity state store: per-thread metadata, active subscriptions, and pending
-- user choices. Processed IDs live in 0001 because history writes them.

CREATE TABLE thread_metadata (
    thread_id TEXT PRIMARY KEY REFERENCES threads (id),
    value TEXT NOT NULL
) STRICT;

CREATE TABLE subscriptions (
    thread_id TEXT NOT NULL REFERENCES threads (id),
    tool_call_id TEXT NOT NULL,
    PRIMARY KEY (thread_id, tool_call_id)
) STRICT, WITHOUT ROWID;

-- Replacing a choice keeps its `id`, and so its position in the list.
CREATE TABLE pending_choices (
    id INTEGER PRIMARY KEY,
    thread_id TEXT NOT NULL REFERENCES threads (id),
    choice_id TEXT NOT NULL,
    body TEXT NOT NULL,
    UNIQUE (thread_id, choice_id)
) STRICT;
