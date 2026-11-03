-- Token usage of every model request, as the provider reported it through rig.
-- rig 0.31 drops cached-token counts on streaming responses, so
-- cached_input_tokens reads 0 until it reports them.

CREATE TABLE usage (
    id INTEGER PRIMARY KEY,
    thread_id TEXT NOT NULL REFERENCES threads (id),
    model TEXT NOT NULL,
    at INTEGER NOT NULL DEFAULT (unixepoch()),
    input_tokens INTEGER NOT NULL,
    cached_input_tokens INTEGER NOT NULL,
    output_tokens INTEGER NOT NULL
) STRICT;

CREATE INDEX usage_thread ON usage (thread_id, at);
