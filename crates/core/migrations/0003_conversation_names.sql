-- Long-lived conversations: an adapter's conversation name points at a root.
-- `/new` repoints the name; the previous root keeps its history.

CREATE TABLE conversations (
    adapter TEXT NOT NULL,
    key TEXT NOT NULL,
    root_id TEXT NOT NULL REFERENCES threads (id),
    PRIMARY KEY (adapter, key)
) STRICT, WITHOUT ROWID;
