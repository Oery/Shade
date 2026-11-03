-- User input accepted by Shade. A row is written with its root and budget
-- before Infinity sees the input. It is settled once its ID is processed
-- (history holds it) or once its turn ended with an error its sender was
-- shown. Unsettled rows are sent again, with the same ID, at startup.

CREATE TABLE inbox (
    id TEXT PRIMARY KEY,
    root_id TEXT NOT NULL REFERENCES threads (id),
    text TEXT NOT NULL,
    received_at INTEGER NOT NULL DEFAULT (unixepoch()),
    -- The error that ended its turn.
    failed TEXT
) STRICT;

CREATE INDEX inbox_unsettled ON inbox (received_at) WHERE failed IS NULL;
