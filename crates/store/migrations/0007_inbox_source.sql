-- The sender's own ID for an input (a Discord message ID), so a message
-- delivered twice is accepted once. Terminal input has none.

ALTER TABLE inbox ADD COLUMN source_key TEXT;

CREATE UNIQUE INDEX inbox_source ON inbox (source_key) WHERE source_key IS NOT NULL;
