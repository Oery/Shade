-- Who proposed a change, so they hear the owner's decision, and whether it
-- deletes the job rather than saving a definition.

ALTER TABLE pending_changes ADD COLUMN thread_id TEXT;
ALTER TABLE pending_changes ADD COLUMN deletes INTEGER NOT NULL DEFAULT 0;
