-- Whether a conversation is the owner's direct messages, as the adapter
-- reported it, so Shade can tell the model where it is talking.

ALTER TABLE conversations ADD COLUMN direct INTEGER NOT NULL DEFAULT 0;
