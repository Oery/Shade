-- One-shot jobs that have run are archived after a while: hidden from the
-- model's and the owner's job lists, with their runs kept.

ALTER TABLE schedules ADD COLUMN archived_at INTEGER;
