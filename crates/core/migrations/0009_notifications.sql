-- Notifications to the owner. The model's notifications count against its
-- budget; the runtime's failure notice has its own allowance of one per
-- budget, so a model cannot use up the path that reports its own failure.

ALTER TABLE budgets ADD COLUMN notifications INTEGER NOT NULL DEFAULT 0;
ALTER TABLE budgets ADD COLUMN failure_noticed INTEGER NOT NULL DEFAULT 0;
