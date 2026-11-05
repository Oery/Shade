-- What each `new_items` watch has already seen. Such a watch keeps the JSON
-- pointer of its list in watches.condition.
--
-- An item stays remembered for a while after it was last listed, so one that
-- leaves a list and comes back is not new.

CREATE TABLE watch_items (
    watch_id TEXT NOT NULL REFERENCES watches (id),
    item_id TEXT NOT NULL,
    last_seen_at INTEGER NOT NULL,
    PRIMARY KEY (watch_id, item_id)
) STRICT, WITHOUT ROWID;

CREATE INDEX watch_items_age ON watch_items (last_seen_at);
