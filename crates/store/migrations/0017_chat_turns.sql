-- The inbox remains the durable input ledger. Chat turns add FIFO dispatch
-- and lifecycle state without starting a budget at acceptance time.
CREATE TABLE chat_turns (
    seq INTEGER PRIMARY KEY,
    input_id TEXT NOT NULL UNIQUE REFERENCES inbox (id),
    root_id TEXT NOT NULL REFERENCES threads (id),
    status TEXT NOT NULL CHECK (status IN ('queued', 'running', 'completed', 'failed', 'interrupted')),
    delivery TEXT NOT NULL,
    client_message_id TEXT,
    started_at INTEGER,
    finished_at INTEGER,
    error TEXT,
    UNIQUE (root_id, client_message_id)
) STRICT;

CREATE INDEX chat_queue ON chat_turns (status, seq);
CREATE UNIQUE INDEX chat_running ON chat_turns (root_id) WHERE status = 'running';

-- Adopt legacy owner inputs. The latest processed input remains running if
-- the history ends with an unanswered tool, so recovery holds its queue.
WITH pending_tools AS (
    SELECT DISTINCT m.thread_id AS root_id FROM messages m
    WHERE json_extract(m.body, '$.kind') = 'tool_call'
      AND NOT EXISTS (
          SELECT 1 FROM messages later WHERE later.thread_id = m.thread_id AND later.ord > m.ord
          AND json_extract(later.body, '$.kind') NOT IN ('tool_call', 'tool_result'))
      AND NOT EXISTS (
          SELECT 1 FROM messages result WHERE result.thread_id = m.thread_id AND result.ord > m.ord
          AND json_extract(result.body, '$.kind') = 'tool_result'
          AND json_extract(result.body, '$.result.id') = json_extract(m.body, '$.call.id'))
), legacy AS (
    SELECT i.id, i.root_id, i.failed, i.received_at, i.rowid AS arrival,
           b.started_at AS budget_started,
           CASE WHEN i.failed IS NOT NULL THEN 'failed'
                WHEN NOT EXISTS (SELECT 1 FROM processed_ids p WHERE p.thread_id = i.root_id AND p.id = i.id) THEN 'queued'
                WHEN EXISTS (SELECT 1 FROM pending_tools p WHERE p.root_id = i.root_id)
                     AND i.id = (SELECT newest.id FROM inbox newest
                                 JOIN processed_ids p ON p.thread_id = newest.root_id AND p.id = newest.id
                                 WHERE newest.root_id = i.root_id ORDER BY newest.received_at DESC, newest.rowid DESC LIMIT 1)
                     THEN 'running'
                ELSE 'completed' END AS status,
           CASE WHEN g.source = 'discord' THEN 'discord:' || COALESCE(c.key, '') ELSE 'terminal' END AS delivery
    FROM inbox i JOIN grants g ON g.root_id = i.root_id
    JOIN threads t ON t.id = i.root_id LEFT JOIN budgets b ON b.id = t.budget_id
    LEFT JOIN conversations c ON c.root_id = i.root_id AND c.adapter = 'discord'
    WHERE g.origin = 'chat'
)
INSERT INTO chat_turns (input_id, root_id, status, delivery, started_at, finished_at, error)
SELECT id, root_id, status, delivery,
       CASE WHEN status = 'running' THEN budget_started END,
       CASE WHEN status IN ('completed', 'failed') THEN unixepoch() END,
       failed
FROM legacy ORDER BY received_at, arrival;
