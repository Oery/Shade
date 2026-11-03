//! Bounded observer records and views over the existing durable ledgers.

use rusqlite::{Connection, params};

use crate::db::{Db, DbError};

pub struct Event {
    pub thread: String,
    pub at: i64,
    pub kind: String,
    pub summary: String,
}

pub struct Entry {
    pub at: i64,
    pub thread: String,
    pub kind: String,
    pub summary: String,
}

pub struct Root {
    pub id: String,
    pub origin: String,
    pub source: String,
    pub caps: Option<String>,
    pub run: Option<String>,
    pub status: String,
    pub entries: Vec<Entry>,
}

#[derive(Clone)]
pub struct Audit {
    db: Db,
}

impl Audit {
    pub fn new(db: Db) -> Self {
        Self { db }
    }

    pub async fn record(&self, mut event: Event) -> Result<(), DbError> {
        event.kind.truncate(event.kind.floor_char_boundary(64));
        event.summary.truncate(event.summary.floor_char_boundary(512));

        self.db
            .call(move |connection| {
                connection.execute(
                    "INSERT INTO audit_events (thread_id, at, kind, summary) VALUES (?1, ?2, ?3, ?4)",
                    params![event.thread, event.at, event.kind, event.summary],
                )?;

                Ok(())
            })
            .await
    }

    /// At most 20 roots, each with its last 50 ledger/observer entries.
    /// Arguments, outputs, prompts and webhook payloads are never selected.
    pub async fn recent(&self, since: i64, root: Option<String>) -> Result<Vec<Root>, DbError> {
        self.db
            .call(move |connection| {
                let sql = "WITH activity AS (
                         SELECT id AS root_id, created_at AS at FROM threads WHERE parent_id IS NULL
                         UNION ALL SELECT t.root_id, a.at FROM audit_events a JOIN threads t ON t.id = a.thread_id
                         UNION ALL SELECT t.root_id, COALESCE(o.finished_at, o.started_at)
                           FROM tool_operations o JOIN threads t ON t.id = o.thread_id
                         UNION ALL SELECT t.root_id, u.at FROM usage u JOIN threads t ON t.id = u.thread_id
                         UNION ALL SELECT root_id, received_at FROM inbox
                         UNION ALL SELECT root_id, COALESCE(finished_at, started_at) FROM runs
                         UNION ALL SELECT root_id, COALESCE(finished_at, started_at) FROM hook_events
                       ), recent AS (SELECT root_id, MAX(at) AS at FROM activity GROUP BY root_id HAVING MAX(at) >= ?1)
                       SELECT g.root_id, g.origin, g.source, g.caps,
                              COALESCE(r.id, h.id),
                              COALESCE(r.status, h.status, CASE WHEN g.origin IN ('chat', 'guest') THEN 'chat' ELSE 'unavailable' END)
                       FROM grants g JOIN recent a ON a.root_id = g.root_id
                       LEFT JOIN runs r ON r.root_id = g.root_id
                       LEFT JOIN hook_events h ON h.root_id = g.root_id
                       WHERE (?2 IS NULL OR g.root_id = ?2)
                       ORDER BY a.at DESC, g.root_id LIMIT 20";
                let mut statement = connection.prepare(sql)?;
                let rows = statement.query_map(params![since, root], |row| {
                    Ok(Root {
                        id: row.get(0)?,
                        origin: row.get(1)?,
                        source: row.get(2)?,
                        caps: row.get(3)?,
                        run: row.get(4)?,
                        status: row.get(5)?,
                        entries: Vec::new(),
                    })
                })?;
                let mut roots = Vec::new();

                for row in rows {
                    let mut root = row?;
                    root.entries = entries(connection, &root.id, since)?;
                    roots.push(root);
                }

                Ok(roots)
            })
            .await
    }
}

fn entries(connection: &Connection, root: &str, since: i64) -> Result<Vec<Entry>, DbError> {
    let sql = "SELECT at, thread_id, kind, summary FROM (
                 SELECT a.at, a.thread_id, a.kind, a.summary, a.id AS sequence
                   FROM audit_events a JOIN threads t ON t.id = a.thread_id WHERE t.root_id = ?1
                 UNION ALL
                 SELECT o.started_at, o.thread_id, 'dispatch', o.tool || ' call=' || o.call_id, 0
                   FROM tool_operations o JOIN threads t ON t.id = o.thread_id WHERE t.root_id = ?1
                 UNION ALL
                 SELECT o.finished_at, o.thread_id, 'outcome', o.tool || ' call=' || o.call_id || ' ' ||
                   CASE WHEN o.state = 'done' AND o.result LIKE 'Error:%' THEN 'error (captured)'
                        WHEN o.state = 'done' THEN 'result captured' ELSE o.state END, 0
                   FROM tool_operations o JOIN threads t ON t.id = o.thread_id
                   WHERE t.root_id = ?1 AND o.finished_at IS NOT NULL
                 UNION ALL
                 SELECT b.started_at, b.root_id, 'budget', 'limit exceeded: ' || b.exceeded, b.id
                   FROM budgets b WHERE b.root_id = ?1 AND b.exceeded IS NOT NULL
               ) WHERE at >= ?2 ORDER BY at DESC, sequence DESC, kind DESC LIMIT 50";
    let mut statement = connection.prepare(sql)?;
    let rows = statement.query_map(params![root, since], |row| {
        Ok(Entry { at: row.get(0)?, thread: row.get(1)?, kind: row.get(2)?, summary: row.get(3)? })
    })?;
    let mut entries = Vec::new();

    for row in rows {
        entries.push(row?);
    }

    entries.reverse();

    Ok(entries)
}

#[cfg(test)]
mod tests {
    use super::*;
    use infinity_agent_core::ThreadId;

    use crate::BoxError;
    use crate::conversation::Conversations;
    use crate::operations::Operations;

    #[tokio::test]
    async fn filters_follow_activity_and_tool_results_are_never_selected() -> Result<(), BoxError> {
        let db = Db::in_memory()?;
        let conversations = Conversations::new(db.clone());

        for name in ["old", "active"] {
            conversations.register_root(&ThreadId::from(name.to_owned())).await?;
        }

        db.call(|connection| {
            connection.execute("UPDATE threads SET created_at = 0", [])?;

            Ok(())
        })
        .await?;
        let audit = Audit::new(db.clone());
        let operations = Operations::new(db);
        operations.start("active", "call-1", "mail_send").await?;
        operations.finish("active", "call-1", "Error: password=do-not-show-this").await?;
        audit.record(Event { thread: "active".to_owned(), at: 100, kind: "turn".to_owned(), summary: "completed".to_owned() }).await?;

        let roots = audit.recent(50, None).await?;
        assert_eq!(roots.len(), 1);
        assert_eq!(roots[0].id, "active");
        assert!(roots[0].entries.iter().any(|entry| entry.kind == "outcome" && entry.summary.contains("error (captured)")));
        assert!(roots[0].entries.iter().all(|entry| !entry.summary.contains("do-not-show-this")));
        assert!(audit.recent(50, Some("old".to_owned())).await?.is_empty());
        assert!(audit.recent(i64::MAX, None).await?.is_empty());

        Ok(())
    }
}
