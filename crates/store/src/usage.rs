//! Token usage per model request, for seeing what conversations cost.

use infinity_provider_protocol::Usage;
use rusqlite::params;

use crate::db::{Db, DbError};

/// One conversation's model requests, its compaction children included.
pub struct ConversationUsage {
    pub root: String,
    /// `adapter:key` while a name still points at the conversation.
    pub name: Option<String>,
    pub requests: i64,
    pub input_tokens: i64,
    pub cached_input_tokens: i64,
    pub output_tokens: i64,
    /// Input tokens of the latest request: what the next turn resends.
    pub last_input_tokens: i64,
    pub last_at: i64,
    pub origin: String,
    pub source: String,
    pub status: String,
    /// Absent if any request had no configured price.
    pub estimated_usd: Option<f64>,
}

pub struct OriginUsage {
    pub origin: String,
    pub source: String,
    pub requests: i64,
    pub input_tokens: i64,
    pub cached_input_tokens: i64,
    pub output_tokens: i64,
    pub estimated_usd: Option<f64>,
}

#[derive(Clone)]
pub struct Usages {
    db: Db,
}

impl Usages {
    pub fn new(db: Db) -> Self {
        Self { db }
    }

    /// The first recorded request's input tokens, for tests.
    #[cfg(any(test, feature = "testing"))]
    pub async fn first_input_tokens(&self) -> Result<Option<i64>, DbError> {
        use rusqlite::OptionalExtension;

        self.db.call(|connection| Ok(connection.query_row("SELECT input_tokens FROM usage", [], |row| row.get(0)).optional()?)).await
    }

    pub async fn record(&self, thread: String, model: String, usage: Usage) -> Result<(), DbError> {
        self.record_priced(thread, model, usage, None).await
    }

    pub async fn record_priced(&self, thread: String, model: String, usage: Usage, estimated_usd: Option<f64>) -> Result<(), DbError> {
        let counts = [usage.input_tokens, usage.cached_input_tokens, usage.output_tokens].map(saturate);

        self.db
            .call(move |connection| {
                let sql = "INSERT INTO usage (thread_id, model, input_tokens, cached_input_tokens, output_tokens, estimated_usd)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6)";
                connection.execute(sql, params![thread, model, counts[0], counts[1], counts[2], estimated_usd])?;

                Ok(())
            })
            .await
    }

    /// Totals for the `limit` most recently active conversations, latest first.
    pub async fn by_conversation(&self, limit: i64) -> Result<Vec<ConversationUsage>, DbError> {
        self.conversations(limit, None).await
    }

    pub async fn for_root(&self, root: String) -> Result<Vec<ConversationUsage>, DbError> {
        self.conversations(1, Some(root)).await
    }

    async fn conversations(&self, limit: i64, root: Option<String>) -> Result<Vec<ConversationUsage>, DbError> {
        self.db
            .call(move |connection| {
                let sql = "SELECT t.root_id,
                                  (SELECT c.adapter || ':' || c.key FROM conversations c WHERE c.root_id = t.root_id),
                                  COUNT(*), SUM(u.input_tokens), SUM(u.cached_input_tokens), SUM(u.output_tokens),
                                  (SELECT u2.input_tokens FROM usage u2 JOIN threads t2 ON t2.id = u2.thread_id
                                   WHERE t2.root_id = t.root_id ORDER BY u2.id DESC LIMIT 1),
                                  MAX(u.at), COALESCE(g.origin, 'unknown'), COALESCE(g.source, ''),
                                  COALESCE(r.status, h.status, CASE WHEN g.origin IN ('chat', 'guest') THEN 'chat' ELSE 'unavailable' END),
                                  CASE WHEN COUNT(u.estimated_usd) = COUNT(*) THEN SUM(u.estimated_usd) END
                           FROM usage u JOIN threads t ON t.id = u.thread_id
                           LEFT JOIN grants g ON g.root_id = t.root_id
                           LEFT JOIN runs r ON r.root_id = t.root_id
                           LEFT JOIN hook_events h ON h.root_id = t.root_id
                           WHERE (?2 IS NULL OR t.root_id = ?2)
                           GROUP BY t.root_id ORDER BY MAX(u.id) DESC LIMIT ?1";
                let mut statement = connection.prepare(sql)?;
                let rows = statement.query_map(params![limit.clamp(1, 100), root], |row| {
                    Ok(ConversationUsage {
                        root: row.get(0)?,
                        name: row.get(1)?,
                        requests: row.get(2)?,
                        input_tokens: row.get(3)?,
                        cached_input_tokens: row.get(4)?,
                        output_tokens: row.get(5)?,
                        last_input_tokens: row.get(6)?,
                        last_at: row.get(7)?,
                        origin: row.get(8)?,
                        source: row.get(9)?,
                        status: row.get(10)?,
                        estimated_usd: row.get(11)?,
                    })
                })?;

                let mut conversations = Vec::new();

                for row in rows {
                    conversations.push(row?);
                }

                Ok(conversations)
            })
            .await
    }

    /// Totals across all history, including roots outside the conversation view.
    pub async fn by_origin(&self) -> Result<Vec<OriginUsage>, DbError> {
        self.db
            .call(|connection| {
                let sql = "SELECT COALESCE(g.origin, 'unknown'), COALESCE(g.source, ''), COUNT(*),
                              SUM(u.input_tokens), SUM(u.cached_input_tokens), SUM(u.output_tokens),
                              CASE WHEN COUNT(u.estimated_usd) = COUNT(*) THEN SUM(u.estimated_usd) END
                       FROM usage u JOIN threads t ON t.id = u.thread_id
                       LEFT JOIN grants g ON g.root_id = t.root_id
                       GROUP BY g.origin, g.source ORDER BY g.origin, g.source LIMIT 100";
                let mut statement = connection.prepare(sql)?;
                let rows = statement.query_map([], |row| {
                    Ok(OriginUsage {
                        origin: row.get(0)?,
                        source: row.get(1)?,
                        requests: row.get(2)?,
                        input_tokens: row.get(3)?,
                        cached_input_tokens: row.get(4)?,
                        output_tokens: row.get(5)?,
                        estimated_usd: row.get(6)?,
                    })
                })?;
                let mut totals = Vec::new();

                for row in rows {
                    totals.push(row?);
                }

                Ok(totals)
            })
            .await
    }
}

/// SQLite integers are signed; a count past `i64::MAX` is not a real count.
fn saturate(count: u64) -> i64 {
    i64::try_from(count).unwrap_or(i64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;
    use infinity_agent_core::ThreadId;
    use infinity_agent_core::traits::{ConversationStore, SpawnContext};
    use rap_protocol::ToolCallId;

    use crate::BoxError;
    use crate::conversation::Conversations;

    #[tokio::test]
    async fn children_share_totals_and_partial_prices_are_never_presented_as_complete() -> Result<(), BoxError> {
        let db = Db::in_memory()?;
        let conversations = Conversations::new(db.clone());
        let root = ThreadId::from("root".to_owned());
        conversations.register_root(&root).await?;
        let child = conversations.spawn_thread(&root, &ToolCallId::from("compact"), false, SpawnContext::Fresh).await?;
        conversations.mark_thread_as_compaction(&child).await?;
        let usages = Usages::new(db);
        let usage = Usage { input_tokens: 100, cached_input_tokens: 20, output_tokens: 10, ..Usage::default() };

        usages.record_priced("root".to_owned(), "model-a".to_owned(), usage, Some(0.1)).await?;
        usages.record_priced(child.as_str().to_owned(), "model-b".to_owned(), usage, Some(0.2)).await?;
        let totals = usages.by_conversation(20).await?;
        assert_eq!(totals.len(), 1);
        assert_eq!(
            (totals[0].requests, totals[0].input_tokens, totals[0].cached_input_tokens, totals[0].output_tokens),
            (2, 200, 40, 20)
        );
        assert!((totals[0].estimated_usd.unwrap_or_default() - 0.3).abs() < 0.000001);

        usages.record("root".to_owned(), "unpriced".to_owned(), usage).await?;
        let totals = usages.by_conversation(20).await?;
        let origins = usages.by_origin().await?;
        assert_eq!(totals[0].requests, 3);
        assert_eq!(totals[0].estimated_usd, None);
        assert_eq!(origins.len(), 1);
        assert_eq!((origins[0].requests, origins[0].input_tokens), (3, 300));
        assert_eq!(origins[0].estimated_usd, None);
        assert!(usages.for_root("missing".to_owned()).await?.is_empty());

        Ok(())
    }
}
