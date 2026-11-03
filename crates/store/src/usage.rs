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
        let counts = [usage.input_tokens, usage.cached_input_tokens, usage.output_tokens].map(saturate);

        self.db
            .call(move |connection| {
                let sql =
                    "INSERT INTO usage (thread_id, model, input_tokens, cached_input_tokens, output_tokens) VALUES (?1, ?2, ?3, ?4, ?5)";
                connection.execute(sql, params![thread, model, counts[0], counts[1], counts[2]])?;

                Ok(())
            })
            .await
    }

    /// Totals for the `limit` most recently active conversations, latest first.
    pub async fn by_conversation(&self, limit: i64) -> Result<Vec<ConversationUsage>, DbError> {
        self.db
            .call(move |connection| {
                let sql = "SELECT t.root_id,
                                  (SELECT c.adapter || ':' || c.key FROM conversations c WHERE c.root_id = t.root_id),
                                  COUNT(*), SUM(u.input_tokens), SUM(u.cached_input_tokens), SUM(u.output_tokens),
                                  (SELECT u2.input_tokens FROM usage u2 JOIN threads t2 ON t2.id = u2.thread_id
                                   WHERE t2.root_id = t.root_id ORDER BY u2.id DESC LIMIT 1),
                                  MAX(u.at)
                           FROM usage u JOIN threads t ON t.id = u.thread_id
                           GROUP BY t.root_id ORDER BY MAX(u.at) DESC LIMIT ?1";
                let mut statement = connection.prepare(sql)?;
                let rows = statement.query_map([limit], |row| {
                    Ok(ConversationUsage {
                        root: row.get(0)?,
                        name: row.get(1)?,
                        requests: row.get(2)?,
                        input_tokens: row.get(3)?,
                        cached_input_tokens: row.get(4)?,
                        output_tokens: row.get(5)?,
                        last_input_tokens: row.get(6)?,
                        last_at: row.get(7)?,
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
}

/// SQLite integers are signed; a count past `i64::MAX` is not a real count.
fn saturate(count: u64) -> i64 {
    i64::try_from(count).unwrap_or(i64::MAX)
}
