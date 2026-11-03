//! Infinity's `ConversationStore` on SQLite.
//!
//! Semantics follow Infinity's reference `InMemoryConversationStore`, with two
//! deliberate differences:
//!
//! - Roots exist only through registration ([`super::roots`]).
//!   `ensure_root_thread` never creates one, so input for an unknown ID cannot
//!   mint a root. Children and messages need an existing thread.
//! - `append_messages` records the IDs of user and subscription-event inputs as
//!   processed in the same transaction as the history rows. That makes "the
//!   input ID is processed" the durable acceptance boundary.

use async_trait::async_trait;
use infinity_agent_core::ThreadId;
use infinity_agent_core::message::InfinityMessage;
use infinity_agent_core::traits::{ConversationStore, SpawnContext};
use rap_protocol::ToolCallId;
use rusqlite::{Connection, OptionalExtension, Statement, params};

#[cfg(test)]
use super::roots::insert_root;
use crate::store::db::{Db, DbError};

#[derive(Clone)]
pub struct Conversations {
    db: Db,
}

/// One history row ready to insert.
struct Row {
    message_id: String,
    body: String,
    /// User text and subscription events are not idempotent, so their IDs are deduplicated.
    is_input: bool,
}

impl Conversations {
    pub fn new(db: Db) -> Self {
        Self { db }
    }

    /// Creates a root with a chosen ID, for tests. Entry points use [`Roots`](super::roots::Roots).
    #[cfg(test)]
    pub async fn register_root(&self, thread_id: &ThreadId<str>) -> Result<(), DbError> {
        let id = thread_id.as_str().to_owned();

        self.db.call(move |connection| insert_root(connection, &id)).await
    }

    /// Reads one boolean column of a thread. Unknown threads read as `false`, as in the reference store.
    async fn flag(&self, thread_id: &ThreadId<str>, sql: &'static str) -> Result<bool, DbError> {
        let id = thread_id.as_str().to_owned();

        let flag = self.db.call(move |connection| Ok(connection.query_row(sql, [&id], |row| row.get(0)).optional()?)).await?;

        Ok(flag.unwrap_or(false))
    }

    /// Sets one boolean column of a thread. Unknown threads are ignored, as in the reference store.
    async fn set_flag(&self, thread_id: &ThreadId<str>, sql: &'static str) -> Result<(), DbError> {
        let id = thread_id.as_str().to_owned();

        self.db
            .call(move |connection| {
                connection.execute(sql, [&id])?;

                Ok(())
            })
            .await
    }
}

#[async_trait]
impl ConversationStore for Conversations {
    type Error = DbError;

    /// Infinity calls this for every thread it loads and ignores the result.
    async fn ensure_root_thread(&self, thread_id: &ThreadId<str>) -> Result<(), DbError> {
        if self.thread_exists(thread_id).await? {
            return Ok(());
        }

        Err(DbError::UnknownThread(thread_id.as_str().to_owned()))
    }

    async fn thread_exists(&self, thread_id: &ThreadId<str>) -> Result<bool, DbError> {
        let id = thread_id.as_str().to_owned();

        self.db
            .call(move |connection| {
                let found = connection.query_row("SELECT 1 FROM threads WHERE id = ?1", [&id], |_| Ok(())).optional()?;

                Ok(found.is_some())
            })
            .await
    }

    async fn load_history_up_to(
        &self, session_id: &ThreadId<str>, start_from: Option<i64>, up_to: Option<i64>,
    ) -> Result<Vec<InfinityMessage>, DbError> {
        let id = session_id.as_str().to_owned();
        let start = start_from.unwrap_or(0);
        let end = up_to.unwrap_or(i64::MAX);

        let bodies = self.db.call(move |connection| history(connection, &id, start, end)).await?;

        let mut messages = Vec::with_capacity(bodies.len());

        for body in bodies {
            messages.push(serde_json::from_str(&body)?);
        }

        Ok(messages)
    }

    async fn append_messages(&self, session_id: &ThreadId<str>, messages: Vec<(InfinityMessage, String)>) -> Result<(), DbError> {
        let id = session_id.as_str().to_owned();
        let mut rows = Vec::with_capacity(messages.len());

        for (message, message_id) in messages {
            let is_input = matches!(message, InfinityMessage::User { .. } | InfinityMessage::SubscriptionEvent { .. });
            rows.push(Row { message_id, body: serde_json::to_string(&message)?, is_input });
        }

        self.db.call(move |connection| append(connection, &id, &rows)).await
    }

    /// The parent must exist; the child joins its root.
    async fn spawn_thread(
        &self, parent_thread_id: &ThreadId<str>, spawn_tool_call_id: &ToolCallId<str>, is_for_subscription_event: bool,
        context: SpawnContext,
    ) -> Result<ThreadId, DbError> {
        let id = uuid::Uuid::new_v4().to_string();
        let parent = parent_thread_id.as_str().to_owned();
        let tool_call = spawn_tool_call_id.as_str().to_owned();

        let (order, fresh) = match context {
            SpawnContext::InheritUpTo(order) => (order as i64, false),
            SpawnContext::Fresh => (0, true),
        };

        let inserted = self
            .db
            .call(move |connection| {
                let inserted = connection.execute(
                    "INSERT INTO threads (id, parent_id, root_id, spawn_order, spawn_tool_call_id, subscription_event, fresh_context)
                     SELECT ?1, id, root_id, ?3, ?4, ?5, ?6 FROM threads WHERE id = ?2",
                    params![&id, &parent, order, &tool_call, is_for_subscription_event, fresh],
                )?;

                Ok((inserted, id, parent))
            })
            .await?;

        match inserted {
            (1, id, _) => Ok(ThreadId::from(id)),
            (_, _, parent) => Err(DbError::UnknownThread(parent)),
        }
    }

    async fn is_thread_closed(&self, thread_id: &ThreadId<str>) -> Result<bool, DbError> {
        self.flag(thread_id, "SELECT closed FROM threads WHERE id = ?1").await
    }

    async fn close_thread(&self, thread_id: &ThreadId<str>) -> Result<(), DbError> {
        self.set_flag(thread_id, "UPDATE threads SET closed = 1 WHERE id = ?1").await
    }

    async fn is_subscription_event_thread(&self, thread_id: &ThreadId<str>) -> Result<bool, DbError> {
        self.flag(thread_id, "SELECT subscription_event FROM threads WHERE id = ?1").await
    }

    async fn is_fresh_context_thread(&self, thread_id: &ThreadId<str>) -> Result<bool, DbError> {
        self.flag(thread_id, "SELECT fresh_context FROM threads WHERE id = ?1").await
    }

    async fn get_thread_parent_info(&self, thread_id: &ThreadId<str>) -> Result<Option<(ThreadId, ToolCallId)>, DbError> {
        let id = thread_id.as_str().to_owned();

        let row = self
            .db
            .call(move |connection| {
                let sql = "SELECT parent_id, spawn_tool_call_id FROM threads WHERE id = ?1";
                let row: Option<(Option<String>, Option<String>)> =
                    connection.query_row(sql, [&id], |row| Ok((row.get(0)?, row.get(1)?))).optional()?;

                Ok(row)
            })
            .await?;

        match row {
            Some((Some(parent), Some(tool_call))) => Ok(Some((ThreadId::from(parent), ToolCallId::from(tool_call)))),
            _ => Ok(None),
        }
    }

    /// Structural ancestry from the root down, ignoring Fresh boundaries.
    async fn get_ancestor_chain(&self, thread_id: &ThreadId<str>) -> Result<Vec<(ThreadId, i64)>, DbError> {
        let id = thread_id.as_str().to_owned();

        let chain = self.db.call(move |connection| ancestors(connection, id)).await?;

        Ok(chain.into_iter().rev().map(|(parent, order)| (ThreadId::from(parent), order)).collect())
    }

    async fn mark_thread_as_compaction(&self, thread_id: &ThreadId<str>) -> Result<(), DbError> {
        self.set_flag(thread_id, "UPDATE threads SET compaction = 1 WHERE id = ?1").await
    }

    async fn is_compaction_thread(&self, thread_id: &ThreadId<str>) -> Result<bool, DbError> {
        self.flag(thread_id, "SELECT compaction FROM threads WHERE id = ?1").await
    }

    async fn get_thread_spawn_order(&self, thread_id: &ThreadId<str>) -> Result<Option<i64>, DbError> {
        let id = thread_id.as_str().to_owned();

        let order = self
            .db
            .call(move |connection| {
                let order = connection.query_row("SELECT spawn_order FROM threads WHERE id = ?1", [&id], |row| row.get(0)).optional()?;

                Ok(order)
            })
            .await?;

        Ok(order.flatten())
    }

    async fn save_compaction_summary(&self, thread_id: &ThreadId<str>, summary: &str, up_to_order: i64) -> Result<(), DbError> {
        let id = thread_id.as_str().to_owned();
        let summary = summary.to_owned();

        self.db
            .call(move |connection| {
                let sql = "INSERT INTO compaction_summaries (thread_id, summary, up_to_order) VALUES (?1, ?2, ?3)";
                connection.execute(sql, params![&id, &summary, up_to_order])?;

                Ok(())
            })
            .await
    }

    async fn load_latest_compaction_summary_up_to(
        &self, thread_id: &ThreadId<str>, up_to_order: Option<i64>,
    ) -> Result<Option<(String, i64)>, DbError> {
        let id = thread_id.as_str().to_owned();
        let bound = up_to_order.unwrap_or(i64::MAX);

        self.db
            .call(move |connection| {
                let sql = "SELECT summary, up_to_order FROM compaction_summaries
                           WHERE thread_id = ?1 AND up_to_order <= ?2 ORDER BY id DESC LIMIT 1";
                let summary = connection.query_row(sql, params![&id, bound], |row| Ok((row.get(0)?, row.get(1)?))).optional()?;

                Ok(summary)
            })
            .await
    }
}

/// Bodies of rows `start..end`, counted from the start of the thread.
fn history(connection: &mut Connection, thread_id: &str, start: i64, end: i64) -> Result<Vec<String>, DbError> {
    let sql = "SELECT body FROM messages WHERE thread_id = ?1 AND ord >= ?2 AND ord < ?3 ORDER BY ord";
    let mut statement = connection.prepare_cached(sql)?;
    let rows = statement.query_map(params![thread_id, start, end], |row| row.get(0))?;

    let mut bodies = Vec::new();

    for body in rows {
        bodies.push(body?);
    }

    Ok(bodies)
}

/// Appends history and records processed input IDs in one transaction.
fn append(connection: &mut Connection, thread_id: &str, rows: &[Row]) -> Result<(), DbError> {
    let transaction = connection.transaction()?;

    {
        let next: i64 =
            transaction.query_row("SELECT COALESCE(MAX(ord) + 1, 0) FROM messages WHERE thread_id = ?1", [thread_id], |row| row.get(0))?;
        let mut message = transaction.prepare_cached("INSERT INTO messages (thread_id, ord, message_id, body) VALUES (?1, ?2, ?3, ?4)")?;
        let mut processed = transaction.prepare_cached("INSERT OR IGNORE INTO processed_ids (thread_id, id) VALUES (?1, ?2)")?;

        for (ord, row) in (next..).zip(rows) {
            message.execute(params![thread_id, ord, &row.message_id, &row.body])?;

            if row.is_input {
                processed.execute(params![thread_id, &row.message_id])?;
            }
        }
    }

    transaction.commit()?;

    Ok(())
}

/// Parents of `thread_id` with their spawn orders, nearest first.
fn ancestors(connection: &mut Connection, thread_id: String) -> Result<Vec<(String, i64)>, DbError> {
    let mut statement = connection.prepare_cached("SELECT parent_id, spawn_order FROM threads WHERE id = ?1")?;
    let mut chain = Vec::new();
    let mut next = parent_of(&mut statement, &thread_id)?;

    while let Some((parent, order)) = next {
        next = parent_of(&mut statement, &parent)?;
        chain.push((parent, order));
    }

    Ok(chain)
}

fn parent_of(statement: &mut Statement<'_>, thread_id: &str) -> Result<Option<(String, i64)>, DbError> {
    let row: Option<(Option<String>, Option<i64>)> = statement.query_row([thread_id], |row| Ok((row.get(0)?, row.get(1)?))).optional()?;

    match row {
        Some((Some(parent), order)) => Ok(Some((parent, order.unwrap_or(0)))),
        _ => Ok(None),
    }
}

#[cfg(test)]
mod tests;
