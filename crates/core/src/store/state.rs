//! Infinity's `StateStore` on SQLite.
//!
//! Semantics follow Infinity's reference `InMemoryStateStore`, except that
//! state needs an existing thread. Processed IDs share the table that
//! `Conversations::append_messages` writes, so an input in history is always
//! seen as processed, even if Infinity's separate write never happened.

use std::collections::HashSet;

use async_trait::async_trait;
use infinity_agent_core::ThreadId;
use infinity_agent_core::system::UserChoice;
use infinity_agent_core::traits::StateStore;
use rap_protocol::{ChoiceId, ToolCallId};
use rusqlite::{Connection, OptionalExtension, params};

use crate::store::db::{Db, DbError};

#[derive(Clone)]
pub struct States {
    db: Db,
}

impl States {
    pub fn new(db: Db) -> Self {
        Self { db }
    }

    /// Runs one statement with a thread ID and a second text parameter.
    async fn execute(&self, sql: &'static str, thread_id: &ThreadId<str>, value: String) -> Result<(), DbError> {
        let id = thread_id.as_str().to_owned();

        self.db
            .call(move |connection| {
                connection.execute(sql, params![&id, &value])?;

                Ok(())
            })
            .await
    }

    /// Reads one text column for every row of a thread.
    async fn column(&self, sql: &'static str, thread_id: &ThreadId<str>) -> Result<Vec<String>, DbError> {
        let id = thread_id.as_str().to_owned();

        self.db.call(move |connection| texts(connection, sql, &id)).await
    }
}

#[async_trait]
impl StateStore for States {
    type Error = DbError;

    async fn get_processed_ids(&self, thread_id: &ThreadId<str>) -> Result<HashSet<String>, DbError> {
        let ids = self.column("SELECT id FROM processed_ids WHERE thread_id = ?1", thread_id).await?;

        Ok(ids.into_iter().collect())
    }

    async fn add_processed_message_ids(&self, thread_id: &ThreadId<str>, message_ids: Vec<String>) -> Result<(), DbError> {
        let id = thread_id.as_str().to_owned();

        self.db
            .call(move |connection| {
                let transaction = connection.transaction()?;

                {
                    let mut insert = transaction.prepare_cached("INSERT OR IGNORE INTO processed_ids (thread_id, id) VALUES (?1, ?2)")?;

                    for message_id in &message_ids {
                        insert.execute(params![&id, message_id])?;
                    }
                }

                transaction.commit()?;

                Ok(())
            })
            .await
    }

    async fn get_metadata(&self, root_thread_id: &ThreadId<str>) -> Result<Option<serde_json::Value>, DbError> {
        let id = root_thread_id.as_str().to_owned();

        let value: Option<String> = self
            .db
            .call(move |connection| {
                let sql = "SELECT value FROM thread_metadata WHERE thread_id = ?1";

                Ok(connection.query_row(sql, [&id], |row| row.get(0)).optional()?)
            })
            .await?;

        match value {
            Some(value) => Ok(Some(serde_json::from_str(&value)?)),
            None => Ok(None),
        }
    }

    async fn set_metadata(&self, root_thread_id: &ThreadId<str>, metadata: serde_json::Value) -> Result<(), DbError> {
        let sql = "INSERT INTO thread_metadata (thread_id, value) VALUES (?1, ?2)
                   ON CONFLICT (thread_id) DO UPDATE SET value = excluded.value";

        self.execute(sql, root_thread_id, serde_json::to_string(&metadata)?).await
    }

    async fn get_active_subscriptions(&self, thread_id: &ThreadId<str>) -> Result<Vec<ToolCallId>, DbError> {
        let sql = "SELECT tool_call_id FROM subscriptions WHERE thread_id = ?1 ORDER BY tool_call_id";
        let ids = self.column(sql, thread_id).await?;

        Ok(ids.into_iter().map(ToolCallId::from).collect())
    }

    async fn add_active_subscription(&self, thread_id: &ThreadId<str>, tool_call_id: &ToolCallId<str>) -> Result<(), DbError> {
        let sql = "INSERT OR IGNORE INTO subscriptions (thread_id, tool_call_id) VALUES (?1, ?2)";

        self.execute(sql, thread_id, tool_call_id.as_str().to_owned()).await
    }

    async fn remove_active_subscription(&self, thread_id: &ThreadId<str>, tool_call_id: &ToolCallId<str>) -> Result<(), DbError> {
        let sql = "DELETE FROM subscriptions WHERE thread_id = ?1 AND tool_call_id = ?2";

        self.execute(sql, thread_id, tool_call_id.as_str().to_owned()).await
    }

    /// Replacing a choice with the same ID keeps its position, as in the reference store.
    async fn add_pending_user_choice(&self, thread_id: &ThreadId<str>, choice: UserChoice) -> Result<(), DbError> {
        let id = thread_id.as_str().to_owned();
        let body = serde_json::to_string(&choice)?;
        let choice_id = choice.id.as_str().to_owned();

        self.db
            .call(move |connection| {
                let sql = "INSERT INTO pending_choices (thread_id, choice_id, body) VALUES (?1, ?2, ?3)
                           ON CONFLICT (thread_id, choice_id) DO UPDATE SET body = excluded.body";
                connection.execute(sql, params![&id, &choice_id, &body])?;

                Ok(())
            })
            .await
    }

    async fn remove_pending_user_choice(&self, thread_id: &ThreadId<str>, choice_id: &ChoiceId<str>) -> Result<(), DbError> {
        let sql = "DELETE FROM pending_choices WHERE thread_id = ?1 AND choice_id = ?2";

        self.execute(sql, thread_id, choice_id.as_str().to_owned()).await
    }

    async fn get_pending_user_choices(&self, thread_id: &ThreadId<str>) -> Result<Vec<UserChoice>, DbError> {
        let bodies = self.column("SELECT body FROM pending_choices WHERE thread_id = ?1 ORDER BY id", thread_id).await?;
        let mut choices = Vec::with_capacity(bodies.len());

        for body in bodies {
            choices.push(serde_json::from_str(&body)?);
        }

        Ok(choices)
    }
}

fn texts(connection: &mut Connection, sql: &str, thread_id: &str) -> Result<Vec<String>, DbError> {
    let mut statement = connection.prepare_cached(sql)?;
    let rows = statement.query_map([thread_id], |row| row.get(0))?;

    let mut values = Vec::new();

    for value in rows {
        values.push(value?);
    }

    Ok(values)
}
