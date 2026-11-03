//! Tool operations: the durable record of each asynchronous Shade tool call.
//!
//! A call is recorded before its work starts and its result before the
//! result is delivered. After a crash, a call with a captured result gets
//! that result again; any other call has an unknown outcome and is never
//! run a second time.

use std::collections::HashSet;

use infinity_agent_core::message::InfinityMessage;
use infinity_provider_protocol::message::ToolCall;
use rusqlite::{Connection, OptionalExtension, params};

use crate::store::db::{Db, DbError};

/// Infinity's compaction child calls this and closes; its result never comes.
const CLOSE_THREAD: &str = "close_thread";

/// What recording a call found.
#[derive(Debug, PartialEq)]
pub enum Started {
    /// First dispatch: the work may run.
    New,
    /// The call was dispatched before. Its captured result, if any; it must not run again.
    Again(Option<String>),
}

#[derive(Clone)]
pub struct Operations {
    db: Db,
}

impl Operations {
    pub fn new(db: Db) -> Self {
        Self { db }
    }

    /// Records the call as running, unless it already was recorded.
    pub async fn start(&self, thread: &str, call: &str, tool: &str) -> Result<Started, DbError> {
        let (thread, call, tool) = (thread.to_owned(), call.to_owned(), tool.to_owned());

        self.db
            .call(move |connection| {
                let sql =
                    "INSERT INTO tool_operations (thread_id, call_id, tool, state) VALUES (?1, ?2, ?3, 'running') ON CONFLICT DO NOTHING";

                if connection.execute(sql, params![thread, call, tool])? == 1 {
                    return Ok(Started::New);
                }

                Ok(Started::Again(captured(connection, &thread, &call)?))
            })
            .await
    }

    /// Captures the result. It is durable before anyone sees it.
    pub async fn finish(&self, thread: &str, call: &str, result: &str) -> Result<(), DbError> {
        let (thread, call, result) = (thread.to_owned(), call.to_owned(), result.to_owned());

        self.db
            .call(move |connection| {
                let sql = "UPDATE tool_operations SET state = 'done', result = ?3, finished_at = unixepoch() WHERE thread_id = ?1 AND call_id = ?2";
                connection.execute(sql, params![thread, call, result])?;

                Ok(())
            })
            .await
    }

    /// The captured result of a call, if it has one.
    pub async fn captured(&self, thread: &str, call: &str) -> Result<Option<String>, DbError> {
        let (thread, call) = (thread.to_owned(), call.to_owned());

        self.db.call(move |connection| captured(connection, &thread, &call)).await
    }

    /// For startup recovery: the captured result of an unanswered call, or
    /// `None` after marking its outcome unknown. Calls recorded by no row
    /// (the daemon died before dispatch finished) get one.
    pub async fn settle(&self, thread: &str, call: &str, tool: &str) -> Result<Option<String>, DbError> {
        let (thread, call, tool) = (thread.to_owned(), call.to_owned(), tool.to_owned());

        self.db
            .call(move |connection| {
                let transaction = connection.transaction()?;

                if let Some(result) = captured(&transaction, &thread, &call)? {
                    return Ok(Some(result));
                }

                let sql =
                    "INSERT INTO tool_operations (thread_id, call_id, tool, state, finished_at) VALUES (?1, ?2, ?3, 'unknown', unixepoch())
                    ON CONFLICT DO UPDATE SET state = 'unknown', finished_at = unixepoch()";
                transaction.execute(sql, params![thread, call, tool])?;
                transaction.commit()?;

                Ok(None)
            })
            .await
    }

    /// Tool calls ending an open thread's history without a result, by thread.
    ///
    /// Infinity accepts a late result only while its call is in the run of
    /// calls and results at the end of the history; anything after it settles
    /// the call. So those are the only calls that can still be waiting.
    pub async fn unanswered(&self) -> Result<Vec<(String, Vec<ToolCall>)>, DbError> {
        self.db.call(unanswered_calls).await
    }
}

fn unanswered_calls(connection: &mut Connection) -> Result<Vec<(String, Vec<ToolCall>)>, DbError> {
    let mut threads = Vec::new();
    let mut statement = connection.prepare("SELECT id FROM threads WHERE closed = 0")?;

    for id in statement.query_map([], |row| row.get::<_, String>(0))? {
        threads.push(id?);
    }

    let mut waiting = Vec::new();

    for thread in threads {
        let calls = unanswered(trailing_tools(connection, &thread)?);

        if !calls.is_empty() {
            waiting.push((thread, calls));
        }
    }

    Ok(waiting)
}

/// The tool calls and results that end a thread's history, newest first.
fn trailing_tools(connection: &Connection, thread: &str) -> Result<Vec<InfinityMessage>, DbError> {
    let mut statement = connection.prepare_cached("SELECT body FROM messages WHERE thread_id = ?1 ORDER BY ord DESC")?;
    let mut rows = statement.query([thread])?;
    let mut trailing = Vec::new();

    while let Some(row) = rows.next()? {
        let body: String = row.get(0)?;
        let message: InfinityMessage = serde_json::from_str(&body)?;

        if !matches!(message, InfinityMessage::ToolCall { .. } | InfinityMessage::ToolResult { .. }) {
            break;
        }

        trailing.push(message);
    }

    Ok(trailing)
}

/// Calls in `trailing` that have no result, oldest first.
fn unanswered(trailing: Vec<InfinityMessage>) -> Vec<ToolCall> {
    let mut answered = HashSet::new();
    let mut calls = Vec::new();

    for message in trailing.into_iter().rev() {
        match message {
            InfinityMessage::ToolCall { call, .. } => calls.push(call),
            InfinityMessage::ToolResult { result, .. } => {
                answered.insert(result.id);
            }
            _ => {}
        }
    }

    calls.retain(|call| !answered.contains(&call.id) && call.function.name != CLOSE_THREAD);

    calls
}

/// Records a call answered inside the caller's transaction, with the change it made.
pub fn insert_done(connection: &Connection, thread: &str, call: &str, tool: &str, result: &str) -> Result<(), DbError> {
    let sql =
        "INSERT INTO tool_operations (thread_id, call_id, tool, state, result, finished_at) VALUES (?1, ?2, ?3, 'done', ?4, unixepoch())";
    connection.execute(sql, params![thread, call, tool, result])?;

    Ok(())
}

pub fn captured(connection: &Connection, thread: &str, call: &str) -> Result<Option<String>, DbError> {
    let sql = "SELECT result FROM tool_operations WHERE thread_id = ?1 AND call_id = ?2 AND state = 'done'";
    let result = connection.query_row(sql, params![thread, call], |row| row.get(0)).optional()?;

    Ok(result)
}

#[cfg(test)]
mod tests {
    use infinity_agent_core::message::InfinityMessage;
    use infinity_provider_protocol::message::{Text, ToolCall, ToolResult, ToolResultContent};

    use super::unanswered;

    fn call(id: &str, tool: &str) -> InfinityMessage {
        InfinityMessage::ToolCall { call: ToolCall::new(id, tool, serde_json::json!({})), display_as: None }
    }

    fn result(id: &str) -> InfinityMessage {
        let content = vec![ToolResultContent::Text(Text { text: "done".to_owned() })];

        InfinityMessage::ToolResult { result: ToolResult { id: id.to_owned(), call_id: None, content }, display_segments: None }
    }

    #[test]
    fn only_calls_still_waiting_are_unanswered() {
        let cases: [(Vec<InfinityMessage>, &[&str]); 4] = [
            (vec![call("a", "shell"), result("a")], &[]),
            // Parallel calls: a later call's result does not answer an earlier one.
            (vec![call("a", "shell"), call("b", "shell"), result("b")], &["a"]),
            (vec![call("a", "shell"), call("b", "shell")], &["a", "b"]),
            (vec![call("c", "close_thread")], &[]),
        ];

        for (oldest_first, expected) in cases {
            let newest_first = oldest_first.into_iter().rev().collect();
            let ids: Vec<String> = unanswered(newest_first).into_iter().map(|call| call.id).collect();

            assert_eq!(ids, expected);
        }
    }
}
