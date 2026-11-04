use rusqlite::{Connection, OptionalExtension, params};
use shade_proto::api::{Approval, Conversation, ConversationDetail, Page};

use super::{Chats, TURN_COLUMNS, owner, turn_row};
use crate::db::DbError;
use crate::roots::{self, Adapter};

const CONVERSATION_COLUMNS: &str = "t.id, g.source, c.key, t.created_at,
    MAX(t.created_at, COALESCE((SELECT MAX(i.received_at) FROM inbox i WHERE i.root_id = t.id), t.created_at)) AS activity";
const CONVERSATION_FROM: &str = "FROM threads t JOIN grants g ON g.root_id = t.id
    LEFT JOIN conversations c ON c.root_id = t.id
    WHERE t.parent_id IS NULL AND g.origin = 'chat'";

impl Chats {
    pub async fn create(&self, name: Option<String>) -> Result<Conversation, DbError> {
        self.db
            .call(move |connection| {
                let transaction = connection.transaction()?;
                let id = roots::new_id();
                roots::register(&transaction, &id, Adapter::Api)?;

                if let Some(name) = name {
                    let sql = "INSERT INTO conversations (adapter, key, root_id) VALUES ('api', ?1, ?2)
                           ON CONFLICT (adapter, key) DO UPDATE SET root_id = excluded.root_id";
                    transaction.execute(sql, params![name, id])?;
                }

                let conversation = conversation(&transaction, &id)?;
                transaction.commit()?;

                Ok(conversation)
            })
            .await
    }

    pub async fn list(&self, cursor: Option<(i64, String)>, limit: usize) -> Result<Page<Conversation>, DbError> {
        self.db
            .call(move |connection| {
                let (at, id) = cursor.unwrap_or((i64::MAX, String::new()));
                let sql = format!(
                    "SELECT * FROM (SELECT {CONVERSATION_COLUMNS} {CONVERSATION_FROM})
                              WHERE activity < ?1 OR (activity = ?1 AND id < ?2) ORDER BY activity DESC, id DESC LIMIT ?3"
                );
                let mut statement = connection.prepare(&sql)?;
                let rows = statement.query_map(params![at, id, (limit + 1) as i64], conversation_row)?;
                let mut items = rows.collect::<Result<Vec<_>, _>>()?;
                let more = items.len() > limit;
                items.truncate(limit);
                let next_cursor = if more {
                    items.last().map(|item| format!("{}.{}", item.activity, item.id))
                } else {
                    None
                };

                Ok(Page { items, next_cursor })
            })
            .await
    }

    pub async fn detail(&self, root: String) -> Result<ConversationDetail, DbError> {
        self.db
            .call(move |connection| {
                // One transaction keeps metadata, turns and approvals consistent.
                let transaction = connection.transaction()?;
                let conversation = conversation(&transaction, &root)?;
                let sql = format!(
                    "SELECT {TURN_COLUMNS} FROM chat_turns t JOIN inbox i ON i.id = t.input_id
                              WHERE t.root_id = ?1 AND t.status IN ('queued', 'running') ORDER BY t.seq"
                );
                let mut statement = transaction.prepare(&sql)?;
                let rows = statement.query_map([&root], turn_row)?;
                let mut active_turn = None;
                let mut queued_turns = Vec::new();

                for row in rows {
                    let turn = row?;

                    if turn.status == shade_proto::api::TurnStatus::Running {
                        active_turn = Some(turn);
                    } else {
                        queued_turns.push(turn);
                    }
                }

                drop(statement);
                let approvals = approvals(&transaction, &root)?;
                transaction.commit()?;

                Ok(ConversationDetail { conversation, active_turn, queued_turns, approvals })
            })
            .await
    }

    /// Terminal states remain readable after disconnecting or restarting.
    pub async fn turns(&self, root: String, after: i64, limit: usize) -> Result<Page<shade_proto::api::Turn>, DbError> {
        self.db
            .call(move |connection| {
                owner(connection, &root)?;
                let sql = format!(
                    "SELECT {TURN_COLUMNS}, t.seq FROM chat_turns t JOIN inbox i ON i.id = t.input_id
                              WHERE t.root_id = ?1 AND t.seq > ?2 ORDER BY t.seq LIMIT ?3"
                );
                let mut statement = connection.prepare(&sql)?;
                let rows =
                    statement.query_map(params![root, after, (limit + 1) as i64], |row| Ok((turn_row(row)?, row.get::<_, i64>(8)?)))?;
                let mut items = Vec::new();
                let mut last = after;
                let mut more = false;

                for row in rows {
                    let (turn, seq) = row?;

                    if items.len() == limit {
                        more = true;
                        break;
                    }

                    items.push(turn);
                    last = seq;
                }

                Ok(Page { items, next_cursor: more.then(|| last.to_string()) })
            })
            .await
    }

    pub async fn owner_root(&self, thread: String) -> Result<String, DbError> {
        self.db
            .call(move |connection| {
                let sql = "SELECT t.root_id FROM threads t JOIN grants g ON g.root_id = t.root_id WHERE t.id = ?1 AND g.origin = 'chat'";
                let root = connection.query_row(sql, [&thread], |row| row.get(0)).optional()?;

                match root {
                    Some(root) => Ok(root),
                    None => Err(DbError::UnknownThread(thread)),
                }
            })
            .await
    }

    pub async fn proposal_belongs(&self, root: String, code: String) -> Result<bool, DbError> {
        self.db
            .call(move |connection| {
                owner(connection, &root)?;
                let sql = "SELECT 1 FROM pending_changes p JOIN threads t ON t.id = p.thread_id WHERE p.code = ?1 AND t.root_id = ?2";

                Ok(connection.query_row(sql, params![code, root], |_| Ok(())).optional()?.is_some())
            })
            .await
    }
}

fn conversation(connection: &Connection, id: &str) -> Result<Conversation, DbError> {
    let sql = format!("SELECT {CONVERSATION_COLUMNS} {CONVERSATION_FROM} AND t.id = ?1");
    let found = connection.query_row(&sql, [id], conversation_row).optional()?;

    match found {
        Some(conversation) => Ok(conversation),
        None => Err(DbError::UnknownThread(id.to_owned())),
    }
}

fn conversation_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<Conversation> {
    Ok(Conversation { id: row.get(0)?, source: row.get(1)?, name: row.get(2)?, created_at: row.get(3)?, activity: row.get(4)? })
}

fn approvals(connection: &Connection, root: &str) -> Result<Vec<Approval>, DbError> {
    let sql = "SELECT p.code, p.receipt, p.created_at FROM pending_changes p JOIN threads t ON t.id = p.thread_id
               WHERE t.root_id = ?1 ORDER BY p.created_at, p.code";
    let mut statement = connection.prepare(sql)?;
    let rows = statement.query_map([root], |row| Ok(Approval { code: row.get(0)?, text: row.get(1)?, created_at: row.get(2)? }))?;

    Ok(rows.collect::<Result<_, _>>()?)
}
