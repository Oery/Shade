//! Owner chat: durable FIFO turns and the public conversation read model.

mod catalog;
mod history;
mod queue;

pub use history::StoredMessage;

#[cfg(test)]
mod tests;

use rusqlite::{Connection, OptionalExtension, Row};
use shade_proto::api::{Turn, TurnStatus};

use crate::db::{Db, DbError};
use crate::inbox::Input;
use crate::roots::{Adapter, Target};

#[derive(Clone)]
pub struct Chats {
    db: Db,
}

pub struct Enqueue {
    pub adapter: Adapter,
    pub target: Target,
    pub text: String,
    pub source_key: Option<String>,
    pub client_message_id: Option<String>,
    /// `api`, `terminal`, or `discord:<channel>`; chosen by the adapter.
    pub delivery: String,
    pub per_conversation: u32,
    pub total: u32,
}

pub enum Accepted {
    New(Turn),
    Repeated(Turn),
    Conflict,
    Full,
}

pub struct Claimed {
    pub input: Input,
    pub delivery: String,
}

impl Chats {
    pub fn new(db: Db) -> Self {
        Self { db }
    }
}

const TURN_COLUMNS: &str = "i.id, i.root_id, i.text, t.status, i.received_at, t.started_at, t.finished_at, t.error";

fn turn_row(row: &Row<'_>) -> rusqlite::Result<Turn> {
    let status: String = row.get(3)?;
    let status = match status.as_str() {
        "queued" => TurnStatus::Queued,
        "running" => TurnStatus::Running,
        "completed" => TurnStatus::Completed,
        "failed" => TurnStatus::Failed,
        "interrupted" => TurnStatus::Interrupted,
        _ => return Err(rusqlite::Error::InvalidQuery),
    };

    Ok(Turn {
        id: row.get(0)?,
        conversation_id: row.get(1)?,
        text: row.get(2)?,
        status,
        received_at: row.get(4)?,
        started_at: row.get(5)?,
        finished_at: row.get(6)?,
        error: row.get(7)?,
    })
}

fn owner(connection: &Connection, root: &str) -> Result<(), DbError> {
    let sql = "SELECT 1 FROM threads t JOIN grants g ON g.root_id = t.id WHERE t.id = ?1 AND t.parent_id IS NULL AND g.origin = 'chat'";
    let exists = connection.query_row(sql, [root], |_| Ok(())).optional()?;

    if exists.is_none() {
        return Err(DbError::UnknownThread(root.to_owned()));
    }

    Ok(())
}

fn turn(connection: &Connection, id: &str) -> Result<Option<Turn>, DbError> {
    let sql = format!("SELECT {TURN_COLUMNS} FROM chat_turns t JOIN inbox i ON i.id = t.input_id WHERE i.id = ?1");

    Ok(connection.query_row(&sql, [id], turn_row).optional()?)
}
