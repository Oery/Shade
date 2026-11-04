use infinity_agent_core::ThreadId;
use rusqlite::{Connection, OptionalExtension, params};
use shade_proto::api::{Turn, TurnStatus};

use super::{Accepted, Chats, Claimed, Enqueue, TURN_COLUMNS, turn, turn_row};
use crate::db::DbError;
use crate::grants::start_budget;
use crate::inbox::{self, Input};
use crate::roots;

impl Chats {
    /// Acceptance, deduplication and queue admission share one transaction.
    pub async fn enqueue(&self, request: Enqueue) -> Result<Accepted, DbError> {
        self.db.call(move |connection| enqueue(connection, request)).await
    }

    /// Check space before applying an owner decision that needs a follow-up.
    pub async fn has_capacity(&self, root: String, per_conversation: u32, total: u32) -> Result<bool, DbError> {
        self.db
            .call(move |connection| {
                let queued: u32 =
                    connection.query_row("SELECT COUNT(*) FROM chat_turns WHERE root_id = ?1 AND status = 'queued'", [&root], |row| {
                        row.get(0)
                    })?;
                let waiting: u32 = connection.query_row("SELECT COUNT(*) FROM chat_turns WHERE status = 'queued'", [], |row| row.get(0))?;

                Ok(queued < per_conversation && waiting < total)
            })
            .await
    }

    /// Claim the oldest runnable input. One active turn per root is enforced
    /// by a unique index as well as this query. Waiting creates no budget.
    pub async fn claim(&self) -> Result<Option<Claimed>, DbError> {
        self.db.call(claim).await
    }

    pub async fn finish(&self, id: &str, status: TurnStatus, error: Option<String>) -> Result<(), DbError> {
        let id = id.to_owned();

        self.db
            .call(move |connection| {
                let transaction = connection.transaction()?;
                let sql =
                    "UPDATE chat_turns SET status = ?2, finished_at = unixepoch(), error = ?3 WHERE input_id = ?1 AND status = 'running'";
                transaction.execute(sql, params![id, status.name(), error])?;

                if let Some(error) = &error {
                    transaction.execute("UPDATE inbox SET failed = ?2 WHERE id = ?1", params![id, error])?;
                }

                transaction.commit()?;

                Ok(())
            })
            .await
    }

    pub async fn turn(&self, id: &str) -> Result<Option<Turn>, DbError> {
        let id = id.to_owned();

        self.db.call(move |connection| turn(connection, &id)).await
    }

    pub async fn running(&self, root: &str) -> Result<Option<Turn>, DbError> {
        let root = root.to_owned();

        self.db.call(move |connection| {
            let sql = format!("SELECT {TURN_COLUMNS} FROM chat_turns t JOIN inbox i ON i.id = t.input_id WHERE t.root_id = ?1 AND t.status = 'running'");

            Ok(connection.query_row(&sql, [root], turn_row).optional()?)
        }).await
    }

    /// Completed/failed owner turns must not be reopened just because their
    /// last tool result had not reached history when the process stopped.
    pub async fn resume_allowed(&self, root: String) -> Result<bool, DbError> {
        self.db
            .call(move |connection| {
                let sql = "SELECT NOT EXISTS (SELECT 1 FROM chat_turns WHERE root_id = ?1)
                       OR EXISTS (SELECT 1 FROM chat_turns WHERE root_id = ?1 AND status = 'running')";

                Ok(connection.query_row(sql, [root], |row| row.get(0))?)
            })
            .await
    }

    pub async fn delivery(&self, id: &str) -> Result<Option<String>, DbError> {
        let id = id.to_owned();

        self.db
            .call(move |connection| {
                Ok(connection.query_row("SELECT delivery FROM chat_turns WHERE input_id = ?1", [id], |row| row.get(0)).optional()?)
            })
            .await
    }

    /// A processed input must never be resent. Only a recoverable pending
    /// tool can continue it; everything else gets an explicit terminal state.
    pub async fn recover(&self, continuing: Vec<String>) -> Result<(), DbError> {
        self.db.call(move |connection| {
            let transaction = connection.transaction()?;
            let sql = "SELECT t.input_id, t.root_id, EXISTS (SELECT 1 FROM processed_ids p WHERE p.thread_id = t.root_id AND p.id = t.input_id)
                        , i.failed FROM chat_turns t JOIN inbox i ON i.id = t.input_id WHERE t.status = 'running'";
            let mut statement = transaction.prepare(sql)?;
            let rows = statement.query_map([], |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?, row.get::<_, bool>(2)?, row.get::<_, Option<String>>(3)?)))?;
            let turns = rows.collect::<Result<Vec<_>, _>>()?;
            drop(statement);

            for (id, root, processed, failed) in turns {
                if let Some(error) = failed {
                    transaction.execute("UPDATE chat_turns SET status = 'failed', finished_at = unixepoch(), error = ?2 WHERE input_id = ?1", params![id, error])?;
                } else if !processed {
                    transaction.execute("UPDATE chat_turns SET status = 'queued' WHERE input_id = ?1", [&id])?;
                } else if !continuing.contains(&root) {
                    transaction.execute("UPDATE chat_turns SET status = 'interrupted', finished_at = unixepoch(), error = 'daemon stopped before the turn finished' WHERE input_id = ?1", [&id])?;
                }
            }

            transaction.commit()?;

            Ok(())
        }).await
    }
}

fn enqueue(connection: &mut Connection, request: Enqueue) -> Result<Accepted, DbError> {
    let transaction = connection.transaction()?;

    if let Some(source) = &request.source_key {
        let seen: Option<String> =
            transaction.query_row("SELECT id FROM inbox WHERE source_key = ?1", [source], |row| row.get(0)).optional()?;

        if let Some(id) = seen {
            return match turn(&transaction, &id)? {
                Some(turn) => Ok(Accepted::Repeated(turn)),
                None => Ok(Accepted::Conflict),
            };
        }
    }

    let root = roots::resolve(&transaction, request.adapter, &request.target)?;

    if let Some(client_id) = &request.client_message_id {
        let sql = "SELECT input_id FROM chat_turns WHERE root_id = ?1 AND client_message_id = ?2";
        let seen: Option<String> = transaction.query_row(sql, params![root, client_id], |row| row.get(0)).optional()?;

        if let Some(id) = seen {
            let previous = turn(&transaction, &id)?.ok_or(DbError::Closed)?;

            return if previous.text == request.text {
                Ok(Accepted::Repeated(previous))
            } else {
                Ok(Accepted::Conflict)
            };
        }
    }

    let queued: u32 =
        transaction.query_row("SELECT COUNT(*) FROM chat_turns WHERE root_id = ?1 AND status = 'queued'", [&root], |row| {
            row.get(0)
        })?;
    let total: u32 = transaction.query_row("SELECT COUNT(*) FROM chat_turns WHERE status = 'queued'", [], |row| row.get(0))?;

    if queued >= request.per_conversation || total >= request.total {
        return Ok(Accepted::Full);
    }

    let id = roots::new_id();
    inbox::insert(&transaction, &id, &root, &request.text, request.source_key.as_deref())?;
    let sql = "INSERT INTO chat_turns (input_id, root_id, status, delivery, client_message_id) VALUES (?1, ?2, 'queued', ?3, ?4)";
    transaction.execute(sql, params![id, root, request.delivery, request.client_message_id])?;
    let accepted = turn(&transaction, &id)?.ok_or(DbError::Closed)?;
    transaction.commit()?;

    Ok(Accepted::New(accepted))
}

fn claim(connection: &mut Connection) -> Result<Option<Claimed>, DbError> {
    let transaction = connection.transaction()?;
    let sql = "SELECT t.input_id, t.root_id, i.text, t.delivery, t.started_at FROM chat_turns t JOIN inbox i ON i.id = t.input_id
               WHERE t.status = 'queued' AND NOT EXISTS (SELECT 1 FROM chat_turns a WHERE a.root_id = t.root_id AND a.status = 'running')
               ORDER BY t.seq LIMIT 1";
    let found = transaction
        .query_row(sql, [], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, Option<i64>>(4)?,
            ))
        })
        .optional()?;

    let Some((id, root, text, delivery, started)) = found else {
        return Ok(None);
    };

    if started.is_none() {
        start_budget(&transaction, &root)?;
    }

    transaction.execute(
        "UPDATE chat_turns SET status = 'running', started_at = COALESCE(started_at, unixepoch()) WHERE input_id = ?1",
        [&id],
    )?;
    transaction.commit()?;

    Ok(Some(Claimed { input: Input { root: ThreadId::from(root), id, text }, delivery }))
}
