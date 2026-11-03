//! The inbox: user input is durable before Infinity sees it.
//!
//! Accepting an input registers its root (on first use), starts the turn's
//! budget, and records the input, all in one transaction. Infinity writes the
//! input's ID to `processed_ids` with the history that holds it, so "processed"
//! is the acknowledgment and needs no write of its own. Until then the input
//! is unsettled and is sent again, with the same ID, when the daemon starts.

use infinity_agent_core::ThreadId;
use rusqlite::{Connection, OptionalExtension, params};

use crate::db::{Db, DbError};
use crate::grants::start_budget;
use crate::roots::{self, Adapter, Target};

/// An input recorded and ready to send.
pub struct Input {
    pub root: ThreadId,
    /// Infinity's dedup ID for this input, stable across replays.
    pub id: String,
    pub text: String,
}

#[derive(Clone)]
pub struct Inbox {
    db: Db,
}

impl Inbox {
    pub fn new(db: Db) -> Self {
        Self { db }
    }

    /// How many inputs were ever accepted, for tests.
    #[cfg(any(test, feature = "testing"))]
    pub async fn count(&self) -> Result<i64, DbError> {
        self.db.call(|connection| Ok(connection.query_row("SELECT COUNT(*) FROM inbox", [], |row| row.get(0))?)).await
    }

    /// Records `text` for `target`'s conversation and starts its turn.
    /// `None` when `source_key` was accepted before: the sender delivered it twice.
    pub async fn accept(
        &self, adapter: Adapter, target: Target, text: String, source_key: Option<String>,
    ) -> Result<Option<Input>, DbError> {
        let id = uuid::Uuid::new_v4().to_string();

        self.db
            .call(move |connection| {
                let transaction = connection.transaction()?;
                let seen = transaction.query_row("SELECT 1 FROM inbox WHERE source_key = ?1", [&source_key], |_| Ok(())).optional()?;

                if seen.is_some() {
                    return Ok(None);
                }

                let root = roots::turn(&transaction, adapter, &target)?;

                insert(&transaction, &id, &root, &text, source_key.as_deref())?;
                transaction.commit()?;

                Ok(Some(Input { root: ThreadId::from(root), id, text }))
            })
            .await
    }

    /// Records `text` as a new turn of the root that owns `thread`. `None`
    /// when `source_key` was accepted before, or the thread is unknown.
    pub async fn accept_for_thread(&self, thread: String, text: String, source_key: String) -> Result<Option<Input>, DbError> {
        let id = uuid::Uuid::new_v4().to_string();

        self.db
            .call(move |connection| {
                let transaction = connection.transaction()?;
                let seen = transaction.query_row("SELECT 1 FROM inbox WHERE source_key = ?1", [&source_key], |_| Ok(())).optional()?;
                let root: Option<String> =
                    transaction.query_row("SELECT root_id FROM threads WHERE id = ?1", [&thread], |row| row.get(0)).optional()?;

                let (None, Some(root)) = (seen, root) else {
                    return Ok(None);
                };

                start_budget(&transaction, &root)?;
                insert(&transaction, &id, &root, &text, Some(&source_key))?;
                transaction.commit()?;

                Ok(Some(Input { root: ThreadId::from(root), id, text }))
            })
            .await
    }

    /// Settles an input whose turn ended with an error its sender saw, so a
    /// restart does not run it again behind their back.
    pub async fn fail(&self, id: &str, error: &str) -> Result<(), DbError> {
        let (id, error) = (id.to_owned(), error.to_owned());

        self.db
            .call(move |connection| {
                connection.execute("UPDATE inbox SET failed = ?2 WHERE id = ?1", params![id, error])?;

                Ok(())
            })
            .await
    }

    /// Inputs neither processed nor failed, oldest first.
    pub async fn unsettled(&self) -> Result<Vec<Input>, DbError> {
        self.db
            .call(|connection| {
                let sql = "SELECT id, root_id, text FROM inbox i WHERE failed IS NULL
                           AND NOT EXISTS (SELECT 1 FROM processed_ids p WHERE p.thread_id = i.root_id AND p.id = i.id)
                           ORDER BY received_at, rowid";
                let mut statement = connection.prepare(sql)?;
                let rows = statement.query_map([], |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?, row.get(2)?)))?;

                let mut inputs = Vec::new();

                for row in rows {
                    let (id, root, text) = row?;
                    inputs.push(Input { root: ThreadId::from(root), id, text });
                }

                Ok(inputs)
            })
            .await
    }
}

/// Records an input for `root`. Runs inside the caller's transaction.
pub(crate) fn insert(connection: &Connection, id: &str, root: &str, text: &str, source_key: Option<&str>) -> Result<(), DbError> {
    let sql = "INSERT INTO inbox (id, root_id, text, source_key) VALUES (?1, ?2, ?3, ?4)";
    connection.execute(sql, params![id, root, text, source_key])?;

    Ok(())
}
