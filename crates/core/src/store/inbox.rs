//! The inbox: user input is durable before Infinity sees it.
//!
//! Accepting an input registers its root (on first use), starts the turn's
//! budget, and records the input, all in one transaction. Infinity writes the
//! input's ID to `processed_ids` with the history that holds it, so "processed"
//! is the acknowledgment and needs no write of its own. Until then the input
//! is unsettled and is sent again, with the same ID, when the daemon starts.

use infinity_agent_core::ThreadId;
use rusqlite::{OptionalExtension, params};

use crate::store::db::{Db, DbError};
use crate::store::roots::{self, Adapter, Target};

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

                let sql = "INSERT INTO inbox (id, root_id, text, source_key) VALUES (?1, ?2, ?3, ?4)";
                transaction.execute(sql, params![&id, &root, &text, &source_key])?;
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
