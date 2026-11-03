//! Root registration: the only way conversations come into existence.
//!
//! Every entry point creates roots here, together with their grant, before
//! Infinity attaches, so thread configuration never resolves for a thread
//! Shade did not register.

use infinity_agent_core::ThreadId;
use rusqlite::{Connection, OptionalExtension, params};

use crate::policy::{Grant, Origin};
use crate::store::db::{Db, DbError};
use crate::store::grants::{insert_grant, start_budget};

/// Which conversation an input belongs to.
pub enum Target {
    /// A new root reachable by no name (`shade ask`).
    Fresh,
    /// A named conversation, created on first use.
    Named(String),
}

/// Where a named conversation lives. Names are scoped per adapter.
#[derive(Clone, Copy)]
pub enum Adapter {
    Terminal,
}

impl Adapter {
    fn name(self) -> &'static str {
        match self {
            Self::Terminal => "terminal",
        }
    }
}

#[derive(Clone)]
pub struct Roots {
    db: Db,
}

impl Roots {
    pub fn new(db: Db) -> Self {
        Self { db }
    }

    /// Points `key` at a new root. The previous root keeps its history.
    pub async fn replace(&self, adapter: Adapter, key: String) -> Result<ThreadId, DbError> {
        self.db
            .call(move |connection| {
                let transaction = connection.transaction()?;
                let id = point(&transaction, adapter, &key)?;
                transaction.commit()?;

                Ok(ThreadId::from(id))
            })
            .await
    }
}

/// Starts a turn in `target`'s root, creating it on first use. Each turn
/// gets its own budget. Runs inside the caller's transaction.
pub fn turn(connection: &Connection, adapter: Adapter, target: &Target) -> Result<String, DbError> {
    let id = match target {
        Target::Fresh => {
            let id = new_id();
            register(connection, &id, adapter)?;
            id
        }
        Target::Named(key) => named(connection, adapter, key)?,
    };

    start_budget(connection, &id)?;

    Ok(id)
}

fn named(connection: &Connection, adapter: Adapter, key: &str) -> Result<String, DbError> {
    let sql = "SELECT root_id FROM conversations WHERE adapter = ?1 AND key = ?2";
    let found: Option<String> = connection.query_row(sql, params![adapter.name(), key], |row| row.get(0)).optional()?;

    match found {
        Some(id) => Ok(id),
        None => point(connection, adapter, key),
    }
}

/// Creates a chat root and its grant. Chat grants follow the current `[chat]` policy.
pub fn register(connection: &Connection, id: &str, adapter: Adapter) -> Result<(), DbError> {
    connection.execute("INSERT INTO threads (id, root_id) VALUES (?1, ?1)", [id])?;

    let grant = Grant { origin: Origin::Chat, source: adapter.name().to_owned(), caps: None };

    insert_grant(connection, id, &grant)
}

/// Creates a root and points `key` at it.
fn point(connection: &Connection, adapter: Adapter, key: &str) -> Result<String, DbError> {
    let id = new_id();
    register(connection, &id, adapter)?;

    let sql = "INSERT INTO conversations (adapter, key, root_id) VALUES (?1, ?2, ?3)
               ON CONFLICT (adapter, key) DO UPDATE SET root_id = excluded.root_id";
    connection.execute(sql, params![adapter.name(), key, &id])?;

    Ok(id)
}

fn new_id() -> String {
    uuid::Uuid::new_v4().to_string()
}

#[cfg(test)]
mod tests {
    use infinity_agent_core::ThreadId;

    use super::{Adapter, Roots, Target};
    use crate::error::BoxError;
    use crate::store::db::{Db, DbError};

    async fn count(db: &Db, sql: &'static str) -> Result<i64, BoxError> {
        Ok(db.call(move |connection| Ok(connection.query_row(sql, [], |row| row.get(0))?)).await?)
    }

    impl Roots {
        /// A turn in the terminal conversation `key`, in its own transaction.
        pub async fn turn(&self, adapter: Adapter, key: String) -> Result<ThreadId, DbError> {
            self.db
                .call(move |connection| {
                    let transaction = connection.transaction()?;
                    let id = super::turn(&transaction, adapter, &Target::Named(key))?;
                    transaction.commit()?;

                    Ok(ThreadId::from(id))
                })
                .await
        }
    }

    /// Two first messages racing for one name must share one root.
    #[tokio::test]
    async fn concurrent_first_use_creates_one_root() -> Result<(), BoxError> {
        let db = Db::in_memory()?;
        let roots = Roots::new(db.clone());

        let (first, second) = tokio::join!(
            roots.turn(Adapter::Terminal, "default".to_owned()),
            roots.turn(Adapter::Terminal, "default".to_owned())
        );

        assert_eq!(first?, second?);
        assert_eq!(count(&db, "SELECT COUNT(*) FROM threads").await?, 1);

        Ok(())
    }

    /// `/new` repoints the name and keeps the old root; other names are untouched.
    #[tokio::test]
    async fn replace_keeps_previous_root() -> Result<(), BoxError> {
        let db = Db::in_memory()?;
        let roots = Roots::new(db.clone());

        let old = roots.turn(Adapter::Terminal, "default".to_owned()).await?;
        let other = roots.turn(Adapter::Terminal, "work".to_owned()).await?;
        let new = roots.replace(Adapter::Terminal, "default".to_owned()).await?;

        assert_ne!(old, new);
        assert_eq!(roots.turn(Adapter::Terminal, "default".to_owned()).await?, new);
        assert_eq!(roots.turn(Adapter::Terminal, "work".to_owned()).await?, other);
        assert_eq!(count(&db, "SELECT COUNT(*) FROM threads").await?, 3);

        Ok(())
    }
}
