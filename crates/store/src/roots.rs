//! Root registration: the only way conversations come into existence.
//!
//! Every entry point creates roots here, together with their grant, before
//! Infinity attaches, so thread configuration never resolves for a thread
//! Shade did not register.

use infinity_agent_core::ThreadId;
use rusqlite::{Connection, OptionalExtension, params};
use shade_policy::{Grant, Origin};

use crate::db::{Db, DbError};
use crate::grants::{insert_grant, start_budget};

/// Which conversation an input belongs to.
pub enum Target {
    /// A new root reachable by no name (`shade ask`).
    Fresh,
    /// A named conversation, created on first use.
    Named(String),
    /// A named conversation that is the owner's direct messages.
    Direct(String),
}

/// A named conversation.
pub struct Conversation {
    pub adapter: Adapter,
    pub key: String,
    /// The owner's direct messages, rather than a channel others may read.
    pub direct: bool,
}

/// Where a named conversation lives. Names are scoped per adapter.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Adapter {
    Terminal,
    /// Keys are channel IDs.
    Discord,
}

impl Adapter {
    fn name(self) -> &'static str {
        match self {
            Self::Terminal => "terminal",
            Self::Discord => "discord",
        }
    }

    fn parse(name: &str) -> Option<Self> {
        [Self::Terminal, Self::Discord].into_iter().find(|adapter| adapter.name() == name)
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

    /// A root with no grant, which no entry point creates, for tests.
    #[cfg(any(test, feature = "testing"))]
    pub async fn insert_ungranted(&self, id: &'static str) -> Result<(), DbError> {
        self.db.call(move |connection| Ok(connection.execute("INSERT INTO threads (id, root_id) VALUES (?1, ?1)", [id]).map(drop)?)).await
    }

    /// The conversation name that currently points at the root of `thread`, if any.
    pub async fn conversation_of(&self, thread: &ThreadId<str>) -> Result<Option<Conversation>, DbError> {
        let id = thread.as_str().to_owned();

        let row: Option<(String, String, bool)> = self
            .db
            .call(move |connection| {
                let sql = "SELECT c.adapter, c.key, c.direct FROM threads t JOIN conversations c ON c.root_id = t.root_id WHERE t.id = ?1";

                Ok(connection.query_row(sql, [&id], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?))).optional()?)
            })
            .await?;

        let Some((adapter, key, direct)) = row else {
            return Ok(None);
        };

        Ok(Adapter::parse(&adapter).map(|adapter| Conversation { adapter, key, direct }))
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
pub(crate) fn turn(connection: &Connection, adapter: Adapter, target: &Target) -> Result<String, DbError> {
    let id = match target {
        Target::Fresh => {
            let id = new_id();
            register(connection, &id, adapter)?;
            id
        }
        Target::Named(key) => named(connection, adapter, key)?,
        Target::Direct(key) => {
            let id = named(connection, adapter, key)?;
            let sql = "UPDATE conversations SET direct = 1 WHERE adapter = ?1 AND key = ?2";
            connection.execute(sql, params![adapter.name(), key])?;
            id
        }
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
pub(crate) fn register(connection: &Connection, id: &str, adapter: Adapter) -> Result<(), DbError> {
    let grant = Grant { origin: Origin::Chat, source: adapter.name().to_owned(), caps: None };

    insert_root(connection, id, &grant)
}

/// Creates a root with its grant: the only way a root comes to exist.
pub(crate) fn insert_root(connection: &Connection, id: &str, grant: &Grant) -> Result<(), DbError> {
    connection.execute("INSERT INTO threads (id, root_id) VALUES (?1, ?1)", [id])?;

    insert_grant(connection, id, grant)
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

pub(crate) fn new_id() -> String {
    uuid::Uuid::new_v4().to_string()
}

#[cfg(test)]
mod tests {
    use infinity_agent_core::ThreadId;

    use super::{Adapter, Roots, Target};
    use crate::BoxError;
    use crate::db::{Db, DbError};

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

    /// A DM stays known as one after `/new` points it at a fresh root.
    #[tokio::test]
    async fn direct_messages_stay_direct_after_new() -> Result<(), BoxError> {
        let db = Db::in_memory()?;
        let roots = Roots::new(db.clone());

        db.call(|connection| {
            let transaction = connection.transaction()?;
            super::turn(&transaction, Adapter::Discord, &Target::Direct("7".to_owned()))?;
            super::turn(&transaction, Adapter::Discord, &Target::Named("8".to_owned()))?;
            transaction.commit()?;

            Ok(())
        })
        .await?;

        let fresh = roots.replace(Adapter::Discord, "7".to_owned()).await?;
        let server = roots.turn(Adapter::Discord, "8".to_owned()).await?;

        for (root, direct) in [(fresh, true), (server, false)] {
            let found = roots.conversation_of(&root).await?.map(|conversation| conversation.direct);
            assert_eq!(found, Some(direct), "{root}");
        }

        Ok(())
    }
}
