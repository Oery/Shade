//! Grants and budgets: whom a root acts for, and how much work it may do.
//!
//! A thread's grant is its structural root's, found through `threads.root_id`,
//! so Fresh-context children keep their root's authority. A thread's budget is
//! the one current when it was spawned: a new chat turn moves the root to a
//! new budget without resetting work still running from an earlier turn.

use infinity_agent_core::ThreadId;
use rusqlite::{Connection, OptionalExtension, params};

use crate::policy::{Charge, Grant, Limits, Origin, Usage};
use crate::store::db::{Db, DbError};

/// Why a budget admitted nothing.
#[derive(Debug, PartialEq)]
pub enum Refusal {
    /// The thread was never given a budget.
    NoBudget,
    /// The limit this budget reached, named as in the config.
    Limit(String),
}

#[derive(Clone)]
pub struct Grants {
    db: Db,
}

impl Grants {
    pub fn new(db: Db) -> Self {
        Self { db }
    }

    /// The grant of the root that owns `thread`. `None` for unknown threads,
    /// roots without a grant, and unknown origins: all of them fail closed.
    pub async fn of(&self, thread: &ThreadId<str>) -> Result<Option<Grant>, DbError> {
        let id = thread.as_str().to_owned();

        let row = self
            .db
            .call(move |connection| {
                let sql = "SELECT g.origin, g.source, g.caps FROM threads t JOIN grants g ON g.root_id = t.root_id WHERE t.id = ?1";
                let row = connection
                    .query_row(sql, [&id], |row| Ok((row.get::<_, String>(0)?, row.get(1)?, row.get::<_, Option<String>>(2)?)))
                    .optional()?;

                Ok(row)
            })
            .await?;

        let Some((origin, source, caps)) = row else {
            return Ok(None);
        };

        let Some(origin) = Origin::parse(&origin) else {
            tracing::warn!(%origin, "grant has an unknown origin");
            return Ok(None);
        };

        let caps = match caps {
            Some(json) => Some(serde_json::from_str(&json)?),
            None => None,
        };

        Ok(Some(Grant { origin, source, caps }))
    }

    /// Counts one unit of work against `thread`'s budget unless a limit
    /// refuses it. A refusal is final: the budget admits nothing afterwards.
    pub async fn charge(&self, thread: &ThreadId<str>, charge: Charge, limits: Limits) -> Result<Option<Refusal>, DbError> {
        let id = thread.as_str().to_owned();

        self.db.call(move |connection| charge_budget(connection, &id, charge, limits)).await
    }
}

pub fn insert_grant(connection: &Connection, root: &str, grant: &Grant) -> Result<(), DbError> {
    let caps = match &grant.caps {
        Some(caps) => Some(serde_json::to_string(caps)?),
        None => None,
    };

    let sql = "INSERT INTO grants (root_id, origin, source, caps) VALUES (?1, ?2, ?3, ?4)";
    connection.execute(sql, params![root, grant.origin.name(), &grant.source, caps])?;

    Ok(())
}

/// Moves `root` to a fresh budget. Threads spawned later share it.
pub fn start_budget(connection: &Connection, root: &str) -> Result<(), DbError> {
    connection.execute("INSERT INTO budgets (root_id) VALUES (?1)", [root])?;

    let budget = connection.last_insert_rowid();
    connection.execute("UPDATE threads SET budget_id = ?2 WHERE id = ?1", params![root, budget])?;

    Ok(())
}

fn charge_budget(connection: &mut Connection, thread: &str, charge: Charge, limits: Limits) -> Result<Option<Refusal>, DbError> {
    let transaction = connection.transaction()?;

    let sql = "SELECT b.id, b.completions, b.tool_calls, unixepoch() - b.started_at, b.exceeded
               FROM threads t JOIN budgets b ON b.id = t.budget_id WHERE t.id = ?1";
    let row = transaction
        .query_row(sql, [thread], |row| {
            let usage = Usage { completions: row.get(1)?, tool_calls: row.get(2)?, elapsed_seconds: row.get(3)? };

            Ok((row.get::<_, i64>(0)?, usage, row.get::<_, Option<String>>(4)?))
        })
        .optional()?;

    let Some((budget, usage, exceeded)) = row else {
        return Ok(Some(Refusal::NoBudget));
    };

    if let Some(limit) = exceeded {
        return Ok(Some(Refusal::Limit(limit)));
    }

    if let Some(limit) = limits.check(&usage, charge) {
        transaction.execute("UPDATE budgets SET exceeded = ?2 WHERE id = ?1", params![budget, limit])?;
        transaction.commit()?;
        tracing::warn!(budget, limit, "limit reached");

        return Ok(Some(Refusal::Limit(limit.to_owned())));
    }

    let sql = match charge {
        Charge::Completion => "UPDATE budgets SET completions = completions + 1 WHERE id = ?1",
        Charge::ToolCall => "UPDATE budgets SET tool_calls = tool_calls + 1 WHERE id = ?1",
    };

    transaction.execute(sql, [budget])?;
    transaction.commit()?;

    Ok(None)
}

#[cfg(test)]
mod tests {
    use infinity_agent_core::ThreadId;
    use infinity_agent_core::traits::{ConversationStore, SpawnContext};
    use rap_protocol::ToolCallId;

    use super::{Grants, Refusal};
    use crate::error::BoxError;
    use crate::policy::{Charge, Limits, Origin};
    use crate::store::conversation::Conversations;
    use crate::store::db::Db;
    use crate::store::roots::{Adapter, Roots};

    /// A child shares its turn's budget, keeps it after the next turn starts,
    /// and keeps its root's grant across a Fresh-context boundary.
    #[tokio::test]
    async fn children_share_their_turns_budget() -> Result<(), BoxError> {
        let db = Db::in_memory()?;
        let grants = Grants::new(db.clone());
        let roots = Roots::new(db.clone());
        let limits = Limits { max_tool_calls: 1, ..Limits::default() };

        let root = roots.turn(Adapter::Terminal, "default".to_owned()).await?;
        let call = ToolCallId::from("call-1".to_owned());
        let child: ThreadId = Conversations::new(db).spawn_thread(&root, &call, false, SpawnContext::Fresh).await?;

        let owner = grants.of(&child).await?;
        assert_eq!(owner.map(|grant| grant.origin), Some(Origin::Chat));

        // The child's call uses up the turn, so the root is refused too.
        assert_eq!(grants.charge(&child, Charge::ToolCall, limits).await?, None);
        let refused = Some(Refusal::Limit("max_tool_calls".to_owned()));
        assert_eq!(grants.charge(&root, Charge::ToolCall, limits).await?, refused);
        // A refusal is final, whatever is asked next.
        assert_eq!(grants.charge(&root, Charge::Completion, limits).await?, refused);

        // A new turn frees the root but not work left over from the old one.
        roots.turn(Adapter::Terminal, "default".to_owned()).await?;
        assert_eq!(grants.charge(&root, Charge::ToolCall, limits).await?, None);
        assert_eq!(grants.charge(&child, Charge::Completion, limits).await?, refused);

        Ok(())
    }
}
