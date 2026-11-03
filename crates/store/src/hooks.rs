//! Webhook events: accepted before the sender hears `202`, run later.
//!
//! Accepting checks the provider's delivery ID (a repeat gets the earlier
//! acceptance), the hook's queue and rate limits, and records the event in
//! one transaction. Starting an event registers a fresh root with the
//! hook's grant, a budget and an inbox input, so from there the ordinary
//! inbox replay carries it across restarts. Hook events and scheduled runs
//! share one background capacity.

use infinity_agent_core::ThreadId;
use rusqlite::{Connection, OptionalExtension, params};
use shade_policy::{Grant, Origin};

use crate::db::{Db, DbError};
use crate::grants::start_budget;
use crate::inbox::{self, Input};
use crate::roots::{insert_root, new_id};

/// How long finished events are kept, which bounds delivery-ID deduplication.
const RETENTION: i64 = 7 * 86_400;

/// Limits on what one hook may have accepted.
#[derive(Clone, Copy)]
pub struct Admission {
    /// Events waiting to start.
    pub queued: u32,
    /// Events accepted within the last minute.
    pub per_minute: u32,
}

/// What accepting an event did.
#[derive(Debug, PartialEq)]
pub enum Accepted {
    New(String),
    /// The provider delivered this ID before: the event it was accepted as.
    Repeated(String),
    /// Refused for now; the sender may retry. Names the limit.
    Busy(&'static str),
}

/// An event waiting to start.
pub struct Queued {
    pub event: String,
    pub hook: String,
    pub input: String,
}

#[derive(Clone)]
pub struct HookEvents {
    db: Db,
}

impl HookEvents {
    pub fn new(db: Db) -> Self {
        Self { db }
    }

    pub async fn accept(&self, hook: String, delivery: Option<String>, input: String, limits: Admission) -> Result<Accepted, DbError> {
        self.db
            .call(move |connection| {
                let transaction = connection.transaction()?;
                let accepted = accept_event(&transaction, &hook, delivery.as_deref(), &input, limits)?;
                transaction.commit()?;

                Ok(accepted)
            })
            .await
    }

    /// Queued events that fit in what `capacity` leaves after every running
    /// background run, oldest first.
    pub async fn startable(&self, capacity: u32) -> Result<Vec<Queued>, DbError> {
        self.db
            .call(move |connection| {
                let room = capacity.saturating_sub(running_background(connection)?);
                let sql = "SELECT id, hook, input FROM hook_events WHERE status = 'queued' ORDER BY received_at, rowid LIMIT ?1";
                let mut statement = connection.prepare(sql)?;
                let rows = statement.query_map([room], |row| Ok(Queued { event: row.get(0)?, hook: row.get(1)?, input: row.get(2)? }))?;

                Ok(rows.collect::<Result<_, _>>()?)
            })
            .await
    }

    /// Starts a queued event: its root, the hook's grant, a budget and the
    /// input, together. `None` when it is no longer queued.
    pub async fn start(&self, event: String) -> Result<Option<Input>, DbError> {
        self.db
            .call(move |connection| {
                let transaction = connection.transaction()?;
                let input = start_event(&transaction, &event)?;
                transaction.commit()?;

                Ok(input)
            })
            .await
    }

    /// Records how a running event ended. A failure after its budget ran out is `limit_exceeded`.
    pub async fn finish(&self, event: &str, error: Option<&str>) -> Result<(), DbError> {
        let (event, error) = (event.to_owned(), error.map(str::to_owned));

        self.db
            .call(move |connection| {
                let sql = "UPDATE hook_events SET finished_at = unixepoch(), error = ?2, status = CASE
                               WHEN ?2 IS NULL THEN 'completed'
                               WHEN (SELECT b.exceeded FROM threads t JOIN budgets b ON b.id = t.budget_id WHERE t.id = hook_events.root_id)
                                   IS NOT NULL THEN 'limit_exceeded'
                               ELSE 'failed' END
                           WHERE id = ?1 AND status = 'running'";
                connection.execute(sql, params![event, error])?;

                Ok(())
            })
            .await
    }

    /// The event and hook whose root is `root`, if it is one.
    pub async fn event_of(&self, root: &ThreadId<str>) -> Result<Option<(String, String)>, DbError> {
        let root = root.as_str().to_owned();

        self.db
            .call(move |connection| {
                let sql = "SELECT id, hook FROM hook_events WHERE root_id = ?1";

                Ok(connection.query_row(sql, [&root], |row| Ok((row.get(0)?, row.get(1)?))).optional()?)
            })
            .await
    }

    /// Marks running events whose roots are not in `resumed` as interrupted.
    /// Returns their roots, events and hooks.
    pub async fn interrupt_all_but(&self, resumed: Vec<String>) -> Result<Vec<(ThreadId, String, String)>, DbError> {
        self.db.call(move |connection| interrupt_events(connection, &resumed)).await
    }

    /// Forgets finished events past the retention window.
    pub async fn prune(&self) -> Result<usize, DbError> {
        self.db
            .call(|connection| {
                let sql = "DELETE FROM hook_events WHERE finished_at IS NOT NULL AND finished_at < unixepoch() - ?1";

                Ok(connection.execute(sql, [RETENTION])?)
            })
            .await
    }
}

#[cfg(any(test, feature = "testing"))]
impl HookEvents {
    /// Every event's status, oldest first.
    pub async fn statuses(&self) -> Result<Vec<String>, DbError> {
        self.db
            .call(|connection| {
                let mut statement = connection.prepare("SELECT status FROM hook_events ORDER BY received_at, rowid")?;
                let rows = statement.query_map([], |row| row.get(0))?;

                Ok(rows.collect::<Result<_, _>>()?)
            })
            .await
    }
}

/// Running scheduled runs and hook events together: the background work
/// the shared capacity counts.
pub(crate) fn running_background(connection: &Connection) -> Result<u32, DbError> {
    let sql = "SELECT (SELECT COUNT(*) FROM runs WHERE status = 'running') + (SELECT COUNT(*) FROM hook_events WHERE status = 'running')";

    Ok(connection.query_row(sql, [], |row| row.get(0))?)
}

fn accept_event(connection: &Connection, hook: &str, delivery: Option<&str>, input: &str, limits: Admission) -> Result<Accepted, DbError> {
    if let Some(delivery) = delivery {
        let sql = "SELECT id FROM hook_events WHERE hook = ?1 AND delivery_id = ?2";

        if let Some(event) = connection.query_row(sql, params![hook, delivery], |row| row.get(0)).optional()? {
            return Ok(Accepted::Repeated(event));
        }
    }

    let sql = "SELECT COUNT(*) FILTER (WHERE status = 'queued'), COUNT(*) FILTER (WHERE received_at > unixepoch() - 60)
               FROM hook_events WHERE hook = ?1";
    let (queued, recent): (u32, u32) = connection.query_row(sql, [hook], |row| Ok((row.get(0)?, row.get(1)?)))?;

    if queued >= limits.queued {
        return Ok(Accepted::Busy("queued events"));
    }

    if recent >= limits.per_minute {
        return Ok(Accepted::Busy("events per minute"));
    }

    let event = new_id();
    let sql = "INSERT INTO hook_events (id, hook, delivery_id, input) VALUES (?1, ?2, ?3, ?4)";
    connection.execute(sql, params![&event, hook, delivery, input])?;

    Ok(Accepted::New(event))
}

fn start_event(connection: &Connection, event: &str) -> Result<Option<Input>, DbError> {
    let sql = "SELECT hook, input FROM hook_events WHERE id = ?1 AND status = 'queued'";
    let row: Option<(String, String)> = connection.query_row(sql, [event], |row| Ok((row.get(0)?, row.get(1)?))).optional()?;

    let Some((hook, text)) = row else {
        return Ok(None);
    };

    // The grant names only the hook: its caps follow `[hooks.<name>]`, never the payload.
    let root = new_id();
    insert_root(connection, &root, &Grant { origin: Origin::Webhook, source: hook, caps: None })?;
    start_budget(connection, &root)?;

    let id = new_id();
    inbox::insert(connection, &id, &root, &text, Some(&format!("hook:{event}")))?;

    let sql = "UPDATE hook_events SET status = 'running', root_id = ?2, started_at = unixepoch() WHERE id = ?1";
    connection.execute(sql, params![event, &root])?;

    Ok(Some(Input { root: ThreadId::from(root), id, text }))
}

fn interrupt_events(connection: &mut Connection, resumed: &[String]) -> Result<Vec<(ThreadId, String, String)>, DbError> {
    let transaction = connection.transaction()?;
    let mut interrupted = Vec::new();

    let mut statement = transaction.prepare("SELECT id, root_id, hook FROM hook_events WHERE status = 'running'")?;
    let rows = statement.query_map([], |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?, row.get::<_, String>(2)?)))?;

    for row in rows {
        let (event, root, hook) = row?;

        if !resumed.contains(&root) {
            interrupted.push((ThreadId::from(root), event, hook));
        }
    }

    drop(statement);

    for (_, event, _) in &interrupted {
        let sql =
            "UPDATE hook_events SET status = 'interrupted', finished_at = unixepoch(), error = 'interrupted by a restart' WHERE id = ?1";
        transaction.execute(sql, [event])?;
    }

    transaction.commit()?;

    Ok(interrupted)
}

#[cfg(test)]
mod tests {
    use shade_policy::{Grant, Origin};

    use super::{Accepted, Admission, HookEvents};
    use crate::BoxError;
    use crate::db::Db;
    use crate::grants::Grants;

    const ROOMY: Admission = Admission { queued: 100, per_minute: 100 };

    async fn accept(events: &HookEvents, hook: &str, delivery: Option<&str>, limits: Admission) -> Result<Accepted, BoxError> {
        Ok(events.accept(hook.to_owned(), delivery.map(str::to_owned), "body".to_owned(), limits).await?)
    }

    fn new(accepted: &Accepted) -> Option<&str> {
        match accepted {
            Accepted::New(event) => Some(event),
            _ => None,
        }
    }

    /// A delivery ID is accepted once per hook; requests without one are
    /// separate events even when identical; limits refuse, for a retry.
    #[tokio::test]
    async fn deliveries_are_accepted_once_and_within_limits() -> Result<(), BoxError> {
        let events = HookEvents::new(Db::in_memory()?);

        let first = accept(&events, "github", Some("d1"), ROOMY).await?;
        let Some(event) = new(&first) else {
            return Err(format!("{first:?}").into());
        };

        assert_eq!(accept(&events, "github", Some("d1"), ROOMY).await?, Accepted::Repeated(event.to_owned()));
        assert!(new(&accept(&events, "gitlab", Some("d1"), ROOMY).await?).is_some(), "IDs belong to their hook");

        let (one, two) = (accept(&events, "github", None, ROOMY).await?, accept(&events, "github", None, ROOMY).await?);
        assert!(new(&one).is_some() && new(&two).is_some() && one != two, "identical events without IDs were merged");

        let full = Admission { queued: 3, per_minute: 100 };
        assert_eq!(accept(&events, "github", Some("d2"), full).await?, Accepted::Busy("queued events"));

        let slow = Admission { queued: 100, per_minute: 1 };
        assert!(new(&accept(&events, "rate", None, slow).await?).is_some());
        assert_eq!(accept(&events, "rate", None, slow).await?, Accepted::Busy("events per minute"));

        Ok(())
    }

    /// A run's grant names only its hook, whatever the payload says.
    #[tokio::test]
    async fn an_events_grant_comes_from_its_hook() -> Result<(), BoxError> {
        let db = Db::in_memory()?;
        let events = HookEvents::new(db.clone());
        let body = r#"{"caps": ["shell", "schedules"], "origin": "chat", "source": "terminal"}"#;
        events.accept("github".to_owned(), None, body.to_owned(), ROOMY).await?;

        let Some(queued) = events.startable(4).await?.into_iter().next() else {
            return Err("nothing to start".into());
        };

        let Some(input) = events.start(queued.event).await? else {
            return Err("the event did not start".into());
        };

        let grant = Grants::new(db).of(&input.root).await?;
        assert_eq!(grant, Some(Grant { origin: Origin::Webhook, source: "github".to_owned(), caps: None }));

        Ok(())
    }
}
