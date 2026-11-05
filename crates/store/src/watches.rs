//! Watches on MCP resources and what they fired.
//!
//! An observation is one transaction: the watch's state moves on, and what
//! it fired is queued, for either destination. A queued run starts later in
//! the capacity background work shares, under a fresh root; a conversation
//! watch's is delivered to the conversation that created it. An observation
//! carries the version it evaluated, so a read that raced a change to the
//! watch writes nothing.

use infinity_agent_core::ThreadId;
use rusqlite::{Connection, OptionalExtension, Row, params};
use shade_policy::{Grant, Origin};

use crate::db::{Db, DbError};
use crate::grants::start_budget;
use crate::hooks::running_background;
use crate::inbox::{self, Input};
use crate::operations;
use crate::roots::{Adapter, insert_root, new_id};
use crate::schedules::{Applied, Receipt};

/// Most watches that may exist at once, paused ones included.
pub const MAX_WATCHES: i64 = 50;

/// Consecutive failures before the owner hears about them.
pub const ALERT_AFTER: i64 = 5;

/// When a matching observation fires.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Mode {
    /// Only when the condition turns true.
    OnChange,
    /// Whenever it is true.
    EveryMatch,
}

impl Mode {
    pub fn name(self) -> &'static str {
        match self {
            Self::OnChange => "on_change",
            Self::EveryMatch => "every_match",
        }
    }

    pub fn parse(name: &str) -> Option<Self> {
        [Self::OnChange, Self::EveryMatch].into_iter().find(|mode| mode.name() == name)
    }
}

/// Where a firing goes.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Destination {
    /// A fresh background run holding the watch's caps.
    Run,
    /// A turn in the conversation that created it, which ends the watch.
    Conversation,
}

impl Destination {
    pub fn name(self) -> &'static str {
        match self {
            Self::Run => "run",
            Self::Conversation => "conversation",
        }
    }

    pub fn parse(name: &str) -> Option<Self> {
        [Self::Run, Self::Conversation].into_iter().find(|destination| destination.name() == name)
    }
}

/// What a watch observes and does.
#[derive(Debug, PartialEq)]
pub struct Definition {
    pub title: String,
    pub server: String,
    pub uri: String,
    /// A typed condition, as JSON.
    pub condition: String,
    pub mode: Mode,
    pub cooldown_seconds: i64,
    /// Oldest acceptable observation, for a resource with a freshness envelope.
    pub max_age_seconds: Option<i64>,
    pub prompt: String,
    /// Delegated to each run. Empty for a conversation watch.
    pub caps: Vec<String>,
    pub destination: Destination,
}

/// A stored watch.
#[derive(Debug)]
pub struct Watch {
    pub id: String,
    pub version: i64,
    pub definition: Definition,
    pub enabled: bool,
    pub creator_root: Option<String>,
    /// `None` until the first successful observation sets the baseline.
    pub last_condition: Option<bool>,
    pub last_value: Option<String>,
    pub last_error: Option<String>,
    pub failures: i64,
    pub last_fired_at: Option<i64>,
}

/// A change made by a watch tool.
pub enum Change {
    Create {
        id: String,
        creator_root: String,
        definition: Definition,
    },
    /// Applies only if the watch is still at `version`.
    Update {
        id: String,
        version: i64,
        enabled: bool,
        definition: Definition,
    },
    Delete {
        id: String,
    },
}

/// A successful observation of one watch.
pub struct Observation {
    pub watch: String,
    /// The version it was evaluated under.
    pub version: i64,
    pub condition: bool,
    pub value: String,
    /// The input of what it fired, if it did.
    pub fired: Option<String>,
    pub at: i64,
}

/// A run that may start, or a firing to deliver.
#[derive(Debug)]
pub struct Queued {
    pub run: String,
    pub watch: String,
    pub title: String,
    pub input: String,
    /// The conversation it goes to; `None` for a fresh run.
    pub conversation: Option<String>,
}

/// A run, as its outcome reporter needs it.
pub struct Run {
    pub id: String,
    pub title: String,
}

#[derive(Clone)]
pub struct Watches {
    db: Db,
}

impl Watches {
    pub fn new(db: Db) -> Self {
        Self { db }
    }

    /// Applies `change` and records its result as the call's, in one
    /// transaction: a call answered before replays its result instead.
    pub async fn apply(&self, change: Change, receipt: Receipt) -> Result<Applied, DbError> {
        self.db
            .call(move |connection| {
                let transaction = connection.transaction()?;

                if let Some(result) = operations::captured(&transaction, &receipt.thread, &receipt.call)? {
                    return Ok(Applied::Replayed(result));
                }

                let applied = match apply_change(&transaction, change)? {
                    Some(refusal) => Applied::Refused(format!("Error: {refusal}")),
                    None => Applied::Done(receipt.text),
                };

                let result = match &applied {
                    Applied::Done(text) | Applied::Refused(text) | Applied::Replayed(text) => text,
                };

                operations::insert_done(&transaction, &receipt.thread, &receipt.call, receipt.tool, result)?;
                transaction.commit()?;

                Ok(applied)
            })
            .await
    }

    pub async fn get(&self, id: &str) -> Result<Option<Watch>, DbError> {
        let id = id.to_owned();

        self.db
            .call(move |connection| {
                let sql = format!("SELECT {COLUMNS} FROM watches WHERE id = ?1 AND deleted_at IS NULL");

                Ok(connection.query_row(&sql, [&id], watch_row).optional()?)
            })
            .await
    }

    /// Every watch, oldest first.
    pub async fn list(&self) -> Result<Vec<Watch>, DbError> {
        self.db
            .call(|connection| {
                let sql = format!("SELECT {COLUMNS} FROM watches WHERE deleted_at IS NULL ORDER BY created_at, rowid");
                let mut statement = connection.prepare(&sql)?;
                let rows = statement.query_map([], watch_row)?;

                Ok(rows.collect::<Result<_, _>>()?)
            })
            .await
    }

    /// Every `(server, uri)` an active watch observes.
    pub async fn sources(&self) -> Result<Vec<(String, String)>, DbError> {
        self.db
            .call(|connection| {
                let sql = "SELECT DISTINCT server, uri FROM watches WHERE enabled = 1 AND deleted_at IS NULL ORDER BY server, uri";
                let mut statement = connection.prepare(sql)?;
                let rows = statement.query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?;

                Ok(rows.collect::<Result<_, _>>()?)
            })
            .await
    }

    /// The active watches on one resource.
    pub async fn of_source(&self, server: &str, uri: &str) -> Result<Vec<Watch>, DbError> {
        let (server, uri) = (server.to_owned(), uri.to_owned());

        self.db
            .call(move |connection| {
                let sql = format!("SELECT {COLUMNS} FROM watches WHERE server = ?1 AND uri = ?2 AND enabled = 1 AND deleted_at IS NULL");
                let mut statement = connection.prepare(&sql)?;
                let rows = statement.query_map([&server, &uri], watch_row)?;

                Ok(rows.collect::<Result<_, _>>()?)
            })
            .await
    }

    /// Records an observation and what it fired. `false` when the watch
    /// changed since it was read, in which case nothing was written.
    pub async fn observe(&self, observation: Observation) -> Result<bool, DbError> {
        self.db
            .call(move |connection| {
                let transaction = connection.transaction()?;
                let recorded = record(&transaction, &observation)?;
                transaction.commit()?;

                Ok(recorded)
            })
            .await
    }

    /// Records a failed observation, leaving the condition state as it was.
    /// `true` when this failure is the one to tell the owner about.
    pub async fn fail(&self, watch: &str, version: i64, error: &str) -> Result<bool, DbError> {
        let (watch, error) = (watch.to_owned(), error.to_owned());

        self.db
            .call(move |connection| {
                let sql = "UPDATE watches SET failures = failures + 1, last_error = ?3 WHERE id = ?1 AND version = ?2";
                connection.execute(sql, params![watch, version, error])?;

                let sql = "UPDATE watches SET alerted = 1 WHERE id = ?1 AND version = ?2 AND alerted = 0 AND failures >= ?3";

                Ok(connection.execute(sql, params![watch, version, ALERT_AFTER])? == 1)
            })
            .await
    }

    /// Queued runs that fit in what `capacity` leaves after every running
    /// background run, oldest first. A watch has at most one running.
    pub async fn startable(&self, capacity: u32) -> Result<Vec<Queued>, DbError> {
        self.db
            .call(move |connection| {
                let room = capacity.saturating_sub(running_background(connection)?);
                let sql = "SELECT r.id, w.id, w.title, r.input, NULL
                           FROM watch_runs r JOIN watches w ON w.id = r.watch_id
                           WHERE r.status = 'queued' AND w.destination = 'run'
                           AND NOT EXISTS (SELECT 1 FROM watch_runs a WHERE a.watch_id = r.watch_id AND a.status = 'running')
                           ORDER BY r.fired_at, r.rowid LIMIT ?1";
                let mut statement = connection.prepare(sql)?;
                let rows = statement.query_map([room], queued_row)?;

                Ok(rows.collect::<Result<_, _>>()?)
            })
            .await
    }

    /// Firings waiting to reach their conversation, oldest first.
    pub async fn deliverable(&self) -> Result<Vec<Queued>, DbError> {
        self.db
            .call(|connection| {
                let sql = "SELECT r.id, w.id, w.title, r.input, w.creator_root
                           FROM watch_runs r JOIN watches w ON w.id = r.watch_id
                           WHERE r.status = 'queued' AND w.destination = 'conversation'
                           ORDER BY r.fired_at, r.rowid";
                let mut statement = connection.prepare(sql)?;
                let rows = statement.query_map([], queued_row)?;

                Ok(rows.collect::<Result<_, _>>()?)
            })
            .await
    }

    /// Starts a queued run: its root, delegated grant, budget and input are
    /// recorded together. `None` when the run is no longer queued.
    pub async fn start(&self, run: String) -> Result<Option<Input>, DbError> {
        self.db
            .call(move |connection| {
                let transaction = connection.transaction()?;
                let input = start_run(&transaction, &run)?;
                transaction.commit()?;

                Ok(input)
            })
            .await
    }

    /// Marks a firing as handed to its conversation.
    pub async fn delivered(&self, run: String) -> Result<(), DbError> {
        self.db
            .call(move |connection| {
                let sql = "UPDATE watch_runs SET status = 'delivered', finished_at = unixepoch() WHERE id = ?1 AND status = 'queued'";
                connection.execute(sql, [&run])?;

                Ok(())
            })
            .await
    }

    /// Records how a running run ended. A failure after its budget ran out is `limit_exceeded`.
    pub async fn finish(&self, run: &str, error: Option<&str>) -> Result<(), DbError> {
        let (run, error) = (run.to_owned(), error.map(str::to_owned));

        self.db
            .call(move |connection| {
                let sql = "UPDATE watch_runs SET finished_at = unixepoch(), error = ?2, status = CASE
                               WHEN ?2 IS NULL THEN 'completed'
                               WHEN (SELECT b.exceeded FROM threads t JOIN budgets b ON b.id = t.budget_id WHERE t.id = watch_runs.root_id)
                                   IS NOT NULL THEN 'limit_exceeded'
                               ELSE 'failed' END
                           WHERE id = ?1 AND status = 'running'";
                connection.execute(sql, params![run, error])?;

                Ok(())
            })
            .await
    }

    /// The run whose root is `root`, if it is one.
    pub async fn run_of(&self, root: &ThreadId<str>) -> Result<Option<Run>, DbError> {
        let root = root.as_str().to_owned();

        self.db
            .call(move |connection| {
                let sql = "SELECT r.id, w.title FROM watch_runs r JOIN watches w ON w.id = r.watch_id WHERE r.root_id = ?1";

                Ok(connection.query_row(sql, [&root], |row| Ok(Run { id: row.get(0)?, title: row.get(1)? })).optional()?)
            })
            .await
    }

    /// Interrupts running runs whose root is not in `resumed`, returning them.
    pub async fn interrupt_all_but(&self, resumed: Vec<String>) -> Result<Vec<(ThreadId, Run)>, DbError> {
        self.db.call(move |connection| interrupt_runs(connection, &resumed)).await
    }

    /// Stops observing a watch, keeping its baseline. `false` when there is no such watch.
    pub async fn pause(&self, id: String) -> Result<bool, DbError> {
        self.set_enabled(id, false).await
    }

    /// Observes a watch again, from its baseline. `false` when there is no such watch.
    pub async fn resume(&self, id: String) -> Result<bool, DbError> {
        self.set_enabled(id, true).await
    }

    async fn set_enabled(&self, id: String, enabled: bool) -> Result<bool, DbError> {
        let sql = "UPDATE watches SET enabled = ?2 WHERE id = ?1 AND deleted_at IS NULL";

        self.db.call(move |connection| Ok(connection.execute(sql, params![id, enabled])? == 1)).await
    }

    /// Deletes a watch. Its runs are kept. `false` when there is no such watch.
    pub async fn delete(&self, id: String) -> Result<bool, DbError> {
        self.db.call(move |connection| delete_watch(connection, &id)).await
    }
}

/// Ends the conversation watches of the root `key` points at, which is about
/// to be replaced. Runs inside the caller's transaction.
pub(crate) fn end_with_conversation(connection: &Connection, adapter: Adapter, key: &str) -> Result<(), DbError> {
    let sql = "UPDATE watches SET enabled = 0, deleted_at = unixepoch()
               WHERE destination = 'conversation' AND deleted_at IS NULL
               AND creator_root = (SELECT root_id FROM conversations WHERE adapter = ?1 AND key = ?2)";
    connection.execute(sql, params![adapter.name(), key])?;

    Ok(())
}

fn delete_watch(connection: &Connection, id: &str) -> Result<bool, DbError> {
    let sql = "UPDATE watches SET enabled = 0, deleted_at = unixepoch() WHERE id = ?1 AND deleted_at IS NULL";

    Ok(connection.execute(sql, [id])? == 1)
}

/// What a change refuses, or `None` once it is applied.
fn apply_change(connection: &Connection, change: Change) -> Result<Option<String>, DbError> {
    match change {
        Change::Create { id, creator_root, definition } => create(connection, &id, &creator_root, &definition),
        Change::Update { id, version, enabled, definition } => update(connection, &id, version, enabled, &definition),
        Change::Delete { id } => match delete_watch(connection, &id)? {
            true => Ok(None),
            false => Ok(Some(format!("no watch {id}"))),
        },
    }
}

fn create(connection: &Connection, id: &str, creator_root: &str, definition: &Definition) -> Result<Option<String>, DbError> {
    let count: i64 = connection.query_row("SELECT COUNT(*) FROM watches WHERE deleted_at IS NULL", [], |row| row.get(0))?;

    if count >= MAX_WATCHES {
        return Ok(Some(format!("there are already {MAX_WATCHES} watches; delete one first")));
    }

    let sql = "INSERT INTO watches (id, title, server, uri, condition, mode, cooldown_seconds, max_age_seconds, prompt, caps,
                                    destination, creator_root)
               VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)";
    let caps = serde_json::to_string(&definition.caps)?;

    connection.execute(
        sql,
        params![
            id,
            definition.title,
            definition.server,
            definition.uri,
            definition.condition,
            definition.mode.name(),
            definition.cooldown_seconds,
            definition.max_age_seconds,
            definition.prompt,
            caps,
            definition.destination.name(),
            creator_root,
        ],
    )?;

    Ok(None)
}

/// A change to what is observed or how starts a new baseline; a new prompt or
/// title keeps it. The new version drops any read still in flight.
fn update(connection: &Connection, id: &str, version: i64, enabled: bool, definition: &Definition) -> Result<Option<String>, DbError> {
    let sql = "UPDATE watches SET
                   last_condition = CASE WHEN server = ?3 AND uri = ?4 AND condition = ?5 THEN last_condition END,
                   last_value = CASE WHEN server = ?3 AND uri = ?4 AND condition = ?5 THEN last_value END,
                   title = ?6, server = ?3, uri = ?4, condition = ?5, mode = ?7, cooldown_seconds = ?8, max_age_seconds = ?9,
                   prompt = ?10, caps = ?11, enabled = ?12, version = version + 1
               WHERE id = ?1 AND version = ?2 AND deleted_at IS NULL";
    let caps = serde_json::to_string(&definition.caps)?;

    let changed = connection.execute(
        sql,
        params![
            id,
            version,
            definition.server,
            definition.uri,
            definition.condition,
            definition.title,
            definition.mode.name(),
            definition.cooldown_seconds,
            definition.max_age_seconds,
            definition.prompt,
            caps,
            enabled,
        ],
    )?;

    match changed {
        1 => Ok(None),
        _ => Ok(Some(format!("watch {id} changed since it was read; read it and try again"))),
    }
}

/// A conversation watch that fired is done, in the same transaction.
fn record(connection: &Connection, observation: &Observation) -> Result<bool, DbError> {
    let Observation { watch, version, condition, value, fired, at } = observation;

    let sql = "UPDATE watches SET last_condition = ?3, last_value = ?4, last_error = NULL, failures = 0, alerted = 0,
                   last_fired_at = CASE WHEN ?5 THEN ?6 ELSE last_fired_at END
               WHERE id = ?1 AND version = ?2 AND enabled = 1 AND deleted_at IS NULL";

    if connection.execute(sql, params![watch, version, condition, value, fired.is_some(), at])? == 0 {
        return Ok(false);
    }

    let Some(input) = fired else {
        return Ok(true);
    };

    let sql = "UPDATE watches SET enabled = 0, deleted_at = ?2 WHERE id = ?1 AND destination = 'conversation'";
    connection.execute(sql, params![watch, at])?;

    let waiting =
        connection.query_row("SELECT 1 FROM watch_runs WHERE watch_id = ?1 AND status = 'queued'", [watch], |_| Ok(())).optional()?;

    if waiting.is_none() {
        let sql = "INSERT INTO watch_runs (id, watch_id, version, input, fired_at) VALUES (?1, ?2, ?3, ?4, ?5)";
        connection.execute(sql, params![new_id(), watch, version, input, at])?;
    }

    Ok(true)
}

fn start_run(connection: &Connection, run: &str) -> Result<Option<Input>, DbError> {
    let sql = "SELECT w.id, w.caps, r.input FROM watch_runs r JOIN watches w ON w.id = r.watch_id WHERE r.id = ?1 AND r.status = 'queued'";
    let row: Option<(String, String, String)> =
        connection.query_row(sql, [run], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?))).optional()?;

    let Some((watch, caps, text)) = row else {
        return Ok(None);
    };

    let root = new_id();
    let grant = Grant { origin: Origin::Watch, source: watch, caps: Some(serde_json::from_str(&caps)?) };
    insert_root(connection, &root, &grant)?;
    start_budget(connection, &root)?;

    let id = new_id();
    inbox::insert(connection, &id, &root, &text, Some(&source_key(run)))?;

    let sql = "UPDATE watch_runs SET status = 'running', root_id = ?2, started_at = unixepoch() WHERE id = ?1";
    connection.execute(sql, params![run, &root])?;

    Ok(Some(Input { root: ThreadId::from(root), id, text }))
}

/// The inbox key of a firing, which makes delivering it again harmless.
pub fn source_key(run: &str) -> String {
    format!("watch-run:{run}")
}

fn interrupt_runs(connection: &mut Connection, resumed: &[String]) -> Result<Vec<(ThreadId, Run)>, DbError> {
    let transaction = connection.transaction()?;
    let mut interrupted = Vec::new();

    let sql = "SELECT r.id, r.root_id, w.title FROM watch_runs r JOIN watches w ON w.id = r.watch_id WHERE r.status = 'running'";
    let mut statement = transaction.prepare(sql)?;
    let rows = statement.query_map([], |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?, row.get::<_, String>(2)?)))?;

    for row in rows {
        let (id, root, title) = row?;

        if !resumed.contains(&root) {
            interrupted.push((ThreadId::from(root), Run { id, title }));
        }
    }

    drop(statement);

    for (_, run) in &interrupted {
        let sql =
            "UPDATE watch_runs SET status = 'interrupted', finished_at = unixepoch(), error = 'interrupted by a restart' WHERE id = ?1";
        transaction.execute(sql, [&run.id])?;
    }

    transaction.commit()?;

    Ok(interrupted)
}

const COLUMNS: &str = "id, version, title, server, uri, condition, mode, cooldown_seconds, max_age_seconds, prompt, caps, destination,
                       enabled, creator_root, last_condition, last_value, last_error, failures, last_fired_at";

fn watch_row(row: &Row<'_>) -> rusqlite::Result<Watch> {
    let mode: String = row.get(6)?;
    let caps: String = row.get(10)?;
    let destination: String = row.get(11)?;

    let definition = Definition {
        title: row.get(2)?,
        server: row.get(3)?,
        uri: row.get(4)?,
        condition: row.get(5)?,
        mode: Mode::parse(&mode).unwrap_or(Mode::OnChange),
        cooldown_seconds: row.get(7)?,
        max_age_seconds: row.get(8)?,
        prompt: row.get(9)?,
        caps: serde_json::from_str(&caps).unwrap_or_default(),
        destination: Destination::parse(&destination).unwrap_or(Destination::Run),
    };

    Ok(Watch {
        id: row.get(0)?,
        version: row.get(1)?,
        definition,
        enabled: row.get(12)?,
        creator_root: row.get(13)?,
        last_condition: row.get(14)?,
        last_value: row.get(15)?,
        last_error: row.get(16)?,
        failures: row.get(17)?,
        last_fired_at: row.get(18)?,
    })
}

fn queued_row(row: &Row<'_>) -> rusqlite::Result<Queued> {
    Ok(Queued { run: row.get(0)?, watch: row.get(1)?, title: row.get(2)?, input: row.get(3)?, conversation: row.get(4)? })
}

#[cfg(test)]
mod tests {
    use infinity_agent_core::ThreadId;

    use super::{Change, Definition, Destination, Mode, Observation, Watches};
    use crate::BoxError;
    use crate::conversation::Conversations;
    use crate::db::Db;
    use crate::roots::{Adapter, Roots};
    use crate::schedules::{Applied, Receipt};

    fn definition(destination: Destination) -> Definition {
        Definition {
            title: "Price".to_owned(),
            server: "feed".to_owned(),
            uri: "feed://price".to_owned(),
            condition: r#"{"op":">","value":100}"#.to_owned(),
            mode: Mode::OnChange,
            cooldown_seconds: 300,
            max_age_seconds: None,
            prompt: "Tell me.".to_owned(),
            caps: vec!["notify".to_owned()],
            destination,
        }
    }

    fn receipt(call: &str) -> Receipt {
        Receipt { thread: "chat".to_owned(), call: call.to_owned(), tool: "watch_create", text: "done".to_owned() }
    }

    fn observation(version: i64, fired: Option<&str>) -> Observation {
        Observation { watch: "w".to_owned(), version, condition: true, value: "110".to_owned(), fired: fired.map(str::to_owned), at: 1000 }
    }

    async fn setup(destination: Destination) -> Result<(Db, Watches), BoxError> {
        let db = Db::in_memory()?;
        Conversations::new(db.clone()).register_root(&ThreadId::from("chat".to_owned())).await?;

        let watches = Watches::new(db.clone());
        let create = Change::Create { id: "w".to_owned(), creator_root: "chat".to_owned(), definition: definition(destination) };
        watches.apply(create, receipt("create")).await?;

        Ok((db, watches))
    }

    /// A read that raced a change to its watch writes nothing, neither the
    /// state nor a run. A changed source loses its baseline, a changed
    /// prompt keeps it.
    #[tokio::test]
    async fn observations_of_an_old_version_write_nothing() -> Result<(), BoxError> {
        let (_db, watches) = setup(Destination::Run).await?;
        assert!(watches.observe(observation(1, None)).await?);

        let prompt = Definition { prompt: "New.".to_owned(), ..definition(Destination::Run) };
        let update = Change::Update { id: "w".to_owned(), version: 1, enabled: true, definition: prompt };
        watches.apply(update, receipt("prompt")).await?;

        assert!(!watches.observe(observation(1, Some("fired"))).await?);
        assert!(watches.startable(4).await?.is_empty());

        let kept = watches.get("w").await?.ok_or("watch")?;
        assert_eq!(kept.last_condition, Some(true));

        let source = Definition { uri: "feed://other".to_owned(), ..definition(Destination::Run) };
        let update = Change::Update { id: "w".to_owned(), version: 2, enabled: true, definition: source };
        watches.apply(update, receipt("source")).await?;

        let reset = watches.get("w").await?.ok_or("watch")?;
        assert_eq!(reset.last_condition, None);

        Ok(())
    }

    /// One failure reports nothing and leaves the condition alone; the fifth
    /// in a row reports once, and only a success starts a new episode.
    #[tokio::test]
    async fn a_failure_episode_is_reported_once() -> Result<(), BoxError> {
        let (_db, watches) = setup(Destination::Run).await?;
        watches.observe(observation(1, None)).await?;

        let mut alerts = Vec::new();

        for _ in 0..7 {
            alerts.push(watches.fail("w", 1, "no answer").await?);
        }

        assert_eq!(alerts, [false, false, false, false, true, false, false]);
        assert_eq!(watches.get("w").await?.ok_or("watch")?.last_condition, Some(true));

        watches.observe(observation(1, None)).await?;

        let mut alerts = Vec::new();

        for _ in 0..5 {
            alerts.push(watches.fail("w", 1, "no answer").await?);
        }

        assert_eq!(alerts, [false, false, false, false, true]);

        Ok(())
    }

    /// A firing while one waits moves the state on without a second run.
    #[tokio::test]
    async fn a_watch_has_one_queued_run() -> Result<(), BoxError> {
        let (_db, watches) = setup(Destination::Run).await?;

        assert!(watches.observe(observation(1, Some("first"))).await?);
        assert!(watches.observe(observation(1, Some("second"))).await?);

        let queued = watches.startable(4).await?;
        assert_eq!(queued.iter().map(|run| run.input.as_str()).collect::<Vec<_>>(), ["first"]);
        assert!(watches.startable(0).await?.is_empty());

        Ok(())
    }

    /// A conversation watch ends with the firing that queued its delivery:
    /// the same transaction, so it cannot fire twice or end without one.
    #[tokio::test]
    async fn a_conversation_watch_fires_once() -> Result<(), BoxError> {
        let (_db, watches) = setup(Destination::Conversation).await?;

        assert!(watches.observe(observation(1, Some("fired"))).await?);

        assert!(watches.get("w").await?.is_none());
        assert!(watches.sources().await?.is_empty());
        assert!(!watches.observe(observation(1, Some("again"))).await?);

        let queued = watches.deliverable().await?;
        assert_eq!(queued.len(), 1);
        assert_eq!(queued[0].conversation.as_deref(), Some("chat"));
        assert!(watches.startable(4).await?.is_empty());

        watches.delivered(queued[0].run.to_owned()).await?;
        assert!(watches.deliverable().await?.is_empty());

        Ok(())
    }

    /// `/new` ends only that conversation's conversation watches.
    #[tokio::test]
    async fn replacing_a_conversation_ends_its_watches() -> Result<(), BoxError> {
        let db = Db::in_memory()?;
        let roots = Roots::new(db.clone());
        let root = roots.turn(Adapter::Discord, "7".to_owned()).await?;
        let watches = Watches::new(db.clone());

        let created = [("conversation", Destination::Conversation), ("run", Destination::Run)];

        for (id, destination) in created {
            let create = Change::Create { id: id.to_owned(), creator_root: root.as_str().to_owned(), definition: definition(destination) };
            let receipt = Receipt { thread: root.as_str().to_owned(), call: id.to_owned(), tool: "watch_create", text: String::new() };
            assert!(matches!(watches.apply(create, receipt).await?, Applied::Done(_)));
        }

        roots.replace(Adapter::Discord, "7".to_owned()).await?;

        assert!(watches.get("conversation").await?.is_none());
        assert!(watches.get("run").await?.is_some());

        Ok(())
    }
}
