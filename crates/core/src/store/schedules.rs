//! Scheduled jobs and their runs.
//!
//! Each due occurrence is claimed in one transaction: the job's next
//! occurrence moves on (a one-shot job is disabled) and a queued run is
//! recorded, unless the job already has one waiting, which then stands for
//! both. A queued run starts in a second transaction that registers its
//! root, grant, budget and inbox input, so from there the ordinary inbox
//! replay carries it across restarts. A job has at most one running run.

use infinity_agent_core::ThreadId;
use rusqlite::{Connection, OptionalExtension, params};

use crate::policy::{Grant, Origin};
use crate::store::db::{Db, DbError};
use crate::store::grants::start_budget;
use crate::store::inbox::{self, Input};
use crate::store::operations;
use crate::store::roots::{insert_root, new_id};
use serde::{Deserialize, Serialize};

/// A job whose next occurrence has come.
pub struct Due {
    pub id: String,
    pub kind: String,
    pub spec: String,
    pub timezone: String,
    /// The occurrence, in unix seconds.
    pub next_at: i64,
}

/// What claiming an occurrence did.
#[derive(Debug, PartialEq)]
pub enum Claim {
    Queued,
    /// The job already had a run waiting; this occurrence folded into it.
    Folded,
    /// The job changed since it was read; nothing was done.
    Stale,
}

/// A queued run that may start now.
pub struct Queued {
    pub run: String,
    pub job: String,
    pub title: String,
    pub prompt: String,
    pub timezone: String,
    pub scheduled_for: i64,
}

/// A run, as its outcome reporter needs it.
pub struct Run {
    pub id: String,
    pub title: String,
}

/// Most jobs that may exist at once, paused ones included.
pub const MAX_JOBS: i64 = 100;

/// What a job does and when.
#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct Definition {
    pub title: String,
    /// `cron` or `once`.
    pub kind: String,
    pub spec: String,
    pub timezone: String,
    pub prompt: String,
    /// Delegated to each run.
    pub caps: Vec<String>,
    /// Where its runs may post: `dm`, channel names, or channel IDs. The first is the default.
    pub channels: Vec<String>,
    /// Lives in `jobs/<id>.toml`, which wins over this row at startup.
    pub file: bool,
    pub enabled: bool,
    pub next_at: Option<i64>,
}

/// A proposed change still inside its approval window, for listings.
pub struct Waiting {
    pub code: String,
    /// The receipt shown when it applies; its first line names the job.
    pub receipt: String,
}

/// A change waiting for the owner's approval.
pub struct Pending {
    pub job: String,
    /// `None` for a new job.
    pub base_version: Option<i64>,
    pub definition: Definition,
    pub receipt: String,
    pub created_at: i64,
}

/// A stored job.
pub struct Job {
    pub id: String,
    pub version: i64,
    pub definition: Definition,
    /// The latest run's status and occurrence.
    pub last_run: Option<(String, i64)>,
}

/// A change made by a schedule tool.
pub enum Change {
    Create {
        id: String,
        definition: Definition,
    },
    /// Applies only if the job is still at `version`.
    Update {
        id: String,
        version: i64,
        definition: Definition,
    },
    Delete {
        id: String,
        /// Its file is removed first.
        file: bool,
    },
    /// Waits for the owner: nothing changes until they approve `code`.
    /// `receipt` is shown when it applies.
    Propose {
        code: String,
        id: String,
        base_version: Option<i64>,
        definition: Definition,
        receipt: String,
    },
}

/// The tool call a change answers, and the receipt it answers with.
pub struct Receipt {
    pub thread: String,
    pub call: String,
    pub tool: &'static str,
    pub text: String,
}

/// What applying a change did. Each outcome is the call's recorded result.
pub enum Applied {
    Done(String),
    /// Refused without changing anything: the error to show.
    Refused(String),
    /// The call was answered before; its recorded result.
    Replayed(String),
}

#[derive(Clone)]
pub struct Schedules {
    db: Db,
}

impl Schedules {
    pub fn new(db: Db) -> Self {
        Self { db }
    }

    /// Enabled jobs due at `now`, oldest occurrence first.
    pub async fn due(&self, now: i64) -> Result<Vec<Due>, DbError> {
        self.db
            .call(move |connection| {
                let sql = "SELECT id, kind, spec, timezone, next_at FROM schedules
                           WHERE enabled = 1 AND deleted_at IS NULL AND next_at <= ?1 ORDER BY next_at";
                let mut statement = connection.prepare(sql)?;
                let rows = statement.query_map([now], |row| {
                    Ok(Due { id: row.get(0)?, kind: row.get(1)?, spec: row.get(2)?, timezone: row.get(3)?, next_at: row.get(4)? })
                })?;

                Ok(rows.collect::<Result<_, _>>()?)
            })
            .await
    }

    /// Claims the occurrence at `scheduled_for` and moves the job to
    /// `next_at`. `None` means it never runs again, so it is disabled.
    pub async fn claim(&self, job: String, scheduled_for: i64, next_at: Option<i64>) -> Result<Claim, DbError> {
        self.db
            .call(move |connection| {
                let transaction = connection.transaction()?;
                let claim = claim_occurrence(&transaction, &job, scheduled_for, next_at)?;
                transaction.commit()?;

                Ok(claim)
            })
            .await
    }

    /// Stops a job whose stored definition cannot be read.
    pub async fn disable(&self, job: String) -> Result<(), DbError> {
        self.db
            .call(move |connection| {
                connection.execute("UPDATE schedules SET enabled = 0 WHERE id = ?1", [&job])?;

                Ok(())
            })
            .await
    }

    /// Queued runs that may start without passing `capacity` running runs
    /// in all, or a second running run for one job. Oldest first.
    pub async fn startable(&self, capacity: u32) -> Result<Vec<Queued>, DbError> {
        self.db.call(move |connection| startable_runs(connection, capacity)).await
    }

    /// Starts a queued run: its root, delegated grant, budget and input are
    /// recorded together. `None` when the run is no longer queued.
    pub async fn start(&self, run: String, text: String) -> Result<Option<Input>, DbError> {
        self.db
            .call(move |connection| {
                let transaction = connection.transaction()?;
                let input = start_run(&transaction, &run, text)?;
                transaction.commit()?;

                Ok(input)
            })
            .await
    }

    /// Records how a running run ended. A failure after its budget ran out is `limit_exceeded`.
    pub async fn finish(&self, run: &str, error: Option<&str>) -> Result<(), DbError> {
        let (run, error) = (run.to_owned(), error.map(str::to_owned));

        self.db
            .call(move |connection| {
                let sql = "UPDATE runs SET finished_at = unixepoch(), error = ?2, status = CASE
                               WHEN ?2 IS NULL THEN 'completed'
                               WHEN (SELECT b.exceeded FROM threads t JOIN budgets b ON b.id = t.budget_id WHERE t.id = runs.root_id)
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
                let sql = "SELECT r.id, s.title FROM runs r JOIN schedules s ON s.id = r.schedule_id WHERE r.root_id = ?1";

                Ok(connection.query_row(sql, [&root], |row| Ok(Run { id: row.get(0)?, title: row.get(1)? })).optional()?)
            })
            .await
    }

    /// Marks running runs whose roots are not in `resumed` as interrupted:
    /// a restart left nothing that would finish them. Returns their roots.
    pub async fn interrupt_all_but(&self, resumed: Vec<String>) -> Result<Vec<(ThreadId, Run)>, DbError> {
        self.db.call(move |connection| interrupt_runs(connection, &resumed)).await
    }

    pub async fn list(&self) -> Result<Vec<Job>, DbError> {
        self.db.call(list_jobs).await
    }

    /// A job that is not deleted.
    pub async fn get(&self, id: &str) -> Result<Option<Job>, DbError> {
        let jobs = self.list().await?;

        Ok(jobs.into_iter().find(|job| job.id == id))
    }

    /// Proposed changes younger than `window` seconds, oldest first.
    pub async fn waiting(&self, window: i64) -> Result<Vec<Waiting>, DbError> {
        self.db
            .call(move |connection| {
                let sql = "SELECT code, receipt FROM pending_changes WHERE created_at > unixepoch() - ?1 ORDER BY created_at";
                let mut statement = connection.prepare(sql)?;
                let rows = statement.query_map([window], |row| Ok(Waiting { code: row.get(0)?, receipt: row.get(1)? }))?;

                Ok(rows.collect::<Result<_, _>>()?)
            })
            .await
    }

    /// Whether a job ever had `id`, deleted ones included.
    pub async fn exists(&self, id: &str) -> Result<bool, DbError> {
        let id = id.to_owned();

        self.db
            .call(move |connection| {
                Ok(connection.query_row("SELECT 1 FROM schedules WHERE id = ?1", [&id], |_| Ok(())).optional()?.is_some())
            })
            .await
    }

    /// A change waiting for approval.
    pub async fn pending(&self, code: &str) -> Result<Option<Pending>, DbError> {
        let code = code.to_owned();

        self.db.call(move |connection| pending_change(connection, &code)).await
    }

    /// Applies an approved change and forgets it, in one transaction.
    /// `Some` says why it no longer applies.
    pub async fn approve(&self, code: &str, change: Change) -> Result<Option<String>, DbError> {
        let code = code.to_owned();

        self.db
            .call(move |connection| {
                let transaction = connection.transaction()?;
                let refusal = apply_change(&transaction, change, None)?;
                transaction.execute("DELETE FROM pending_changes WHERE code = ?1", [&code])?;
                transaction.commit()?;

                Ok(refusal)
            })
            .await
    }

    /// Makes file jobs match the files (see [`sync_files`]).
    pub async fn sync_files(&self, present: Vec<String>, changed: Vec<(String, Definition)>) -> Result<(), DbError> {
        self.db.call(move |connection| sync_files(connection, &present, &changed)).await
    }

    /// The channels of the job whose run `thread` belongs to. `None` outside a scheduled run.
    pub async fn channels_of(&self, thread: &ThreadId<str>) -> Result<Option<Vec<String>>, DbError> {
        let thread = thread.as_str().to_owned();

        self.db
            .call(move |connection| {
                let sql = "SELECT s.channels FROM threads t JOIN runs r ON r.root_id = t.root_id JOIN schedules s ON s.id = r.schedule_id
                           WHERE t.id = ?1";
                let channels: Option<String> = connection.query_row(sql, [&thread], |row| row.get(0)).optional()?;

                match channels {
                    Some(json) => Ok(Some(serde_json::from_str(&json)?)),
                    None => Ok(None),
                }
            })
            .await
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

                let applied = match apply_change(&transaction, change, Some(&receipt.thread))? {
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

    /// Stops new runs of a job. `false` when there is no such job.
    pub async fn pause(&self, job: String) -> Result<bool, DbError> {
        let sql = "UPDATE schedules SET enabled = 0 WHERE id = ?1 AND deleted_at IS NULL";

        self.db.call(move |connection| Ok(connection.execute(sql, [&job])? == 1)).await
    }

    /// Lets a job run again from `next_at`.
    pub async fn resume(&self, job: String, next_at: i64) -> Result<(), DbError> {
        self.db
            .call(move |connection| {
                let sql = "UPDATE schedules SET enabled = 1, next_at = ?2 WHERE id = ?1 AND deleted_at IS NULL";
                connection.execute(sql, params![job, next_at])?;

                Ok(())
            })
            .await
    }

    /// Deletes a job. Its runs, even queued ones, are kept. `false` when there is no such job.
    pub async fn delete(&self, job: String) -> Result<bool, DbError> {
        let sql = "UPDATE schedules SET enabled = 0, deleted_at = unixepoch() WHERE id = ?1 AND deleted_at IS NULL";

        self.db.call(move |connection| Ok(connection.execute(sql, [&job])? == 1)).await
    }
}

fn claim_occurrence(connection: &Connection, job: &str, scheduled_for: i64, next_at: Option<i64>) -> Result<Claim, DbError> {
    let sql = "UPDATE schedules SET next_at = ?3, enabled = ?3 IS NOT NULL
               WHERE id = ?1 AND next_at = ?2 AND enabled = 1 AND deleted_at IS NULL";

    if connection.execute(sql, params![job, scheduled_for, next_at])? == 0 {
        return Ok(Claim::Stale);
    }

    let waiting = connection.query_row("SELECT 1 FROM runs WHERE schedule_id = ?1 AND status = 'queued'", [job], |_| Ok(())).optional()?;

    if waiting.is_some() {
        return Ok(Claim::Folded);
    }

    let sql = "INSERT INTO runs (id, schedule_id, version, scheduled_for)
               SELECT ?1, id, version, ?3 FROM schedules WHERE id = ?2";
    connection.execute(sql, params![new_id(), job, scheduled_for])?;

    Ok(Claim::Queued)
}

fn startable_runs(connection: &mut Connection, capacity: u32) -> Result<Vec<Queued>, DbError> {
    let running: u32 = connection.query_row("SELECT COUNT(*) FROM runs WHERE status = 'running'", [], |row| row.get(0))?;

    let sql = "SELECT r.id, s.id, s.title, s.prompt, s.timezone, r.scheduled_for
               FROM runs r JOIN schedules s ON s.id = r.schedule_id
               WHERE r.status = 'queued'
               AND NOT EXISTS (SELECT 1 FROM runs a WHERE a.schedule_id = r.schedule_id AND a.status = 'running')
               ORDER BY r.scheduled_for, r.claimed_at LIMIT ?1";
    let mut statement = connection.prepare(sql)?;
    let rows = statement.query_map([capacity.saturating_sub(running)], |row| {
        Ok(Queued {
            run: row.get(0)?,
            job: row.get(1)?,
            title: row.get(2)?,
            prompt: row.get(3)?,
            timezone: row.get(4)?,
            scheduled_for: row.get(5)?,
        })
    })?;

    Ok(rows.collect::<Result<_, _>>()?)
}

fn start_run(connection: &Connection, run: &str, text: String) -> Result<Option<Input>, DbError> {
    let sql = "SELECT s.id, s.caps, s.version FROM runs r JOIN schedules s ON s.id = r.schedule_id WHERE r.id = ?1 AND r.status = 'queued'";
    let row: Option<(String, String, i64)> =
        connection.query_row(sql, [run], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?))).optional()?;

    let Some((job, caps, version)) = row else {
        return Ok(None);
    };

    let root = new_id();
    let grant = Grant { origin: Origin::Schedule, source: job, caps: Some(serde_json::from_str(&caps)?) };
    insert_root(connection, &root, &grant)?;
    start_budget(connection, &root)?;

    let id = new_id();
    inbox::insert(connection, &id, &root, &text, Some(&format!("run:{run}")))?;

    // A run starts with the definition current now, and records which one that is.
    let sql = "UPDATE runs SET status = 'running', root_id = ?2, version = ?3, started_at = unixepoch() WHERE id = ?1";
    connection.execute(sql, params![run, &root, version])?;

    Ok(Some(Input { root: ThreadId::from(root), id, text }))
}

fn interrupt_runs(connection: &mut Connection, resumed: &[String]) -> Result<Vec<(ThreadId, Run)>, DbError> {
    let transaction = connection.transaction()?;
    let mut interrupted = Vec::new();

    let sql = "SELECT r.id, r.root_id, s.title FROM runs r JOIN schedules s ON s.id = r.schedule_id WHERE r.status = 'running'";
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
        let sql = "UPDATE runs SET status = 'interrupted', finished_at = unixepoch(), error = 'interrupted by a restart' WHERE id = ?1";
        transaction.execute(sql, [&run.id])?;
    }

    transaction.commit()?;

    Ok(interrupted)
}

const COLUMNS: &str = "title, kind, spec, timezone, prompt, caps, channels, file, enabled, next_at";

fn list_jobs(connection: &mut Connection) -> Result<Vec<Job>, DbError> {
    let sql = format!(
        "SELECT s.id, s.version, {}, r.status, r.scheduled_for
         FROM schedules s LEFT JOIN runs r ON r.id = (SELECT id FROM runs WHERE schedule_id = s.id ORDER BY scheduled_for DESC LIMIT 1)
         WHERE s.deleted_at IS NULL ORDER BY s.enabled DESC, s.next_at, s.created_at",
        "s.title, s.kind, s.spec, s.timezone, s.prompt, s.caps, s.channels, s.file, s.enabled, s.next_at"
    );
    let mut statement = connection.prepare(&sql)?;
    let mut rows = statement.query([])?;
    let mut jobs = Vec::new();

    while let Some(row) = rows.next()? {
        let status: Option<String> = row.get(12)?;

        let last_run = match status {
            Some(status) => Some((status, row.get(13)?)),
            None => None,
        };

        jobs.push(Job { id: row.get(0)?, version: row.get(1)?, definition: definition_at(row, 2)?, last_run });
    }

    Ok(jobs)
}

/// The definition in the `COLUMNS` starting at `first`.
fn definition_at(row: &rusqlite::Row<'_>, first: usize) -> Result<Definition, DbError> {
    let caps: String = row.get(first + 5)?;
    let channels: String = row.get(first + 6)?;

    Ok(Definition {
        title: row.get(first)?,
        kind: row.get(first + 1)?,
        spec: row.get(first + 2)?,
        timezone: row.get(first + 3)?,
        prompt: row.get(first + 4)?,
        caps: serde_json::from_str(&caps)?,
        channels: serde_json::from_str(&channels)?,
        file: row.get(first + 7)?,
        enabled: row.get(first + 8)?,
        next_at: row.get(first + 9)?,
    })
}

/// Makes the change, or returns why it cannot be made. `thread` created it, when a tool did.
fn apply_change(connection: &Connection, change: Change, thread: Option<&str>) -> Result<Option<String>, DbError> {
    match change {
        Change::Create { id, definition } => create_job(connection, &id, &definition, thread),
        Change::Update { id, version, definition } => {
            let sql = "UPDATE schedules SET title = ?3, kind = ?4, spec = ?5, timezone = ?6, prompt = ?7, caps = ?8, channels = ?9,
                       file = ?10, enabled = ?11, next_at = ?12, version = version + 1 WHERE id = ?1 AND version = ?2 AND deleted_at IS NULL";
            let new = &definition;
            let (caps, channels) = (serde_json::to_string(&new.caps)?, serde_json::to_string(&new.channels)?);
            let params = params![
                id,
                version,
                new.title,
                new.kind,
                new.spec,
                new.timezone,
                new.prompt,
                caps,
                channels,
                new.file,
                new.enabled,
                new.next_at
            ];

            if connection.execute(sql, params)? == 0 {
                return Ok(Some(format!("job {id} was changed or deleted meanwhile; list it and try again")));
            }

            Ok(None)
        }
        Change::Delete { id, .. } => {
            let sql = "UPDATE schedules SET enabled = 0, deleted_at = unixepoch() WHERE id = ?1 AND deleted_at IS NULL";

            if connection.execute(sql, [&id])? == 0 {
                return Ok(Some(format!("no job {id}")));
            }

            Ok(None)
        }
        Change::Propose { code, id, base_version, definition, receipt } => {
            let sql = "INSERT INTO pending_changes (code, job_id, base_version, definition, receipt) VALUES (?1, ?2, ?3, ?4, ?5)";
            connection.execute(sql, params![code, id, base_version, serde_json::to_string(&definition)?, receipt])?;

            Ok(None)
        }
    }
}

fn create_job(connection: &Connection, id: &str, definition: &Definition, thread: Option<&str>) -> Result<Option<String>, DbError> {
    let count: i64 = connection.query_row("SELECT COUNT(*) FROM schedules WHERE deleted_at IS NULL", [], |row| row.get(0))?;

    if count >= MAX_JOBS {
        return Ok(Some(format!("there are already {MAX_JOBS} jobs; delete one first")));
    }

    let taken = connection.query_row("SELECT 1 FROM schedules WHERE id = ?1", [id], |_| Ok(())).optional()?;

    if taken.is_some() {
        return Ok(Some(format!("a job named {id} exists or existed; choose another name")));
    }

    insert_job(connection, id, definition, thread)?;

    Ok(None)
}

fn insert_job(connection: &Connection, id: &str, definition: &Definition, thread: Option<&str>) -> Result<(), DbError> {
    let sql = format!(
        "INSERT INTO schedules (id, {COLUMNS}, creator_root) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, (SELECT root_id FROM threads WHERE id = ?12))"
    );
    let new = definition;
    let (caps, channels) = (serde_json::to_string(&new.caps)?, serde_json::to_string(&new.channels)?);
    let params =
        params![id, new.title, new.kind, new.spec, new.timezone, new.prompt, caps, channels, new.file, new.enabled, new.next_at, thread];
    connection.execute(&sql, params)?;

    Ok(())
}

fn pending_change(connection: &mut Connection, code: &str) -> Result<Option<Pending>, DbError> {
    let sql = "SELECT job_id, base_version, definition, receipt, created_at FROM pending_changes WHERE code = ?1";
    let row: Option<(String, Option<i64>, String, String, i64)> =
        connection.query_row(sql, [code], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?))).optional()?;

    let Some((job, base_version, definition, receipt, created_at)) = row else {
        return Ok(None);
    };

    Ok(Some(Pending {
        job,
        base_version,
        definition: serde_json::from_str(&definition)?,
        receipt,
        created_at,
    }))
}

/// Brings file jobs in line with their files: each `changed` definition
/// is written as given (a file whose job was deleted brings it back), and
/// file jobs not `present` are deleted. Runs and their history stay.
fn sync_files(connection: &mut Connection, present: &[String], changed: &[(String, Definition)]) -> Result<(), DbError> {
    let transaction = connection.transaction()?;

    for (id, definition) in changed {
        let new = definition;
        let (caps, channels) = (serde_json::to_string(&new.caps)?, serde_json::to_string(&new.channels)?);
        let sql = format!(
            "INSERT INTO schedules (id, {COLUMNS}) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, 1, ?9, ?10)
             ON CONFLICT (id) DO UPDATE SET title = ?2, kind = ?3, spec = ?4, timezone = ?5, prompt = ?6, caps = ?7, channels = ?8,
             file = 1, enabled = ?9, next_at = ?10, deleted_at = NULL, version = version + 1"
        );
        transaction.execute(
            &sql,
            params![id, new.title, new.kind, new.spec, new.timezone, new.prompt, caps, channels, new.enabled, new.next_at],
        )?;
    }

    let mut statement = transaction.prepare("SELECT id FROM schedules WHERE file = 1 AND deleted_at IS NULL")?;
    let stored: Vec<String> = statement.query_map([], |row| row.get(0))?.collect::<Result<_, _>>()?;
    drop(statement);

    for id in stored.iter().filter(|id| !present.contains(id)) {
        tracing::info!(job = %id, "deleting a job whose file is gone");
        transaction.execute("UPDATE schedules SET enabled = 0, deleted_at = unixepoch() WHERE id = ?1", [id])?;
    }

    transaction.commit()?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use rusqlite::params;

    use super::{Claim, Schedules};
    use crate::error::BoxError;
    use crate::store::db::{Db, DbError};

    const HOUR: i64 = 3600;

    impl Schedules {
        /// A job that may use `now`, due at `next_at`.
        pub async fn add(&self, id: &str, kind: &'static str, spec: &'static str, next_at: i64) -> Result<(), DbError> {
            let id = id.to_owned();

            self.db
                .call(move |connection| {
                    let sql = "INSERT INTO schedules (id, title, kind, spec, timezone, prompt, caps, next_at)
                               VALUES (?1, ?1, ?2, ?3, 'Europe/Paris', 'check the weather', '[\"now\"]', ?4)";
                    connection.execute(sql, params![id, kind, spec, next_at])?;

                    Ok(())
                })
                .await
        }

        /// Proposes `title` for `job` as drafted against `base_version`, under `code`.
        pub async fn propose(
            &self, code: &'static str, job: &'static str, base_version: Option<i64>, title: &'static str,
        ) -> Result<(), DbError> {
            self.db
                .call(move |connection| {
                    let jobs = super::list_jobs(connection)?;
                    let Some(found) = jobs.into_iter().find(|found| found.id == job) else {
                        return Err(DbError::UnknownThread(job.to_owned()));
                    };

                    let mut definition = found.definition;
                    definition.title = title.to_owned();
                    let change = super::Change::Propose {
                        code: code.to_owned(),
                        id: job.to_owned(),
                        base_version,
                        definition,
                        receipt: title.to_owned(),
                    };
                    super::apply_change(connection, change, None)?;

                    Ok(())
                })
                .await
        }

        /// Every run's status, oldest occurrence first.
        pub async fn statuses(&self) -> Result<Vec<String>, DbError> {
            self.db
                .call(|connection| {
                    let mut statement = connection.prepare("SELECT status FROM runs ORDER BY scheduled_for")?;
                    let rows = statement.query_map([], |row| row.get(0))?;

                    Ok(rows.collect::<Result<_, _>>()?)
                })
                .await
        }
    }

    /// A job has at most one running and one queued run: occurrences while
    /// one waits fold into it. Capacity holds queued runs back without losing them.
    #[tokio::test]
    async fn occurrences_fold_into_one_waiting_run() -> Result<(), BoxError> {
        let schedules = Schedules::new(Db::in_memory()?);
        schedules.add("a", "cron", "0 * * * *", 0).await?;
        schedules.add("b", "cron", "0 * * * *", 0).await?;

        assert_eq!(schedules.claim("a".to_owned(), 0, Some(HOUR)).await?, Claim::Queued);
        assert_eq!(schedules.claim("b".to_owned(), 0, Some(HOUR)).await?, Claim::Queued);
        assert_eq!(schedules.claim("a".to_owned(), HOUR, Some(2 * HOUR)).await?, Claim::Folded);
        // A claim for an occurrence the job has moved past does nothing.
        assert_eq!(schedules.claim("a".to_owned(), HOUR, Some(2 * HOUR)).await?, Claim::Stale);

        let startable = schedules.startable(1).await?;
        assert_eq!(startable.len(), 1, "capacity 1 started two runs");
        schedules.start(startable[0].run.to_owned(), "go".to_owned()).await?;
        assert!(schedules.startable(1).await?.is_empty());

        // With one run active, the next occurrence queues one more, and only one.
        assert_eq!(schedules.claim("a".to_owned(), 2 * HOUR, Some(3 * HOUR)).await?, Claim::Queued);
        assert_eq!(schedules.claim("a".to_owned(), 3 * HOUR, Some(4 * HOUR)).await?, Claim::Folded);

        // Room for more, but `a` already runs: only `b` may start.
        let startable = schedules.startable(4).await?;
        assert_eq!(startable.iter().map(|queued| queued.job.as_str()).collect::<Vec<_>>(), ["b"]);
        assert_eq!(schedules.statuses().await?, ["running", "queued", "queued"]);

        Ok(())
    }

    /// A one-shot job is disabled by the claim that queues its run, so it can never run twice.
    #[tokio::test]
    async fn one_shot_jobs_are_claimed_once() -> Result<(), BoxError> {
        let schedules = Schedules::new(Db::in_memory()?);
        schedules.add("once", "once", "2026-11-06T09:00:00+01:00", 0).await?;

        assert_eq!(schedules.claim("once".to_owned(), 0, None).await?, Claim::Queued);
        assert!(schedules.due(HOUR).await?.is_empty(), "a one-shot job stayed due");
        assert_eq!(schedules.claim("once".to_owned(), 0, None).await?, Claim::Stale);

        let jobs = schedules.list().await?;
        assert!(jobs.iter().all(|job| !job.definition.enabled && job.definition.next_at.is_none()));
        assert_eq!(schedules.statuses().await?, ["queued"]);

        Ok(())
    }
}
