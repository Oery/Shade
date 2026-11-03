//! The database worker: one thread owns the SQLite connection.
//!
//! Callers send short closures that own their parameters, never core state.
//! The queue is bounded, so a slow disk applies backpressure instead of
//! growing memory.

use std::fs::OpenOptions;
use std::io;
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};
use std::thread;
use std::time::Duration;

use rusqlite::Connection;
use thiserror::Error;
use tokio::sync::{mpsc, oneshot};

pub const DB_FILE: &str = "shade.db";

/// Pending requests before callers wait for the worker.
const DB_QUEUE: usize = 64;

/// How long a statement waits for a lock held by another connection (backups).
const BUSY_TIMEOUT: Duration = Duration::from_secs(5);

/// Applied in order; `PRAGMA user_version` counts how many have run.
const MIGRATIONS: &[&str] = &[
    include_str!("../migrations/0001_conversations.sql"),
    include_str!("../migrations/0002_thread_state.sql"),
    include_str!("../migrations/0003_conversation_names.sql"),
    include_str!("../migrations/0004_grants.sql"),
    include_str!("../migrations/0005_tool_operations.sql"),
    include_str!("../migrations/0006_inbox.sql"),
    include_str!("../migrations/0007_inbox_source.sql"),
    include_str!("../migrations/0008_usage.sql"),
    include_str!("../migrations/0009_notifications.sql"),
    include_str!("../migrations/0010_schedules.sql"),
    include_str!("../migrations/0011_job_channels_files.sql"),
    include_str!("../migrations/0012_pending_thread.sql"),
    include_str!("../migrations/0013_job_archive.sql"),
    include_str!("../migrations/0014_direct_conversations.sql"),
    include_str!("../migrations/0015_hook_events.sql"),
];

type Job = Box<dyn FnOnce(&mut Connection) + Send>;

#[derive(Debug, Error)]
pub enum DbError {
    #[error("the database worker has stopped")]
    Closed,

    #[error("database error: {0}")]
    Sqlite(#[from] rusqlite::Error),

    #[error("stored message is not valid: {0}")]
    Json(#[from] serde_json::Error),

    #[error("thread {0} is not registered")]
    UnknownThread(String),
}

/// Why the database could not be opened.
#[derive(Debug, Error)]
pub enum OpenError {
    #[error("cannot create database {path}: {source}")]
    File { path: PathBuf, source: io::Error },

    #[error("cannot open the database: {0}")]
    Sqlite(#[from] rusqlite::Error),

    #[error("the database has schema version {found}, but this shaded supports up to {supported}; upgrade shaded")]
    SchemaTooNew { found: u32, supported: u32 },

    #[error("cannot start the database worker: {0}")]
    Worker(io::Error),
}

#[derive(Clone)]
pub struct Db {
    jobs: mpsc::Sender<Job>,
}

impl Db {
    /// Opens and migrates the database, then starts the worker.
    ///
    /// A new database file is created `0600`; SQLite gives its WAL files the same mode.
    pub fn open(path: &Path) -> Result<Self, OpenError> {
        let created = OpenOptions::new().write(true).create(true).truncate(false).mode(0o600).open(path);

        if let Err(source) = created {
            return Err(OpenError::File { path: path.to_path_buf(), source });
        }

        let connection = match Connection::open(path) {
            Ok(connection) => connection,
            Err(source) => return Err(OpenError::Sqlite(source)),
        };

        Self::start(connection)
    }

    #[cfg(any(test, feature = "testing"))]
    pub fn in_memory() -> Result<Self, OpenError> {
        let connection = match Connection::open_in_memory() {
            Ok(connection) => connection,
            Err(source) => return Err(OpenError::Sqlite(source)),
        };

        Self::start(connection)
    }

    fn start(mut connection: Connection) -> Result<Self, OpenError> {
        configure(&connection)?;
        migrate(&mut connection)?;

        let (jobs, queue) = mpsc::channel(DB_QUEUE);
        let worker = thread::Builder::new().name("shade-db".to_owned());

        if let Err(source) = worker.spawn(move || serve(connection, queue)) {
            return Err(OpenError::Worker(source));
        }

        Ok(Self { jobs })
    }

    /// Waits until every job queued before this call has finished. Jobs run in
    /// order, so this is a barrier for writes still in the queue at shutdown.
    pub async fn flush(&self) -> Result<(), DbError> {
        self.call(|_| Ok(())).await
    }

    /// Runs `job` on the worker thread and returns its result.
    pub(crate) async fn call<T, F>(&self, job: F) -> Result<T, DbError>
    where
        T: Send + 'static,
        F: FnOnce(&mut Connection) -> Result<T, DbError> + Send + 'static,
    {
        let (reply, result) = oneshot::channel();
        let job: Job = Box::new(move |connection| {
            let _ = reply.send(job(connection));
        });

        if self.jobs.send(job).await.is_err() {
            return Err(DbError::Closed);
        }

        match result.await {
            Ok(result) => result,
            Err(_) => Err(DbError::Closed),
        }
    }
}

/// Runs jobs until every `Db` handle is dropped.
fn serve(mut connection: Connection, mut queue: mpsc::Receiver<Job>) {
    while let Some(job) = queue.blocking_recv() {
        job(&mut connection);
    }
}

fn configure(connection: &Connection) -> Result<(), OpenError> {
    connection.pragma_update_and_check(None, "journal_mode", "WAL", |_| Ok(()))?;
    connection.pragma_update(None, "foreign_keys", true)?;
    connection.busy_timeout(BUSY_TIMEOUT)?;

    Ok(())
}

/// Applies pending migrations, each in its own transaction with its version bump.
fn migrate(connection: &mut Connection) -> Result<(), OpenError> {
    let found: u32 = connection.pragma_query_value(None, "user_version", |row| row.get(0))?;
    let mut supported = 0;

    for (version, migration) in (1u32..).zip(MIGRATIONS) {
        supported = version;

        if version <= found {
            continue;
        }

        let transaction = connection.transaction()?;
        transaction.execute_batch(migration)?;
        transaction.pragma_update(None, "user_version", version)?;
        transaction.commit()?;
    }

    // A newer database ran every migration above, so nothing was applied to it.
    if found > supported {
        return Err(OpenError::SchemaTooNew { found, supported });
    }

    Ok(())
}
