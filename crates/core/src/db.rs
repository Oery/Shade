//! The database worker: one thread owns the SQLite connection.
//!
//! Callers send short closures that own their parameters, never core state.
//! The queue is bounded, so a slow disk applies backpressure instead of
//! growing memory.

use std::fs::OpenOptions;
use std::os::unix::fs::OpenOptionsExt;
use std::path::Path;
use std::thread;
use std::time::Duration;

use rusqlite::Connection;
use thiserror::Error;
use tokio::sync::{mpsc, oneshot};

use crate::Error;

pub const DB_FILE: &str = "shade.db";

/// Pending requests before callers wait for the worker.
const DB_QUEUE: usize = 64;

/// How long a statement waits for a lock held by another connection (backups).
const BUSY_TIMEOUT: Duration = Duration::from_secs(5);

/// Applied in order; `PRAGMA user_version` counts how many have run.
const MIGRATIONS: &[&str] = &[include_str!("../migrations/0001_conversations.sql"), include_str!("../migrations/0002_thread_state.sql")];

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

#[derive(Clone)]
pub struct Db {
    jobs: mpsc::Sender<Job>,
}

impl Db {
    /// Opens and migrates the database, then starts the worker.
    ///
    /// A new database file is created `0600`; SQLite gives its WAL files the same mode.
    pub fn open(path: &Path) -> Result<Self, Error> {
        let created = OpenOptions::new().write(true).create(true).truncate(false).mode(0o600).open(path);

        if let Err(source) = created {
            return Err(Error::DatabaseFile { path: path.to_path_buf(), source });
        }

        let connection = match Connection::open(path) {
            Ok(connection) => connection,
            Err(source) => return Err(Error::Database(source)),
        };

        Self::start(connection)
    }

    #[cfg(test)]
    pub fn in_memory() -> Result<Self, Error> {
        let connection = match Connection::open_in_memory() {
            Ok(connection) => connection,
            Err(source) => return Err(Error::Database(source)),
        };

        Self::start(connection)
    }

    fn start(mut connection: Connection) -> Result<Self, Error> {
        configure(&connection)?;
        migrate(&mut connection)?;

        let (jobs, queue) = mpsc::channel(DB_QUEUE);
        let worker = thread::Builder::new().name("shade-db".to_owned());

        if let Err(source) = worker.spawn(move || serve(connection, queue)) {
            return Err(Error::DatabaseWorker(source));
        }

        Ok(Self { jobs })
    }

    /// Runs `job` on the worker thread and returns its result.
    pub async fn call<T, F>(&self, job: F) -> Result<T, DbError>
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

fn configure(connection: &Connection) -> Result<(), Error> {
    connection.pragma_update_and_check(None, "journal_mode", "WAL", |_| Ok(()))?;
    connection.pragma_update(None, "foreign_keys", true)?;
    connection.busy_timeout(BUSY_TIMEOUT)?;

    Ok(())
}

/// Applies pending migrations, each in its own transaction with its version bump.
fn migrate(connection: &mut Connection) -> Result<(), Error> {
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
        return Err(Error::SchemaTooNew { found, supported });
    }

    Ok(())
}
