//! Backups: a consistent copy of the live database.
//!
//! The copy is taken with `VACUUM INTO` on a read-only connection of its
//! own, on a blocking thread, so the daemon keeps serving while it runs and
//! the copy reflects one moment. The destination is created first, empty
//! and `0600`: an existing file is never overwritten, and the copy is as
//! private as the database. A failed backup removes its partial file.

use std::fs::{self, OpenOptions};
use std::io::{self, ErrorKind};
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};
use std::time::Duration;

use rusqlite::{Connection, OpenFlags};
use thiserror::Error;

use crate::db::Db;

/// How long the copy waits for a lock held by the daemon's own connection.
const BUSY_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Debug, Error)]
pub enum BackupError {
    #[error("this database lives in memory and has no file to back up")]
    InMemory,

    #[error("{0} already exists; back up to a new file")]
    Exists(PathBuf),

    #[error("cannot create {path}: {source}")]
    File { path: PathBuf, source: io::Error },

    #[error("{0} is not a UTF-8 path, which SQLite needs")]
    Path(PathBuf),

    #[error("the copy failed: {0}")]
    Sqlite(#[from] rusqlite::Error),

    #[error("the backup thread stopped")]
    Worker,
}

impl Db {
    /// Copies the database to `destination`, a new file. Returns its size in bytes.
    pub async fn backup(&self, destination: PathBuf) -> Result<u64, BackupError> {
        let Some(source) = self.path() else {
            return Err(BackupError::InMemory);
        };

        match tokio::task::spawn_blocking(move || copy(&source, &destination)).await {
            Ok(result) => result,
            Err(_) => Err(BackupError::Worker),
        }
    }
}

fn copy(source: &Path, destination: &Path) -> Result<u64, BackupError> {
    let created = OpenOptions::new().write(true).create_new(true).mode(0o600).open(destination);

    match created {
        Ok(_) => {}
        Err(error) if error.kind() == ErrorKind::AlreadyExists => return Err(BackupError::Exists(destination.to_owned())),
        Err(source) => return Err(BackupError::File { path: destination.to_owned(), source }),
    }

    let copied = snapshot(source, destination);

    if copied.is_err() {
        let _ = fs::remove_file(destination);
    }

    copied
}

fn snapshot(source: &Path, destination: &Path) -> Result<u64, BackupError> {
    let Some(into) = destination.to_str() else {
        return Err(BackupError::Path(destination.to_owned()));
    };

    let connection = Connection::open_with_flags(source, OpenFlags::SQLITE_OPEN_READ_ONLY)?;
    connection.busy_timeout(BUSY_TIMEOUT)?;
    connection.execute("VACUUM INTO ?1", [into])?;

    match fs::metadata(destination) {
        Ok(metadata) => Ok(metadata.len()),
        Err(source) => Err(BackupError::File { path: destination.to_owned(), source }),
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::os::unix::fs::PermissionsExt;

    use super::BackupError;
    use crate::BoxError;
    use crate::db::Db;

    /// A backup is a private, current-schema copy of what was written, and
    /// it never replaces a file or leaves a partial one behind.
    #[tokio::test]
    async fn backups_are_private_copies_that_never_overwrite() -> Result<(), BoxError> {
        let dir = std::env::temp_dir().join(format!("shade-backup-{}", uuid::Uuid::new_v4()));
        fs::create_dir_all(&dir)?;

        let db = Db::open(&dir.join("shade.db"))?;
        db.call(|connection| Ok(connection.execute("INSERT INTO threads (id, root_id) VALUES ('t', 't')", [])?)).await?;

        let copy = dir.join("copy.db");
        assert!(db.backup(copy.to_owned()).await? > 0);
        assert_eq!(fs::metadata(&copy)?.permissions().mode() & 0o777, 0o600);

        // The copy opens as a database of the current schema, holding the row.
        let restored = Db::open(&copy)?;
        let rows: i64 = restored.call(|connection| Ok(connection.query_row("SELECT COUNT(*) FROM threads", [], |row| row.get(0))?)).await?;
        assert_eq!(rows, 1);

        assert!(matches!(db.backup(copy.to_owned()).await, Err(BackupError::Exists(_))));

        let nowhere = dir.join("missing").join("copy.db");
        assert!(matches!(db.backup(nowhere.to_owned()).await, Err(BackupError::File { .. })));
        assert!(!nowhere.exists());

        fs::remove_dir_all(&dir)?;

        Ok(())
    }
}
