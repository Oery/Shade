//! Data directory ownership: one daemon per directory.

use std::fs::{DirBuilder, File, OpenOptions, TryLockError};
use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt};
use std::path::Path;

use crate::Error;

const LOCK_FILE: &str = "shade.lock";

/// Exclusive ownership of a data directory, released when dropped.
pub struct ExecutionLock {
    _file: File,
}

/// Creates the data directory with mode `0700` if it does not exist.
pub fn create_data_dir(path: &Path) -> Result<(), Error> {
    let result = DirBuilder::new().recursive(true).mode(0o700).create(path);

    if let Err(source) = result {
        return Err(Error::DataDir { path: path.to_path_buf(), source });
    }

    Ok(())
}

pub fn acquire(data_dir: &Path) -> Result<ExecutionLock, Error> {
    let path = data_dir.join(LOCK_FILE);

    let file = match OpenOptions::new().write(true).create(true).truncate(false).mode(0o600).open(&path) {
        Ok(file) => file,
        Err(source) => return Err(Error::Lock { path, source }),
    };

    match file.try_lock() {
        Ok(()) => Ok(ExecutionLock { _file: file }),
        Err(TryLockError::WouldBlock) => Err(Error::AlreadyRunning { path: data_dir.to_path_buf() }),
        Err(TryLockError::Error(source)) => Err(Error::Lock { path, source }),
    }
}
