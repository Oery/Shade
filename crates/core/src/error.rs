use std::path::PathBuf;

use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("cannot locate the data directory: set XDG_DATA_HOME or HOME to an absolute path")]
    NoDataDir,

    #[error("cannot create data directory {path}: {source}")]
    DataDir { path: PathBuf, source: std::io::Error },

    #[error("cannot lock {path}: {source}")]
    Lock { path: PathBuf, source: std::io::Error },

    #[error("another shaded already owns {path}")]
    AlreadyRunning { path: PathBuf },

    #[error("cannot set up control socket {path}: {source}")]
    Socket { path: PathBuf, source: std::io::Error },

    #[error("cannot start the async runtime: {0}")]
    Runtime(std::io::Error),

    #[error("cannot listen for shutdown signals: {0}")]
    Signal(std::io::Error),
}
