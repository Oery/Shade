use std::path::PathBuf;

use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("`{0}` is not implemented yet")]
    NotImplemented(&'static str),

    #[error("cannot resolve path {path}: {source}")]
    Path { path: PathBuf, source: std::io::Error },

    #[error("cannot locate the data directory: set XDG_DATA_HOME or HOME to an absolute path")]
    NoDataDir,

    #[error("shaded is not running (no daemon at {path}); start it with `shaded`")]
    NotRunning { path: PathBuf },

    #[error("cannot connect to shaded at {path}: {source}")]
    Connect { path: PathBuf, source: std::io::Error },

    #[error("connection to shaded failed: {0}")]
    Io(#[from] std::io::Error),

    #[error("invalid message from shaded: {0}")]
    Json(#[from] serde_json::Error),

    #[error("reply from shaded is too long")]
    LineTooLong,

    #[error("shaded closed the connection without replying; check the daemon logs")]
    Closed,

    #[error("{0}")]
    Daemon(String),
}
