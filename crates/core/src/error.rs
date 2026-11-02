use std::path::PathBuf;

use thiserror::Error;

pub type BoxError = Box<dyn std::error::Error + Send + Sync>;

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

    #[error("cannot locate the config file: set XDG_CONFIG_HOME or HOME to an absolute path, or pass --config")]
    NoConfigDir,

    #[error("cannot read config {path}: {source}")]
    ConfigRead { path: PathBuf, source: std::io::Error },

    #[error("invalid config {path}: {source}")]
    ConfigParse { path: PathBuf, source: toml::de::Error },

    #[error("environment variable {0} (model.api_key_env) is not set")]
    MissingApiKey(String),

    #[error("cannot set up the model: {0}")]
    Model(BoxError),

    #[error("cannot start the async runtime: {0}")]
    Runtime(std::io::Error),

    #[error("cannot listen for shutdown signals: {0}")]
    Signal(std::io::Error),
}
