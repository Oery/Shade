use std::path::PathBuf;

use shade_sandbox::SandboxError;
use shade_store::db::OpenError;
use thiserror::Error;

use crate::adapters::discord::DiscordError;

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

    #[error("invalid config {path}: {message}")]
    ConfigInvalid { path: PathBuf, message: String },

    #[error("cannot read secrets {path}: {source}")]
    SecretsRead { path: PathBuf, source: BoxError },

    #[error("secrets file {path} is accessible to other users; run `chmod 600` on it")]
    SecretsExposed { path: PathBuf },

    #[error("{0} (model.api_key_env) is not set in the environment or the .env file next to the config")]
    MissingApiKey(String),

    #[error(transparent)]
    Database(#[from] OpenError),

    #[error("model.compact_at_tokens is {compact_at}; it must be between 1 and {limit} (75% of context_window)")]
    CompactionBudget { compact_at: usize, limit: usize },

    #[error("cannot set up the model: {0}")]
    Model(BoxError),

    #[error("`shell` is granted but the config has no [sandbox] section")]
    SandboxNotConfigured,

    #[error(transparent)]
    Sandbox(#[from] SandboxError),

    #[error(transparent)]
    Discord(#[from] DiscordError),

    #[error("cannot resume work interrupted by the last shutdown: {0}")]
    Recovery(BoxError),

    #[error("invalid job file {0}")]
    JobFiles(String),

    #[error("cannot start the async runtime: {0}")]
    Runtime(std::io::Error),

    #[error("cannot listen for shutdown signals: {0}")]
    Signal(std::io::Error),
}
