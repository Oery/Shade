use std::path::PathBuf;

use thiserror::Error;

pub type BoxError = Box<dyn std::error::Error + Send + Sync>;

#[derive(Debug, Error)]
pub enum Error {
    #[error("failed to initialize logging: {0}")]
    Tracing(BoxError),

    #[error("cannot locate the config file: set XDG_CONFIG_HOME or HOME to an absolute path, or pass --config")]
    NoConfigDir,
    #[error("cannot read config {path}: {source}")]
    ConfigRead { path: PathBuf, source: std::io::Error },
    #[error("invalid config {path}: {source}")]
    ConfigParse { path: PathBuf, source: toml::de::Error },
    #[error("{0} ([api] token_env) must contain a token of at least 32 bytes")]
    ApiToken(String),
    #[error("{name} ([hooks.{hook}] token_env) is missing or shorter than 32 bytes")]
    HookToken { hook: String, name: String },
    #[error(transparent)]
    Http(#[from] shade_http::Error),
    #[error("cannot start the async runtime: {0}")]
    Runtime(std::io::Error),
    #[error("cannot listen for shutdown signals: {0}")]
    Signal(std::io::Error),

    #[error(transparent)]
    Core(#[from] shade_core::Error),
}
