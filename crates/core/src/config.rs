//! Daemon configuration: a human-controlled TOML file.

use std::fs;
use std::path::PathBuf;

use serde::Deserialize;

use crate::Error;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub model: ModelConfig,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelConfig {
    pub provider: Provider,
    pub id: String,
    /// Name of the environment variable holding the API key. Secrets never live in the file.
    pub api_key_env: String,
    /// Replaces the provider's default endpoint, for gateways that speak its API.
    pub base_url: Option<String>,
    /// Header that carries the thread ID on every request, for gateways that route by conversation.
    pub session_header: Option<String>,
    pub context_window: usize,
    pub max_output_tokens: u64,
}

#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Provider {
    /// Anthropic Messages API.
    Anthropic,
    /// OpenAI Chat Completions API, as served by many gateways.
    OpenaiCompatible,
}

/// Reads the file at `path`, or at the XDG default when `path` is `None`.
pub fn load(path: Option<PathBuf>) -> Result<Config, Error> {
    let Some(path) = path.or_else(shade_proto::config_file) else {
        return Err(Error::NoConfigDir);
    };

    let text = match fs::read_to_string(&path) {
        Ok(text) => text,
        Err(source) => return Err(Error::ConfigRead { path, source }),
    };

    match toml::from_str(&text) {
        Ok(config) => Ok(config),
        Err(source) => Err(Error::ConfigParse { path, source }),
    }
}
