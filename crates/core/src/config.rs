//! Daemon configuration: a human-controlled TOML file.

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use chrono_tz::Tz;
use serde::Deserialize;

use crate::Error;
use crate::policy::{Caps, Limits, Policy};

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub model: ModelConfig,
    /// IANA name, such as `Europe/Paris`. UTC when absent.
    pub timezone: Option<String>,
    /// Without a `[chat]` section, chat has no tools.
    #[serde(default)]
    pub chat: ChatConfig,
    #[serde(default)]
    pub hooks: BTreeMap<String, HookConfig>,
    #[serde(default)]
    pub limits: Limits,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChatConfig {
    #[serde(default)]
    pub caps: Caps,
    /// Added to the system prompt of every chat conversation.
    pub prompt: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HookConfig {
    pub caps: Caps,
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

/// The given path, or the XDG default when there is none.
pub fn path(path: Option<PathBuf>) -> Result<PathBuf, Error> {
    match path.or_else(shade_proto::config_file) {
        Some(path) => Ok(path),
        None => Err(Error::NoConfigDir),
    }
}

pub fn load(path: &Path) -> Result<Config, Error> {
    let text = match fs::read_to_string(path) {
        Ok(text) => text,
        Err(source) => return Err(Error::ConfigRead { path: path.to_owned(), source }),
    };

    match toml::from_str(&text) {
        Ok(config) => Ok(config),
        Err(source) => Err(Error::ConfigParse { path: path.to_owned(), source }),
    }
}

/// Builds and checks the policy, so a bad config fails at startup rather than at dispatch.
pub fn policy(
    path: &Path, chat: ChatConfig, hooks: BTreeMap<String, HookConfig>, limits: Limits, timezone: Option<String>,
) -> Result<Policy, Error> {
    let timezone = match timezone {
        Some(name) => match name.parse::<Tz>() {
            Ok(timezone) => timezone,
            Err(_) => return Err(invalid(path, format!("unknown timezone `{name}`"))),
        },
        None => chrono_tz::UTC,
    };

    let hooks = hooks.into_iter().map(|(name, hook)| (name, hook.caps)).collect();
    let policy = Policy { chat: chat.caps, chat_prompt: chat.prompt, hooks, limits, timezone };

    if let Err(message) = policy.validate() {
        return Err(invalid(path, message));
    }

    Ok(policy)
}

fn invalid(path: &Path, message: String) -> Error {
    Error::ConfigInvalid { path: path.to_owned(), message }
}
