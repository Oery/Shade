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
    /// Background runs: scheduled jobs and, later, watches.
    #[serde(default)]
    pub jobs: JobsConfig,
    #[serde(default)]
    pub hooks: BTreeMap<String, HookConfig>,
    #[serde(default)]
    pub limits: Limits,
    /// Where `shell` runs. Required when any origin holds `shell`.
    pub sandbox: Option<SandboxConfig>,
    /// Owner chat over Discord. Off without this section.
    pub discord: Option<DiscordConfig>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DiscordConfig {
    /// Name of the environment variable holding the bot token, like `model.api_key_env`.
    pub token_env: String,
    /// Discord user IDs allowed to chat. Everyone else is ignored.
    pub owners: Vec<u64>,
    /// Channels notifications may name, by short name. `dm` is always the first owner's DMs.
    #[serde(default)]
    pub channels: BTreeMap<String, u64>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChatConfig {
    #[serde(default)]
    pub caps: Caps,
    /// Added to the system prompt of every chat conversation.
    pub prompt: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JobsConfig {
    /// Replaces Shade's default instructions for background runs.
    pub prompt: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HookConfig {
    pub caps: Caps,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SandboxConfig {
    /// Host directory mounted read-write at `/workspace`, the shell's working directory.
    pub workspace: PathBuf,
    /// Host directory mounted read-write at `/vault`, only for roots granted `vault`.
    pub vault: Option<PathBuf>,
    /// Variables copied from the daemon's environment. Everything else is cleared.
    #[serde(default)]
    pub env: Vec<String>,
    #[serde(default)]
    pub isolation: Isolation,
    /// The longest a command may run when it asks for more than the default 30 seconds.
    #[serde(default = "default_max_timeout")]
    pub max_timeout_seconds: u64,
}

fn default_max_timeout() -> u64 {
    300
}

#[derive(Clone, Copy, Debug, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Isolation {
    #[default]
    Bubblewrap,
    /// Development only: shell commands run on the host with the owner's full authority.
    Off,
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
    /// History size, in tokens, past which a conversation is summarized.
    /// Every turn resends the whole history, so this bounds what a turn
    /// costs. 100k (or 75% of the window, if smaller) when absent.
    pub compact_at_tokens: Option<usize>,
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

/// The parts of the config that make up the policy.
pub struct Sections {
    pub chat: ChatConfig,
    pub jobs: JobsConfig,
    pub hooks: BTreeMap<String, HookConfig>,
    pub limits: Limits,
    pub timezone: Option<String>,
    pub channels: BTreeMap<String, u64>,
}

/// Builds and checks the policy, so a bad config fails at startup rather than at dispatch.
pub fn policy(path: &Path, config: Sections) -> Result<Policy, Error> {
    let Sections { chat, jobs, hooks, limits, timezone, channels } = config;

    let timezone = match timezone {
        Some(name) => match name.parse::<Tz>() {
            Ok(timezone) => timezone,
            Err(_) => return Err(invalid(path, format!("unknown timezone `{name}`"))),
        },
        None => chrono_tz::UTC,
    };

    let hooks = hooks.into_iter().map(|(name, hook)| (name, hook.caps)).collect();
    let policy = Policy { chat: chat.caps, chat_prompt: chat.prompt, jobs_prompt: jobs.prompt, channels, hooks, limits, timezone };

    if let Err(message) = policy.validate() {
        return Err(invalid(path, message));
    }

    Ok(policy)
}

fn invalid(path: &Path, message: String) -> Error {
    Error::ConfigInvalid { path: path.to_owned(), message }
}
