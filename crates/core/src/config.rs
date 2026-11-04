//! Runtime settings and policy construction. File parsing belongs to the daemon.

use std::collections::BTreeMap;
use std::path::Path;

use chrono_tz::Tz;
use serde::Deserialize;
use shade_policy::{Caps, Limits, Policy};

use crate::Error;

/// Runtime settings assembled by the daemon; no HTTP transport configuration.
pub struct Config {
    pub model: ModelConfig,
    pub timezone: Option<String>,
    pub chat: ChatConfig,
    pub jobs: JobsConfig,
    pub hooks: BTreeMap<String, Caps>,
    pub limits: Limits,
    pub sandbox: Option<shade_sandbox::Config>,
    pub discord: Option<DiscordConfig>,
    pub mcp: BTreeMap<String, shade_mcp::Config>,
    pub memory: Option<crate::memory::Config>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DiscordConfig {
    /// Name of the environment variable holding the bot token, like `model.api_key_env`.
    pub token_env: String,
    /// Discord user IDs with owner chat permissions, including DMs and management.
    pub owners: Vec<u64>,
    /// Everyone else may mention the bot in server channels with these capabilities.
    #[serde(default)]
    pub guests: GuestConfig,
    /// Channels notifications may name, by short name. `dm` is always the first owner's DMs.
    #[serde(default)]
    pub channels: BTreeMap<String, u64>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GuestConfig {
    /// No tools by default. Management capabilities belong to owners only.
    #[serde(default)]
    pub caps: Caps,
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
    /// Optional USD per million tokens, captured with each request's usage.
    pub prices: Option<crate::usage::Prices>,
}

#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Provider {
    /// Anthropic Messages API.
    Anthropic,
    /// OpenAI Chat Completions API, as served by many gateways.
    OpenaiCompatible,
}

/// The parts of the config that make up the policy.
pub struct Sections {
    pub chat: ChatConfig,
    pub discord_guests: Caps,
    pub jobs: JobsConfig,
    /// Each hook's caps.
    pub hooks: BTreeMap<String, Caps>,
    pub limits: Limits,
    pub timezone: Option<String>,
    pub channels: BTreeMap<String, u64>,
    /// MCP server names, with what each is for.
    pub servers: BTreeMap<String, String>,
}

/// Builds and checks the policy, so a bad config fails at startup rather than at dispatch.
pub fn policy(path: &Path, config: Sections) -> Result<Policy, Error> {
    let Sections { chat, discord_guests, jobs, hooks, limits, timezone, channels, servers } = config;

    let timezone = match timezone {
        Some(name) => match name.parse::<Tz>() {
            Ok(timezone) => timezone,
            Err(_) => return Err(invalid(path, format!("unknown timezone `{name}`"))),
        },
        None => chrono_tz::UTC,
    };

    let policy = Policy {
        chat: chat.caps,
        discord_guests,
        chat_prompt: chat.prompt,
        jobs_prompt: jobs.prompt,
        channels,
        servers,
        hooks,
        limits,
        timezone,
    };

    if let Err(message) = policy.validate() {
        return Err(invalid(path, message));
    }

    Ok(policy)
}

fn invalid(path: &Path, message: String) -> Error {
    Error::ConfigInvalid { path: path.to_owned(), message }
}
