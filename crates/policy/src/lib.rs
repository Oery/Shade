//! Capabilities, origins, and limits: the policy that bounds every grant.
//!
//! Policy comes from the config file and is fixed for the life of the
//! process. A root's authority is its stored grant bounded by the current
//! policy, so capabilities removed from the config disappear on restart.

pub mod channels;

use std::collections::{BTreeMap, BTreeSet};

use chrono_tz::Tz;
use serde::Deserialize;

/// A named piece of authority: one of Shade's own, or an MCP server.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Deserialize)]
#[serde(try_from = "String")]
pub enum Cap {
    Now,
    Notify,
    Shell,
    ShellNetwork,
    Schedules,
    Watches,
    /// The tools of the MCP server configured as `[mcp.<name>]`.
    Server(String),
}

pub type Caps = BTreeSet<Cap>;

impl Cap {
    const BUILTIN: [Self; 6] = [Self::Now, Self::Notify, Self::Shell, Self::ShellNetwork, Self::Schedules, Self::Watches];

    pub fn name(&self) -> &str {
        match self {
            Self::Now => "now",
            Self::Notify => "notify",
            Self::Shell => "shell",
            Self::ShellNetwork => "shell_network",
            Self::Schedules => "schedules",
            Self::Watches => "watches",
            Self::Server(name) => name,
        }
    }

    /// What the capability lets a scheduled run do, for the model choosing caps.
    /// A server's own description is in the policy.
    pub fn description(&self) -> &str {
        match self {
            Self::Now => "read the current time",
            Self::Notify => "send the owner a message (the usual way a job reports)",
            Self::Shell => "run commands in the sandboxed workspace",
            Self::ShellNetwork => "give shell commands network access (needs shell)",
            Self::Schedules => "manage scheduled jobs (owner chat only)",
            Self::Watches => "manage watches (owner chat only)",
            Self::Server(_) => "use this MCP server's tools",
        }
    }

    /// One of Shade's own capabilities, or a name an MCP server may have.
    /// Whether that server is configured is the policy's to check.
    fn parse(name: &str) -> Option<Self> {
        if let Some(cap) = Self::BUILTIN.into_iter().find(|cap| cap.name() == name) {
            return Some(cap);
        }

        is_server_name(name).then(|| Self::Server(name.to_owned()))
    }

    /// Managing schedules and watches stays with owner chat.
    fn is_management(&self) -> bool {
        matches!(self, Self::Schedules | Self::Watches)
    }
}

impl TryFrom<String> for Cap {
    type Error = String;

    fn try_from(name: String) -> Result<Self, String> {
        match Self::parse(&name) {
            Some(cap) => Ok(cap),
            None => Err(format!("unknown capability `{name}`")),
        }
    }
}

/// MCP server names prefix their tools' names, so they are short words:
/// lowercase letters, digits and `_`, starting with a letter.
pub fn is_server_name(name: &str) -> bool {
    let word = name.chars().all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_');

    word && name.starts_with(|c: char| c.is_ascii_lowercase()) && name.len() <= 24
}

/// What started a root.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Origin {
    Chat,
    /// A non-owner mentioning Shade in a Discord server channel.
    Guest,
    Schedule,
    Watch,
    Webhook,
}

impl Origin {
    pub fn name(self) -> &'static str {
        match self {
            Self::Chat => "chat",
            Self::Guest => "guest",
            Self::Schedule => "schedule",
            Self::Watch => "watch",
            Self::Webhook => "webhook",
        }
    }

    pub fn parse(name: &str) -> Option<Self> {
        [Self::Chat, Self::Guest, Self::Schedule, Self::Watch, Self::Webhook].into_iter().find(|origin| origin.name() == name)
    }
}

/// A root's authority as recorded when it was registered.
#[derive(Debug, PartialEq)]
pub struct Grant {
    pub origin: Origin,
    /// The adapter for owner or guest chat, the job ID, or the hook name.
    pub source: String,
    /// Capabilities delegated to a job. Owner chat, guest chat and webhook
    /// roots store none and follow their origin's current policy.
    pub caps: Option<Vec<String>>,
}

/// Shared bounds on one chat turn or background run, children included.
#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct Limits {
    pub max_completion_rounds: u32,
    pub max_tool_calls: u32,
    pub max_run_seconds: u32,
    /// Messages the model may send the owner with `notify`.
    pub max_notifications: u32,
    /// Scheduled runs running at once. Others wait, durably, in the queue.
    pub max_concurrent_background_runs: u32,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_completion_rounds: 40,
            max_tool_calls: 100,
            max_run_seconds: 900,
            max_notifications: 10,
            max_concurrent_background_runs: 4,
        }
    }
}

/// Work counted against a budget.
#[derive(Clone, Copy, Debug)]
pub enum Charge {
    /// One request to the model provider, retries included.
    Completion,
    ToolCall,
    /// One message to the owner, on top of the `notify` call's own charge.
    Notification,
}

/// A budget's counters so far.
pub struct Usage {
    pub completions: i64,
    pub tool_calls: i64,
    pub notifications: i64,
    pub elapsed_seconds: i64,
}

impl Limits {
    /// The limit, named as in the config, that refuses one more `charge`.
    pub fn check(&self, usage: &Usage, charge: Charge) -> Option<&'static str> {
        if usage.elapsed_seconds >= i64::from(self.max_run_seconds) {
            return Some("max_run_seconds");
        }

        match charge {
            Charge::Completion if usage.completions >= i64::from(self.max_completion_rounds) => Some("max_completion_rounds"),
            Charge::ToolCall if usage.tool_calls >= i64::from(self.max_tool_calls) => Some("max_tool_calls"),
            Charge::Notification if usage.notifications >= i64::from(self.max_notifications) => Some("max_notifications"),
            _ => None,
        }
    }
}

pub struct Policy {
    pub chat: Caps,
    /// Non-owner Discord mentions, independent of owner chat.
    pub discord_guests: Caps,
    pub chat_prompt: Option<String>,
    /// Replaces the default system prompt addition of background runs.
    pub jobs_prompt: Option<String>,
    /// Named Discord channels, for notifications.
    pub channels: BTreeMap<String, u64>,
    /// Configured MCP servers by name, with what each is for.
    pub servers: BTreeMap<String, String>,
    pub hooks: BTreeMap<String, Caps>,
    pub limits: Limits,
    pub timezone: Tz,
}

impl Policy {
    /// Checks the rules config parsing cannot express.
    pub fn validate(&self) -> Result<(), String> {
        for name in self.servers.keys() {
            check_server(name)?;
        }

        self.check_caps("[chat]", &self.chat)?;
        self.check_caps("[discord.guests]", &self.discord_guests)?;

        if let Some(cap) = self.discord_guests.iter().find(|cap| cap.is_management()) {
            return Err(format!("[discord.guests]: `{}` belongs to owner chat", cap.name()));
        }

        for (name, id) in &self.channels {
            check_channel(name, *id)?;
        }

        for (name, caps) in &self.hooks {
            let section = format!("[hooks.{name}]");
            self.check_caps(&section, caps)?;

            if let Some(cap) = caps.iter().find(|cap| cap.is_management()) {
                return Err(format!("{section}: `{}` belongs to owner chat and cannot be given to a hook", cap.name()));
            }
        }

        Ok(())
    }

    /// What a capability is for: a server's configured description, or Shade's own.
    pub fn describe<'a>(&'a self, cap: &'a Cap) -> &'a str {
        match cap {
            Cap::Server(name) => self.servers.get(name).map_or(cap.description(), String::as_str),
            builtin => builtin.description(),
        }
    }

    /// Whether any root can hold `cap`. Jobs never exceed owner chat.
    pub fn grants_anywhere(&self, cap: Cap) -> bool {
        self.chat.contains(&cap) || self.discord_guests.contains(&cap) || self.hooks.values().any(|caps| caps.contains(&cap))
    }

    /// The capabilities `grant` holds under this policy. `None` means its
    /// source is gone (a deleted hook) and the root must not run.
    pub fn effective(&self, grant: &Grant) -> Option<Caps> {
        let ceiling = self.ceiling(grant.origin, &grant.source)?;

        let Some(requested) = &grant.caps else {
            return Some(ceiling);
        };

        let mut caps = Caps::new();

        for name in requested {
            match Cap::parse(name) {
                Some(cap) if ceiling.contains(&cap) => {
                    caps.insert(cap);
                }
                _ => tracing::warn!(cap = %name, source = %grant.source, "dropping a capability the current policy does not allow"),
            }
        }

        Some(caps)
    }

    /// The most an origin may hold. Jobs never exceed owner chat and never manage jobs.
    fn ceiling(&self, origin: Origin, source: &str) -> Option<Caps> {
        match origin {
            Origin::Chat => Some(self.chat.clone()),
            Origin::Guest => Some(delegable(&self.discord_guests)),
            Origin::Schedule | Origin::Watch => Some(delegable(&self.chat)),
            Origin::Webhook => self.hooks.get(source).cloned(),
        }
    }
}

/// The caps a holder of `caps` may give a job: its own, minus management.
pub fn delegable(caps: &Caps) -> Caps {
    caps.iter().filter(|cap| !cap.is_management()).cloned().collect()
}

/// Channel names are short words that cannot be mistaken for `dm` or an ID.
fn check_channel(name: &str, id: u64) -> Result<(), String> {
    let word = name.chars().all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-' || c == '_');

    if name == "dm" || name.is_empty() || name.len() > 32 || !word || name.chars().all(|c| c.is_ascii_digit()) {
        return Err(format!(
            "[discord.channels]: `{name}` must be a short lowercase word other than `dm`, not only digits"
        ));
    }

    if id == 0 {
        return Err(format!("[discord.channels]: `{name}` has no channel ID"));
    }

    Ok(())
}

impl Policy {
    fn check_caps(&self, section: &str, caps: &Caps) -> Result<(), String> {
        if caps.contains(&Cap::ShellNetwork) && !caps.contains(&Cap::Shell) {
            return Err(format!("{section}: `shell_network` requires `shell`"));
        }

        for cap in caps {
            if let Cap::Server(name) = cap
                && !self.servers.contains_key(name)
            {
                return Err(format!("{section}: `{name}` is neither a Shade capability nor a server under [mcp]"));
            }
        }

        Ok(())
    }
}

/// A server name must fit in tool names and must not pass for one of Shade's own capabilities.
fn check_server(name: &str) -> Result<(), String> {
    if !is_server_name(name) {
        return Err(format!("[mcp.{name}]: use up to 24 lowercase letters, digits and _, starting with a letter"));
    }

    // `schedule` would collide with the `schedule_*` tools.
    if Cap::BUILTIN.iter().any(|cap| cap.name() == name) || name == "schedule" {
        return Err(format!("[mcp.{name}]: `{name}` is taken by Shade's own capabilities or tools; choose another name"));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::{Cap, Caps, Charge, Grant, Limits, Origin, Policy, Usage};

    type Hooks<'a> = &'a [(&'a str, &'a [Cap])];

    fn caps(list: &[Cap]) -> Caps {
        list.iter().cloned().collect()
    }

    fn policy(chat: &[Cap], hooks: Hooks<'_>) -> Policy {
        let hooks = hooks.iter().map(|(name, list)| ((*name).to_owned(), caps(list))).collect::<BTreeMap<_, _>>();

        Policy {
            chat: caps(chat),
            discord_guests: Caps::new(),
            chat_prompt: None,
            jobs_prompt: None,
            channels: BTreeMap::new(),
            servers: [("vault".to_owned(), "Notes".to_owned())].into_iter().collect(),
            hooks,
            limits: Limits::default(),
            timezone: chrono_tz::UTC,
        }
    }

    fn grant(origin: Origin, source: &str, requested: Option<&[&str]>) -> Grant {
        let caps = requested.map(|list| list.iter().map(|name| (*name).to_owned()).collect());

        Grant { origin, source: source.to_owned(), caps }
    }

    #[test]
    fn grants_never_exceed_current_policy() {
        use Cap::{Notify, Now, Schedules, Shell};

        let chat = [Now, Shell, Schedules];
        let hooks: Hooks<'_> = &[("github", &[Now])];

        let cases = [
            // A cap removed from [chat] disappears from existing conversations.
            (policy(&[Now], hooks), grant(Origin::Chat, "terminal", None), Some(caps(&[Now]))),
            // A job keeps only what owner chat still has, minus management.
            (
                policy(&chat, hooks),
                grant(Origin::Schedule, "job", Some(&["now", "notify", "schedules"])),
                Some(caps(&[Now])),
            ),
            (
                policy(&[Now, Notify], hooks),
                grant(Origin::Watch, "w", Some(&["notify", "watches"])),
                Some(caps(&[Notify])),
            ),
            // Unknown stored names are dropped, not fatal.
            (
                policy(&chat, hooks),
                grant(Origin::Schedule, "job", Some(&["vault", "shell"])),
                Some(caps(&[Shell])),
            ),
            // A hook's grant follows its section; a deleted hook cannot run at all.
            (policy(&chat, hooks), grant(Origin::Webhook, "github", None), Some(caps(&[Now]))),
            (policy(&chat, hooks), grant(Origin::Webhook, "gitlab", None), None),
        ];

        for (policy, grant, expected) in cases {
            assert_eq!(policy.effective(&grant), expected, "{grant:?}");
        }
    }

    #[test]
    fn invalid_policies_fail_startup() {
        use Cap::{Now, Schedules, Shell, ShellNetwork, Watches};

        let cases: [(&[Cap], Hooks<'_>, bool); 5] = [
            (&[Now, Shell, ShellNetwork, Schedules, Watches], &[("github", &[Now, Shell, ShellNetwork])], true),
            (&[ShellNetwork], &[], false),
            (&[Shell], &[("github", &[ShellNetwork])], false),
            (&[Schedules], &[("github", &[Schedules])], false),
            (&[Watches], &[("github", &[Now, Watches])], false),
        ];

        for (chat, hooks, valid) in cases {
            assert_eq!(policy(chat, hooks).validate().is_ok(), valid, "chat {chat:?}, hooks {hooks:?}");
        }
    }

    #[test]
    fn guest_caps_are_independent_and_follow_current_policy() {
        let mut policy = policy(&[Cap::Shell, Cap::Schedules], &[]);
        let guest = grant(Origin::Guest, "discord_guest", None);

        assert_eq!(Origin::parse("guest"), Some(Origin::Guest));
        assert_eq!(policy.effective(&guest), Some(Caps::new()));

        policy.discord_guests.insert(Cap::Now);
        assert_eq!(policy.effective(&guest), Some(caps(&[Cap::Now])));
        assert!(policy.grants_anywhere(Cap::Now));
        assert!(policy.validate().is_ok());

        policy.discord_guests.clear();
        assert_eq!(policy.effective(&guest), Some(Caps::new()));
    }

    #[test]
    fn guest_caps_are_validated_and_never_allow_management() {
        let cases = [
            (caps(&[Cap::Now]), true),
            (caps(&[Cap::Shell, Cap::ShellNetwork]), true),
            (caps(&[Cap::Server("vault".to_owned())]), true),
            (caps(&[Cap::Server("unknown".to_owned())]), false),
            (caps(&[Cap::ShellNetwork]), false),
            (caps(&[Cap::Schedules]), false),
            (caps(&[Cap::Watches]), false),
        ];

        for (caps, valid) in cases {
            let mut policy = policy(&[], &[]);
            policy.discord_guests = caps;
            assert_eq!(policy.validate().is_ok(), valid);

            let effective = policy.effective(&grant(Origin::Guest, "discord_guest", None));
            assert!(effective.is_some_and(|caps| !caps.contains(&Cap::Schedules) && !caps.contains(&Cap::Watches)));
        }
    }

    #[test]
    fn limits_refuse_at_the_boundary() {
        let limits = Limits { max_completion_rounds: 2, max_tool_calls: 1, max_run_seconds: 60, max_notifications: 1, ..Limits::default() };
        let usage =
            |completions, tool_calls, notifications, elapsed_seconds| Usage { completions, tool_calls, notifications, elapsed_seconds };

        let cases = [
            (usage(1, 0, 0, 59), Charge::Completion, None),
            (usage(2, 0, 0, 0), Charge::Completion, Some("max_completion_rounds")),
            (usage(0, 1, 0, 0), Charge::ToolCall, Some("max_tool_calls")),
            (usage(0, 0, 1, 0), Charge::Notification, Some("max_notifications")),
            // Each counter refuses only its own work.
            (usage(0, 0, 1, 0), Charge::ToolCall, None),
            // The deadline wins even with counters to spare.
            (usage(0, 0, 0, 60), Charge::ToolCall, Some("max_run_seconds")),
        ];

        for (usage, charge, expected) in cases {
            assert_eq!(limits.check(&usage, charge), expected, "{charge:?}");
        }
    }

    /// A server is a capability like Shade's own: named only if configured,
    /// never under a name Shade uses, and delegable to jobs when chat holds it.
    #[test]
    fn servers_are_capabilities_by_name() {
        let vault = || Cap::Server("vault".to_owned());
        let unknown = Cap::Server("mail".to_owned());

        assert!(policy(&[vault()], &[]).validate().is_ok());
        assert!(policy(&[unknown], &[]).validate().is_err(), "an unconfigured server was accepted");

        for taken in ["notify", "schedule", "Vault", "9lives"] {
            let mut clash = policy(&[], &[]);
            clash.servers.insert(taken.to_owned(), "x".to_owned());
            assert!(clash.validate().is_err(), "server `{taken}` was accepted");
        }

        let job = grant(Origin::Schedule, "job", Some(&["vault", "mail"]));
        assert_eq!(policy(&[vault()], &[]).effective(&job), Some(caps(&[vault()])));
    }
}
