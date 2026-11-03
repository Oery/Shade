//! Capabilities, origins, and limits: the policy that bounds every grant.
//!
//! Policy comes from the config file and is fixed for the life of the
//! process. A root's authority is its stored grant bounded by the current
//! policy, so capabilities removed from the config disappear on restart.

use std::collections::{BTreeMap, BTreeSet};

use chrono_tz::Tz;
use serde::Deserialize;

/// A named piece of authority. MCP server names join these in M13.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Deserialize)]
#[serde(try_from = "String")]
pub enum Cap {
    Now,
    Notify,
    Shell,
    ShellNetwork,
    Schedules,
    Watches,
}

pub type Caps = BTreeSet<Cap>;

impl Cap {
    const ALL: [Self; 6] = [Self::Now, Self::Notify, Self::Shell, Self::ShellNetwork, Self::Schedules, Self::Watches];

    pub fn name(self) -> &'static str {
        match self {
            Self::Now => "now",
            Self::Notify => "notify",
            Self::Shell => "shell",
            Self::ShellNetwork => "shell_network",
            Self::Schedules => "schedules",
            Self::Watches => "watches",
        }
    }

    fn parse(name: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|cap| cap.name() == name)
    }

    /// Managing schedules and watches stays with owner chat.
    fn is_management(self) -> bool {
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

/// What started a root.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Origin {
    Chat,
    Schedule,
    Watch,
    Webhook,
}

impl Origin {
    pub fn name(self) -> &'static str {
        match self {
            Self::Chat => "chat",
            Self::Schedule => "schedule",
            Self::Watch => "watch",
            Self::Webhook => "webhook",
        }
    }

    pub fn parse(name: &str) -> Option<Self> {
        [Self::Chat, Self::Schedule, Self::Watch, Self::Webhook].into_iter().find(|origin| origin.name() == name)
    }
}

/// A root's authority as recorded when it was registered.
#[derive(Debug, PartialEq)]
pub struct Grant {
    pub origin: Origin,
    /// The adapter for chat, the job ID, or the hook name.
    pub source: String,
    /// Capabilities delegated to a job. Chat and webhook roots store none and
    /// follow their origin's current policy.
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
}

impl Default for Limits {
    fn default() -> Self {
        Self { max_completion_rounds: 40, max_tool_calls: 100, max_run_seconds: 900, max_notifications: 10 }
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
    pub chat_prompt: Option<String>,
    pub hooks: BTreeMap<String, Caps>,
    pub limits: Limits,
    pub timezone: Tz,
}

impl Policy {
    /// Checks the rules config parsing cannot express.
    pub fn validate(&self) -> Result<(), String> {
        check_caps("[chat]", &self.chat)?;

        for (name, caps) in &self.hooks {
            let section = format!("[hooks.{name}]");
            check_caps(&section, caps)?;

            if let Some(cap) = caps.iter().find(|cap| cap.is_management()) {
                return Err(format!("{section}: `{}` belongs to owner chat and cannot be given to a hook", cap.name()));
            }
        }

        Ok(())
    }

    /// Whether any root can hold `cap`. Jobs never exceed owner chat.
    pub fn grants_anywhere(&self, cap: Cap) -> bool {
        self.chat.contains(&cap) || self.hooks.values().any(|caps| caps.contains(&cap))
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
            Origin::Schedule | Origin::Watch => Some(self.chat.iter().copied().filter(|cap| !cap.is_management()).collect()),
            Origin::Webhook => self.hooks.get(source).cloned(),
        }
    }
}

fn check_caps(section: &str, caps: &Caps) -> Result<(), String> {
    if caps.contains(&Cap::ShellNetwork) && !caps.contains(&Cap::Shell) {
        return Err(format!("{section}: `shell_network` requires `shell`"));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::{Cap, Caps, Charge, Grant, Limits, Origin, Policy, Usage};

    type Hooks<'a> = &'a [(&'a str, &'a [Cap])];

    fn caps(list: &[Cap]) -> Caps {
        list.iter().copied().collect()
    }

    fn policy(chat: &[Cap], hooks: Hooks<'_>) -> Policy {
        let hooks = hooks.iter().map(|(name, list)| ((*name).to_owned(), caps(list))).collect::<BTreeMap<_, _>>();

        Policy { chat: caps(chat), chat_prompt: None, hooks, limits: Limits::default(), timezone: chrono_tz::UTC }
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
    fn limits_refuse_at_the_boundary() {
        let limits = Limits { max_completion_rounds: 2, max_tool_calls: 1, max_run_seconds: 60, max_notifications: 1 };
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
}
