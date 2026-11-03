//! Channels a notification may name, and where a scheduled run may post.
//!
//! A channel is written as `dm`, a name from `[discord.channels]`, a channel
//! ID, or a `<#id>` mention, and stored in one canonical spelling. A run
//! posts only to its job's channels, compared in that stored form.

use std::collections::BTreeMap;
use std::num::NonZeroU64;

/// Where a notification goes.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Destination {
    /// The first owner's DMs.
    Owner,
    Channel(NonZeroU64),
}

/// Where a scheduled run may post: only its job's `channels`, the first by default.
pub fn job_destination(channels: &[String], requested: Option<&str>, names: &BTreeMap<String, u64>) -> Result<Destination, String> {
    let Some(written) = requested.or(channels.first().map(String::as_str)) else {
        return Err("this job has no channels to post to".to_owned());
    };

    let (stored, destination) = channel(written, names)?;

    if !channels.contains(&stored) {
        return Err(format!("this job may only post to: {}", channels.join(", ")));
    }

    Ok(destination)
}

/// Reads a channel as written: `dm`, a name from `[discord.channels]`, a
/// channel ID, or a `<#id>` mention. Returns how to store it and where it points.
pub fn channel(written: &str, names: &BTreeMap<String, u64>) -> Result<(String, Destination), String> {
    let written = written.trim();

    if written == "dm" {
        return Ok(("dm".to_owned(), Destination::Owner));
    }

    if let Some(id) = names.get(written).copied().and_then(NonZeroU64::new) {
        return Ok((written.to_owned(), Destination::Channel(id)));
    }

    let id = written.strip_prefix("<#").and_then(|rest| rest.strip_suffix('>')).unwrap_or(written);

    match id.parse::<NonZeroU64>() {
        Ok(id) => Ok((id.to_string(), Destination::Channel(id))),
        Err(_) => {
            let known: Vec<&str> = names.keys().map(String::as_str).collect();
            Err(format!("unknown channel `{written}`; use dm, a channel ID, or one of: {}", known.join(", ")))
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::num::NonZeroU64;

    use super::{Destination, job_destination};

    fn channel(id: u64) -> Option<Destination> {
        NonZeroU64::new(id).map(Destination::Channel)
    }

    /// A scheduled run posts only where its job allows, however the channel is written.
    #[test]
    fn runs_post_only_to_their_jobs_channels() {
        let names: BTreeMap<String, u64> = [("alerts".to_owned(), 42)].into_iter().collect();
        let job = ["alerts".to_owned(), "7".to_owned()];

        let cases = [
            (None, channel(42)),
            (Some("7"), channel(7)),
            (Some("<#7>"), channel(7)),
            (Some("dm"), None),
            (Some("8"), None),
            // The same channel under another spelling than the job's list is refused, not guessed.
            (Some("42"), None),
            (Some("general"), None),
        ];

        for (requested, expected) in cases {
            assert_eq!(job_destination(&job, requested, &names).ok(), expected, "{requested:?}");
        }
    }
}
