//! Notifications: messages to the owner, in their DMs or a Discord channel.
//!
//! Who may post where is decided by the caller's origin, never by the model
//! alone: owner chat anywhere, a scheduled run only in its job's channels,
//! runtime failure notices only in the owner's DMs. A notice goes to the
//! adapter through a queue and counts as sent only when the adapter confirms
//! it. Nothing is resent: when the confirmation is lost, the notice may or
//! may not have arrived, and callers say so.

use std::collections::BTreeMap;
use std::num::NonZeroU64;

use infinity_agent_core::ThreadId;
use thiserror::Error;
use tokio::sync::{mpsc, oneshot};

use crate::store::grants::Grants;

/// Notices waiting for the adapter.
const QUEUE: usize = 16;

/// Longest runtime failure notice, in bytes. The cause is cut to fit.
const MAX_FAILURE_NOTICE: usize = 1500;

/// Where a notice goes.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Destination {
    /// The first owner's DMs.
    Owner,
    Channel(NonZeroU64),
}

/// Why the adapter did not confirm a notice.
pub enum Failure {
    /// The service answered with an error: nothing was posted.
    Refused(String),
    /// No answer: it may have been posted.
    Unknown(String),
}

/// One message, where it goes, and the adapter's answer.
pub struct Notice {
    pub text: String,
    pub destination: Destination,
    pub ack: oneshot::Sender<Result<(), Failure>>,
}

#[derive(Debug, Error)]
pub enum NotifyError {
    #[error("the notification adapter has stopped; nothing was sent")]
    Stopped,

    #[error("Discord refused the notification ({0}); nothing was sent")]
    Refused(String),

    #[error("the adapter did not confirm the notification ({0}); it may or may not have arrived")]
    Unconfirmed(String),
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

#[derive(Clone)]
pub struct Notifier {
    queue: mpsc::Sender<Notice>,
    grants: Grants,
}

impl Notifier {
    /// A notifier and the queue its adapter serves.
    pub fn new(grants: Grants) -> (Self, mpsc::Receiver<Notice>) {
        let (queue, notices) = mpsc::channel(QUEUE);

        (Self { queue, grants }, notices)
    }

    /// Sends `text` to `destination` and waits for the adapter to confirm it.
    pub async fn send(&self, destination: Destination, text: String) -> Result<(), NotifyError> {
        let (ack, answer) = oneshot::channel();

        if self.queue.send(Notice { text, destination, ack }).await.is_err() {
            return Err(NotifyError::Stopped);
        }

        match answer.await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(Failure::Refused(reason))) => Err(NotifyError::Refused(reason)),
            Ok(Err(Failure::Unknown(reason))) => Err(NotifyError::Unconfirmed(reason)),
            Err(_) => Err(NotifyError::Unconfirmed("the adapter stopped".to_owned())),
        }
    }

    /// Tells the owner that work in `thread` failed, once per budget. The
    /// model's own notifications cannot use this allowance up. `text` is
    /// cut to a bounded length.
    pub async fn failure(&self, thread: &ThreadId<str>, mut text: String) {
        if !self.claim_failure_notice(thread).await {
            return;
        }

        text.truncate(text.floor_char_boundary(MAX_FAILURE_NOTICE));

        if let Err(error) = self.send(Destination::Owner, text).await {
            tracing::warn!(%error, %thread, "cannot send a failure notice");
        }
    }

    async fn claim_failure_notice(&self, thread: &ThreadId<str>) -> bool {
        match self.grants.claim_failure_notice(thread).await {
            Ok(claimed) => {
                if !claimed {
                    tracing::debug!(%thread, "this run already sent its failure notice");
                }

                claimed
            }
            Err(error) => {
                tracing::warn!(%error, %thread, "cannot claim a failure notice");
                false
            }
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
