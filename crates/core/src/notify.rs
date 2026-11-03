//! Notifications: messages to the owner, in their DMs or a Discord channel.
//!
//! Who may post where is decided by the caller's origin, never by the model
//! alone: owner chat anywhere, a scheduled run only in its job's channels,
//! runtime failure notices only in the owner's DMs. A notice goes to the
//! adapter through a queue and counts as sent only when the adapter confirms
//! it. Nothing is resent: when the confirmation is lost, the notice may or
//! may not have arrived, and callers say so.

use infinity_agent_core::ThreadId;
use shade_policy::channels::Destination;
use shade_store::grants::Grants;
use thiserror::Error;
use tokio::sync::{mpsc, oneshot};

/// Notices waiting for the adapter.
const QUEUE: usize = 16;

/// Longest runtime failure notice, in bytes. The cause is cut to fit.
const MAX_FAILURE_NOTICE: usize = 1500;

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
