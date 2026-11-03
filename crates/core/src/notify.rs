//! Notifications: messages to the owner's one fixed destination.
//!
//! The model never picks a recipient. A notice goes to the adapter through a
//! queue and counts as sent only when the adapter confirms it. Nothing is
//! resent: when the confirmation is lost, the notice may or may not have
//! arrived, and callers say so.

use infinity_agent_core::ThreadId;
use thiserror::Error;
use tokio::sync::{mpsc, oneshot};

use crate::store::grants::Grants;

/// Notices waiting for the adapter.
const QUEUE: usize = 16;

/// Longest runtime failure notice, in bytes. The cause is cut to fit.
const MAX_FAILURE_NOTICE: usize = 1500;

/// One message for the owner, and the adapter's answer.
pub struct Notice {
    pub text: String,
    /// `Err` carries the adapter's reason.
    pub ack: oneshot::Sender<Result<(), String>>,
}

#[derive(Debug, Error)]
pub enum NotifyError {
    #[error("the notification adapter has stopped; nothing was sent")]
    Stopped,

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

    /// Sends `text` and waits for the adapter to confirm it.
    pub async fn send(&self, text: String) -> Result<(), NotifyError> {
        let (ack, answer) = oneshot::channel();

        if self.queue.send(Notice { text, ack }).await.is_err() {
            return Err(NotifyError::Stopped);
        }

        match answer.await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(reason)) => Err(NotifyError::Unconfirmed(reason)),
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

        if let Err(error) = self.send(text).await {
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
