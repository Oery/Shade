//! `notify`: one message to the owner's fixed destination.
//!
//! Like `shell`, the call is answered asynchronously and recorded before the
//! message leaves, so a restart replays a confirmed result instead of
//! sending again. A notice whose confirmation was lost is never resent: its
//! outcome is unknown, and the result says so.

use std::sync::Arc;

use async_trait::async_trait;
use infinity_agent_core::ThreadId;
use infinity_agent_core::tools::{Tool, ToolContext};
use infinity_agent_core::traits::InputSender;
use rap_protocol::{ProviderCallId, ToolCallId};
use serde::Deserialize;

use crate::authority::Authority;
use crate::error::BoxError;
use crate::notify::Notifier;
use crate::policy::Cap;
use crate::recovery::UNKNOWN_OUTCOME;
use crate::store::operations::Operations;
use crate::tools;

/// Longest message, in UTF-16 units, so the notice fits in one Discord message.
const MAX_MESSAGE: usize = 1800;

/// Marks a notice in the owner's DMs, which are also a chat conversation.
const PREFIX: &str = "🔔 ";

pub struct Notify {
    work: Arc<Work>,
    description: String,
}

/// What a running call needs, shared with its task.
struct Work {
    authority: Authority,
    operations: Operations,
    /// `None` when no adapter is configured: every call is a visible error.
    notifier: Option<Notifier>,
}

/// Where a result goes.
struct Call<M> {
    sender: M,
    thread: ThreadId,
    id: String,
    call_id: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Args {
    message: String,
}

impl Notify {
    pub fn new(authority: Authority, operations: Operations, notifier: Option<Notifier>) -> Self {
        let description = format!(
            "Send a short message to the owner's Discord DMs. In a conversation your reply already reaches the owner; \
             use this when something needs their attention elsewhere, such as the result of background work. \
             One turn or run may send at most {}.",
            authority.policy().limits.max_notifications
        );

        Self { work: Arc::new(Work { authority, operations, notifier }), description }
    }
}

#[async_trait]
impl<M: InputSender + 'static> Tool<M> for Notify {
    fn name(&self) -> &str {
        Cap::Notify.name()
    }

    fn description(&self) -> &str {
        &self.description
    }

    fn parameters(&self) -> serde_json::Value {
        serde_json::json!({
            "type": "object",
            "properties": {
                "message": { "type": "string", "minLength": 1, "maxLength": MAX_MESSAGE },
            },
            "required": ["message"],
            "additionalProperties": false,
        })
    }

    async fn execute(
        &self, args: serde_json::Value, id: ToolCallId, call_id: Option<ProviderCallId>, context: &ToolContext<M>,
    ) -> Result<(), BoxError> {
        let call = Call {
            sender: context.message_sender.clone(),
            thread: context.group_id.clone(),
            id: id.into_inner(),
            call_id: call_id.map(ProviderCallId::into_inner),
        };

        let work = Arc::clone(&self.work);
        tokio::task::spawn_local(async move { work.answer(args, call).await });

        Ok(())
    }
}

impl Work {
    async fn answer<M: InputSender>(&self, args: serde_json::Value, call: Call<M>) {
        let text = self.run(args, &call).await;

        if let Err(error) = tools::deliver(&call.sender, call.thread, call.id, call.call_id, text).await {
            tracing::warn!(%error, "cannot deliver a notify result; restart recovery will");
        }
    }

    async fn run<M>(&self, args: serde_json::Value, call: &Call<M>) -> String {
        if let Err(denied) = self.authority.admit_tool(&call.thread, Cap::Notify).await {
            return format!("Error: {denied}");
        }

        let message = match parse(args) {
            Ok(message) => message,
            Err(error) => return format!("Error: {error}"),
        };

        let Some(notifier) = &self.notifier else {
            return "Error: notifications have no destination; the owner must add a [discord] section to the config".to_owned();
        };

        if let Err(denied) = self.authority.admit_notification(&call.thread).await {
            return format!("Error: {denied}");
        }

        let thread = call.thread.as_str();

        if let Err(answer) = tools::record(&self.operations, thread, &call.id, Cap::Notify.name()).await {
            return answer;
        }

        let text = match notifier.send(format!("{PREFIX}{message}")).await {
            Ok(()) => "Sent to the owner.".to_owned(),
            Err(error) => format!("Error: {error}"),
        };

        if let Err(error) = self.operations.finish(thread, &call.id, &text).await {
            tracing::error!(%error, %thread, "cannot record a notify result");
            return UNKNOWN_OUTCOME.to_owned();
        }

        text
    }
}

fn parse(args: serde_json::Value) -> Result<String, String> {
    let args: Args = match serde_json::from_value(args) {
        Ok(args) => args,
        Err(error) => return Err(format!("invalid arguments: {error}")),
    };

    let message = args.message.trim();

    if message.is_empty() {
        return Err("message is empty".to_owned());
    }

    if message.encode_utf16().count() > MAX_MESSAGE {
        return Err(format!("message is longer than {MAX_MESSAGE} characters; shorten it"));
    }

    Ok(message.to_owned())
}

#[cfg(test)]
mod tests {
    use infinity_agent_core::ThreadId;
    use infinity_agent_core::message::InputMessage;
    use tokio::sync::{mpsc, oneshot};

    use crate::error::BoxError;
    use crate::notify::{Notice, Notifier};
    use crate::policy::{Cap, Limits};
    use crate::recovery::{self, UNKNOWN_OUTCOME};
    use crate::store::conversation::Conversations;
    use crate::store::db::Db;
    use crate::store::grants::Grants;
    use crate::store::operations::Operations;
    use crate::testing::{self, attach, within};
    use crate::tools;

    /// An acknowledgment the adapter never gave.
    type Unanswered = Option<oneshot::Sender<Result<(), String>>>;

    fn root() -> ThreadId {
        ThreadId::from("root".to_owned())
    }

    /// The daemon dies after a notice reached the adapter. A confirmed notice
    /// replays its result; an unconfirmed one is unknown, not claimed as
    /// sent. Neither is sent again.
    #[tokio::test(flavor = "current_thread")]
    async fn interrupted_notifications_are_never_resent() -> Result<(), BoxError> {
        for (confirmed, expected) in [(true, "Sent to the owner."), (false, UNKNOWN_OUTCOME)] {
            let db = Db::in_memory()?;
            Conversations::new(db.clone()).register_root(&root()).await?;

            // The receivers outlive each daemon, so nothing they hold wakes a dropped task.
            let (_queue, _unanswered) = within(first_daemon(&db, confirmed)).await?;
            let mut queue = within(second_daemon(&db, expected)).await?;

            assert!(queue.try_recv().is_err(), "recovery sent the notice again");
        }

        Ok(())
    }

    async fn first_daemon(db: &Db, confirmed: bool) -> Result<(mpsc::Receiver<Notice>, Unanswered), BoxError> {
        let (notifier, mut queue) = Notifier::new(Grants::new(db.clone()));
        let policy = testing::policy(&[Cap::Notify], Limits::default());
        let mut harness = testing::start_with_tools(db, policy, None, Some(notifier)).await?;
        let handle = attach(&mut harness, &root()).await?;

        handle.send(InputMessage::user_text(root(), "tell me"), "input-1").await?;
        harness.model.next_request().await;
        harness.model.send_tool_call("call-1", "notify", serde_json::json!({ "message": "done" }));
        harness.model.finish();

        let Some(Notice { text, ack }) = queue.recv().await else {
            return Err("the notice never reached the adapter".into());
        };

        assert_eq!(text, "🔔 done");

        if !confirmed {
            return Ok((queue, Some(ack)));
        }

        let _ = ack.send(Ok(()));
        // The result was captured and delivered; the daemon dies before the model's reply saves it.
        harness.model.next_request().await;

        Ok((queue, None))
    }

    async fn second_daemon(db: &Db, expected: &str) -> Result<mpsc::Receiver<Notice>, BoxError> {
        let (notifier, queue) = Notifier::new(Grants::new(db.clone()));
        let policy = testing::policy(&[Cap::Notify], Limits::default());
        let mut harness = testing::start_with_tools(db, policy, None, Some(notifier)).await?;
        let sender = harness.system.sender();

        for answer in recovery::answers(&Operations::new(db.clone())).await? {
            tools::deliver(&sender, answer.thread, answer.id, answer.call_id, answer.text).await?;
        }

        // A result under any other call ID would be ignored, and this would time out.
        let request = harness.model.next_request().await;
        assert!(format!("{:?}", request.chat_history).contains(expected), "the model did not see `{expected}`");

        Ok(queue)
    }
}
