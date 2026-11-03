//! Shade's own tools. Each one checks its grant and budget at dispatch, even
//! though thread configuration only offers the tools a grant allows.

pub mod notify;
pub mod now;
pub mod shell;

use infinity_agent_core::ThreadId;
use infinity_agent_core::message::{InputMessage, InputMessageContent};
use infinity_agent_core::traits::InputSender;
use infinity_provider_protocol::message::{Text, ToolResult, ToolResultContent, UserContent};

use crate::recovery::UNKNOWN_OUTCOME;
use crate::store::operations::{Operations, Started};

/// Sends the result of an asynchronous call. The call ID is the dedup ID,
/// so Infinity ignores a result for a call that already has one.
pub async fn deliver<M: InputSender>(
    sender: &M, thread: ThreadId, id: String, call_id: Option<String>, text: String,
) -> Result<(), M::Error> {
    let dedup_id = id.clone();
    let result = ToolResult { id, call_id, content: vec![ToolResultContent::Text(Text { text })] };

    let message = InputMessage {
        content: InputMessageContent::User(UserContent::ToolResult(result)),
        group_id: thread,
        metadata: None,
        synthetic: None,
        display_as: None,
        subscription: false,
    };

    sender.send_to_input_queue(message, &dedup_id).await
}

/// Records an asynchronous call before its work starts. A call recorded
/// before never runs again: the error carries its answer instead.
pub async fn record(operations: &Operations, thread: &str, call: &str, tool: &str) -> Result<(), String> {
    match operations.start(thread, call, tool).await {
        Ok(Started::New) => Ok(()),
        Ok(Started::Again(Some(result))) => Err(result),
        Ok(Started::Again(None)) => Err(UNKNOWN_OUTCOME.to_owned()),
        Err(error) => {
            tracing::error!(%error, %thread, %tool, "cannot record a tool call");
            Err(format!("Error: nothing was done because the call could not be recorded: {error}"))
        }
    }
}
