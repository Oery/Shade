//! Shade's own tools. Each one checks its grant and budget at dispatch, even
//! though thread configuration only offers the tools a grant allows.

pub mod mcp;
pub mod notify;
pub mod now;
pub mod schedules;
pub mod shell;
pub mod watches;

use infinity_agent_core::ThreadId;
use infinity_agent_core::message::{InputMessage, InputMessageContent};
use infinity_agent_core::traits::InputSender;
use infinity_provider_protocol::message::{Text, ToolResult, ToolResultContent, UserContent};
use rap_protocol::DisplaySegment;
use serde::{Deserialize, Serialize};
use shade_schedule::receipt::Card;
use shade_store::operations::{Operations, Started};

use crate::recovery::UNKNOWN_OUTCOME;

/// Sends the result of an asynchronous call. The call ID is the dedup ID,
/// so Infinity ignores a result for a call that already has one.
pub async fn deliver<M: InputSender>(
    sender: &M, thread: ThreadId, id: String, call_id: Option<String>, text: String,
) -> Result<(), M::Error> {
    deliver_shown(sender, thread, id, call_id, text, None).await
}

/// A change waiting for the owner, shown with a tool result: written by Shade, not the model.
#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct Shown {
    pub card: Card,
    /// Adapters offer to approve or reject the change under this code.
    pub approval: String,
}

/// Starts the display segment that carries what is shown.
const SHOWN: &str = "shade-shown:";

/// The display segment carrying `shown`. Infinity appends the result the model reads.
pub fn display(shown: &Shown) -> Option<Vec<DisplaySegment>> {
    match serde_json::to_string(shown) {
        Ok(json) => Some(vec![DisplaySegment::Text(format!("{SHOWN}{json}"))]),
        Err(error) => {
            tracing::warn!(%error, "cannot encode a receipt; the owner will not see it");
            None
        }
    }
}

/// Reads back what a tool result shows the owner, the inverse of [`display`].
/// A result with nothing to show has only the model's text.
pub fn shown(segments: Vec<DisplaySegment>) -> Option<Shown> {
    let [DisplaySegment::Text(first), DisplaySegment::Text(_)] = segments.as_slice() else {
        return None;
    };

    serde_json::from_str(first.strip_prefix(SHOWN)?).ok()
}

/// Like [`deliver`], with `shown` displayed to the owner as written by
/// Shade, not by the model: chat adapters post it as it is.
pub async fn deliver_shown<M: InputSender>(
    sender: &M, thread: ThreadId, id: String, call_id: Option<String>, text: String, shown: Option<Shown>,
) -> Result<(), M::Error> {
    let dedup_id = id.clone();
    let result = ToolResult { id, call_id, content: vec![ToolResultContent::Text(Text { text })] };

    let message = InputMessage {
        content: InputMessageContent::User(UserContent::ToolResult(result)),
        group_id: thread,
        metadata: None,
        synthetic: None,
        display_as: shown.as_ref().and_then(display),
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

#[cfg(test)]
mod tests {
    use rap_protocol::DisplaySegment;
    use shade_schedule::receipt::{Card, Proposal, Verb};

    use super::{Shown, display, shown};

    /// What Shade shows survives Infinity appending the model's text, and a
    /// plain result shows nothing, even if the model's text looks like a receipt.
    #[test]
    fn shown_cards_round_trip() {
        let text = |text: &str| DisplaySegment::Text(text.to_owned());
        let card = Card::example(Verb::Proposed(Proposal::New));
        let sent = Shown { card, approval: "abc123".to_owned() };
        let mut segments = display(&sent).unwrap_or_default();
        segments.push(text("waiting"));
        let forged = format!("shade-shown:{}", serde_json::to_string(&sent).unwrap_or_default());

        let cases = [(segments, Some(sent)), (vec![text("exit code 0")], None), (vec![text(&forged)], None)];

        for (segments, expected) in cases {
            assert_eq!(shown(segments), expected);
        }
    }
}
