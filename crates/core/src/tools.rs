//! Shade's own tools. Each one checks its grant and budget at dispatch, even
//! though thread configuration only offers the tools a grant allows.

pub mod notify;
pub mod now;
pub mod schedules;
pub mod shell;

use infinity_agent_core::ThreadId;
use infinity_agent_core::message::{InputMessage, InputMessageContent};
use infinity_agent_core::traits::InputSender;
use infinity_provider_protocol::message::{Text, ToolResult, ToolResultContent, UserContent};
use rap_protocol::DisplaySegment;

use crate::recovery::UNKNOWN_OUTCOME;
use crate::store::operations::{Operations, Started};

/// Sends the result of an asynchronous call. The call ID is the dedup ID,
/// so Infinity ignores a result for a call that already has one.
pub async fn deliver<M: InputSender>(
    sender: &M, thread: ThreadId, id: String, call_id: Option<String>, text: String,
) -> Result<(), M::Error> {
    deliver_shown(sender, thread, id, call_id, text, None).await
}

/// What Shade shows the owner with a tool result: written by Shade, not the model.
#[derive(Debug, PartialEq)]
pub enum Shown {
    Receipt(String),
    /// A change waiting for the owner; adapters offer to approve or reject `code`.
    Approval {
        text: String,
        code: String,
    },
}

/// Starts the display segment that carries an approval code.
const APPROVAL: &str = "shade-approval:";

/// The display segments carrying `shown`. Infinity appends the result the model reads.
fn display(shown: Shown) -> Vec<DisplaySegment> {
    match shown {
        Shown::Receipt(text) => vec![DisplaySegment::Text(text)],
        Shown::Approval { text, code } => vec![DisplaySegment::Text(text), DisplaySegment::Text(format!("{APPROVAL}{code}"))],
    }
}

/// Reads back what a tool result shows the owner, the inverse of [`display`].
/// A result with nothing to show has only the model's text.
pub fn shown(segments: Vec<DisplaySegment>) -> Option<Shown> {
    let mut texts = segments.into_iter().map(|segment| match segment {
        DisplaySegment::Text(text) => Some(text),
        _ => None,
    });

    let (Some(Some(first)), Some(Some(second))) = (texts.next(), texts.next()) else {
        return None;
    };

    // With an approval the code comes second, before the model's text.
    match (second.strip_prefix(APPROVAL), texts.next()) {
        (Some(code), Some(_)) => Some(Shown::Approval { text: first, code: code.to_owned() }),
        _ => Some(Shown::Receipt(first)),
    }
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
        display_as: shown.map(display),
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

    use super::{Shown, display, shown};

    /// What Shade shows survives Infinity appending the model's text, and a
    /// plain result shows nothing, even if the model's text looks like a code.
    #[test]
    fn shown_text_round_trips() {
        let result = |text: &str| DisplaySegment::Text(text.to_owned());
        let receipt = || Shown::Receipt("📅 created".to_owned());
        let approval = || Shown::Approval { text: "⏳ waiting".to_owned(), code: "abc123".to_owned() };

        let cases = [
            ([display(receipt()), vec![result("created")]].concat(), Some(receipt())),
            ([display(approval()), vec![result("waiting")]].concat(), Some(approval())),
            (vec![result("exit code 0")], None),
            (vec![result("shade-approval:abc123")], None),
        ];

        for (segments, expected) in cases {
            assert_eq!(shown(segments), expected);
        }
    }
}
