//! Follows one turn's events and decides what its sender sees.

use infinity_agent_core::system::AgentEvent;
use rap_protocol::DisplaySegment;

/// What a turn's sender is told.
#[derive(Debug, PartialEq)]
pub enum Step {
    Text(String),
    /// A completion ended on an asynchronous tool call. The turn goes on when
    /// the result arrives; chat adapters post what they have so far.
    Paused,
    Done,
    Failed(String),
    /// A tool result Shade wrote for the owner to see, such as a job receipt.
    Receipt(String),
    /// Newer input to the same conversation interrupted this turn.
    Superseded,
}

impl Step {
    pub fn is_last(&self) -> bool {
        matches!(self, Self::Done | Self::Failed(_) | Self::Superseded)
    }
}

/// Tracks one turn's events.
///
/// A conversation may still be finishing an earlier turn, or already
/// accepting a later message, when this one's input arrives. Infinity
/// reports each accepted input (`UserInput`) with its text, so the turn
/// starts at its own input and ends at the next one.
///
/// A completion that ends on an asynchronous tool call does not end the
/// turn: the tool's result starts another completion. Synchronous tools
/// answer inside the completion, so their results arrive before it finishes.
#[derive(Default)]
pub struct Turn {
    /// The input that starts this turn. `None` for a turn already under way.
    input: Option<String>,
    accepted: bool,
    started: bool,
    /// Tool calls seen without their results.
    pending_tools: usize,
    failure: Option<String>,
}

impl Turn {
    /// The turn `input` starts.
    pub fn for_input(input: &str) -> Self {
        Self { input: Some(input.to_owned()), ..Self::default() }
    }

    /// A turn already under way: a recovered tool result continues it, with no new input to wait for.
    pub fn continuing() -> Self {
        Self { accepted: true, ..Self::default() }
    }

    pub fn on_event(&mut self, event: AgentEvent) -> Option<Step> {
        match event {
            AgentEvent::UserInput { .. } if self.accepted => Some(Step::Superseded),
            AgentEvent::UserInput { text } => {
                self.accepted = self.input.as_deref() == Some(text.as_str());
                None
            }
            AgentEvent::Info { text } => self.on_info(text),
            _ if !self.accepted => None,
            AgentEvent::TextChunk { text } => Some(Step::Text(text)),
            AgentEvent::CompletionStarted => {
                self.started = true;
                None
            }
            AgentEvent::ToolCall { .. } => {
                self.pending_tools += 1;
                None
            }
            AgentEvent::ToolResult { segments } => {
                self.pending_tools = self.pending_tools.saturating_sub(1);
                shown(segments)
            }
            AgentEvent::CompletionFinished { .. } if self.pending_tools > 0 && self.failure.is_none() => {
                self.started = false;
                Some(Step::Paused)
            }
            AgentEvent::CompletionFinished { .. } => Some(self.finish()),
            _ => None,
        }
    }

    /// Before a completion starts, an error means none will run, so it ends the turn.
    fn on_info(&mut self, text: String) -> Option<Step> {
        let message = text.strip_prefix("Error: ")?;
        let message = message.strip_prefix("ProviderError: ").unwrap_or(message).to_owned();

        if !self.started {
            return Some(Step::Failed(message));
        }

        self.failure = Some(message);

        None
    }

    fn finish(&mut self) -> Step {
        match self.failure.take() {
            Some(message) => Step::Failed(message),
            None => Step::Done,
        }
    }
}

/// The owner's copy of a tool result: Shade's tools put it before the result
/// the model reads. Results with nothing to show have a single segment.
fn shown(segments: Vec<DisplaySegment>) -> Option<Step> {
    if segments.len() < 2 {
        return None;
    }

    match segments.into_iter().next() {
        Some(DisplaySegment::Text(text)) => Some(Step::Receipt(text)),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use infinity_agent_core::system::AgentEvent;
    use rap_protocol::DisplaySegment;

    use super::{Step, Turn};

    fn text(text: &str) -> AgentEvent {
        AgentEvent::TextChunk { text: text.to_owned() }
    }

    fn error(text: &str) -> AgentEvent {
        AgentEvent::Info { text: format!("Error: {text}") }
    }

    fn finished() -> AgentEvent {
        AgentEvent::CompletionFinished { usage: None }
    }

    fn input(text: &str) -> AgentEvent {
        AgentEvent::UserInput { text: text.to_owned() }
    }

    fn tool_call() -> AgentEvent {
        AgentEvent::ToolCall { name: "shell".to_owned(), args: serde_json::json!({}), display_as: None }
    }

    fn tool_result() -> AgentEvent {
        AgentEvent::ToolResult { segments: Vec::new() }
    }

    /// What the sender sees for each event sequence, ending at the first final step.
    fn render(events: Vec<AgentEvent>) -> Vec<String> {
        let mut turn = Turn::for_input("hi");
        let mut seen = Vec::new();

        for event in events {
            let Some(step) = turn.on_event(event) else {
                continue;
            };

            let last = step.is_last();

            seen.push(match step {
                Step::Text(text) => text,
                Step::Paused => "paused".to_owned(),
                Step::Done => "done".to_owned(),
                Step::Failed(message) => format!("error: {message}"),
                Step::Receipt(text) => format!("receipt: {text}"),
                Step::Superseded => "superseded".to_owned(),
            });

            if last {
                break;
            }
        }

        seen
    }

    #[test]
    fn turns_end_on_their_own_events() {
        let started = || AgentEvent::CompletionStarted;

        let cases = [
            // The tail of an earlier turn arrives before this input is accepted.
            (vec![text("old"), finished(), input("hi"), started(), text("new"), finished()], vec!["new", "done"]),
            // An earlier message is accepted first; its turn is not this one.
            (
                vec![input("earlier"), started(), text("old"), finished(), input("hi"), started(), text("new"), finished()],
                vec!["new", "done"],
            ),
            // Configuration failed: no completion will run, and no input is accepted.
            (vec![error("no grant")], vec!["error: no grant"]),
            // The provider failed mid-turn; the turn still finishes first.
            (
                vec![input("hi"), started(), text("par"), error("timeout"), finished()],
                vec!["par", "error: timeout"],
            ),
            // A later message interrupts this turn; its reply is not ours.
            (
                vec![input("hi"), started(), text("par"), input("later"), text("theirs"), finished()],
                vec!["par", "superseded"],
            ),
            // An asynchronous tool's result starts another completion that belongs to this turn.
            (
                vec![input("hi"), started(), tool_call(), finished(), tool_result(), started(), text("answer"), finished()],
                vec!["paused", "answer", "done"],
            ),
            // A receipt Shade wrote is shown; the model's copy of the result is not.
            (
                vec![
                    input("hi"),
                    started(),
                    tool_call(),
                    finished(),
                    AgentEvent::ToolResult {
                        segments: vec![DisplaySegment::Text("created".to_owned()), DisplaySegment::Text("created".to_owned())],
                    },
                    started(),
                    finished(),
                ],
                vec!["paused", "receipt: created", "done"],
            ),
            // A synchronous tool answers inside the completion.
            (
                vec![input("hi"), started(), tool_call(), tool_result(), text("12:00"), finished()],
                vec!["12:00", "done"],
            ),
            // The completion after a tool result could not start.
            (
                vec![input("hi"), started(), tool_call(), finished(), tool_result(), error("limit")],
                vec!["paused", "error: limit"],
            ),
            // Informational lines are not errors.
            (
                vec![input("hi"), AgentEvent::Info { text: "retrying".to_owned() }, started(), finished()],
                vec!["done"],
            ),
        ];

        for (events, expected) in cases {
            assert_eq!(render(events), expected);
        }
    }
}
