//! Follows one turn's events and decides what its sender sees.

use infinity_agent_core::system::AgentEvent;

use crate::tools::{self, Shown};

/// What a turn's sender is told.
#[derive(Debug, PartialEq)]
pub enum Step {
    Text(String),
    /// A completion ended on an asynchronous tool call. The turn goes on when
    /// the result arrives; chat adapters post what they have so far.
    Paused,
    Done,
    Failed(String),
    /// What Shade wrote for the owner to see with a tool result, such as a job receipt.
    Shown(Shown),
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
    /// An approval card was shown: the rest of the turn is hidden, and the
    /// model speaks again once the owner has decided.
    quiet: bool,
    /// The current completion has written text.
    spoke: bool,
    /// The next text continues after a completion that wrote and then
    /// paused on a tool: it starts on a new paragraph.
    separate: bool,
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
            AgentEvent::TextChunk { .. } if self.quiet => None,
            AgentEvent::TextChunk { text } => Some(self.text(text)),
            AgentEvent::CompletionStarted => {
                self.started = true;
                self.spoke = false;
                None
            }
            AgentEvent::ToolCall { .. } => {
                self.pending_tools += 1;
                None
            }
            AgentEvent::ToolResult { segments } => {
                self.pending_tools = self.pending_tools.saturating_sub(1);
                let shown = tools::shown(segments)?;
                self.quiet = true;

                Some(Step::Shown(shown))
            }
            AgentEvent::CompletionFinished { .. } if self.pending_tools > 0 && self.failure.is_none() => {
                self.started = false;
                self.separate |= self.spoke;
                Some(Step::Paused)
            }
            AgentEvent::CompletionFinished { .. } => Some(self.finish()),
            _ => None,
        }
    }

    fn text(&mut self, text: String) -> Step {
        self.spoke = true;

        if !std::mem::take(&mut self.separate) {
            return Step::Text(text);
        }

        Step::Text(format!("\n\n{text}"))
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

#[cfg(test)]
mod tests {
    use infinity_agent_core::system::AgentEvent;
    use shade_schedule::receipt::{Card, Proposal, Verb};

    use super::{Step, Turn};
    use crate::tools::{self, Shown};
    use rap_protocol::DisplaySegment;

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

    fn approval() -> AgentEvent {
        let shown = Shown { card: Card::example(Verb::Proposed(Proposal::New)), approval: "abc123".to_owned() };
        let mut segments = tools::display(&shown).unwrap_or_default();
        segments.push(DisplaySegment::Text("waiting".to_owned()));

        AgentEvent::ToolResult { segments }
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
                Step::Shown(shown) => format!("shown: {}", shown.card.title),
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
            // Text after a tool call starts a new paragraph when text came before it.
            (
                vec![
                    input("hi"),
                    started(),
                    text("checking."),
                    tool_call(),
                    finished(),
                    tool_result(),
                    started(),
                    text("found"),
                    text(" it"),
                    finished(),
                ],
                vec!["checking.", "paused", "\n\nfound", " it", "done"],
            ),
            // After an approval card, the model's words wait for the owner's decision.
            (
                vec![input("hi"), started(), text("asking"), tool_call(), finished(), approval(), started(), text("hidden"), finished()],
                vec!["asking", "paused", "shown: Tea", "done"],
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
