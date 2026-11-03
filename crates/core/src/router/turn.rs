//! Follows one turn's events and decides what its sender sees.

use infinity_agent_core::system::AgentEvent;
use shade_proto::Response;

/// Tracks one turn's events and decides what the client sees.
///
/// A resumed conversation may still be finishing an earlier turn when this
/// one's input arrives. Completion events count only after Infinity accepts
/// the input (`UserInput`), so that turn's tail cannot end this one.
///
/// A completion that ends on an asynchronous tool call does not end the
/// turn: the tool's result starts another completion. Synchronous tools
/// answer inside the completion, so their results arrive before it finishes.
#[derive(Default)]
pub struct Turn {
    accepted: bool,
    started: bool,
    /// Tool calls seen without their results.
    pending_tools: usize,
    failure: Option<String>,
}

impl Turn {
    /// A turn already under way: a recovered tool result continues it, with no new input to wait for.
    pub fn continuing() -> Self {
        Self { accepted: true, ..Self::default() }
    }

    pub fn on_event(&mut self, event: AgentEvent) -> Option<Response> {
        match event {
            AgentEvent::UserInput { .. } if self.accepted => Some(Self::superseded()),
            AgentEvent::UserInput { .. } => {
                self.accepted = true;
                None
            }
            AgentEvent::Info { text } => self.on_info(text),
            _ if !self.accepted => None,
            AgentEvent::TextChunk { text } => Some(Response::Text { text }),
            AgentEvent::CompletionStarted => {
                self.started = true;
                None
            }
            AgentEvent::ToolCall { .. } => {
                self.pending_tools += 1;
                None
            }
            AgentEvent::ToolResult { .. } => {
                self.pending_tools = self.pending_tools.saturating_sub(1);
                None
            }
            AgentEvent::CompletionFinished { .. } if self.pending_tools > 0 && self.failure.is_none() => {
                self.started = false;
                None
            }
            AgentEvent::CompletionFinished { .. } => Some(self.finish()),
            _ => None,
        }
    }

    /// Before a completion starts, an error means none will run, so it ends the turn.
    fn on_info(&mut self, text: String) -> Option<Response> {
        let message = text.strip_prefix("Error: ")?.to_owned();

        if !self.started {
            return Some(Response::Error { message });
        }

        self.failure = Some(message);

        None
    }

    /// Newer input for the same conversation (another terminal) interrupted this turn.
    fn superseded() -> Response {
        let message = "interrupted by newer input to this conversation".to_owned();

        Response::Error { message }
    }

    fn finish(&mut self) -> Response {
        match self.failure.take() {
            Some(message) => Response::Error { message },
            None => Response::Done,
        }
    }
}

#[cfg(test)]
mod tests {
    use infinity_agent_core::system::AgentEvent;
    use shade_proto::Response;

    use super::Turn;

    fn text(text: &str) -> AgentEvent {
        AgentEvent::TextChunk { text: text.to_owned() }
    }

    fn error(text: &str) -> AgentEvent {
        AgentEvent::Info { text: format!("Error: {text}") }
    }

    fn finished() -> AgentEvent {
        AgentEvent::CompletionFinished { usage: None }
    }

    fn accepted() -> AgentEvent {
        AgentEvent::UserInput { text: "hi".to_owned() }
    }

    fn tool_call() -> AgentEvent {
        AgentEvent::ToolCall { name: "shell".to_owned(), args: serde_json::json!({}), display_as: None }
    }

    fn tool_result() -> AgentEvent {
        AgentEvent::ToolResult { segments: Vec::new() }
    }

    /// What the client sees for each event sequence, ending at the first final response.
    fn render(events: Vec<AgentEvent>) -> Vec<String> {
        let mut turn = Turn::default();
        let mut seen = Vec::new();

        for event in events {
            match turn.on_event(event) {
                Some(Response::Text { text }) => seen.push(text),
                Some(Response::Done) => {
                    seen.push("done".to_owned());
                    break;
                }
                Some(Response::Error { message }) => {
                    seen.push(format!("error: {message}"));
                    break;
                }
                None => {}
            }
        }

        seen
    }

    #[test]
    fn turns_end_on_their_own_events() {
        let cases = [
            // The tail of an earlier turn arrives before this input is accepted.
            (
                vec![text("old"), finished(), accepted(), AgentEvent::CompletionStarted, text("new"), finished()],
                vec!["new", "done"],
            ),
            // Configuration failed: no completion will run, and no input is accepted.
            (vec![error("no grant")], vec!["error: no grant"]),
            // The provider failed mid-turn; the turn still finishes first.
            (
                vec![accepted(), AgentEvent::CompletionStarted, text("par"), error("timeout"), finished()],
                vec!["par", "error: timeout"],
            ),
            // Another terminal's input interrupts this turn; its reply is not ours.
            (
                vec![accepted(), AgentEvent::CompletionStarted, text("par"), accepted(), text("theirs"), finished()],
                vec!["par", "error: interrupted by newer input to this conversation"],
            ),
            // An asynchronous tool's result starts another completion that belongs to this turn.
            (
                vec![
                    accepted(),
                    AgentEvent::CompletionStarted,
                    tool_call(),
                    finished(),
                    tool_result(),
                    AgentEvent::CompletionStarted,
                    text("answer"),
                    finished(),
                ],
                vec!["answer", "done"],
            ),
            // A synchronous tool answers inside the completion.
            (
                vec![accepted(), AgentEvent::CompletionStarted, tool_call(), tool_result(), text("12:00"), finished()],
                vec!["12:00", "done"],
            ),
            // The completion after a tool result could not start.
            (
                vec![accepted(), AgentEvent::CompletionStarted, tool_call(), finished(), tool_result(), error("limit")],
                vec!["error: limit"],
            ),
            // Informational lines are not errors.
            (
                vec![accepted(), AgentEvent::Info { text: "retrying".to_owned() }, AgentEvent::CompletionStarted, finished()],
                vec!["done"],
            ),
        ];

        for (events, expected) in cases {
            assert_eq!(render(events), expected);
        }
    }
}
