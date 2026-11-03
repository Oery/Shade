//! Owns the Infinity system: registers roots, routes inputs, pumps output.

use std::sync::Arc;

use infinity_agent_core::message::InputMessage;
use infinity_agent_core::system::local::{LaunchingSystem, ThreadHandle};
use infinity_agent_core::system::{AgentEvent, AgentSystemBuilder, NoRapHttp};
use shade_proto::Response;
use tokio::sync::mpsc;

use crate::authority::{Authority, Metered};
use crate::error::BoxError;
use crate::model::Model;
use crate::policy::Policy;
use crate::recovery;
use crate::sandbox::Sandbox;
use crate::store::conversation::Conversations;
use crate::store::db::Db;
use crate::store::grants::Grants;
use crate::store::operations::Operations;
use crate::store::roots::{Adapter, Roots};
use crate::store::state::States;
use crate::thread_config::ThreadTools;

/// Capacity of each reply stream between the router and a socket connection.
pub const REPLY_QUEUE: usize = 64;

/// Work for the router. Each command answers on its `reply` channel.
pub enum Command {
    /// User text for a conversation.
    Prompt { target: Target, text: String, reply: mpsc::Sender<Response> },
    /// Points a terminal conversation name at a fresh root.
    NewConversation { key: String, reply: mpsc::Sender<Response> },
}

/// Which conversation a prompt belongs to.
pub enum Target {
    /// A new root reachable by no name (`shade ask`).
    Fresh,
    /// A named terminal conversation, created on first use.
    Terminal(String),
}

pub struct Router {
    roots: Roots,
    operations: Operations,
    system: LaunchingSystem<Conversations, States, NoRapHttp>,
}

impl Router {
    /// Built-in tools stay off: spawning has no limit hook (see docs/infinity-notes.md).
    pub fn start(model: Model, db: Db, policy: Arc<Policy>, sandbox: Option<Arc<Sandbox>>) -> Self {
        let authority = Authority::new(Grants::new(db.clone()), policy);
        let conversations = Conversations::new(db.clone());
        let operations = Operations::new(db.clone());
        let tools = ThreadTools::new(conversations.clone(), authority.clone(), operations.clone(), sandbox);
        let model = Metered::new(model, authority);
        let builder = AgentSystemBuilder::new_local(conversations, States::new(db.clone()), model);
        let system = builder.without_builtin_tools().thread_config(tools).start();

        Self { roots: Roots::new(db), operations, system }
    }

    /// Answers the tool calls an earlier daemon left waiting. Runs before any new work.
    pub async fn recover(&self) -> Result<(), BoxError> {
        recovery::recover(&self.operations, &self.system.sender()).await
    }

    /// Handles commands until the channel closes. Must run inside a `LocalSet`.
    pub async fn serve(&self, mut commands: mpsc::Receiver<Command>) {
        while let Some(command) = commands.recv().await {
            match command {
                Command::Prompt { target, text, reply } => self.prompt(target, text, reply).await,
                Command::NewConversation { key, reply } => self.new_conversation(key, reply).await,
            }
        }
    }

    /// Interrupts active completions, flushes their history, and stops every thread.
    pub async fn shutdown(self) {
        self.system.shutdown().await;
    }

    async fn prompt(&self, target: Target, text: String, reply: mpsc::Sender<Response>) {
        let handle = match self.attach(target).await {
            Ok(handle) => handle,
            Err(message) => {
                let _ = reply.send(Response::Error { message }).await;
                return;
            }
        };

        let input_id = uuid::Uuid::new_v4().to_string();
        let input = InputMessage::user_text(handle.thread_id().to_owned(), text);

        if handle.send(input, &input_id).await.is_err() {
            let message = "the daemon is shutting down".to_owned();
            let _ = reply.send(Response::Error { message }).await;
            return;
        }

        tokio::task::spawn_local(pump(handle, reply));
    }

    /// Registers the root before attaching, so configuration never resolves for an unregistered thread.
    async fn attach(&self, target: Target) -> Result<ThreadHandle, String> {
        let root = match target {
            Target::Fresh => self.roots.fresh(Adapter::Terminal).await,
            Target::Terminal(key) => self.roots.turn(Adapter::Terminal, key).await,
        };

        let root = match root {
            Ok(root) => root,
            Err(error) => return Err(format!("cannot register conversation: {error}")),
        };

        match self.system.thread_handle(&root).await {
            Ok(Some(handle)) => Ok(handle),
            Ok(None) => Err("registered conversation is missing".to_owned()),
            Err(error) => Err(format!("cannot attach to conversation: {error}")),
        }
    }

    async fn new_conversation(&self, key: String, reply: mpsc::Sender<Response>) {
        if let Err(error) = self.roots.replace(Adapter::Terminal, key).await {
            let message = format!("cannot start a new conversation: {error}");
            let _ = reply.send(Response::Error { message }).await;
            return;
        }

        let text = "Started a new conversation; the previous one is kept.".to_owned();

        if reply.send(Response::Text { text }).await.is_ok() {
            let _ = reply.send(Response::Done).await;
        }
    }
}

/// Forwards one turn's text to the client. A closed client stops forwarding, not the turn.
async fn pump(mut handle: ThreadHandle, reply: mpsc::Sender<Response>) {
    let mut turn = Turn::default();

    while let Some(event) = handle.recv().await {
        let Some(response) = turn.on_event(event) else {
            continue;
        };

        let terminal = !matches!(response, Response::Text { .. });

        if reply.send(response).await.is_err() || terminal {
            return;
        }
    }
}

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
struct Turn {
    accepted: bool,
    started: bool,
    /// Tool calls seen without their results.
    pending_tools: usize,
    failure: Option<String>,
}

impl Turn {
    fn on_event(&mut self, event: AgentEvent) -> Option<Response> {
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
