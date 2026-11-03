//! Owns the Infinity system: registers roots, routes inputs, pumps output.

use infinity_agent_core::message::InputMessage;
use infinity_agent_core::system::local::{LaunchingSystem, ThreadHandle};
use infinity_agent_core::system::{AgentEvent, AgentSystemBuilder, NoRapHttp};
use shade_proto::Response;
use tokio::sync::mpsc;

use crate::model::Model;
use crate::store::conversation::Conversations;
use crate::store::db::Db;
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
    system: LaunchingSystem<Conversations, States, NoRapHttp>,
}

impl Router {
    /// Built-in tools stay off: spawning has no limit hook (see docs/infinity-notes.md).
    pub fn start(model: Model, db: Db) -> Self {
        let conversations = Conversations::new(db.clone());
        let tools = ThreadTools::new(conversations.clone());
        let builder = AgentSystemBuilder::new_local(conversations, States::new(db.clone()), model);
        let system = builder.without_builtin_tools().thread_config(tools).start();

        Self { roots: Roots::new(db), system }
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
            Target::Fresh => self.roots.fresh().await,
            Target::Terminal(key) => self.roots.current(Adapter::Terminal, key).await,
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
#[derive(Default)]
struct Turn {
    accepted: bool,
    started: bool,
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
