//! Owns the Infinity system: registers roots, routes inputs, pumps output.

use infinity_agent_core::ThreadId;
use infinity_agent_core::message::InputMessage;
use infinity_agent_core::stores::InMemoryStateStore;
use infinity_agent_core::system::local::{LaunchingSystem, ThreadHandle};
use infinity_agent_core::system::{AgentEvent, AgentSystemBuilder, NoRapHttp};
use shade_proto::Response;
use tokio::sync::mpsc;

use crate::model::Model;
use crate::store::conversation::Conversations;
use crate::thread_config::ThreadTools;

/// Capacity of each reply stream between the router and a socket connection.
pub const REPLY_QUEUE: usize = 64;

/// A one-shot prompt from the control socket. The router answers on `reply`.
pub struct Ask {
    pub prompt: String,
    pub reply: mpsc::Sender<Response>,
}

pub struct Router {
    conversations: Conversations,
    system: LaunchingSystem<Conversations, InMemoryStateStore, NoRapHttp>,
}

impl Router {
    /// Built-in tools stay off: spawning has no limit hook (see docs/infinity-notes.md).
    pub fn start(model: Model, conversations: Conversations) -> Self {
        let tools = ThreadTools::new(conversations.clone());
        let builder = AgentSystemBuilder::new_local(conversations.clone(), InMemoryStateStore::new(), model);
        let system = builder.without_builtin_tools().thread_config(tools).start();

        Self { conversations, system }
    }

    /// Handles asks until the channel closes. Must run inside a `LocalSet`.
    pub async fn serve(&self, mut asks: mpsc::Receiver<Ask>) {
        while let Some(ask) = asks.recv().await {
            self.start_ask(ask).await;
        }
    }

    /// Interrupts active completions, flushes their history, and stops every thread.
    pub async fn shutdown(self) {
        self.system.shutdown().await;
    }

    async fn start_ask(&self, ask: Ask) {
        let handle = match self.register_root().await {
            Ok(handle) => handle,
            Err(message) => {
                let _ = ask.reply.send(Response::Error { message }).await;
                return;
            }
        };

        let input_id = uuid::Uuid::new_v4().to_string();
        let input = InputMessage::user_text(handle.thread_id().to_owned(), ask.prompt);

        if handle.send(input, &input_id).await.is_err() {
            let message = "the daemon is shutting down".to_owned();
            let _ = ask.reply.send(Response::Error { message }).await;
            return;
        }

        tokio::task::spawn_local(pump(handle, ask.reply));
    }

    /// Records the root before attaching, so configuration never resolves for an unregistered thread.
    async fn register_root(&self) -> Result<ThreadHandle, String> {
        let thread_id = ThreadId::from(uuid::Uuid::new_v4().to_string());

        if let Err(error) = self.conversations.register_root(&thread_id).await {
            return Err(format!("cannot register conversation: {error}"));
        }

        match self.system.thread_handle(&thread_id).await {
            Ok(Some(handle)) => Ok(handle),
            Ok(None) => Err("registered conversation is missing".to_owned()),
            Err(error) => Err(format!("cannot attach to conversation: {error}")),
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
#[derive(Default)]
struct Turn {
    started: bool,
    failure: Option<String>,
}

impl Turn {
    fn on_event(&mut self, event: AgentEvent) -> Option<Response> {
        match event {
            AgentEvent::TextChunk { text } => Some(Response::Text { text }),
            AgentEvent::CompletionStarted => {
                self.started = true;
                None
            }
            AgentEvent::Info { text } => self.on_info(text),
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

    fn finish(&mut self) -> Response {
        match self.failure.take() {
            Some(message) => Response::Error { message },
            None => Response::Done,
        }
    }
}
