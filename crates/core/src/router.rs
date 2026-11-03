//! Owns the Infinity system: accepts input durably, routes it, pumps output.
//!
//! Every input takes one path: it is recorded in the inbox with its root
//! before Infinity sees it, then sent under its stable ID, and a pump follows
//! its turn to the end. A pump outlives its client, so a turn's outcome is
//! always settled. Work interrupted by a restart re-enters the same path.

mod turn;

#[cfg(test)]
mod tests;

use std::sync::Arc;

use infinity_agent_core::ThreadId;
use infinity_agent_core::message::InputMessage;
use infinity_agent_core::system::local::{LaunchingSystem, ThreadHandle};
use infinity_agent_core::system::{AgentSystemBuilder, NoRapHttp};
use shade_proto::Response;
use tokio::sync::mpsc;

use crate::authority::{Authority, Metered};
use crate::error::BoxError;
use crate::model::Model;
use crate::policy::Policy;
use crate::recovery;
use crate::router::turn::Turn;
use crate::sandbox::Sandbox;
use crate::store::conversation::Conversations;
use crate::store::db::Db;
use crate::store::grants::Grants;
use crate::store::inbox::{Inbox, Input};
use crate::store::operations::Operations;
use crate::store::roots::{Adapter, Roots, Target};
use crate::store::state::States;
use crate::thread_config::ThreadTools;
use crate::tools;

/// Capacity of each reply stream between the router and a socket connection.
pub const REPLY_QUEUE: usize = 64;

pub type System = LaunchingSystem<Conversations, States, NoRapHttp>;

/// Input from an adapter.
pub enum Inbound {
    /// User text for a conversation.
    Prompt { target: Target, text: String, outbound: Outbound },
    /// Points a terminal conversation name at a fresh root.
    NewConversation { key: String, reply: mpsc::Sender<Response> },
}

/// Where a turn's output goes. The turn runs to its end either way.
pub enum Outbound {
    /// A connected client. If it leaves, the turn finishes without it.
    Client(mpsc::Sender<Response>),
    /// Nobody: work resumed after a restart. The outcome is logged.
    Detached,
}

impl Outbound {
    async fn send(&self, thread: &str, response: Response) {
        match self {
            Self::Client(reply) => {
                let _ = reply.send(response).await;
            }
            Self::Detached => log_detached(thread, &response),
        }
    }
}

pub struct Router {
    db: Db,
    inbox: Inbox,
    roots: Roots,
    operations: Operations,
    system: System,
}

impl Router {
    /// Built-in tools stay off: spawning has no limit hook (see docs/infinity-notes.md).
    pub fn start(model: Model, db: Db, policy: Arc<Policy>, sandbox: Option<Arc<Sandbox>>) -> Self {
        let authority = Authority::new(Grants::new(db.clone()), policy);
        let conversations = Conversations::new(db.clone());
        let tools = ThreadTools::new(conversations.clone(), authority.clone(), Operations::new(db.clone()), sandbox);
        let model = Metered::new(model, authority);
        let builder = AgentSystemBuilder::new_local(conversations, States::new(db.clone()), model);
        let system = builder.without_builtin_tools().thread_config(tools).start();

        Self::new(db, system)
    }

    pub fn new(db: Db, system: System) -> Self {
        let inbox = Inbox::new(db.clone());
        let roots = Roots::new(db.clone());
        let operations = Operations::new(db.clone());

        Self { db, inbox, roots, operations, system }
    }

    /// Handles input until the channel closes. Must run inside a `LocalSet`.
    pub async fn serve(&self, mut inbound: mpsc::Receiver<Inbound>) {
        while let Some(input) = inbound.recv().await {
            match input {
                Inbound::Prompt { target, text, outbound } => self.prompt(target, text, outbound).await,
                Inbound::NewConversation { key, reply } => self.new_conversation(key, reply).await,
            }
        }
    }

    /// Finishes what a previous daemon left: answers its waiting tool calls,
    /// then sends its unprocessed inputs again under their own IDs. Runs
    /// before any new input is accepted.
    pub async fn resume(&self) -> Result<(), BoxError> {
        for answer in recovery::answers(&self.operations).await? {
            let handle = self.attach(&answer.thread).await?;
            tools::deliver(&self.system.sender(), answer.thread, answer.id, answer.call_id, answer.text).await?;
            tokio::task::spawn_local(pump(handle, Turn::continuing(), Outbound::Detached, None));
        }

        for input in self.inbox.unsettled().await? {
            tracing::info!(root = %input.root, input = %input.id, "resending an input accepted before the restart");
            self.dispatch(input, Outbound::Detached).await;
        }

        Ok(())
    }

    /// Interrupts active completions, flushes their history, stops every
    /// thread, then waits for the database to finish queued writes.
    pub async fn shutdown(self) {
        self.system.shutdown().await;

        if let Err(error) = self.db.flush().await {
            tracing::error!(%error, "cannot flush the database at shutdown");
        }
    }

    async fn prompt(&self, target: Target, text: String, outbound: Outbound) {
        match self.inbox.accept(Adapter::Terminal, target, text).await {
            Ok(input) => self.dispatch(input, outbound).await,
            Err(error) => outbound.send("", error_response(format!("cannot accept the message: {error}"))).await,
        }
    }

    /// Sends a recorded input and follows its turn.
    async fn dispatch(&self, input: Input, outbound: Outbound) {
        let Input { root, id, text } = input;

        let handle = match self.attach(&root).await {
            Ok(handle) => handle,
            Err(message) => {
                settle(&self.inbox, &id, &message).await;
                outbound.send(root.as_str(), error_response(message)).await;
                return;
            }
        };

        // Unsettled until processed: if this fails, the next start sends it again.
        if handle.send(InputMessage::user_text(root, text), &id).await.is_err() {
            let message = "the daemon is shutting down; the message will be answered after it restarts".to_owned();
            outbound.send(handle.thread_id().as_str(), error_response(message)).await;
            return;
        }

        tokio::task::spawn_local(pump(handle, Turn::default(), outbound, Some((self.inbox.clone(), id))));
    }

    /// Roots are registered before this, so configuration never resolves for an unregistered thread.
    async fn attach(&self, root: &ThreadId<str>) -> Result<ThreadHandle, String> {
        match self.system.thread_handle(root).await {
            Ok(Some(handle)) => Ok(handle),
            Ok(None) => Err(format!("conversation {root} is missing")),
            Err(error) => Err(format!("cannot attach to conversation {root}: {error}")),
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

/// Follows one turn to its end and forwards it to `outbound`. An input whose
/// turn fails is settled before its sender hears of it, so it is never run
/// again after a restart. A pump cut short by shutdown leaves its input to
/// be resent.
async fn pump(mut handle: ThreadHandle, mut turn: Turn, outbound: Outbound, input: Option<(Inbox, String)>) {
    while let Some(event) = handle.recv().await {
        let Some(response) = turn.on_event(event) else {
            continue;
        };

        let last = !matches!(response, Response::Text { .. });

        if let (Response::Error { message }, Some((inbox, id))) = (&response, &input) {
            settle(inbox, id, message).await;
        }

        outbound.send(handle.thread_id().as_str(), response).await;

        if last {
            return;
        }
    }
}

async fn settle(inbox: &Inbox, id: &str, message: &str) {
    if let Err(error) = inbox.fail(id, message).await {
        tracing::error!(%error, input = %id, "cannot record a failed input; it will be resent after a restart");
    }
}

fn error_response(message: String) -> Response {
    Response::Error { message }
}

fn log_detached(thread: &str, response: &Response) {
    match response {
        Response::Text { .. } => {}
        Response::Done => tracing::info!(%thread, "finished resumed work"),
        Response::Error { message } => tracing::warn!(%thread, %message, "resumed work failed"),
    }
}
