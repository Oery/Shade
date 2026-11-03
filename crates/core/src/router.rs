//! Owns the Infinity system: accepts input durably, routes it, pumps output.
//!
//! Every input takes one path: it is recorded in the inbox with its root
//! before Infinity sees it, then sent under its stable ID, and a pump follows
//! its turn to the end. A pump outlives its client, so a turn's outcome is
//! always settled. Work interrupted by a restart re-enters the same path.

pub mod turn;

#[cfg(test)]
mod tests;

use std::sync::Arc;

use infinity_agent_core::ThreadId;
use infinity_agent_core::message::InputMessage;
use infinity_agent_core::system::local::{LaunchingSystem, ThreadHandle};
use infinity_agent_core::system::{AgentSystemBuilder, NoRapHttp};
use serenity::all::Http;
use shade_proto::Response;
use tokio::sync::mpsc;

use crate::adapters::discord::Reply;
use crate::authority::{Authority, Metered};
use crate::error::BoxError;
use crate::model::Model;
use crate::policy::Policy;
use crate::recovery;
use crate::router::turn::{Step, Turn};
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
    Prompt {
        adapter: Adapter,
        target: Target,
        text: String,
        /// The sender's own ID for this input, so a redelivery is accepted once.
        source_key: Option<String>,
        outbound: Outbound,
    },
    /// Points a conversation name at a fresh root.
    NewConversation { adapter: Adapter, key: String, reply: mpsc::Sender<Response> },
}

/// Where a turn's output goes. The turn runs to its end either way.
pub enum Outbound {
    /// A connected client. If it leaves, the turn finishes without it.
    Client(mpsc::Sender<Response>),
    /// A Discord channel.
    Discord(Reply),
    /// Nobody: resumed work in a conversation no adapter listens to. The outcome is logged.
    Detached,
}

impl Outbound {
    async fn send(&mut self, thread: &str, step: Step) {
        match self {
            Self::Client(reply) => {
                if let Some(response) = response_for(step) {
                    let _ = reply.send(response).await;
                }
            }
            Self::Discord(reply) => reply.send(step).await,
            Self::Detached => log_detached(thread, &step),
        }
    }
}

pub struct Router {
    db: Db,
    inbox: Inbox,
    roots: Roots,
    operations: Operations,
    system: System,
    /// For replies to resumed work in Discord conversations.
    discord: Option<Arc<Http>>,
}

impl Router {
    /// Built-in tools stay off: spawning has no limit hook (see docs/infinity-notes.md).
    pub fn start(model: Model, db: Db, policy: Arc<Policy>, sandbox: Option<Arc<Sandbox>>, discord: Option<Arc<Http>>) -> Self {
        let authority = Authority::new(Grants::new(db.clone()), policy);
        let conversations = Conversations::new(db.clone());
        let tools = ThreadTools::new(conversations.clone(), authority.clone(), Operations::new(db.clone()), sandbox);
        let model = Metered::new(model, authority);
        let builder = AgentSystemBuilder::new_local(conversations, States::new(db.clone()), model);
        let system = builder.without_builtin_tools().thread_config(tools).start();

        Self::new(db, system, discord)
    }

    pub fn new(db: Db, system: System, discord: Option<Arc<Http>>) -> Self {
        let inbox = Inbox::new(db.clone());
        let roots = Roots::new(db.clone());
        let operations = Operations::new(db.clone());

        Self { db, inbox, roots, operations, system, discord }
    }

    /// Handles input until the channel closes. Must run inside a `LocalSet`.
    pub async fn serve(&self, mut inbound: mpsc::Receiver<Inbound>) {
        while let Some(input) = inbound.recv().await {
            match input {
                Inbound::Prompt { adapter, target, text, source_key, outbound } => {
                    self.prompt(adapter, target, text, source_key, outbound).await;
                }
                Inbound::NewConversation { adapter, key, reply } => self.new_conversation(adapter, key, reply).await,
            }
        }
    }

    /// Finishes what a previous daemon left: answers its waiting tool calls,
    /// then sends its unprocessed inputs again under their own IDs. Runs
    /// before any new input is accepted.
    pub async fn resume(&self) -> Result<(), BoxError> {
        for answer in recovery::answers(&self.operations).await? {
            let handle = self.attach(&answer.thread).await?;
            let outbound = self.outbound_for(&answer.thread).await;
            tools::deliver(&self.system.sender(), answer.thread, answer.id, answer.call_id, answer.text).await?;
            tokio::task::spawn_local(pump(handle, Turn::continuing(), outbound, None));
        }

        for input in self.inbox.unsettled().await? {
            tracing::info!(root = %input.root, input = %input.id, "resending an input accepted before the restart");
            let outbound = self.outbound_for(&input.root).await;
            self.dispatch(input, outbound).await;
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

    async fn prompt(&self, adapter: Adapter, target: Target, text: String, source_key: Option<String>, mut outbound: Outbound) {
        match self.inbox.accept(adapter, target, text, source_key).await {
            Ok(Some(input)) => self.dispatch(input, outbound).await,
            Ok(None) => tracing::debug!("ignoring a message delivered twice"),
            Err(error) => outbound.send("", Step::Failed(format!("cannot accept the message: {error}"))).await,
        }
    }

    /// Sends a recorded input and follows its turn.
    async fn dispatch(&self, input: Input, mut outbound: Outbound) {
        let Input { root, id, text } = input;

        let handle = match self.attach(&root).await {
            Ok(handle) => handle,
            Err(message) => {
                settle(&self.inbox, &id, &message).await;
                outbound.send(root.as_str(), Step::Failed(message)).await;
                return;
            }
        };

        let turn = Turn::for_input(&text);

        // Unsettled until processed: if this fails, the next start sends it again.
        if handle.send(InputMessage::user_text(root, text), &id).await.is_err() {
            let message = "the daemon is shutting down; the message will be answered after it restarts".to_owned();
            outbound.send(handle.thread_id().as_str(), Step::Failed(message)).await;
            return;
        }

        tokio::task::spawn_local(pump(handle, turn, outbound, Some((self.inbox.clone(), id))));
    }

    /// Where resumed work in `thread` reports: its Discord channel, if it is one.
    async fn outbound_for(&self, thread: &ThreadId<str>) -> Outbound {
        let Some(http) = &self.discord else {
            return Outbound::Detached;
        };

        let key = match self.roots.conversation_of(thread).await {
            Ok(Some((Adapter::Discord, key))) => key,
            Ok(_) => return Outbound::Detached,
            Err(error) => {
                tracing::warn!(%error, %thread, "cannot find where resumed work reports");
                return Outbound::Detached;
            }
        };

        match Reply::to_channel(Arc::clone(http), &key) {
            Some(reply) => Outbound::Discord(reply),
            None => Outbound::Detached,
        }
    }

    /// Roots are registered before this, so configuration never resolves for an unregistered thread.
    async fn attach(&self, root: &ThreadId<str>) -> Result<ThreadHandle, String> {
        match self.system.thread_handle(root).await {
            Ok(Some(handle)) => Ok(handle),
            Ok(None) => Err(format!("conversation {root} is missing")),
            Err(error) => Err(format!("cannot attach to conversation {root}: {error}")),
        }
    }

    async fn new_conversation(&self, adapter: Adapter, key: String, reply: mpsc::Sender<Response>) {
        if let Err(error) = self.roots.replace(adapter, key).await {
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
async fn pump(mut handle: ThreadHandle, mut turn: Turn, mut outbound: Outbound, input: Option<(Inbox, String)>) {
    while let Some(event) = handle.recv().await {
        let Some(step) = turn.on_event(event) else {
            continue;
        };

        let last = step.is_last();

        if let (Some(message), Some((inbox, id))) = (failure(&step), &input) {
            settle(inbox, id, message).await;
        }

        outbound.send(handle.thread_id().as_str(), step).await;

        if last {
            return;
        }
    }
}

/// Why a turn ended without its answer, if it did.
fn failure(step: &Step) -> Option<&str> {
    match step {
        Step::Failed(message) => Some(message),
        Step::Superseded => Some(SUPERSEDED),
        _ => None,
    }
}

const SUPERSEDED: &str = "interrupted by newer input to this conversation";

async fn settle(inbox: &Inbox, id: &str, message: &str) {
    if let Err(error) = inbox.fail(id, message).await {
        tracing::error!(%error, input = %id, "cannot record a failed input; it will be resent after a restart");
    }
}

fn response_for(step: Step) -> Option<Response> {
    match step {
        Step::Text(text) => Some(Response::Text { text }),
        Step::Paused => None,
        Step::Done => Some(Response::Done),
        Step::Failed(message) => Some(Response::Error { message }),
        Step::Superseded => Some(Response::Error { message: SUPERSEDED.to_owned() }),
    }
}

fn log_detached(thread: &str, step: &Step) {
    match step {
        Step::Done => tracing::info!(%thread, "finished resumed work"),
        Step::Failed(message) => tracing::warn!(%thread, %message, "resumed work failed"),
        _ => {}
    }
}
