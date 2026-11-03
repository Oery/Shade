//! Owns the Infinity system: accepts input durably, routes it, pumps output.
//!
//! Every input takes one path: it is recorded in the inbox with its root
//! before Infinity sees it, then sent under its stable ID, and a pump follows
//! its turn to the end. A pump outlives its client, so a turn's outcome is
//! always settled. Work interrupted by a restart re-enters the same path.

mod schedules;
pub mod turn;

#[cfg(test)]
mod tests;

use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use infinity_agent_core::ThreadId;
use infinity_agent_core::message::InputMessage;
use infinity_agent_core::system::local::{LaunchingSystem, ThreadHandle};
use infinity_agent_core::system::{AgentSystemBuilder, NoRapHttp};
use serenity::all::Http;
use shade_policy::Policy;
use shade_proto::{Manage, Response};
use shade_store::conversation::Conversations;
use shade_store::db::Db;
use shade_store::grants::Grants;
use shade_store::inbox::{Inbox, Input};
use shade_store::operations::Operations;
use shade_store::roots::{Adapter, Roots, Target};
use shade_store::schedules::{Run, Schedules};
use shade_store::state::States;
use shade_store::usage::Usages;
use tokio::sync::mpsc;
use tokio::time::MissedTickBehavior;

use crate::adapters::discord::Reply;
use crate::authority::{Authority, Metered};
use crate::error::BoxError;
use crate::jobfiles::JobFiles;
use crate::model::Model;
use crate::notify::Notifier;
use crate::recovery;
use crate::router::turn::{Step, Turn};
use crate::thread_config::{Services, ThreadTools};
use crate::tools::Shown;
use crate::{tools, usage};

/// Capacity of each reply stream between the router and a socket connection.
pub const REPLY_QUEUE: usize = 64;

/// How often due jobs are claimed and queued runs started.
const SCHEDULE_TICK: Duration = Duration::from_secs(30);

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
    /// Token usage by conversation.
    Usage { reply: mpsc::Sender<Response> },
    /// Owner management of scheduled jobs.
    Schedule { action: Manage, reply: mpsc::Sender<Response> },
}

/// Where a turn's output goes. The turn runs to its end either way.
pub enum Outbound {
    /// A connected client. If it leaves, the turn finishes without it.
    Client(mpsc::Sender<Response>),
    /// A Discord channel.
    Discord(Reply),
    /// Nobody: resumed work in a conversation no adapter listens to. The
    /// outcome is logged, and a failure is sent to the owner when possible.
    Detached(Option<Notifier>),
    /// A scheduled run. Its outcome is recorded, and a failure is sent to the owner.
    Run { run: Run, schedules: Schedules, notifier: Option<Notifier> },
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
            Self::Detached(notifier) => {
                log_detached(thread, &step);

                if let (Step::Failed(message), Some(notifier)) = (&step, notifier) {
                    let text = format!("⚠️ Work resumed after a restart failed in conversation {thread}: {message}");
                    notifier.failure(&ThreadId::from(thread.to_owned()), text).await;
                }
            }
            Self::Run { run, schedules, notifier } => schedules::report(run, schedules, notifier.as_ref(), thread, step).await,
        }
    }
}

pub struct Router {
    db: Db,
    policy: Arc<Policy>,
    inbox: Inbox,
    roots: Roots,
    operations: Operations,
    usages: Usages,
    schedules: Schedules,
    system: System,
    /// For replies to resumed work in Discord conversations.
    discord: Option<Arc<Http>>,
    /// Reports failed work nobody is watching.
    notifier: Option<Notifier>,
    job_files: Arc<JobFiles>,
}

impl Router {
    /// Built-in tools stay off: spawning has no limit hook (see docs/infinity-notes.md).
    pub fn start(model: Model, db: Db, policy: Arc<Policy>, services: Services, discord: Option<Arc<Http>>) -> Self {
        let authority = Authority::new(Grants::new(db.clone()), Arc::clone(&policy));
        let conversations = Conversations::new(db.clone());
        let tools = ThreadTools::new(conversations.clone(), authority.clone(), db.clone(), services.clone());
        let model = Metered::new(model, authority, Usages::new(db.clone()));
        let builder = AgentSystemBuilder::new_local(conversations, States::new(db.clone()), model);
        let system = builder.without_builtin_tools().thread_config(tools).start();

        Self::new(db, system, policy, discord, services)
    }

    pub fn new(db: Db, system: System, policy: Arc<Policy>, discord: Option<Arc<Http>>, services: Services) -> Self {
        let Services { notifier, job_files, .. } = services;
        let inbox = Inbox::new(db.clone());
        let roots = Roots::new(db.clone());
        let operations = Operations::new(db.clone());
        let usages = Usages::new(db.clone());
        let schedules = Schedules::new(db.clone());

        Self { db, policy, inbox, roots, operations, usages, schedules, system, discord, notifier, job_files }
    }

    /// Handles input and runs due jobs until the channel closes. Must run
    /// inside a `LocalSet`. The first tick is immediate, so jobs missed
    /// while the daemon was down run at startup.
    pub async fn serve(&self, mut inbound: mpsc::Receiver<Inbound>) {
        let mut ticks = tokio::time::interval(SCHEDULE_TICK);
        ticks.set_missed_tick_behavior(MissedTickBehavior::Delay);

        loop {
            tokio::select! {
                input = inbound.recv() => match input {
                    Some(input) => self.handle(input).await,
                    None => return,
                },
                _ = ticks.tick() => self.schedule(Utc::now()).await,
            }
        }
    }

    async fn handle(&self, input: Inbound) {
        match input {
            Inbound::Prompt { adapter, target, text, source_key, outbound } => {
                self.prompt(adapter, target, text, source_key, outbound).await;
            }
            Inbound::NewConversation { adapter, key, reply } => self.new_conversation(adapter, key, reply).await,
            Inbound::Usage { reply } => self.usage(reply).await,
            Inbound::Schedule { action, reply } => self.manage(action, reply).await,
        }
    }

    /// Finishes what a previous daemon left: answers its waiting tool calls,
    /// then sends its unprocessed inputs again under their own IDs. Runs
    /// before any new input is accepted.
    pub async fn resume(&self) -> Result<(), BoxError> {
        let mut resumed = Vec::new();

        for answer in recovery::answers(&self.operations).await? {
            let handle = self.attach(&answer.thread).await?;
            let outbound = self.outbound_for(&answer.thread).await;
            resumed.push(answer.thread.as_str().to_owned());
            tools::deliver(&self.system.sender(), answer.thread, answer.id, answer.call_id, answer.text).await?;
            tokio::task::spawn_local(pump(handle, Turn::continuing(), outbound, None));
        }

        for input in self.inbox.unsettled().await? {
            tracing::info!(root = %input.root, input = %input.id, "resending an input accepted before the restart");
            let outbound = self.outbound_for(&input.root).await;
            resumed.push(input.root.as_str().to_owned());
            self.dispatch(input, outbound).await;
        }

        self.interrupt_runs(resumed).await?;

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
        match self.schedules.run_of(thread).await {
            Ok(Some(run)) => return self.run_outbound(run),
            Ok(None) => {}
            Err(error) => tracing::warn!(%error, %thread, "cannot tell whether resumed work is a scheduled run"),
        }

        let detached = Outbound::Detached(self.notifier.clone());

        let Some(http) = &self.discord else {
            return detached;
        };

        let key = match self.roots.conversation_of(thread).await {
            Ok(Some((Adapter::Discord, key))) => key,
            Ok(_) => return detached,
            Err(error) => {
                tracing::warn!(%error, %thread, "cannot find where resumed work reports");
                return detached;
            }
        };

        match Reply::to_channel(Arc::clone(http), &key) {
            Some(reply) => Outbound::Discord(reply),
            None => detached,
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

    async fn usage(&self, reply: mpsc::Sender<Response>) {
        let text = match self.usages.by_conversation(usage::SHOWN).await {
            Ok(conversations) => usage::render(&conversations),
            Err(error) => {
                let _ = reply.send(Response::Error { message: format!("cannot read usage: {error}") }).await;
                return;
            }
        };

        if reply.send(Response::Text { text }).await.is_ok() {
            let _ = reply.send(Response::Done).await;
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
        Step::Shown(Shown { card, approval: code }) => Some(Response::Text {
            text: format!("\n{}\n   `shade schedule approve {code}` or `shade schedule reject {code}`\n", card.text()),
        }),
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
