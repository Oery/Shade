//! Owns the Infinity system: accepts input durably, routes it, pumps output.
//!
//! Every input takes one path: it is recorded in the inbox with its root
//! before Infinity sees it, then sent under its stable ID, and a pump follows
//! its turn to the end. A pump outlives its client, so a turn's outcome is
//! always settled. Work interrupted by a restart re-enters the same path.

mod chat;
mod client;
mod hooks;
mod resume;
mod schedules;
pub mod turn;
mod watches;

#[cfg(test)]
mod tests;

#[cfg(test)]
mod chat_tests;

#[cfg(test)]
mod watch_tests;

use std::cell::RefCell;
use std::path::PathBuf;
use std::rc::Rc;
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
use shade_schedule::jobfiles::JobFiles;
use shade_store::audit::Audit;
use shade_store::conversation::Conversations;
use shade_store::db::Db;
use shade_store::grants::Grants;
use shade_store::hooks::HookEvents;
use shade_store::inbox::{Inbox, Input};
use shade_store::operations::Operations;
use shade_store::roots::{Adapter, Conversation, Roots, Target};
use shade_store::schedules::{Run, Schedules};
use shade_store::state::States;
use shade_store::usage::Usages;
use shade_store::watches::Watches;
use tokio::sync::{Notify, mpsc};
use tokio::time::MissedTickBehavior;

use crate::adapters::discord::Reply;
use crate::authority::{Authority, Metered};
use crate::chat::ChatRuntime;
use crate::deadline::Deadline;
use crate::error::BoxError;
use crate::model::Model;
use crate::notify::Notifier;
use crate::recovery;
use crate::router::turn::{Step, Turn};
use crate::telemetry::Telemetry;
use crate::thread_config::{Services, ThreadTools};
use crate::tools::Shown;
use crate::usage;
use shade_store::chat::Chats;

/// Capacity of each reply stream between the router and a socket connection.
pub const REPLY_QUEUE: usize = 64;

/// How often due jobs are claimed and queued runs started.
const SCHEDULE_TICK: Duration = Duration::from_secs(30);

pub type System = LaunchingSystem<Conversations, States, NoRapHttp>;

/// Input from an adapter.
pub enum Inbound {
    Client {
        command: crate::client::Command,
        reply: tokio::sync::oneshot::Sender<Result<crate::client::Reply, crate::client::ClientError>>,
    },
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
    NewConversation {
        adapter: Adapter,
        key: String,
        reply: mpsc::Sender<Response>,
    },
    /// Token usage by conversation.
    Usage {
        reply: mpsc::Sender<Response>,
    },
    Audit {
        since: i64,
        root: Option<String>,
        reply: mpsc::Sender<Response>,
    },
    /// Owner management of scheduled jobs.
    Schedule {
        action: Manage,
        reply: mpsc::Sender<Response>,
    },
    /// Owner management of watches.
    Watch {
        action: Manage,
        reply: mpsc::Sender<Response>,
    },
    /// A background run may start now: a webhook event was accepted, or a watch fired.
    StartBackground,
    /// A copy of the database to a new file.
    Backup {
        path: PathBuf,
        reply: mpsc::Sender<Response>,
    },
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
    /// A run started by a webhook event. Its outcome is recorded, and a failure is sent to the owner.
    Hook { event: String, hook: String, events: HookEvents, notifier: Option<Notifier> },
    /// A run a watch fired. Its outcome is recorded, and a failure is sent to the owner.
    Watch { run: shade_store::watches::Run, watches: Watches, notifier: Option<Notifier> },
}

impl Outbound {
    fn delivery(&self) -> String {
        match self {
            Self::Discord(reply) => reply.channel_key(),
            _ => "terminal".to_owned(),
        }
    }

    async fn send(&mut self, thread: &str, step: Step) {
        match self {
            Self::Client(reply) => {
                if let Some(response) = response_for(step) {
                    // Give a healthy socket writer time to drain a burst, but
                    // detach a stalled client so the turn can still settle.
                    let sent = tokio::time::timeout(Duration::from_secs(1), reply.send(response)).await;

                    if !matches!(sent, Ok(Ok(()))) {
                        *self = Self::Detached(None);
                    }
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
            Self::Hook { event, hook, events, notifier } => hooks::report(event, hook, events, notifier.as_ref(), thread, step).await,
            Self::Watch { run, watches, notifier } => watches::report(run, watches, notifier.as_ref(), thread, step).await,
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
    telemetry: Telemetry,
    schedules: Schedules,
    hooks: HookEvents,
    watches: Watches,
    /// Wakes the watcher when the owner changes what is watched.
    watches_changed: Arc<Notify>,
    system: System,
    /// For replies to resumed work in Discord conversations.
    discord: Option<Arc<Http>>,
    /// Reports failed work nobody is watching.
    notifier: Option<Notifier>,
    job_files: Arc<JobFiles>,
    chat: Rc<ChatRuntime>,
    completed: RefCell<Option<mpsc::Receiver<()>>>,
}

impl Router {
    /// Built-in tools stay off: spawning has no limit hook (see docs/infinity-notes.md).
    pub fn start(model: Model, db: Db, policy: Arc<Policy>, services: Services, discord: Option<Arc<Http>>) -> Self {
        let authority = Authority::new(Grants::new(db.clone()), Arc::clone(&policy)).with_redactor(Arc::clone(&services.redactor));
        let telemetry = Telemetry::new(db.clone(), Arc::clone(&services.redactor));
        let conversations = Conversations::new(db.clone());
        let tools = ThreadTools::new(conversations.clone(), authority.clone(), db.clone(), services.clone());
        let prices = model.prices();
        let model = Metered::new(model, authority, telemetry.clone(), services.memory.clone(), prices);
        let builder = AgentSystemBuilder::new_local(conversations, States::new(db.clone()), model);
        let system = builder.without_builtin_tools().thread_config(tools).start();

        Self::new(db, system, policy, discord, services, telemetry)
    }

    pub fn new(db: Db, system: System, policy: Arc<Policy>, discord: Option<Arc<Http>>, services: Services, telemetry: Telemetry) -> Self {
        let Services { notifier, job_files, watches_changed, .. } = services;
        let inbox = Inbox::new(db.clone());
        let roots = Roots::new(db.clone());
        let operations = Operations::new(db.clone());
        let usages = Usages::new(db.clone());
        let schedules = Schedules::new(db.clone());
        let hooks = HookEvents::new(db.clone());
        let watches = Watches::new(db.clone());

        let (wakeup, completed) = mpsc::channel(1);
        let chat = Rc::new(ChatRuntime::new(Chats::new(db.clone()), wakeup));

        Self {
            db,
            policy,
            inbox,
            roots,
            operations,
            usages,
            telemetry,
            schedules,
            hooks,
            watches,
            watches_changed,
            system,
            discord,
            notifier,
            job_files,
            chat,
            completed: RefCell::new(Some(completed)),
        }
    }

    /// Handles input and runs due jobs until the channel closes. Must run
    /// inside a `LocalSet`. The first tick is immediate, so jobs missed
    /// while the daemon was down run at startup.
    pub async fn serve(&self, mut inbound: mpsc::Receiver<Inbound>) {
        let Some(mut completed) = self.completed.borrow_mut().take() else {
            return;
        };

        let mut ticks = tokio::time::interval(SCHEDULE_TICK);
        ticks.set_missed_tick_behavior(MissedTickBehavior::Delay);

        loop {
            tokio::select! {
                input = inbound.recv() => match input {
                    Some(input) => self.handle(input).await,
                    None => return,
                },
                _ = completed.recv() => self.start_chats().await,
                _ = ticks.tick() => {
                    self.start_chats().await;
                    self.schedule(Utc::now()).await;
                },
            }
        }
    }

    async fn handle(&self, input: Inbound) {
        match input {
            Inbound::Client { command, reply } => {
                let result = self.client_command(command).await;
                let _ = reply.send(result);
            }
            Inbound::Prompt { adapter, target, text, source_key, outbound } => {
                self.prompt(adapter, target, text, source_key, outbound).await;
            }
            Inbound::NewConversation { adapter, key, reply } => self.new_conversation(adapter, key, reply).await,
            Inbound::Usage { reply } => self.usage(reply).await,
            Inbound::Audit { since, root, reply } => self.audit(since, root, reply).await,
            Inbound::Schedule { action, reply } => self.manage(action, reply).await,
            Inbound::Watch { action, reply } => self.manage_watches(action, reply).await,
            Inbound::StartBackground => self.start_background(Utc::now()).await,
            Inbound::Backup { path, reply } => self.backup(path, reply).await,
        }
    }

    /// Finishes what a previous daemon left: answers its waiting tool calls,
    /// then sends its unprocessed inputs again under their own IDs. Runs
    /// before any new input is accepted.
    pub async fn resume(&self) -> Result<(), BoxError> {
        let mut resumed = Vec::new();
        let answers = recovery::answers(&self.operations).await?;
        let continuing = answers.iter().map(|answer| answer.thread.as_str().to_owned()).collect();
        self.chat.store.recover(continuing).await?;

        self.resume_tools(answers, &mut resumed).await?;
        self.resume_inputs(&mut resumed).await?;

        self.interrupt_hook_events(resumed.to_owned()).await?;
        self.interrupt_watch_runs(resumed.to_owned()).await?;
        self.interrupt_runs(resumed).await?;
        self.start_chats().await;

        Ok(())
    }

    /// Interrupts active completions, flushes their history, stops every
    /// thread, then waits for the database to finish queued writes.
    pub async fn shutdown(self) {
        self.system.shutdown().await;
        self.telemetry.flush().await;

        if let Err(error) = self.db.flush().await {
            tracing::error!(%error, "cannot flush the database at shutdown");
        }
    }

    async fn prompt(&self, adapter: Adapter, target: Target, text: String, source_key: Option<String>, mut outbound: Outbound) {
        if adapter != Adapter::DiscordGuest {
            self.enqueue_chat(adapter, target, text, source_key, outbound).await;
            return;
        }

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
                self.finish_chat(root.as_str(), &id, &Step::Failed(message.to_owned())).await;
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

        self.follow(handle, turn, outbound, Some((self.inbox.clone(), id))).await;
    }

    /// A deadline also ends the outward turn during Infinity's retry backoff.
    async fn follow(&self, handle: ThreadHandle, turn: Turn, mut outbound: Outbound, input: Option<(Inbox, String)>) {
        let grants = Grants::new(self.db.clone());
        let deadline = match Deadline::for_thread(grants, handle.thread_id(), self.policy.limits.max_run_seconds).await {
            Ok(deadline) => deadline,
            Err(error) => {
                let message = error.to_string();

                if let Some((inbox, id)) = &input {
                    settle(inbox, id, &message).await;
                }

                if let Some((_, id)) = &input {
                    self.finish_chat(handle.thread_id().as_str(), id, &Step::Failed(message.to_owned())).await;
                }

                outbound.send(handle.thread_id().as_str(), Step::Failed(message)).await;
                return;
            }
        };

        let tracked = match &input {
            Some((_, id)) => Some(id.to_owned()),
            None => match self.chat.store.running(handle.thread_id().as_str()).await {
                Ok(Some(turn)) => Some(turn.id),
                _ => None,
            },
        };
        let tracked = match tracked {
            Some(id) if self.chat.tracks(handle.thread_id().as_str(), &id) => Some((Rc::clone(&self.chat), id)),
            _ => None,
        };

        tokio::task::spawn_local(pump(handle, turn, outbound, input, self.telemetry.clone(), deadline, tracked));
    }

    /// Where resumed work in `thread` reports: its run's record, or its Discord channel, if it is one.
    async fn outbound_for(&self, thread: &ThreadId<str>) -> Outbound {
        if let Some(outbound) = self.background_outbound(thread).await {
            return outbound;
        }

        let detached = Outbound::Detached(self.notifier.clone());

        let Some(http) = &self.discord else {
            return detached;
        };

        let key = match self.roots.conversation_of(thread).await {
            Ok(Some(Conversation { adapter: Adapter::Discord | Adapter::DiscordGuest, key, .. })) => key,
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

    /// The outbound of a scheduled or webhook run, when `thread` belongs to one.
    async fn background_outbound(&self, thread: &ThreadId<str>) -> Option<Outbound> {
        match self.schedules.run_of(thread).await {
            Ok(Some(run)) => return Some(self.run_outbound(run)),
            Ok(None) => {}
            Err(error) => tracing::warn!(%error, %thread, "cannot tell whether resumed work is a scheduled run"),
        }

        if let Some(outbound) = self.resumed_watch(thread).await {
            return Some(outbound);
        }

        match self.hooks.event_of(thread).await {
            Ok(event) => event.map(|(event, hook)| self.hook_outbound(event, hook)),
            Err(error) => {
                tracing::warn!(%error, %thread, "cannot tell whether resumed work is a webhook run");
                None
            }
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

    /// Copies the database while the daemon keeps serving.
    async fn backup(&self, path: PathBuf, reply: mpsc::Sender<Response>) {
        let bytes = match self.db.backup(path.to_owned()).await {
            Ok(bytes) => bytes,
            Err(error) => {
                let _ = reply.send(Response::Error { message: format!("cannot back up: {error}") }).await;
                return;
            }
        };

        let text = format!(
            "Backed up the database to {} ({} KiB). It does not include the config, its .env, job files, or the vault.",
            path.display(),
            bytes.div_ceil(1024)
        );

        if reply.send(Response::Text { text }).await.is_ok() {
            let _ = reply.send(Response::Done).await;
        }
    }

    async fn usage(&self, reply: mpsc::Sender<Response>) {
        self.telemetry.flush().await;

        let text = match self.usage_text().await {
            Ok(text) => text,
            Err(error) => {
                let _ = reply.send(Response::Error { message: format!("cannot read usage: {error}") }).await;
                return;
            }
        };

        if reply.send(Response::Text { text }).await.is_ok() {
            let _ = reply.send(Response::Done).await;
        }
    }

    async fn usage_text(&self) -> Result<String, shade_store::db::DbError> {
        let conversations = self.usages.by_conversation(usage::SHOWN).await?;
        let origins = self.usages.by_origin().await?;
        let text = usage::render(&conversations, &origins);

        Ok(format!("{text}\nObservations dropped this process: {}\n", self.telemetry.dropped()))
    }

    async fn audit(&self, since: i64, root: Option<String>, reply: mpsc::Sender<Response>) {
        self.telemetry.flush().await;

        let chunks = match self.audit_chunks(since, root).await {
            Ok(chunks) => chunks,
            Err(error) => {
                let _ = reply.send(Response::Error { message: format!("cannot read audit: {error}") }).await;
                return;
            }
        };

        for text in chunks {
            if reply.send(Response::Text { text }).await.is_err() {
                return;
            }
        }

        let _ = reply.send(Response::Done).await;
    }

    async fn audit_chunks(&self, since: i64, root: Option<String>) -> Result<Vec<String>, shade_store::db::DbError> {
        let roots = Audit::new(self.db.clone()).recent(since, root).await?;
        let mut chunks = Vec::new();

        for root in roots {
            let usage = self.usages.for_root(root.id.to_owned()).await?;
            chunks.push(crate::audit::render(&root, &self.policy, &usage));
        }

        if chunks.is_empty() {
            chunks.push("No matching activity.\n".to_owned());
        }

        chunks.push(format!("\nUp to 20 roots, last 50 entries each. Tool ledger is durable; observer/usage records are best effort.\nObservations dropped this process: {}\n", self.telemetry.dropped()));

        Ok(chunks)
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
async fn pump(
    mut handle: ThreadHandle, mut turn: Turn, mut outbound: Outbound, input: Option<(Inbox, String)>, telemetry: Telemetry,
    deadline: Deadline, tracked: Option<(Rc<ChatRuntime>, String)>,
) {
    loop {
        let step = match deadline.run(handle.recv()).await {
            Ok(Some(event)) => {
                telemetry.observe(handle.thread_id().as_str(), &event);

                if let Some((chat, id)) = &tracked {
                    chat.observe(handle.thread_id().as_str(), id, &event);
                }

                let Some(step) = turn.on_event(event) else {
                    continue;
                };

                step
            }
            Ok(None) => return,
            Err(error) => Step::Failed(error.to_string()),
        };

        let step = match step {
            Step::Failed(message) => Step::Failed(telemetry.redact(message)),
            step => step,
        };

        let last = step.is_last();

        if let (Some(message), Some((inbox, id))) = (failure(&step), &input) {
            settle(inbox, id, message).await;
        }

        if last {
            let summary = match &step {
                Step::Failed(_) => "failed",
                Step::Superseded => "superseded",
                _ => "completed",
            };
            telemetry.event(handle.thread_id().as_str(), "turn", summary.to_owned());
        }

        if let Err(error) = publish_step(tracked.as_ref(), handle.thread_id().as_str(), &step).await {
            tracing::error!(%error, "cannot finish chat turn; queue remains blocked until recovery");
            outbound.send(handle.thread_id().as_str(), Step::Failed("cannot save the turn status".to_owned())).await;
            return;
        }

        outbound.send(handle.thread_id().as_str(), step).await;

        if last {
            return;
        }
    }
}

async fn publish_step(tracked: Option<&(Rc<ChatRuntime>, String)>, root: &str, step: &Step) -> Result<(), shade_store::db::DbError> {
    let Some((chat, id)) = tracked else {
        return Ok(());
    };

    chat.step(root, id, step);

    chat.finish(root, id, step).await
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
