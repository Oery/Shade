//! Watches through the router: starting what they fired, delivering a
//! conversation watch's firing to its conversation, recording how each run
//! ended, and the owner's `shade watch` commands.

use infinity_agent_core::ThreadId;
use shade_proto::{Manage, Response};
use shade_store::db::DbError;
use shade_store::grants::Grants;
use shade_store::roots::{Adapter, Target};
use shade_store::watches::{Queued, Run, Watches, source_key};
use tokio::sync::mpsc;

use super::{Outbound, Router, SUPERSEDED};
use crate::notify::Notifier;
use crate::router::turn::Step;

impl Router {
    /// Starts fired runs, oldest first, in what capacity is left.
    pub(super) async fn start_watch_runs(&self) -> Result<(), DbError> {
        for queued in self.watches.startable(self.policy.limits.max_concurrent_background_runs).await? {
            let Some(input) = self.watches.start(queued.run.to_owned()).await? else {
                continue;
            };

            tracing::info!(run = %queued.run, watch = %queued.watch, root = %input.root, "starting a watch run");
            let outbound = self.watch_outbound(Run { id: queued.run, title: queued.title });
            self.dispatch(input, outbound).await;
        }

        Ok(())
    }

    /// Hands each firing to its conversation's queue. A full queue leaves it
    /// waiting for the next tick; delivering twice is harmless, as its source
    /// key makes the second one a repeat.
    pub(super) async fn deliver_watch_firings(&self) -> Result<(), DbError> {
        for queued in self.watches.deliverable().await? {
            if let Some(root) = queued.conversation.to_owned()
                && self.followup_capacity(Some(&root)).await.is_ok()
            {
                self.deliver(queued, root).await?;
            }
        }

        Ok(())
    }

    pub(super) async fn deliver(&self, queued: Queued, root: String) -> Result<(), DbError> {
        let (outbound, delivery) = self.conversation_outbound(&root).await;
        let key = source_key(&queued.run);
        let request = self.chat_request(Adapter::Api, Target::Existing(root), queued.input, Some(key), delivery);

        tracing::info!(run = %queued.run, watch = %queued.watch, "delivering a watch to its conversation");
        self.accept_chat(request, outbound).await;

        self.watches.delivered(queued.run).await
    }

    /// Where a turn the owner did not start replies: an API conversation
    /// keeps it in history and events, any other posts where it lives.
    async fn conversation_outbound(&self, root: &str) -> (Outbound, String) {
        let thread = ThreadId::from(root.to_owned());
        let grant = Grants::new(self.db.clone()).of(&thread).await;

        if matches!(grant, Ok(Some(grant)) if grant.source == "api") {
            return (Outbound::Detached(None), "api".to_owned());
        }

        let outbound = self.outbound_for(&thread).await;
        let delivery = outbound.delivery();

        (outbound, delivery)
    }

    pub(super) fn watch_outbound(&self, run: Run) -> Outbound {
        Outbound::Watch { run, watches: self.watches.clone(), notifier: self.notifier.clone() }
    }

    /// The outbound of a resumed watch run, if `thread` is one.
    pub(super) async fn resumed_watch(&self, thread: &ThreadId<str>) -> Option<Outbound> {
        match self.watches.run_of(thread).await {
            Ok(run) => run.map(|run| self.watch_outbound(run)),
            Err(error) => {
                tracing::warn!(%error, %thread, "cannot tell whether resumed work is a watch run");
                None
            }
        }
    }

    /// After a restart, watch runs that nothing resumed are interrupted, not run again.
    pub(super) async fn interrupt_watch_runs(&self, resumed: Vec<String>) -> Result<(), DbError> {
        for (root, run) in self.watches.interrupt_all_but(resumed).await? {
            tracing::warn!(run = %run.id, "a watch run was interrupted by a restart");

            if let Some(notifier) = &self.notifier {
                let text = format!(
                    "⚠️ A run of the watch \"{}\" was interrupted by a restart. Some of its work may have been done.",
                    run.title
                );
                notifier.failure(&root, text).await;
            }
        }

        Ok(())
    }

    pub(super) async fn manage_watches(&self, action: Manage, reply: mpsc::Sender<Response>) {
        let result = match action {
            Manage::List => self.list_watches().await,
            Manage::Pause { id } => self.pause_watch(id).await,
            Manage::Resume { id } => self.resume_watch(id).await,
            Manage::Rm { id } => self.delete_watch(id).await,
            Manage::Approve { .. } | Manage::Reject { .. } => Err("watches have nothing to approve".to_owned()),
        };

        self.watches_changed.notify_one();

        let text = match result {
            Ok(text) => text,
            Err(message) => {
                let _ = reply.send(Response::Error { message }).await;
                return;
            }
        };

        if reply.send(Response::Text { text }).await.is_ok() {
            let _ = reply.send(Response::Done).await;
        }
    }

    async fn list_watches(&self) -> Result<String, String> {
        match self.watches.list().await {
            Ok(watches) => Ok(shade_watch::list(&watches)),
            Err(error) => Err(format!("cannot read watches: {error}")),
        }
    }

    async fn pause_watch(&self, id: String) -> Result<String, String> {
        match self.watches.pause(id.to_owned()).await {
            Ok(true) => Ok(format!("Paused {id}. A run already queued or running still finishes.")),
            Ok(false) => Err(format!("no watch {id}")),
            Err(error) => Err(format!("cannot pause {id}: {error}")),
        }
    }

    async fn resume_watch(&self, id: String) -> Result<String, String> {
        match self.watches.resume(id.to_owned()).await {
            Ok(true) => Ok(format!("Resumed {id}. It keeps its baseline, so a change while paused shows at its next read.")),
            Ok(false) => Err(format!("no watch {id}")),
            Err(error) => Err(format!("cannot resume {id}: {error}")),
        }
    }

    async fn delete_watch(&self, id: String) -> Result<String, String> {
        match self.watches.delete(id.to_owned()).await {
            Ok(true) => Ok(format!("Deleted {id}. Its run history is kept; a run already queued or running still finishes.")),
            Ok(false) => Err(format!("no watch {id}")),
            Err(error) => Err(format!("cannot delete {id}: {error}")),
        }
    }
}

/// Records how a run ended when its turn does, and tells the owner of a failure.
pub(super) async fn report(run: &Run, watches: &Watches, notifier: Option<&Notifier>, thread: &str, step: Step) {
    let error = match &step {
        Step::Done => None,
        Step::Failed(message) => Some(message.as_str()),
        Step::Superseded => Some(SUPERSEDED),
        Step::Text(_) | Step::Paused | Step::Shown(_) => return,
    };

    tracing::info!(run = %run.id, error, "a watch run ended");

    if let Err(db_error) = watches.finish(&run.id, error).await {
        tracing::error!(error = %db_error, run = %run.id, "cannot record how a watch run ended");
    }

    if let (Some(message), Some(notifier)) = (error, notifier) {
        let text = format!("⚠️ A run of the watch \"{}\" failed: {message}", run.title);
        notifier.failure(&ThreadId::from(thread.to_owned()), text).await;
    }
}
