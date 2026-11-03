//! Webhook events through the router: starting accepted events within the
//! background capacity they share with scheduled runs, recording how each
//! run ended, and interrupting the ones a restart cannot resume.

use infinity_agent_core::ThreadId;
use shade_store::db::DbError;
use shade_store::hooks::HookEvents;

use super::{Outbound, Router, SUPERSEDED};
use crate::notify::Notifier;
use crate::router::turn::Step;

impl Router {
    /// Starts accepted events, oldest first, in what capacity is left.
    pub(super) async fn start_hook_events(&self) -> Result<(), DbError> {
        for queued in self.hooks.startable(self.policy.limits.max_concurrent_background_runs).await? {
            let Some(input) = self.hooks.start(queued.event.to_owned()).await? else {
                continue;
            };

            tracing::info!(event = %queued.event, hook = %queued.hook, root = %input.root, "starting a webhook run");
            let outbound = self.hook_outbound(queued.event, queued.hook);
            self.dispatch(input, outbound).await;
        }

        Ok(())
    }

    pub(super) fn hook_outbound(&self, event: String, hook: String) -> Outbound {
        Outbound::Hook { event, hook, events: self.hooks.clone(), notifier: self.notifier.clone() }
    }

    /// After a restart, event runs that nothing resumed are interrupted, not run again.
    pub(super) async fn interrupt_hook_events(&self, resumed: Vec<String>) -> Result<(), DbError> {
        for (root, event, hook) in self.hooks.interrupt_all_but(resumed).await? {
            tracing::warn!(%event, %hook, "a webhook run was interrupted by a restart");

            if let Some(notifier) = &self.notifier {
                let text =
                    format!("⚠️ A run started by the `{hook}` webhook was interrupted by a restart. Some of its work may have been done.");
                notifier.failure(&root, text).await;
            }
        }

        Ok(())
    }

    /// Forgets finished events past their retention, which also ends their delivery IDs' deduplication.
    pub(super) async fn prune_hook_events(&self) {
        match self.hooks.prune().await {
            Ok(0) => {}
            Ok(count) => tracing::debug!(count, "forgot old webhook events"),
            Err(error) => tracing::warn!(%error, "cannot forget old webhook events"),
        }
    }
}

/// Records how an event's run ended when its turn does, and tells the owner of a failure.
pub(super) async fn report(event: &str, hook: &str, events: &HookEvents, notifier: Option<&Notifier>, thread: &str, step: Step) {
    let error = match &step {
        Step::Done => None,
        Step::Failed(message) => Some(message.as_str()),
        Step::Superseded => Some(SUPERSEDED),
        Step::Text(_) | Step::Paused | Step::Shown(_) => return,
    };

    tracing::info!(%event, %hook, error, "a webhook run ended");

    if let Err(db_error) = events.finish(event, error).await {
        tracing::error!(error = %db_error, %event, "cannot record how a webhook run ended");
    }

    if let (Some(message), Some(notifier)) = (error, notifier) {
        let text = format!("⚠️ A run started by the `{hook}` webhook failed: {message}");
        notifier.failure(&ThreadId::from(thread.to_owned()), text).await;
    }
}
