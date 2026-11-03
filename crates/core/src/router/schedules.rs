//! Scheduled runs through the router: claiming due jobs, starting queued
//! runs within capacity, recording how each run ended, and the owner's
//! `shade schedule` commands.

use chrono::{DateTime, Utc};
use infinity_agent_core::ThreadId;
use shade_proto::{Manage, Response};
use tokio::sync::mpsc;

use super::{Outbound, Router, SUPERSEDED};
use crate::notify::Notifier;
use crate::router::turn::Step;
use crate::schedule::{self, Trigger};
use crate::store::db::DbError;
use crate::store::schedules::{Due, Listed, Run, Schedules};

impl Router {
    /// Claims every due occurrence, then starts queued runs while capacity allows.
    pub async fn schedule(&self, now: DateTime<Utc>) {
        if let Err(error) = self.claim_due(now).await {
            tracing::error!(%error, "cannot claim due jobs");
        }

        if let Err(error) = self.start_queued(now).await {
            tracing::error!(%error, "cannot start queued runs");
        }
    }

    /// Each due job gets one claim, however many occurrences it missed.
    pub(super) async fn claim_due(&self, now: DateTime<Utc>) -> Result<(), DbError> {
        for due in self.schedules.due(now.timestamp()).await? {
            self.claim(due, now).await?;
        }

        Ok(())
    }

    /// The next occurrence is computed from `now`, not from the missed one,
    /// so downtime folds into this claim.
    async fn claim(&self, due: Due, now: DateTime<Utc>) -> Result<(), DbError> {
        let Some(trigger) = trigger_of(&due) else {
            return self.schedules.disable(due.id).await;
        };

        let next = trigger.next_after(now).map(|next| next.timestamp());
        tracing::info!(job = %due.id, scheduled_for = due.next_at, ?next, "claiming an occurrence");

        self.schedules.claim(due.id, due.next_at, next).await?;

        Ok(())
    }

    async fn start_queued(&self, now: DateTime<Utc>) -> Result<(), DbError> {
        for queued in self.schedules.startable(self.policy.limits.max_concurrent_background_runs).await? {
            let scheduled_for = DateTime::from_timestamp(queued.scheduled_for, 0).unwrap_or(now);
            let text = schedule::input(&queued.title, &queued.job, &queued.timezone, scheduled_for, now, &queued.prompt);

            let Some(input) = self.schedules.start(queued.run.to_owned(), text).await? else {
                continue;
            };

            tracing::info!(run = %queued.run, job = %queued.job, root = %input.root, "starting a scheduled run");
            let outbound = self.run_outbound(Run { id: queued.run, title: queued.title });
            self.dispatch(input, outbound).await;
        }

        Ok(())
    }

    pub(super) fn run_outbound(&self, run: Run) -> Outbound {
        Outbound::Run { run, schedules: self.schedules.clone(), notifier: self.notifier.clone() }
    }

    /// After a restart, runs that nothing resumed cannot finish: they are
    /// interrupted, visibly, rather than run again.
    pub(super) async fn interrupt_runs(&self, resumed: Vec<String>) -> Result<(), DbError> {
        for (root, run) in self.schedules.interrupt_all_but(resumed).await? {
            tracing::warn!(run = %run.id, "a scheduled run was interrupted by a restart");

            if let Some(notifier) = &self.notifier {
                let text = format!(
                    "⚠️ Scheduled job \"{}\" was interrupted by a restart (run {}). Some of its work may have been done.",
                    run.title, run.id
                );
                notifier.failure(&root, text).await;
            }
        }

        Ok(())
    }

    pub(super) async fn manage(&self, action: Manage, reply: mpsc::Sender<Response>) {
        let result = match action {
            Manage::List => self.list_jobs().await,
            Manage::Pause { id } => self.pause_job(id).await,
            Manage::Resume { id } => self.resume_job(id, Utc::now()).await,
            Manage::Rm { id } => self.delete_job(id).await,
        };

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

    async fn list_jobs(&self) -> Result<String, String> {
        match self.schedules.list().await {
            Ok(jobs) => Ok(render(&jobs)),
            Err(error) => Err(format!("cannot read jobs: {error}")),
        }
    }

    async fn pause_job(&self, id: String) -> Result<String, String> {
        match self.schedules.pause(id.to_owned()).await {
            Ok(true) => Ok(format!("Paused {id}. A run already queued or running still finishes.")),
            Ok(false) => Err(format!("no job {id}")),
            Err(error) => Err(format!("cannot pause {id}: {error}")),
        }
    }

    async fn delete_job(&self, id: String) -> Result<String, String> {
        match self.schedules.delete(id.to_owned()).await {
            Ok(true) => Ok(format!("Deleted {id}. Its run history is kept; a run already queued or running still finishes.")),
            Ok(false) => Err(format!("no job {id}")),
            Err(error) => Err(format!("cannot delete {id}: {error}")),
        }
    }

    /// A resumed cron job runs from its next occurrence after `now`: time
    /// spent paused is not caught up. A one-shot job that never ran keeps its instant.
    async fn resume_job(&self, id: String, now: DateTime<Utc>) -> Result<String, String> {
        let job = match self.schedules.list().await {
            Ok(jobs) => jobs.into_iter().find(|job| job.id == id),
            Err(error) => return Err(format!("cannot read jobs: {error}")),
        };

        let Some(job) = job else {
            return Err(format!("no job {id}"));
        };

        let trigger = Trigger::parse(&job.kind, &job.spec, &job.timezone)?;

        let next = match trigger {
            Trigger::Once => job.next_at,
            Trigger::Cron { .. } => trigger.next_after(now).map(|next| next.timestamp()),
        };

        let Some(next) = next else {
            return Err(format!("{id} has no occurrence left to run"));
        };

        if let Err(error) = self.schedules.resume(id.to_owned(), next).await {
            return Err(format!("cannot resume {id}: {error}"));
        }

        Ok(format!("Resumed {id}. Next run: {}", when(Some(next), &job.timezone)))
    }
}

/// A due job's timing, or `None` (logged) for a damaged definition, which is disabled.
fn trigger_of(due: &Due) -> Option<Trigger> {
    match Trigger::parse(&due.kind, &due.spec, &due.timezone) {
        Ok(trigger) => Some(trigger),
        Err(message) => {
            tracing::error!(job = %due.id, %message, "disabling a job whose definition cannot be read");
            None
        }
    }
}

/// Records how a run ended when its turn does, and tells the owner of a failure.
pub(super) async fn report(run: &Run, schedules: &Schedules, notifier: Option<&Notifier>, thread: &str, step: Step) {
    let error = match &step {
        Step::Done => None,
        Step::Failed(message) => Some(message.as_str()),
        Step::Superseded => Some(SUPERSEDED),
        Step::Text(_) | Step::Paused => return,
    };

    tracing::info!(run = %run.id, error, "a scheduled run ended");

    if let Err(db_error) = schedules.finish(&run.id, error).await {
        tracing::error!(error = %db_error, run = %run.id, "cannot record how a run ended");
    }

    if let (Some(message), Some(notifier)) = (error, notifier) {
        let text = format!("⚠️ Scheduled job \"{}\" failed (run {}): {message}", run.title, run.id);
        notifier.failure(&ThreadId::from(thread.to_owned()), text).await;
    }
}

fn render(jobs: &[Listed]) -> String {
    if jobs.is_empty() {
        return "No scheduled jobs.".to_owned();
    }

    let mut text = String::new();

    for job in jobs {
        let state = match (job.enabled, job.next_at) {
            (true, _) => "enabled",
            (false, Some(_)) => "paused",
            (false, None) => "done",
        };
        let last = match &job.last_run {
            Some((status, at)) => format!("{status} (due {})", when(Some(*at), &job.timezone)),
            None => "none".to_owned(),
        };

        text.push_str(&format!("{}  {}  [{state}]\n", job.id, job.title));
        text.push_str(&format!("    {} `{}` in {}\n", job.kind, job.spec, job.timezone));
        text.push_str(&format!("    next: {}  last run: {last}\n", when(job.next_at, &job.timezone)));
    }

    text.pop();

    text
}

fn when(at: Option<i64>, timezone: &str) -> String {
    let Some(instant) = at.and_then(|at| DateTime::from_timestamp(at, 0)) else {
        return "none".to_owned();
    };

    schedule::local(instant, timezone.parse().unwrap_or(chrono_tz::UTC))
}
