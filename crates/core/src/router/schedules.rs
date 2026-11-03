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
use crate::schedule::APPROVAL_WINDOW;
use crate::schedule::{self, Trigger};
use crate::store::db::DbError;
use crate::store::schedules::{Change, Definition, Due, Run, Schedules};

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

    /// Makes the jobs table match the job files. Runs at startup; the error names a bad file.
    pub async fn load_job_files(&self, now: DateTime<Utc>) -> Result<(), String> {
        let stored = match self.schedules.list().await {
            Ok(jobs) => jobs,
            Err(error) => return Err(format!("cannot read jobs: {error}")),
        };

        let files = self.job_files.load(&self.policy, &stored, now)?;
        let present: Vec<String> = files.iter().map(|(id, _)| id.to_owned()).collect();

        let changed: Vec<(String, Definition)> =
            files.into_iter().filter(|(id, definition)| !stored.iter().any(|job| &job.id == id && &job.definition == definition)).collect();

        tracing::info!(files = present.len(), changed = changed.len(), "loaded job files");

        match self.schedules.sync_files(present, changed).await {
            Ok(()) => Ok(()),
            Err(error) => Err(format!("cannot store job files: {error}")),
        }
    }

    /// Applies a change owner chat proposed, if it is recent and the job has
    /// not changed since. A cron job's next run counts from now; a one-shot
    /// job whose time has passed is refused rather than run late.
    pub(super) async fn approve(&self, code: String, now: DateTime<Utc>) -> Result<String, String> {
        let _files = self.job_files.lock().await;

        let mut pending = match self.schedules.pending(&code).await {
            Ok(Some(pending)) => pending,
            Ok(None) => return Err(format!("no change waits under code {code}")),
            Err(error) => return Err(format!("cannot read pending changes: {error}")),
        };

        if now.timestamp() - pending.created_at > APPROVAL_WINDOW {
            return Err(format!("change {code} expired; ask for it again"));
        }

        self.still_current(&pending.job, pending.base_version).await?;

        if pending.definition.enabled {
            let next = schedule::resumed_next(&pending.definition, now)?;

            if next <= now.timestamp() {
                return Err(format!("the time of change {code} has passed; ask for it again"));
            }

            pending.definition.next_at = Some(next);
        }

        if pending.definition.file {
            self.job_files.write(&pending.job, &pending.definition)?;
        }

        let change = match pending.base_version {
            Some(version) => Change::Update { id: pending.job, version, definition: pending.definition },
            None => Change::Create { id: pending.job, definition: pending.definition },
        };

        match self.schedules.approve(&code, change).await {
            Ok(None) => Ok(pending.receipt),
            Ok(Some(refusal)) => Err(refusal),
            Err(error) => Err(format!("cannot apply change {code}: {error}")),
        }
    }

    async fn reject(&self, code: String) -> Result<String, String> {
        match self.schedules.reject(&code).await {
            Ok(true) => Ok(format!("Dropped change {code}; nothing was changed.")),
            Ok(false) => Err(format!("no change waits under code {code}")),
            Err(error) => Err(format!("cannot drop change {code}: {error}")),
        }
    }

    /// Refuses a change drafted against a job that has changed since, or a new job whose name was taken.
    async fn still_current(&self, id: &str, base_version: Option<i64>) -> Result<(), String> {
        let current = match self.schedules.get(id).await {
            Ok(job) => job.map(|job| job.version),
            Err(error) => return Err(format!("cannot read job {id}: {error}")),
        };

        let taken = match base_version {
            Some(_) => false,
            None => matches!(self.schedules.exists(id).await, Ok(true)),
        };

        if current != base_version || taken {
            return Err(format!("job {id} changed since this change was drafted; ask for it again"));
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
            Manage::Approve { code } => self.approve(code, Utc::now()).await,
            Manage::Reject { code } => self.reject(code).await,
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
            Ok(jobs) => match self.schedules.waiting(APPROVAL_WINDOW).await {
                Ok(waiting) => Ok(schedule::list(&jobs, &waiting)),
                Err(error) => Err(format!("cannot read pending changes: {error}")),
            },
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

    /// A file job loses its file too, or it would come back at the next start.
    async fn delete_job(&self, id: String) -> Result<String, String> {
        let _files = self.job_files.lock().await;

        if let Ok(Some(job)) = self.schedules.get(&id).await
            && job.definition.file
        {
            self.job_files.remove(&id)?;
        }

        match self.schedules.delete(id.to_owned()).await {
            Ok(true) => Ok(format!("Deleted {id}. Its run history is kept; a run already queued or running still finishes.")),
            Ok(false) => Err(format!("no job {id}")),
            Err(error) => Err(format!("cannot delete {id}: {error}")),
        }
    }

    /// A resumed cron job runs from its next occurrence after `now`: time
    /// spent paused is not caught up. A one-shot job that never ran keeps its instant.
    async fn resume_job(&self, id: String, now: DateTime<Utc>) -> Result<String, String> {
        let job = match self.schedules.get(&id).await {
            Ok(Some(job)) => job.definition,
            Ok(None) => return Err(format!("no job {id}")),
            Err(error) => return Err(format!("cannot read jobs: {error}")),
        };

        let next = schedule::resumed_next(&job, now)?;

        if let Err(error) = self.schedules.resume(id.to_owned(), next).await {
            return Err(format!("cannot resume {id}: {error}"));
        }

        Ok(format!("Resumed {id}. Next run: {}", schedule::when(Some(next), &job.timezone)))
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
        Step::Text(_) | Step::Paused | Step::Shown(_) => return,
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
