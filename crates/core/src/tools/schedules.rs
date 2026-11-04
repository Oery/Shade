//! `schedule_create`, `schedule_list`, `schedule_update`, `schedule_delete`:
//! owner chat manages scheduled jobs.
//!
//! Only owner chat holding `schedules` gets these, and every call checks
//! both again. A job's caps must be ones the conversation holds, minus
//! management. Each change is stored in one transaction with its receipt
//! as the call's result, so a repeated call replays the receipt instead of
//! changing anything twice. The receipt is written by Shade and shown to
//! the owner as it is, not paraphrased by the model.

use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use infinity_agent_core::ThreadId;
use infinity_agent_core::tools::{Tool, ToolContext};
use infinity_agent_core::traits::InputSender;
use rap_protocol::{ProviderCallId, ToolCallId};
use serde::Deserialize;
use serde::de::DeserializeOwned;
use shade_policy::channels;
use shade_policy::{Cap, Caps, Policy};
use shade_schedule::TriggerArgs;
use shade_schedule::jobfiles::{self, JobFiles};
use shade_schedule::receipt::{Card, Proposal, Verb};
use shade_store::db::Db;
use shade_store::operations::Operations;
use shade_store::roots::{Adapter, Conversation, Roots};
use shade_store::schedules::{Applied, Change, Definition, MAX_JOBS, Pending, Receipt, Schedules};

use crate::authority::Authority;
use crate::error::BoxError;
use crate::tools::{self, Shown};

const MAX_TITLE: usize = 100;
const MAX_PROMPT: usize = 4000;
const MAX_CHANNELS: usize = 10;

#[derive(Clone, Copy, PartialEq)]
pub enum Action {
    Create,
    List,
    Update,
    Delete,
}

impl Action {
    pub const ALL: [Self; 4] = [Self::Create, Self::List, Self::Update, Self::Delete];

    fn name(self) -> &'static str {
        match self {
            Self::Create => "schedule_create",
            Self::List => "schedule_list",
            Self::Update => "schedule_update",
            Self::Delete => "schedule_delete",
        }
    }
}

pub struct ScheduleTool {
    action: Action,
    description: String,
    parameters: serde_json::Value,
    work: Arc<Work>,
}

/// What a running call needs, shared by the four tools and their tasks.
pub struct Work {
    authority: Authority,
    schedules: Schedules,
    operations: Operations,
    roots: Roots,
    job_files: Arc<JobFiles>,
}

/// A validated change: what it does, what the model is told, and what the owner sees.
struct Prepared {
    change: Change,
    said: String,
    card: Card,
}

/// A call's result, and Shade's receipt for the owner when it changed something.
struct Answer {
    text: String,
    shown: Option<Shown>,
}

/// Where a result goes.
struct Call<M> {
    sender: M,
    thread: ThreadId,
    id: String,
    call_id: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CreateArgs {
    title: String,
    prompt: String,
    trigger: TriggerArgs,
    caps: Vec<String>,
    channels: Option<Vec<String>>,
    /// Keep the job in `jobs/<file>.toml`, under that ID.
    file: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct UpdateArgs {
    id: String,
    title: Option<String>,
    prompt: Option<String>,
    trigger: Option<TriggerArgs>,
    caps: Option<Vec<String>>,
    channels: Option<Vec<String>>,
    paused: Option<bool>,
}

impl UpdateArgs {
    /// Only pausing or resuming: the job does nothing new, so it needs no approval.
    fn only_pausing(&self) -> bool {
        self.title.is_none() && self.prompt.is_none() && self.trigger.is_none() && self.caps.is_none() && self.channels.is_none()
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct DeleteArgs {
    id: String,
}

impl Work {
    pub fn new(authority: Authority, db: Db, job_files: Arc<JobFiles>) -> Arc<Self> {
        let (schedules, operations, roots) = (Schedules::new(db.clone()), Operations::new(db.clone()), Roots::new(db));

        Arc::new(Self { authority, schedules, operations, roots, job_files })
    }
}

impl ScheduleTool {
    /// The tool for `action`, described for a conversation holding `caps`.
    pub fn new(action: Action, work: &Arc<Work>, caps: &Caps) -> Self {
        let delegable = shade_policy::delegable(caps);
        let timezone = work.authority.policy().timezone.name();

        let parameters = parameters(action, &delegable, work.authority.policy(), timezone);

        Self { action, description: describe(action), parameters, work: Arc::clone(work) }
    }
}

#[async_trait]
impl<M: InputSender + 'static> Tool<M> for ScheduleTool {
    fn name(&self) -> &str {
        self.action.name()
    }

    fn description(&self) -> &str {
        &self.description
    }

    fn parameters(&self) -> serde_json::Value {
        self.parameters.clone()
    }

    async fn execute(
        &self, args: serde_json::Value, id: ToolCallId, call_id: Option<ProviderCallId>, context: &ToolContext<M>,
    ) -> Result<(), BoxError> {
        let call = Call {
            sender: context.message_sender.clone(),
            thread: context.group_id.clone(),
            id: id.into_inner(),
            call_id: call_id.map(ProviderCallId::into_inner),
        };

        let (work, action) = (Arc::clone(&self.work), self.action);
        tokio::task::spawn_local(async move { work.answer(action, args, call).await });

        Ok(())
    }
}

impl Work {
    async fn answer<M: InputSender>(&self, action: Action, args: serde_json::Value, call: Call<M>) {
        let Answer { text, shown } = match self.run(action, args, &call).await {
            Ok(answer) => answer,
            Err(message) => Answer { text: format!("Error: {message}"), shown: None },
        };

        if let Err(error) = tools::deliver_shown(&call.sender, call.thread, call.id, call.call_id, text, shown).await {
            tracing::warn!(%error, "cannot deliver a schedule tool result; restart recovery will");
        }
    }

    async fn run<M>(&self, action: Action, args: serde_json::Value, call: &Call<M>) -> Result<Answer, String> {
        let deadline = match self.authority.deadline(&call.thread).await {
            Ok(deadline) => deadline,
            Err(error) => return Err(error.to_string()),
        };

        match deadline.run(self.apply(action, args, call)).await {
            Ok(result) => result,
            Err(error) => Err(format!("{error}. {}", crate::recovery::UNKNOWN_OUTCOME)),
        }
    }

    async fn apply<M>(&self, action: Action, args: serde_json::Value, call: &Call<M>) -> Result<Answer, String> {
        let authorized = match self.authority.admit_management(&call.thread, &Cap::Schedules).await {
            Ok(authorized) => authorized,
            Err(denied) => return Err(denied.to_string()),
        };

        if action == Action::List {
            return Ok(Answer { text: self.list().await?, shown: None });
        }

        let thread = call.thread.as_str();

        // A repeated call is answered before its arguments are checked again: time has moved on.
        match self.operations.captured(thread, &call.id).await {
            Ok(Some(result)) => return Ok(Answer { text: result, shown: None }),
            Ok(None) => {}
            Err(error) => return Err(format!("cannot read earlier results: {error}")),
        }

        // Every change is read, written to its file and stored under one lock,
        // so a job file never disagrees with SQLite while the daemon runs.
        let _files = self.job_files.lock().await;
        let Prepared { mut change, said, card } = self.change(action, args, &authorized.caps, &call.thread).await?;
        self.write_file(&change)?;

        // A proposal is the only change the owner is shown: as a card asking for approval.
        let code = match &mut change {
            Change::Propose(pending) => {
                pending.thread = Some(thread.to_owned());
                Some(pending.code.to_owned())
            }
            _ => None,
        };

        let receipt = Receipt { thread: thread.to_owned(), call: call.id.to_owned(), tool: action.name(), text: said };

        let applied = match self.schedules.apply(change, receipt).await {
            Ok(applied) => applied,
            Err(error) => return Err(format!("nothing was changed because the change could not be stored: {error}")),
        };

        let answer = match applied {
            Applied::Done(said) => Answer { text: said, shown: code.map(|approval| Shown { card, approval }) },
            Applied::Refused(text) | Applied::Replayed(text) => Answer { text, shown: None },
        };

        Ok(answer)
    }

    /// The change a call asks for, validated, with its receipt.
    async fn change(&self, action: Action, args: serde_json::Value, held: &Caps, thread: &ThreadId) -> Result<Prepared, String> {
        let now = Utc::now();

        match action {
            Action::Create => self.create(parse(args)?, held, thread, now).await,
            Action::Update => self.update(parse(args)?, held, now).await,
            Action::Delete => self.delete(parse(args)?, now).await,
            Action::List => Err("schedule_list changes nothing".to_owned()),
        }
    }

    async fn list(&self) -> Result<String, String> {
        match self.schedules.list().await {
            Ok(jobs) => match self.schedules.waiting(shade_schedule::APPROVAL_WINDOW).await {
                Ok(waiting) => Ok(shade_schedule::list(&jobs, &waiting)),
                Err(error) => Err(format!("cannot read pending changes: {error}")),
            },
            Err(error) => Err(format!("cannot read jobs: {error}")),
        }
    }

    /// A file job is written to its file before SQLite: the files win at startup.
    fn write_file(&self, change: &Change) -> Result<(), String> {
        match change {
            Change::Create { id, definition } | Change::Update { id, definition, .. } if definition.file => {
                self.job_files.write(id, definition)
            }
            Change::Delete { id, file: true } => self.job_files.remove(id),
            _ => Ok(()),
        }
    }

    async fn create(&self, args: CreateArgs, held: &Caps, thread: &ThreadId, now: DateTime<Utc>) -> Result<Prepared, String> {
        let timing = shade_schedule::timing(args.trigger, self.authority.policy().timezone, now)?;

        let channels = match args.channels {
            Some(channels) => self.channels(channels)?,
            None => vec![self.home_channel(thread).await?],
        };

        let (id, file) = self.new_id(args.file).await?;

        let definition = Definition {
            title: bounded("title", args.title, MAX_TITLE)?,
            kind: timing.kind.to_owned(),
            spec: timing.spec,
            timezone: timing.timezone.name().to_owned(),
            prompt: bounded("prompt", args.prompt, MAX_PROMPT)?,
            caps: delegate(args.caps, held)?,
            channels,
            file,
            enabled: true,
            next_at: Some(timing.next_at.timestamp()),
        };

        let card = Card::new(Verb::Created, &id, &definition, self.home(), now);

        Ok(self.gate(id, None, definition, card))
    }

    /// A short random ID, or the job file name, which must be new.
    async fn new_id(&self, file: Option<String>) -> Result<(String, bool), String> {
        let count = match self.schedules.list().await {
            Ok(jobs) => jobs.len(),
            Err(error) => return Err(format!("cannot read jobs: {error}")),
        };

        if count >= usize::try_from(MAX_JOBS).unwrap_or(usize::MAX) {
            return Err(format!("there are already {MAX_JOBS} jobs; delete one first"));
        }

        let Some(name) = file else {
            let mut id = uuid::Uuid::new_v4().simple().to_string();
            id.truncate(8);

            return Ok((id, false));
        };

        jobfiles::check_name(&name)?;

        match self.schedules.exists(&name).await {
            Ok(false) => Ok((name, true)),
            Ok(true) => Err(format!("a job named {name} exists or existed; choose another file name")),
            Err(error) => Err(format!("cannot read jobs: {error}")),
        }
    }

    /// A job file is the owner's: owner chat may draft one, but it is
    /// written only when the owner approves. Other jobs change at once.
    fn gate(&self, id: String, base_version: Option<i64>, definition: Definition, card: Card) -> Prepared {
        if !definition.file {
            let said = said(&card, &id);

            let change = match base_version {
                Some(version) => Change::Update { id, version, definition },
                None => Change::Create { id, definition },
            };

            return Prepared { change, said, card };
        }

        let proposal = if base_version.is_none() {
            Proposal::New
        } else {
            Proposal::Change
        };

        propose(id, base_version, definition, card, proposal)
    }

    fn home(&self) -> chrono_tz::Tz {
        self.authority.policy().timezone
    }

    /// The server channel of the conversation `thread` belongs to, or the owner's DMs.
    async fn home_channel(&self, thread: &ThreadId) -> Result<String, String> {
        match self.roots.conversation_of(thread).await {
            Ok(Some(Conversation { adapter: Adapter::Discord, key, direct: false })) => {
                Ok(channels::channel(&key, &self.authority.policy().channels)?.0)
            }
            Ok(_) => Ok("dm".to_owned()),
            Err(error) => Err(format!("cannot find this conversation's channel: {error}")),
        }
    }

    /// Owner chat may give a job any channel; each is checked and stored in one spelling.
    fn channels(&self, written: Vec<String>) -> Result<Vec<String>, String> {
        if written.is_empty() || written.len() > MAX_CHANNELS {
            return Err(format!("give between 1 and {MAX_CHANNELS} channels"));
        }

        let mut channels = Vec::new();

        for channel in written {
            let (stored, _) = channels::channel(&channel, &self.authority.policy().channels)?;

            if !channels.contains(&stored) {
                channels.push(stored);
            }
        }

        Ok(channels)
    }

    /// Changes only what is given. A new trigger counts from now and
    /// re-enables the job; nothing missed in the past is run.
    async fn update(&self, args: UpdateArgs, held: &Caps, now: DateTime<Utc>) -> Result<Prepared, String> {
        let job = match self.schedules.get(&args.id).await {
            Ok(Some(job)) => job,
            Ok(None) => return Err(format!("no job {}", args.id)),
            Err(error) => return Err(format!("cannot read job {}: {error}", args.id)),
        };

        let only_pausing = args.only_pausing();
        let mut definition = job.definition;

        if let Some(channels) = args.channels {
            definition.channels = self.channels(channels)?;
        }

        if let Some(title) = args.title {
            definition.title = bounded("title", title, MAX_TITLE)?;
        }

        if let Some(prompt) = args.prompt {
            definition.prompt = bounded("prompt", prompt, MAX_PROMPT)?;
        }

        if let Some(caps) = args.caps {
            definition.caps = delegate(caps, held)?;
        }

        if let Some(trigger) = args.trigger {
            let timing = shade_schedule::timing(trigger, self.authority.policy().timezone, now)?;
            definition.kind = timing.kind.to_owned();
            definition.spec = timing.spec;
            definition.timezone = timing.timezone.name().to_owned();
            definition.next_at = Some(timing.next_at.timestamp());
            definition.enabled = true;
        } else if args.paused == Some(false) && !definition.enabled {
            definition.next_at = Some(shade_schedule::resumed_next(&definition, now)?);
        }

        if let Some(paused) = args.paused {
            definition.enabled = !paused;
        }

        let card = Card::new(Verb::Updated, &job.id, &definition, self.home(), now);

        if only_pausing {
            let said = said(&card, &job.id);
            return Ok(Prepared { change: Change::Update { id: job.id, version: job.version, definition }, said, card });
        }

        Ok(self.gate(job.id, Some(job.version), definition, card))
    }

    async fn delete(&self, args: DeleteArgs, now: DateTime<Utc>) -> Result<Prepared, String> {
        let job = match self.schedules.get(&args.id).await {
            Ok(Some(job)) => job,
            Ok(None) => return Err(format!("no job {}", args.id)),
            Err(error) => return Err(format!("cannot read job {}: {error}", args.id)),
        };

        let card = Card::new(Verb::Deleted, &job.id, &job.definition, self.home(), now);

        if job.definition.file {
            return Ok(propose(job.id, Some(job.version), job.definition, card, Proposal::Delete));
        }

        let said = said(&card, &job.id);

        Ok(Prepared { change: Change::Delete { id: job.id, file: false }, said, card })
    }
}

/// A change that waits for the owner, shown to them as a card with buttons.
fn propose(id: String, base_version: Option<i64>, definition: Definition, card: Card, proposal: Proposal) -> Prepared {
    let mut code = uuid::Uuid::new_v4().simple().to_string();
    code.truncate(6);

    let undone = match proposal {
        Proposal::New => "created",
        Proposal::Change => "changed",
        Proposal::Delete => "deleted",
    };

    let said = format!(
        "Job {id} is not {undone} yet: job files change only with the owner's approval, and they now see a card asking \
         for it. Nothing you write in this turn is shown to them, so end it. You will get a message when they decide."
    );

    // What `shade schedule approve` shows once it applies.
    let receipt = card.text();
    let card = Card { verb: Verb::Proposed(proposal), ..card };
    let deletes = proposal == Proposal::Delete;
    let pending = Pending { code, job: id, base_version, definition, deletes, receipt, thread: None, created_at: 0 };

    Prepared { change: Change::Propose(pending), said, card }
}

/// What the model is told about a change that applied: the details, to
/// confirm in its own words.
fn said(card: &Card, id: &str) -> String {
    format!("Done (job {id}):\n{}\nConfirm it to the owner in one short sentence.", card.text())
}

fn parse<T: DeserializeOwned>(args: serde_json::Value) -> Result<T, String> {
    match serde_json::from_value(args) {
        Ok(args) => Ok(args),
        Err(error) => Err(format!("invalid arguments: {error}")),
    }
}

fn bounded(field: &str, text: String, max: usize) -> Result<String, String> {
    let text = text.trim();

    if text.is_empty() {
        return Err(format!("{field} is empty"));
    }

    if text.chars().count() > max {
        return Err(format!("{field} is longer than {max} characters"));
    }

    Ok(text.to_owned())
}

/// The caps a job may get from a conversation holding `held`: its own,
/// minus management. Anything else is refused by name.
fn delegate(requested: Vec<String>, held: &Caps) -> Result<Vec<String>, String> {
    let delegable = shade_policy::delegable(held);
    let mut caps = Caps::new();
    let mut refused = Vec::new();

    for name in requested {
        match Cap::try_from(name) {
            Ok(cap) if delegable.contains(&cap) => {
                caps.insert(cap);
            }
            Ok(cap) => refused.push(cap.name().to_owned()),
            Err(unknown) => return Err(unknown),
        }
    }

    if !refused.is_empty() {
        return Err(format!(
            "cannot give a job {}: a job only gets caps this conversation holds, and never schedules or watches",
            refused.join(", ")
        ));
    }

    if caps.contains(&Cap::ShellNetwork) && !caps.contains(&Cap::Shell) {
        return Err("shell_network requires shell".to_owned());
    }

    Ok(caps.into_iter().map(|cap| cap.name().to_owned()).collect())
}

fn describe(action: Action) -> String {
    let text = match action {
        Action::Create => {
            "Create a scheduled job: at each trigger, a fresh background run gets `prompt` and only `caps`. The run does \
             not see this conversation, so write a self-contained prompt; it reaches the owner only through notify, so \
             reminders need the notify cap. A job file (`file`) waits for the owner's approval; other jobs apply at once."
        }
        Action::List => "List scheduled jobs with their triggers, next runs, caps, prompts, and last runs.",
        Action::Update => {
            "Change a scheduled job; omitted fields stay as they are. A new trigger counts from now and resumes the job. \
             paused stops or resumes runs. Runs already started keep their old definition. Changing a job file waits \
             for the owner's approval, except pausing or resuming it."
        }
        Action::Delete => "Delete a scheduled job. Its run history is kept. Deleting a job file waits for the owner's approval.",
    };

    text.to_owned()
}

fn parameters(action: Action, delegable: &Caps, policy: &Policy, timezone: &str) -> serde_json::Value {
    let id = serde_json::json!({ "type": "string", "description": "The job's ID, from its receipt or schedule_list." });

    match action {
        Action::Create => serde_json::json!({
            "type": "object",
            "properties": {
                "title": { "type": "string", "maxLength": MAX_TITLE },
                "prompt": { "type": "string", "maxLength": MAX_PROMPT, "description": "What each run should do, self-contained." },
                "trigger": trigger(timezone),
                "caps": caps(delegable, policy),
                "channels": channels(),
                "file": {
                    "type": "string",
                    "description": "Keep a long-lived job in the owner's job files as jobs/<file>.toml, under this ID \
                                    (lowercase letters, digits, - and _). Omit for reminders and short-lived jobs.",
                },
            },
            "required": ["title", "prompt", "trigger", "caps"],
            "additionalProperties": false,
        }),
        Action::List => serde_json::json!({ "type": "object", "properties": {}, "additionalProperties": false }),
        Action::Update => serde_json::json!({
            "type": "object",
            "properties": {
                "id": id,
                "title": { "type": "string", "maxLength": MAX_TITLE },
                "prompt": { "type": "string", "maxLength": MAX_PROMPT },
                "trigger": trigger(timezone),
                "caps": caps(delegable, policy),
                "channels": channels(),
                "paused": { "type": "boolean" },
            },
            "required": ["id"],
            "additionalProperties": false,
        }),
        Action::Delete => serde_json::json!({
            "type": "object",
            "properties": { "id": id },
            "required": ["id"],
            "additionalProperties": false,
        }),
    }
}

fn trigger(timezone: &str) -> serde_json::Value {
    let zone = format!("IANA time zone, such as Europe/Paris. Defaults to {timezone}.");

    serde_json::json!({
        "anyOf": [
            {
                "type": "object",
                "properties": {
                    "kind": { "type": "string", "enum": ["cron"] },
                    "expression": {
                        "type": "string",
                        "description": "Five fields: minute hour day-of-month month day-of-week, such as `0 8 * * 1-5`.",
                    },
                    "timezone": { "type": "string", "description": zone },
                },
                "required": ["kind", "expression"],
                "additionalProperties": false,
            },
            {
                "type": "object",
                "properties": {
                    "kind": { "type": "string", "enum": ["once"] },
                    "at": {
                        "type": "string",
                        "description": "When to run: 2026-11-06T09:00:00+01:00 with its offset, or 2026-11-06T09:00 with `timezone`.",
                    },
                    "timezone": { "type": "string", "description": "IANA time zone `at` is read in when it has no offset." },
                },
                "required": ["kind", "at"],
                "additionalProperties": false,
            },
        ],
    })
}

fn channels() -> serde_json::Value {
    serde_json::json!({
        "type": "array",
        "items": { "type": "string" },
        "description": "Where runs may notify, the first by default: `dm` (the owner's DMs), a configured channel \
                        name, a channel ID, or a `<#id>` mention the owner wrote. Defaults to this conversation.",
    })
}

fn caps(delegable: &Caps, policy: &Policy) -> serde_json::Value {
    let names: Vec<&str> = delegable.iter().map(|cap| cap.name()).collect();
    let described: Vec<String> = delegable.iter().map(|cap| format!("{}: {}", cap.name(), policy.describe(cap))).collect();

    let description = if described.is_empty() {
        "This conversation has no caps to give; use an empty list.".to_owned()
    } else {
        format!("What each run may use. {}.", described.join("; "))
    };

    serde_json::json!({
        "type": "array",
        "items": { "type": "string", "enum": names },
        "uniqueItems": true,
        "description": description,
    })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use infinity_agent_core::ThreadId;
    use serde_json::json;
    use shade_policy::{Cap, Limits};
    use shade_schedule::jobfiles::JobFiles;
    use shade_store::conversation::Conversations;
    use shade_store::db::Db;
    use shade_store::grants::Grants;
    use shade_store::schedules::Schedules;

    use super::{Action, Answer, Call, Work};
    use crate::authority::Authority;
    use crate::error::BoxError;
    use crate::testing::{self, Dirs};

    struct Setup {
        work: Arc<Work>,
        schedules: Schedules,
        grants: Grants,
        job_files: Arc<JobFiles>,
        _dirs: Dirs,
    }

    /// Owner chat holding `caps` and `schedules`.
    async fn setup(caps: &[Cap]) -> Result<Setup, BoxError> {
        let db = Db::in_memory()?;
        Conversations::new(db.clone()).register_root(&ThreadId::from("chat".to_owned())).await?;

        let mut caps = caps.to_vec();
        caps.push(Cap::Schedules);

        let grants = Grants::new(db.clone());
        let authority = Authority::new(grants.clone(), testing::policy(&caps, Limits::default()));
        let dirs = Dirs::new();
        let job_files = Arc::new(JobFiles::new(dirs.root.join("jobs")));
        let work = Work::new(authority, db.clone(), Arc::clone(&job_files));

        Ok(Setup { work, schedules: Schedules::new(db), grants, job_files, _dirs: dirs })
    }

    async fn call(work: &Work, thread: &str, action: Action, id: &str, args: serde_json::Value) -> Result<Answer, String> {
        let call = Call { sender: (), thread: ThreadId::from(thread.to_owned()), id: id.to_owned(), call_id: None };

        work.run(action, args, &call).await
    }

    fn create_args() -> serde_json::Value {
        json!({ "title": "Tea", "prompt": "Remind the owner about tea.", "trigger": { "kind": "cron", "expression": "0 16 * * *" }, "caps": ["now"] })
    }

    /// Starts the job's next occurrence and returns the run's root.
    async fn start_run(schedules: &Schedules, job: &str) -> Result<ThreadId, BoxError> {
        let next_at = schedules.get(job).await?.and_then(|job| job.definition.next_at).unwrap_or_default();
        schedules.claim(job.to_owned(), next_at, Some(next_at + 86_400)).await?;

        for queued in schedules.startable(4).await? {
            if let Some(input) = schedules.start(queued.run, "go".to_owned()).await? {
                return Ok(input.root);
            }
        }

        Err("no run started".into())
    }

    /// A repeated call replays its receipt, and an invalid change leaves the
    /// job exactly as it was. A scheduled run cannot manage jobs at all.
    #[tokio::test]
    async fn repeated_and_refused_calls_change_nothing() -> Result<(), BoxError> {
        let Setup { work, schedules, .. } = setup(&[Cap::Now, Cap::Notify]).await?;

        let Ok(Answer { text: receipt, shown: None }) = call(&work, "chat", Action::Create, "call-1", create_args()).await else {
            return Err("the job was not created".into());
        };

        let replay = call(&work, "chat", Action::Create, "call-1", create_args()).await;
        assert!(matches!(replay, Ok(Answer { text, shown: None }) if text == receipt));

        let jobs = schedules.list().await?;
        assert_eq!(jobs.len(), 1, "a repeated call created a second job");
        let (id, before) = (jobs[0].id.to_owned(), jobs[0].version);

        let refused = [
            json!({ "id": id, "caps": ["shell"] }),
            json!({ "id": id, "caps": ["schedules"] }),
            json!({ "id": id, "title": "Coffee", "trigger": { "kind": "once", "at": "2027-10-31T02:30", "timezone": "Europe/Paris" } }),
            json!({ "id": id, "title": " " }),
            json!({ "id": "nope", "title": "Coffee" }),
        ];

        for (n, args) in refused.into_iter().enumerate() {
            let result = call(&work, "chat", Action::Update, &format!("update-{n}"), args).await;
            assert!(result.is_err(), "update {n} was accepted");

            let after = schedules.get(&id).await?.map(|job| (job.version, job.definition.title));
            assert_eq!(after, Some((before, "Tea".to_owned())), "update {n} changed the job");
        }

        let run = start_run(&schedules, &id).await?;
        let from_run = call(&work, run.as_str(), Action::Create, "call-2", create_args()).await;
        assert!(matches!(&from_run, Err(message) if message.contains("not granted")), "{:?}", from_run.err());

        Ok(())
    }

    /// New caps reach runs started after the update, never one already running.
    #[tokio::test]
    async fn updated_caps_reach_only_later_runs() -> Result<(), BoxError> {
        let Setup { work, schedules, grants, .. } = setup(&[Cap::Now, Cap::Notify]).await?;
        call(&work, "chat", Action::Create, "call-1", create_args()).await?;
        let id = schedules.list().await?.into_iter().map(|job| job.id).next().unwrap_or_default();

        let first = start_run(&schedules, &id).await?;
        call(&work, "chat", Action::Update, "call-2", json!({ "id": id, "caps": ["notify"] })).await?;

        // The job runs one at a time: the first run ends before the next starts.
        if let Some(run) = schedules.run_of(&first).await? {
            schedules.finish(&run.id, None).await?;
        }

        let second = start_run(&schedules, &id).await?;

        for (root, expected) in [(first, "now"), (second, "notify")] {
            let caps = grants.of(&root).await?.and_then(|grant| grant.caps);
            assert_eq!(caps, Some(vec![expected.to_owned()]));
        }

        Ok(())
    }

    /// Job files are the owner's: creating or deleting one waits for their
    /// approval and writes nothing until then. Other jobs change at once,
    /// whatever their caps, and show the owner nothing.
    #[tokio::test]
    async fn job_files_wait_for_approval() -> Result<(), BoxError> {
        let Setup { work, schedules, job_files, .. } = setup(&[Cap::Now, Cap::Notify, Cap::Shell]).await?;
        let job = |caps: &[&str], file: Option<&str>| json!({ "title": "Brief", "prompt": "Summarize.", "trigger": { "kind": "cron", "expression": "0 8 * * *" }, "caps": caps, "file": file });

        let quick = call(&work, "chat", Action::Create, "call-1", job(&["now", "shell"], None)).await?;
        assert!(quick.shown.is_none(), "{}", quick.text);
        assert_eq!(schedules.list().await?.len(), 1);

        let file = call(&work, "chat", Action::Create, "call-2", job(&["now"], Some("daily"))).await?;
        assert!(file.shown.is_some(), "{}", file.text);
        assert!(schedules.get("daily").await?.is_none(), "a job file was created without approval");
        assert!(!job_files.path("daily").exists(), "a waiting change wrote its file");

        // A job file the owner wrote: deleting it waits too.
        let Some(mut definition) = schedules.list().await?.into_iter().next().map(|job| job.definition) else {
            return Err("no job".into());
        };
        definition.file = true;
        schedules.sync_files(vec!["daily".to_owned()], vec![("daily".to_owned(), definition)]).await?;

        let delete = call(&work, "chat", Action::Delete, "call-3", json!({ "id": "daily" })).await?;
        assert!(delete.shown.is_some(), "{}", delete.text);
        assert!(schedules.get("daily").await?.is_some(), "a job file was deleted without approval");

        Ok(())
    }
}
