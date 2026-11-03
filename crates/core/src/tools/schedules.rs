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

use crate::authority::Authority;
use crate::error::BoxError;
use crate::policy::{self, Cap, Caps};
use crate::schedule::{self, TriggerArgs};
use crate::store::operations::Operations;
use crate::store::schedules::{Applied, Change, Definition, Receipt, Schedules};
use crate::tools;

const MAX_TITLE: usize = 100;
const MAX_PROMPT: usize = 4000;

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
}

/// A call's result, and Shade's receipt for the owner when it changed something.
struct Answer {
    text: String,
    shown: Option<String>,
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
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct UpdateArgs {
    id: String,
    title: Option<String>,
    prompt: Option<String>,
    trigger: Option<TriggerArgs>,
    caps: Option<Vec<String>>,
    paused: Option<bool>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct DeleteArgs {
    id: String,
}

impl Work {
    pub fn new(authority: Authority, schedules: Schedules, operations: Operations) -> Arc<Self> {
        Arc::new(Self { authority, schedules, operations })
    }
}

impl ScheduleTool {
    /// The tool for `action`, described for a conversation holding `caps`.
    pub fn new(action: Action, work: &Arc<Work>, caps: &Caps) -> Self {
        let delegable = policy::delegable(caps);
        let timezone = work.authority.policy().timezone.name();

        Self { action, description: describe(action), parameters: parameters(action, &delegable, timezone), work: Arc::clone(work) }
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
        let authorized = match self.authority.admit_management(&call.thread, Cap::Schedules).await {
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

        let (change, text) = self.change(action, args, &authorized.caps).await?;

        let receipt = Receipt { thread: thread.to_owned(), call: call.id.to_owned(), tool: action.name(), text };

        let applied = match self.schedules.apply(change, receipt).await {
            Ok(applied) => applied,
            Err(error) => return Err(format!("nothing was changed because the change could not be stored: {error}")),
        };

        let answer = match applied {
            Applied::Done(receipt) => Answer { text: receipt.to_owned(), shown: Some(receipt) },
            Applied::Refused(text) | Applied::Replayed(text) => Answer { text, shown: None },
        };

        Ok(answer)
    }

    /// The change a call asks for, validated, with its receipt.
    async fn change(&self, action: Action, args: serde_json::Value, held: &Caps) -> Result<(Change, String), String> {
        let now = Utc::now();

        match action {
            Action::Create => self.create(parse(args)?, held, now),
            Action::Update => self.update(parse(args)?, held, now).await,
            Action::Delete => self.delete(parse(args)?).await,
            Action::List => Err("schedule_list changes nothing".to_owned()),
        }
    }

    async fn list(&self) -> Result<String, String> {
        match self.schedules.list().await {
            Ok(jobs) => Ok(schedule::list(&jobs)),
            Err(error) => Err(format!("cannot read jobs: {error}")),
        }
    }

    fn create(&self, args: CreateArgs, held: &Caps, now: DateTime<Utc>) -> Result<(Change, String), String> {
        let timing = schedule::timing(args.trigger, self.authority.policy().timezone, now)?;

        let definition = Definition {
            title: bounded("title", args.title, MAX_TITLE)?,
            kind: timing.kind.to_owned(),
            spec: timing.spec,
            timezone: timing.timezone.name().to_owned(),
            prompt: bounded("prompt", args.prompt, MAX_PROMPT)?,
            caps: delegate(args.caps, held)?,
            enabled: true,
            next_at: Some(timing.next_at.timestamp()),
        };

        let mut id = uuid::Uuid::new_v4().simple().to_string();
        id.truncate(8);

        let text = format!("📅 Created scheduled job {}", schedule::describe(&id, &definition));

        Ok((Change::Create { id, definition }, text))
    }

    /// Changes only what is given. A new trigger counts from now and
    /// re-enables the job; nothing missed in the past is run.
    async fn update(&self, args: UpdateArgs, held: &Caps, now: DateTime<Utc>) -> Result<(Change, String), String> {
        let job = match self.schedules.get(&args.id).await {
            Ok(Some(job)) => job,
            Ok(None) => return Err(format!("no job {}", args.id)),
            Err(error) => return Err(format!("cannot read job {}: {error}", args.id)),
        };

        let mut definition = job.definition;

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
            let timing = schedule::timing(trigger, self.authority.policy().timezone, now)?;
            definition.kind = timing.kind.to_owned();
            definition.spec = timing.spec;
            definition.timezone = timing.timezone.name().to_owned();
            definition.next_at = Some(timing.next_at.timestamp());
            definition.enabled = true;
        } else if args.paused == Some(false) && !definition.enabled {
            definition.next_at = Some(schedule::resumed_next(&definition, now)?);
        }

        if let Some(paused) = args.paused {
            definition.enabled = !paused;
        }

        let text = format!("📅 Updated scheduled job {}", schedule::describe(&job.id, &definition));

        Ok((Change::Update { id: job.id, version: job.version, definition }, text))
    }

    async fn delete(&self, args: DeleteArgs) -> Result<(Change, String), String> {
        let job = match self.schedules.get(&args.id).await {
            Ok(Some(job)) => job,
            Ok(None) => return Err(format!("no job {}", args.id)),
            Err(error) => return Err(format!("cannot read job {}: {error}", args.id)),
        };

        let text = format!(
            "🗑️ Deleted scheduled job \"{}\" ({}). A run already queued or running still finishes.",
            job.definition.title, job.id
        );

        Ok((Change::Delete { id: job.id }, text))
    }
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
    let delegable = policy::delegable(held);
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
             reminders need the notify cap. The owner is shown a receipt."
        }
        Action::List => "List scheduled jobs with their triggers, next runs, caps, prompts, and last runs.",
        Action::Update => {
            "Change a scheduled job; omitted fields stay as they are. A new trigger counts from now and resumes the job. \
             paused stops or resumes runs. Runs already started keep their old definition. The owner is shown a receipt."
        }
        Action::Delete => "Delete a scheduled job. Its run history is kept. The owner is shown a receipt.",
    };

    text.to_owned()
}

fn parameters(action: Action, delegable: &Caps, timezone: &str) -> serde_json::Value {
    let id = serde_json::json!({ "type": "string", "description": "The job's ID, from its receipt or schedule_list." });

    match action {
        Action::Create => serde_json::json!({
            "type": "object",
            "properties": {
                "title": { "type": "string", "maxLength": MAX_TITLE },
                "prompt": { "type": "string", "maxLength": MAX_PROMPT, "description": "What each run should do, self-contained." },
                "trigger": trigger(timezone),
                "caps": caps(delegable),
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
                "caps": caps(delegable),
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

fn caps(delegable: &Caps) -> serde_json::Value {
    let names: Vec<&str> = delegable.iter().map(|cap| cap.name()).collect();
    let described: Vec<String> = delegable.iter().map(|cap| format!("{}: {}", cap.name(), cap.description())).collect();

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

    use super::{Action, Answer, Call, Work};
    use crate::authority::Authority;
    use crate::error::BoxError;
    use crate::policy::{Cap, Limits};
    use crate::store::conversation::Conversations;
    use crate::store::db::Db;
    use crate::store::grants::Grants;
    use crate::store::operations::Operations;
    use crate::store::schedules::Schedules;
    use crate::testing;

    struct Setup {
        work: Arc<Work>,
        schedules: Schedules,
        grants: Grants,
    }

    async fn setup() -> Result<Setup, BoxError> {
        let db = Db::in_memory()?;
        Conversations::new(db.clone()).register_root(&ThreadId::from("chat".to_owned())).await?;

        let grants = Grants::new(db.clone());
        let policy = testing::policy(&[Cap::Now, Cap::Notify, Cap::Schedules], Limits::default());
        let authority = Authority::new(grants.clone(), policy);
        let schedules = Schedules::new(db.clone());
        let work = Work::new(authority, schedules.clone(), Operations::new(db));

        Ok(Setup { work, schedules, grants })
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
        let Setup { work, schedules, .. } = setup().await?;

        let Ok(Answer { text: receipt, shown: Some(_) }) = call(&work, "chat", Action::Create, "call-1", create_args()).await else {
            return Err("the job was not created".into());
        };

        // The replay is not shown to the owner again.
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
        let Setup { work, schedules, grants } = setup().await?;
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
}
