//! `watch_create`, `watch_list`, `watch_update`, `watch_delete`: owner chat
//! manages watches on MCP resources.
//!
//! Only owner chat holding `watches` gets these, and every call checks both
//! again. Watching a server needs that server's capability, and a run's caps
//! must be ones the conversation holds, minus management. A new source is
//! read once, so a mistyped URI or pointer is refused here rather than
//! failing quietly later. Each change is stored in one transaction with its
//! receipt, so a repeated call replays it instead of changing anything twice.

use std::sync::Arc;

use async_trait::async_trait;
use chrono::Utc;
use infinity_agent_core::ThreadId;
use infinity_agent_core::tools::{Tool, ToolContext};
use infinity_agent_core::traits::InputSender;
use rap_protocol::{ProviderCallId, ToolCallId};
use serde::Deserialize;
use shade_condition::Condition;
use shade_mcp::Servers;
use shade_policy::{Cap, Caps};
use shade_store::chat::Chats;
use shade_store::db::Db;
use shade_store::grants::Grants;
use shade_store::operations::Operations;
use shade_store::roots::{Adapter, Conversation, Roots};
use shade_store::schedules::{Applied, Receipt};
use shade_store::watches::{Change, Definition, Destination, Mode, Watch, Watches};
use tokio::sync::Notify;

use crate::authority::Authority;
use crate::error::BoxError;
use crate::tools::schedules::{bounded, caps_schema, delegate, parse};
use crate::tools::{self};

const MAX_TITLE: usize = 100;
const MAX_PROMPT: usize = 4000;
const MAX_URI: usize = 1000;

/// Seconds a watch stays quiet after firing, unless told otherwise.
const DEFAULT_COOLDOWN: i64 = 300;

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
            Self::Create => "watch_create",
            Self::List => "watch_list",
            Self::Update => "watch_update",
            Self::Delete => "watch_delete",
        }
    }
}

pub struct WatchTool {
    action: Action,
    parameters: serde_json::Value,
    work: Arc<Work>,
}

/// What a running call needs, shared by the four tools and their tasks.
pub struct Work {
    authority: Authority,
    watches: Watches,
    operations: Operations,
    roots: Roots,
    grants: Grants,
    chats: Chats,
    servers: Arc<Servers>,
    /// Wakes the watcher after a change.
    changed: Arc<Notify>,
}

/// Where a result goes.
struct Call<M> {
    sender: M,
    thread: ThreadId,
    id: String,
    call_id: Option<String>,
}

/// A validated change and what the model is told about it.
struct Prepared {
    change: Change,
    said: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CreateArgs {
    title: String,
    server: String,
    uri: String,
    condition: Condition,
    prompt: String,
    /// `run` or `conversation`.
    destination: String,
    caps: Option<Vec<String>>,
    mode: Option<String>,
    cooldown_seconds: Option<i64>,
    max_age_seconds: Option<i64>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct UpdateArgs {
    id: String,
    title: Option<String>,
    uri: Option<String>,
    condition: Option<Condition>,
    prompt: Option<String>,
    caps: Option<Vec<String>>,
    mode: Option<String>,
    cooldown_seconds: Option<i64>,
    max_age_seconds: Option<i64>,
    paused: Option<bool>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct DeleteArgs {
    id: String,
}

impl Work {
    pub fn new(authority: Authority, db: Db, servers: Arc<Servers>, changed: Arc<Notify>) -> Arc<Self> {
        Arc::new(Self {
            authority,
            watches: Watches::new(db.clone()),
            operations: Operations::new(db.clone()),
            roots: Roots::new(db.clone()),
            grants: Grants::new(db.clone()),
            chats: Chats::new(db),
            servers,
            changed,
        })
    }
}

impl WatchTool {
    /// The tool for `action`, described for a conversation holding `caps`.
    pub fn new(action: Action, work: &Arc<Work>, caps: &Caps) -> Self {
        let delegable = shade_policy::delegable(caps);
        let parameters = parameters(action, caps_schema(&delegable, work.authority.policy()));

        Self { action, parameters, work: Arc::clone(work) }
    }
}

#[async_trait]
impl<M: InputSender + 'static> Tool<M> for WatchTool {
    fn name(&self) -> &str {
        self.action.name()
    }

    fn description(&self) -> &str {
        describe(self.action)
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
        let text = match self.run(action, args, &call).await {
            Ok(text) => text,
            Err(message) => format!("Error: {message}"),
        };
        let text = self.authority.redact(text);

        if let Err(error) = tools::deliver(&call.sender, call.thread, call.id, call.call_id, text).await {
            tracing::warn!(%error, "cannot deliver a watch tool result; restart recovery will");
        }
    }

    async fn run<M>(&self, action: Action, args: serde_json::Value, call: &Call<M>) -> Result<String, String> {
        let deadline = match self.authority.deadline(&call.thread).await {
            Ok(deadline) => deadline,
            Err(error) => return Err(error.to_string()),
        };

        match deadline.run(self.apply(action, args, call)).await {
            Ok(result) => result,
            Err(error) => Err(format!("{error}. {}", crate::recovery::UNKNOWN_OUTCOME)),
        }
    }

    async fn apply<M>(&self, action: Action, args: serde_json::Value, call: &Call<M>) -> Result<String, String> {
        let authorized = match self.authority.admit_management(&call.thread, &Cap::Watches).await {
            Ok(authorized) => authorized,
            Err(denied) => return Err(denied.to_string()),
        };

        if action == Action::List {
            return self.list().await;
        }

        let thread = call.thread.as_str();

        // A repeated call is answered before its arguments are checked again.
        match self.operations.captured(thread, &call.id).await {
            Ok(Some(result)) => return Ok(result),
            Ok(None) => {}
            Err(error) => return Err(format!("cannot read earlier results: {error}")),
        }

        let Prepared { change, said } = self.prepare(action, args, &authorized.caps, &call.thread).await?;
        let receipt = Receipt { thread: thread.to_owned(), call: call.id.to_owned(), tool: action.name(), text: said };

        let applied = match self.watches.apply(change, receipt).await {
            Ok(applied) => applied,
            Err(error) => return Err(format!("nothing was changed because the change could not be stored: {error}")),
        };

        self.changed.notify_one();

        match applied {
            Applied::Done(text) | Applied::Refused(text) | Applied::Replayed(text) => Ok(text),
        }
    }

    async fn prepare(&self, action: Action, args: serde_json::Value, held: &Caps, thread: &ThreadId) -> Result<Prepared, String> {
        match action {
            Action::Create => self.create(parse(args)?, held, thread).await,
            Action::Update => self.update(parse(args)?, held).await,
            Action::Delete => Ok(delete(parse(args)?)),
            Action::List => Err("watch_list changes nothing".to_owned()),
        }
    }

    async fn list(&self) -> Result<String, String> {
        match self.watches.list().await {
            Ok(watches) => Ok(shade_watch::list(&watches)),
            Err(error) => Err(format!("cannot read watches: {error}")),
        }
    }

    async fn create(&self, args: CreateArgs, held: &Caps, thread: &ThreadId) -> Result<Prepared, String> {
        let Some(destination) = Destination::parse(&args.destination) else {
            return Err(format!("unknown destination `{}`", args.destination));
        };

        let caps = match (destination, args.caps) {
            (Destination::Run, Some(caps)) => delegate(caps, held)?,
            (Destination::Run, None) => return Err("a `run` watch needs `caps`".to_owned()),
            (Destination::Conversation, None) => Vec::new(),
            (Destination::Conversation, Some(_)) => {
                return Err("a `conversation` watch uses this conversation's caps; drop `caps`".to_owned());
            }
        };

        if destination == Destination::Conversation {
            self.reachable(thread).await?;
        }

        let definition = Definition {
            title: bounded("title", args.title, MAX_TITLE)?,
            server: args.server,
            uri: bounded("uri", args.uri, MAX_URI)?,
            condition: condition_text(&args.condition)?,
            mode: mode(args.mode.as_deref())?,
            cooldown_seconds: cooldown(args.cooldown_seconds)?,
            max_age_seconds: max_age(args.max_age_seconds)?,
            prompt: bounded("prompt", args.prompt, MAX_PROMPT)?,
            caps,
            destination,
        };

        let matches = self.probe(&definition, held).await?;
        let creator_root = self.root_of(thread).await?;
        let id = new_id();
        let said = said(&id, &definition, matches);

        Ok(Prepared { change: Change::Create { id, creator_root, definition }, said })
    }

    async fn update(&self, args: UpdateArgs, held: &Caps) -> Result<Prepared, String> {
        let current = match self.watches.get(&args.id).await {
            Ok(Some(watch)) => watch,
            Ok(None) => return Err(format!("no watch {}", args.id)),
            Err(error) => return Err(format!("cannot read watches: {error}")),
        };

        let Watch { version, enabled, definition, .. } = current;
        let enabled = args.paused.map_or(enabled, |paused| !paused);
        let reread = args.uri.is_some() || args.condition.is_some() || args.max_age_seconds.is_some();

        if args.caps.is_some() && definition.destination == Destination::Conversation {
            return Err("a `conversation` watch has no caps of its own".to_owned());
        }

        let merged = Definition {
            title: optional("title", args.title, MAX_TITLE, definition.title)?,
            uri: optional("uri", args.uri, MAX_URI, definition.uri)?,
            condition: match &args.condition {
                Some(condition) => condition_text(condition)?,
                None => definition.condition,
            },
            mode: match args.mode.as_deref() {
                Some(_) => mode(args.mode.as_deref())?,
                None => definition.mode,
            },
            cooldown_seconds: match args.cooldown_seconds {
                Some(_) => cooldown(args.cooldown_seconds)?,
                None => definition.cooldown_seconds,
            },
            max_age_seconds: match args.max_age_seconds {
                Some(_) => max_age(args.max_age_seconds)?,
                None => definition.max_age_seconds,
            },
            prompt: optional("prompt", args.prompt, MAX_PROMPT, definition.prompt)?,
            caps: match args.caps {
                Some(caps) => delegate(caps, held)?,
                None => definition.caps,
            },
            server: definition.server,
            destination: definition.destination,
        };

        let said = match reread {
            true => said(&args.id, &merged, self.probe(&merged, held).await?),
            false => format!("Updated watch {}. Confirm it to the owner in one short sentence.", args.id),
        };

        Ok(Prepared { change: Change::Update { id: args.id, version, enabled, definition: merged }, said })
    }

    /// Whether the condition holds now, once the source is known to be
    /// watchable: held by this conversation, subscribable, and readable.
    async fn probe(&self, definition: &Definition, held: &Caps) -> Result<bool, String> {
        let name = &definition.server;

        if !held.contains(&Cap::Server(name.to_owned())) {
            return Err(format!("this conversation does not hold `{name}`, so it cannot watch that server"));
        }

        let Some(server) = self.servers.get(name) else {
            return Err(format!("no server `{name}`"));
        };

        server.connect().await?;

        if !server.features().await.subscribe {
            return Err(format!("the {name} server does not offer resource subscriptions, so it cannot be watched"));
        }

        let document = server.snapshot(&definition.uri).await?;

        shade_watch::evaluate(&document, &definition.condition, definition.max_age_seconds, Utc::now().timestamp())
    }

    /// A conversation watch needs someone to read its turn. The terminal has
    /// no live client: the reply would stay unseen in history.
    async fn reachable(&self, thread: &ThreadId) -> Result<(), String> {
        let api = matches!(self.grants.of(thread).await, Ok(Some(grant)) if grant.source == "api");

        let discord = match self.roots.conversation_of(thread).await {
            Ok(conversation) => matches!(conversation, Some(Conversation { adapter: Adapter::Discord, .. })),
            Err(error) => return Err(format!("cannot find this conversation: {error}")),
        };

        if api || discord {
            return Ok(());
        }

        Err("this terminal conversation has no live reader; use a `run` watch that notifies the owner".to_owned())
    }

    async fn root_of(&self, thread: &ThreadId) -> Result<String, String> {
        match self.chats.owner_root(thread.as_str().to_owned()).await {
            Ok(root) => Ok(root),
            Err(error) => Err(format!("cannot find this conversation: {error}")),
        }
    }
}

fn delete(args: DeleteArgs) -> Prepared {
    let said = format!("Deleted watch {}. Confirm it to the owner in one short sentence.", args.id);

    Prepared { change: Change::Delete { id: args.id }, said }
}

/// What the model is told: the first reading never fires, so it must know
/// whether the condition already holds.
fn said(id: &str, definition: &Definition, matches: bool) -> String {
    let now = match matches {
        true => "The condition holds right now. That does not fire: it fires the next time it turns true after being false.",
        false => "The condition does not hold right now, and fires when it turns true.",
    };
    let then = match definition.destination {
        Destination::Run => "A background run starts when it fires.",
        Destination::Conversation => "A message arrives in this conversation when it fires, and the watch then ends.",
    };

    format!(
        "Watching {} (watch {id}). {now} {then} Confirm it to the owner in one short sentence.",
        definition.uri
    )
}

fn condition_text(condition: &Condition) -> Result<String, String> {
    condition.validate()?;

    match serde_json::to_string(condition) {
        Ok(text) => Ok(text),
        Err(error) => Err(format!("cannot store the condition: {error}")),
    }
}

fn mode(name: Option<&str>) -> Result<Mode, String> {
    let name = name.unwrap_or(Mode::OnChange.name());

    match Mode::parse(name) {
        Some(mode) => Ok(mode),
        None => Err(format!("unknown mode `{name}`")),
    }
}

fn cooldown(seconds: Option<i64>) -> Result<i64, String> {
    match seconds {
        Some(seconds) if seconds < 0 => Err("cooldown_seconds cannot be negative".to_owned()),
        Some(seconds) => Ok(seconds),
        None => Ok(DEFAULT_COOLDOWN),
    }
}

fn max_age(seconds: Option<i64>) -> Result<Option<i64>, String> {
    match seconds {
        Some(seconds) if seconds <= 0 => Err("max_age_seconds must be positive".to_owned()),
        other => Ok(other),
    }
}

fn optional(field: &str, text: Option<String>, max: usize, current: String) -> Result<String, String> {
    match text {
        Some(text) => bounded(field, text, max),
        None => Ok(current),
    }
}

fn new_id() -> String {
    let mut id = uuid::Uuid::new_v4().simple().to_string();
    id.truncate(8);

    id
}

fn describe(action: Action) -> &'static str {
    match action {
        Action::Create => {
            "Watch an MCP resource and act when a condition on its JSON turns true. No model runs until then. \
             Use destination `conversation` for 'tell me here when X': one message in this conversation, then the watch ends. \
             Use destination `run` for standing work: each firing starts a background run holding `caps`, which reaches the \
             owner only through `notify`. The first reading never fires, even if it already matches. Values that come and \
             go between reads are not seen. List a server's resources first to find the URI."
        }
        Action::List => "List watches: what each observes, whether it matches, and whether it is failing.",
        Action::Update => {
            "Change a watch, or pause it. Changing the URI or condition starts a new baseline, so that change never fires by \
             itself. To change its server or destination, delete it and create another."
        }
        Action::Delete => "Delete a watch. A run it already fired still finishes.",
    }
}

fn parameters(action: Action, caps: serde_json::Value) -> serde_json::Value {
    let id = serde_json::json!({ "type": "string", "description": "The watch's ID, from watch_list." });

    match action {
        Action::List => serde_json::json!({ "type": "object", "properties": {} }),
        Action::Delete => serde_json::json!({ "type": "object", "properties": { "id": id }, "required": ["id"] }),
        Action::Create => create_parameters(caps),
        Action::Update => serde_json::json!({
            "type": "object",
            "properties": {
                "id": id,
                "title": { "type": "string" },
                "uri": { "type": "string" },
                "condition": condition_schema(),
                "prompt": { "type": "string" },
                "caps": caps,
                "mode": mode_schema(),
                "cooldown_seconds": { "type": "integer", "minimum": 0 },
                "max_age_seconds": { "type": "integer", "minimum": 1 },
                "paused": { "type": "boolean" },
            },
            "required": ["id"],
        }),
    }
}

fn create_parameters(caps: serde_json::Value) -> serde_json::Value {
    serde_json::json!({
        "type": "object",
        "properties": {
            "title": { "type": "string", "description": "A short name for the watch." },
            "server": { "type": "string", "description": "The MCP server's name." },
            "uri": { "type": "string", "description": "The resource's URI." },
            "condition": condition_schema(),
            "prompt": { "type": "string", "description": "What to do when it fires." },
            "destination": { "type": "string", "enum": ["run", "conversation"] },
            "caps": caps,
            "mode": mode_schema(),
            "cooldown_seconds": {
                "type": "integer",
                "minimum": 0,
                "description": "Seconds after firing during which it does not fire again. 300 by default.",
            },
            "max_age_seconds": {
                "type": "integer",
                "minimum": 1,
                "description": "For a resource with `observed_at` and `availability` fields: the oldest value to trust.",
            },
        },
        "required": ["title", "server", "uri", "condition", "prompt", "destination"],
    })
}

fn condition_schema() -> serde_json::Value {
    serde_json::json!({
        "type": "object",
        "description": "Compares one value of the resource's JSON with a constant.",
        "properties": {
            "pointer": { "type": "string", "description": "RFC 6901 JSON pointer. Empty means the whole document." },
            "op": { "type": "string", "enum": ["==", "!=", ">", ">=", "<", "<="] },
            "value": { "description": "A number, string, boolean or null. Ordering compares numbers only." },
        },
        "required": ["op", "value"],
    })
}

fn mode_schema() -> serde_json::Value {
    serde_json::json!({
        "type": "string",
        "enum": ["on_change", "every_match"],
        "description": "`on_change`, the default, fires when the condition turns true. `every_match` fires on each change that matches.",
    })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use infinity_agent_core::ThreadId;
    use serde_json::{Value, json};
    use shade_mcp::Servers;
    use shade_mcp::testing::Feed;
    use shade_policy::{Cap, Limits};
    use shade_store::chat::{Chats, Enqueue};
    use shade_store::conversation::Conversations;
    use shade_store::db::Db;
    use shade_store::grants::Grants;
    use shade_store::roots::{Adapter, Target};
    use shade_store::watches::{Destination, Observation, Watches};

    use super::{Action, Call, Work};
    use crate::authority::Authority;
    use crate::error::BoxError;
    use crate::testing;

    const URI: &str = "feed://price";

    struct Setup {
        work: Arc<Work>,
        watches: Watches,
        feed: Feed,
        /// A conversation on the API, which has a live reader.
        api: String,
        /// A terminal conversation, which does not.
        terminal: String,
    }

    async fn setup(caps: &[Cap]) -> Result<Setup, BoxError> {
        let db = Db::in_memory()?;
        let feed = Feed::default();
        feed.set(URI, r#"{"price":90}"#).await;

        let servers = Arc::new(Servers::new(vec![feed.connect("feed").await?]));
        let authority = Authority::new(Grants::new(db.clone()), testing::policy(caps, Limits::default()));
        let work = Work::new(authority, db.clone(), servers, Default::default());

        let api = api_conversation(&db).await?;
        let terminal = "terminal".to_owned();
        Conversations::new(db.clone()).register_root(&ThreadId::from(terminal.to_owned())).await?;

        Ok(Setup { work, watches: Watches::new(db), feed, api, terminal })
    }

    /// An API conversation in the middle of a turn, as a tool call finds it.
    async fn api_conversation(db: &Db) -> Result<String, BoxError> {
        let chats = Chats::new(db.clone());
        let root = chats.create(None).await?.id;

        let turn = Enqueue {
            adapter: Adapter::Api,
            target: Target::Existing(root.to_owned()),
            text: "watch this".to_owned(),
            source_key: None,
            delivery: "api".to_owned(),
            client_message_id: None,
            per_conversation: 1,
            total: 1,
        };

        chats.enqueue(turn).await?;
        chats.claim().await?;

        Ok(root)
    }

    async fn call(work: &Work, thread: &str, action: Action, id: &str, args: Value) -> Result<String, String> {
        let call = Call { sender: (), thread: ThreadId::from(thread.to_owned()), id: id.to_owned(), call_id: None };

        work.run(action, args, &call).await
    }

    fn held() -> [Cap; 3] {
        [Cap::Watches, Cap::Notify, Cap::Server("feed".to_owned())]
    }

    fn run_args() -> Value {
        json!({
            "title": "Price",
            "server": "feed",
            "uri": URI,
            "condition": { "pointer": "/price", "op": ">", "value": 100 },
            "prompt": "Tell me.",
            "destination": "run",
            "caps": ["notify"],
        })
    }

    fn with(key: &str, value: Value) -> Value {
        let mut args = run_args();
        args[key] = value;

        args
    }

    fn without_caps(mut args: Value) -> Value {
        if let Some(fields) = args.as_object_mut() {
            fields.remove("caps");
        }

        args
    }

    /// Each of these would otherwise store a watch that can never work, or
    /// one holding more than the conversation that made it.
    #[tokio::test]
    async fn bad_watches_are_refused_when_created() -> Result<(), BoxError> {
        let setup = setup(&held()).await?;

        let cases = [
            (with("uri", json!("feed://typo")), "no feed://typo"),
            (with("condition", json!({ "pointer": "/missing", "op": ">", "value": 1 })), "nothing at"),
            (with("condition", json!({ "pointer": "/price", "op": ">", "value": "a" })), "compares numbers"),
            (with("server", json!("other")), "does not hold `other`"),
            (with("caps", json!(["schedules"])), "cannot give a job"),
            (with("caps", json!(["watches"])), "cannot give a job"),
            (with("caps", Value::Null), "needs `caps`"),
            (with("destination", json!("conversation")), "drop `caps`"),
            (with("destination", json!("elsewhere")), "unknown destination"),
            (with("cooldown_seconds", json!(-1)), "negative"),
        ];

        for (index, (args, expected)) in cases.into_iter().enumerate() {
            let refused = call(&setup.work, &setup.api, Action::Create, &format!("call-{index}"), args).await;

            assert!(refused.as_ref().is_err_and(|message| message.contains(expected)), "{expected}: {refused:?}");
        }

        assert!(setup.watches.list().await?.is_empty());

        Ok(())
    }

    /// A server the conversation does not hold is not even read.
    #[tokio::test]
    async fn watching_needs_the_servers_capability() -> Result<(), BoxError> {
        let setup = setup(&[Cap::Watches, Cap::Notify]).await?;

        let refused = call(&setup.work, &setup.api, Action::Create, "call", run_args()).await;

        assert!(refused.is_err_and(|message| message.contains("does not hold `feed`")));
        assert_eq!(setup.feed.reads(), 0);

        Ok(())
    }

    /// The terminal has no live reader, so only a `run` watch makes sense there.
    #[tokio::test]
    async fn conversation_watches_need_a_reader() -> Result<(), BoxError> {
        let setup = setup(&held()).await?;
        let args = with("destination", json!("conversation"));
        let args = without_caps(args);

        let refused = call(&setup.work, &setup.terminal, Action::Create, "call-1", args.to_owned()).await;
        assert!(refused.is_err_and(|message| message.contains("no live reader")));

        call(&setup.work, &setup.terminal, Action::Create, "call-2", run_args()).await?;
        call(&setup.work, &setup.api, Action::Create, "call-3", args).await?;

        let stored = setup.watches.list().await?;
        let created: Vec<(Destination, Option<&str>)> =
            stored.iter().map(|watch| (watch.definition.destination, watch.creator_root.as_deref())).collect();

        assert_eq!(
            created,
            [(Destination::Run, Some("terminal")), (Destination::Conversation, Some(setup.api.as_str()))]
        );

        Ok(())
    }

    /// A call dispatched again gets its first answer, and no second watch.
    #[tokio::test]
    async fn repeated_calls_change_nothing() -> Result<(), BoxError> {
        let setup = setup(&held()).await?;

        let first = call(&setup.work, &setup.api, Action::Create, "call", run_args()).await?;
        let second = call(&setup.work, &setup.api, Action::Create, "call", run_args()).await?;

        assert_eq!(first, second);
        assert_eq!(setup.watches.list().await?.len(), 1);

        Ok(())
    }

    /// The model is told when the first reading already matches, because
    /// nothing will fire for it.
    #[tokio::test]
    async fn the_model_learns_a_match_will_not_fire() -> Result<(), BoxError> {
        let setup = setup(&held()).await?;
        setup.feed.set(URI, r#"{"price":110}"#).await;

        let said = call(&setup.work, &setup.api, Action::Create, "call", run_args()).await?;

        assert!(said.contains("does not fire"), "{said}");

        Ok(())
    }

    /// Changing what is watched starts over; changing the prompt does not.
    #[tokio::test]
    async fn only_a_new_source_loses_the_baseline() -> Result<(), BoxError> {
        let setup = setup(&held()).await?;
        call(&setup.work, &setup.api, Action::Create, "create", run_args()).await?;

        let watch = setup.watches.list().await?.pop().ok_or("no watch")?;
        let observation =
            Observation { watch: watch.id.to_owned(), version: 1, condition: false, value: "90".to_owned(), fired: None, at: 1000 };
        setup.watches.observe(observation).await?;

        call(&setup.work, &setup.api, Action::Update, "prompt", json!({ "id": watch.id, "prompt": "New." })).await?;
        assert_eq!(setup.watches.get(&watch.id).await?.ok_or("gone")?.last_condition, Some(false));

        let condition = json!({ "pointer": "/price", "op": ">", "value": 200 });
        call(
            &setup.work,
            &setup.api,
            Action::Update,
            "condition",
            json!({ "id": watch.id, "condition": condition }),
        )
        .await?;
        assert_eq!(setup.watches.get(&watch.id).await?.ok_or("gone")?.last_condition, None);

        Ok(())
    }

    /// Managing watches stays with owner chat: a run a watch fired cannot
    /// reach these tools, even with a grant naming `watches`.
    #[tokio::test]
    async fn runs_cannot_manage_watches() -> Result<(), BoxError> {
        let setup = setup(&held()).await?;
        setup.watches.add("w", &setup.api, Destination::Run).await?;

        let observation = Observation {
            watch: "w".to_owned(),
            version: 1,
            condition: true,
            value: "1".to_owned(),
            fired: Some("go".to_owned()),
            at: 1000,
        };
        setup.watches.observe(observation).await?;

        let queued = setup.watches.startable(4).await?.pop().ok_or("no run")?;
        let input = setup.watches.start(queued.run).await?.ok_or("not started")?;

        let refused = call(&setup.work, input.root.as_str(), Action::List, "call", json!({})).await;

        assert!(refused.as_ref().is_err_and(|message| message.contains("`watches` is not granted")), "{refused:?}");

        Ok(())
    }
}
