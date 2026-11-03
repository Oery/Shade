//! Per-thread configuration resolved by Infinity on each thread load.
//!
//! The root's grant decides which tools a thread gets. Resolution fails for
//! threads without one (unregistered, ungranted, or whose hook was removed),
//! so Infinity's habit of loading any ID it is sent cannot reach the model.
//!
//! Built-in tools are off (spawning has no limit hook), but Infinity's
//! auto-compaction child must call `close_thread` to hand back its summary.
//! Compaction threads therefore get that one tool and nothing else.

use std::rc::Rc;
use std::sync::Arc;

use async_trait::async_trait;
use infinity_agent_core::ThreadId;
use infinity_agent_core::system::local::ChannelSender;
use infinity_agent_core::system::{NoRapHttp, ThreadConfig, ThreadConfigSource};
use infinity_agent_core::tools::thread::CloseThreadTool;
use infinity_agent_core::traits::ConversationStore;
use shade_mcp::Servers;
use shade_policy::{Cap, Caps, Origin};
use shade_sandbox::Sandbox;
use shade_schedule::jobfiles::JobFiles;
use shade_store::conversation::Conversations;
use shade_store::db::Db;
use shade_store::operations::Operations;
use shade_store::roots::{Adapter, Conversation, Roots};

use crate::authority::Authority;
use crate::error::BoxError;
use crate::notify::Notifier;
use crate::tools::mcp::McpTool;
use crate::tools::notify::Notify;
use crate::tools::now::Now;
use crate::tools::schedules::{self, Action, ScheduleTool};
use crate::tools::shell::Shell;

/// Owner chat, always; the owner's `[chat] prompt` follows and can change the style.
const CHAT_PROMPT: &str = "You are chatting with your owner, on Discord or in a terminal. Keep replies short: answer in a \
                           sentence or two, and offer details, options or caveats only when asked. When a tool result \
                           says the owner now sees an approval card, end your turn without writing: nothing more is \
                           shown to them, and you will be told their decision.";

/// Background runs have no reader: only `notify` reaches the owner.
const JOBS_PROMPT: &str = "You are running a background job for your owner, started by a schedule. Nobody reads your \
                           replies. If the result needs the owner's attention, send it with the notify tool; otherwise \
                           finish without notifying.";

/// What tools and the router reach besides the database.
#[derive(Clone)]
pub struct Services {
    /// Present whenever some origin holds `shell`.
    pub sandbox: Option<Arc<Sandbox>>,
    /// Present when an adapter can reach the owner. `notify` is offered without one and says so.
    pub notifier: Option<Notifier>,
    pub job_files: Arc<JobFiles>,
    /// Configured MCP servers, running or not.
    pub mcp: Arc<Servers>,
}

pub struct ThreadTools {
    conversations: Conversations,
    authority: Authority,
    operations: Operations,
    services: Services,
    db: Db,
}

impl ThreadTools {
    pub fn new(conversations: Conversations, authority: Authority, db: Db, services: Services) -> Self {
        Self { conversations, authority, operations: Operations::new(db.clone()), services, db }
    }
}

impl ThreadTools {
    /// The tools of every granted MCP server that has started. A server that
    /// is down offers nothing until it starts.
    async fn add_mcp_tools(&self, caps: &Caps, config: &mut ThreadConfig<ChannelSender, NoRapHttp>) {
        for cap in caps {
            let Cap::Server(name) = cap else {
                continue;
            };

            let Some(server) = self.services.mcp.get(name) else {
                continue;
            };

            for tool in server.tools().await {
                if let Some(tool) = McpTool::new(self.authority.clone(), self.operations.clone(), server, tool) {
                    config.tools.push(Rc::new(tool));
                }
            }
        }
    }

    /// Where a chat conversation takes place, so the model knows who can read it.
    async fn place(&self, thread: &ThreadId<str>) -> Result<&'static str, BoxError> {
        let place = match Roots::new(self.db.clone()).conversation_of(thread).await? {
            Some(Conversation { adapter: Adapter::Discord, direct: true, .. }) => {
                "This conversation is the owner's Discord DMs: only they read it, and `dm` is this place."
            }
            Some(Conversation { adapter: Adapter::Discord, direct: false, .. }) => {
                "This conversation is a Discord server channel: everyone in it can read your replies."
            }
            _ => "This conversation is in the owner's terminal; notifications reach them on Discord.",
        };

        Ok(place)
    }
}

#[async_trait(?Send)]
impl ThreadConfigSource<ChannelSender, NoRapHttp> for ThreadTools {
    async fn resolve(&self, thread_id: &ThreadId<str>) -> Result<ThreadConfig<ChannelSender, NoRapHttp>, BoxError> {
        let authorized = self.authority.authorize(thread_id).await?;
        let mut config = ThreadConfig { tools: Vec::new(), extra_system_prompt: None, rap_notifier: None };

        if self.conversations.is_compaction_thread(thread_id).await? {
            let close = CloseThreadTool::<_, NoRapHttp> { conversation_store: self.conversations.clone(), rap_notifier: None };
            config.tools.push(Rc::new(close));

            return Ok(config);
        }

        let policy = self.authority.policy();

        if authorized.caps.contains(&Cap::Now) {
            config.tools.push(Rc::new(Now::new(self.authority.clone(), policy.timezone)));
        }

        if authorized.caps.contains(&Cap::Notify) {
            let notify = Notify::new(self.authority.clone(), self.db.clone(), self.services.notifier.clone());
            config.tools.push(Rc::new(notify));
        }

        // Management stays with owner chat even if a grant somehow held it.
        if authorized.caps.contains(&Cap::Schedules) && authorized.origin == Origin::Chat {
            let work = schedules::Work::new(self.authority.clone(), self.db.clone(), Arc::clone(&self.services.job_files));

            for action in Action::ALL {
                config.tools.push(Rc::new(ScheduleTool::new(action, &work, &authorized.caps)));
            }
        }

        self.add_mcp_tools(&authorized.caps, &mut config).await;

        if authorized.caps.contains(&Cap::Shell)
            && let Some(sandbox) = &self.services.sandbox
        {
            let network = authorized.caps.contains(&Cap::ShellNetwork);
            let shell = Shell::new(Arc::clone(sandbox), self.authority.clone(), self.operations.clone(), network);
            config.tools.push(Rc::new(shell));
        }

        config.extra_system_prompt = match authorized.origin {
            Origin::Chat => {
                let place = self.place(thread_id).await?;

                match &policy.chat_prompt {
                    Some(prompt) => Some(format!("{CHAT_PROMPT} {place}\n\n{prompt}")),
                    None => Some(format!("{CHAT_PROMPT} {place}")),
                }
            }
            Origin::Schedule | Origin::Watch => Some(policy.jobs_prompt.as_deref().unwrap_or(JOBS_PROMPT).to_owned()),
            Origin::Webhook => None,
        };

        Ok(config)
    }
}

#[cfg(test)]
mod tests {
    use infinity_agent_core::ThreadId;
    use infinity_agent_core::message::{InputMessage, InputMessageContent, SyntheticKind, TaggedSyntheticKind};
    use infinity_agent_core::system::AgentEvent;
    use infinity_agent_core::traits::{ConversationStore, InputSender};
    use infinity_provider_protocol::CompletionRequest;
    use infinity_provider_protocol::message::UserContent;
    use shade_store::db::Db;

    use crate::error::BoxError;
    use crate::testing::{self, attach, reaches_model, reply, until, within};

    fn offers_close(request: &CompletionRequest) -> bool {
        request.tools.iter().any(|tool| tool.name == "close_thread")
    }

    fn compaction(root: &ThreadId) -> InputMessage {
        InputMessage {
            content: InputMessageContent::User(UserContent::text(String::new())),
            group_id: root.to_owned(),
            metadata: None,
            synthetic: Some(SyntheticKind::Tagged(TaggedSyntheticKind::Compaction)),
            display_as: None,
            subscription: false,
        }
    }

    /// Auto-compaction finishes through `close_thread` although built-in tools are off.
    #[tokio::test(flavor = "current_thread")]
    async fn compaction_completes_with_builtins_off() -> Result<(), BoxError> {
        within(compact()).await
    }

    async fn compact() -> Result<(), BoxError> {
        let db = Db::in_memory()?;
        let mut harness = testing::start(&db).await?;
        let root = ThreadId::from("root".to_owned());
        harness.conversations.register_root(&root).await?;
        let mut handle = attach(&mut harness, &root).await?;

        handle.send(InputMessage::user_text(root.to_owned(), "hi"), "input-1").await?;
        assert!(!offers_close(&harness.model.next_request().await), "root threads must not get close_thread");
        reply(&mut harness, &mut handle, "hello").await;

        handle.send(compaction(&root), "compact-1").await?;
        assert!(offers_close(&harness.model.next_request().await), "the compaction thread needs close_thread");

        let child = harness.conversations.compaction_thread().await?;
        let close = serde_json::json!({ "thread_id": child, "report_to_parent": "they said hi" });
        harness.model.send_tool_call("call-close", "close_thread", close);
        harness.model.finish();
        until(&mut handle, |event| matches!(event, AgentEvent::CompactionApplied)).await;

        let summary = harness.conversations.load_latest_compaction_summary_up_to(&root, None).await?;
        assert_eq!(summary, Some(("they said hi".to_owned(), 2)));

        harness.system.shutdown().await;

        Ok(())
    }

    /// Infinity loads any thread ID it is sent; an unregistered one must stop before the model.
    #[tokio::test(flavor = "current_thread")]
    async fn unregistered_thread_never_reaches_the_model() -> Result<(), BoxError> {
        within(async {
            let db = Db::in_memory()?;
            let mut harness = testing::start(&db).await?;
            let ghost = ThreadId::from("ghost".to_owned());

            let input = InputMessage::user_text(ghost.to_owned(), "hi");
            harness.system.sender().send_to_input_queue(input, "input-1").await?;

            assert!(!reaches_model(&mut harness, &ghost).await, "an unregistered thread called the model");
            assert!(!harness.conversations.thread_exists(&ghost).await?);

            Ok(())
        })
        .await
    }
}
