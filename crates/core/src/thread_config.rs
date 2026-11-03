//! Per-thread configuration resolved by Infinity on each thread load.
//!
//! Built-in tools are off (spawning has no limit hook), but Infinity's
//! auto-compaction child must call `close_thread` to hand back its summary.
//! Compaction threads therefore get that one tool and nothing else.
//!
//! Resolution fails for threads Shade never registered, so Infinity's habit of
//! loading any ID it is sent cannot reach the model.

use std::rc::Rc;

use async_trait::async_trait;
use infinity_agent_core::ThreadId;
use infinity_agent_core::system::local::ChannelSender;
use infinity_agent_core::system::{NoRapHttp, ThreadConfig, ThreadConfigSource};
use infinity_agent_core::tools::thread::CloseThreadTool;
use infinity_agent_core::traits::ConversationStore;

use crate::error::BoxError;
use crate::store::conversation::Conversations;

pub struct ThreadTools {
    conversations: Conversations,
}

impl ThreadTools {
    pub fn new(conversations: Conversations) -> Self {
        Self { conversations }
    }
}

#[async_trait(?Send)]
impl ThreadConfigSource<ChannelSender, NoRapHttp> for ThreadTools {
    async fn resolve(&self, thread_id: &ThreadId<str>) -> Result<ThreadConfig<ChannelSender, NoRapHttp>, BoxError> {
        if !self.conversations.thread_exists(thread_id).await? {
            return Err(format!("thread {thread_id} is not registered").into());
        }

        let mut config = ThreadConfig { tools: Vec::new(), extra_system_prompt: None, rap_notifier: None };

        if self.conversations.is_compaction_thread(thread_id).await? {
            let close = CloseThreadTool::<_, NoRapHttp> { conversation_store: self.conversations.clone(), rap_notifier: None };
            config.tools.push(Rc::new(close));
        }

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

    use crate::error::BoxError;
    use crate::store::db::Db;
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

        let sql = "SELECT id FROM threads WHERE compaction = 1";
        let child: String = db.call(move |connection| Ok(connection.query_row(sql, [], |row| row.get(0))?)).await?;
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
