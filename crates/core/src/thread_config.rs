//! Per-thread configuration resolved by Infinity on each thread load.
//!
//! Built-in tools are off (spawning has no limit hook), but Infinity's
//! auto-compaction child must call `close_thread` to hand back its summary.
//! Compaction threads therefore get that one tool and nothing else.

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
    use std::sync::Arc;
    use std::time::Duration;

    use infinity_agent_core::ThreadId;
    use infinity_agent_core::message::{InputMessage, InputMessageContent, SyntheticKind, TaggedSyntheticKind};
    use infinity_agent_core::stores::InMemoryStateStore;
    use infinity_agent_core::system::local::ThreadHandle;
    use infinity_agent_core::system::{AgentEvent, AgentSystemBuilder, StaticModel};
    use infinity_agent_core::traits::ConversationStore;
    use infinity_provider_protocol::message::UserContent;
    use infinity_provider_protocol::mock::mock_model;
    use infinity_provider_protocol::{CompletionRequest, ModelEntry, SingleModelProvider};
    use tokio::task::LocalSet;

    use super::ThreadTools;
    use crate::db::Db;
    use crate::error::BoxError;
    use crate::store::conversation::Conversations;

    fn offers_close(request: &CompletionRequest) -> bool {
        request.tools.iter().any(|tool| tool.name == "close_thread")
    }

    async fn until(handle: &mut ThreadHandle, wanted: fn(&AgentEvent) -> bool) {
        while let Some(event) = handle.recv().await {
            if wanted(&event) {
                return;
            }
        }
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
        let local = LocalSet::new();
        let test = local.run_until(compact());

        match tokio::time::timeout(Duration::from_secs(10), test).await {
            Ok(result) => result,
            Err(_) => Err("compaction did not finish".into()),
        }
    }

    async fn compact() -> Result<(), BoxError> {
        let (mock, mut model) = mock_model();
        let entry = ModelEntry {
            model_id: "mock".to_owned(),
            display_name: "mock".to_owned(),
            context_window: 100_000,
            max_output_tokens: None,
            supports_image_input: false,
        };
        let static_model = StaticModel::new(Arc::new(SingleModelProvider::new(entry, mock)), "mock").await?;

        let db = Db::in_memory()?;
        let conversations = Conversations::new(db.clone());
        let builder = AgentSystemBuilder::new_local(conversations.clone(), InMemoryStateStore::new(), static_model);
        let system = builder.without_builtin_tools().thread_config(ThreadTools::new(conversations.clone())).start();

        let root = ThreadId::from("root".to_owned());
        conversations.register_root(&root).await?;

        let Some(mut handle) = system.thread_handle(&root).await? else {
            return Err("registered root cannot be attached".into());
        };

        handle.send(InputMessage::user_text(root.to_owned(), "hi"), "input-1").await?;
        assert!(!offers_close(&model.next_request().await), "root threads must not get close_thread");

        model.send_text("hello");
        model.finish();
        until(&mut handle, |event| matches!(event, AgentEvent::CompletionFinished { .. })).await;

        handle.send(compaction(&root), "compact-1").await?;
        assert!(offers_close(&model.next_request().await), "the compaction thread needs close_thread");

        let child: String =
            db.call(|connection| Ok(connection.query_row("SELECT id FROM threads WHERE compaction = 1", [], |row| row.get(0))?)).await?;
        let close = serde_json::json!({ "thread_id": child, "report_to_parent": "they said hi" });
        model.send_tool_call("call-close", "close_thread", close);
        model.finish();
        until(&mut handle, |event| matches!(event, AgentEvent::CompactionApplied)).await;

        let summary = conversations.load_latest_compaction_summary_up_to(&root, None).await?;
        assert_eq!(summary, Some(("they said hi".to_owned(), 2)));

        system.shutdown().await;

        Ok(())
    }
}
