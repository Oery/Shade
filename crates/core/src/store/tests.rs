//! Restart and replay through the real runtime on both SQLite stores.
//!
//! Starting a second system on the same database stands in for a daemon restart:
//! everything the first one knew is either in SQLite or gone.

use infinity_agent_core::ThreadId;
use infinity_agent_core::message::{InfinityMessage, InputMessage, InputMessageContent};
use infinity_agent_core::stores::InMemoryStateStore;
use infinity_agent_core::system::UserChoice;
use infinity_agent_core::traits::{ConversationStore, StateStore};
use infinity_provider_protocol::message::{AssistantContent, Text, ToolCall, ToolResult, ToolResultContent, UserContent};
use rap_protocol::ChoiceId;

use super::conversation::Conversations;
use super::state::States;
use crate::db::Db;
use crate::error::BoxError;
use crate::testing::{self, attach, reaches_model, reply, within};

fn root() -> ThreadId {
    ThreadId::from("root".to_owned())
}

fn user(text: &str) -> InfinityMessage {
    InfinityMessage::User { content: UserContent::text(text.to_owned()) }
}

fn tool_result(call_id: &str) -> InputMessage {
    let result =
        ToolResult { id: call_id.to_owned(), call_id: None, content: vec![ToolResultContent::Text(Text { text: "done".to_owned() })] };

    InputMessage {
        content: InputMessageContent::User(UserContent::ToolResult(result)),
        group_id: root(),
        metadata: None,
        synthetic: None,
        display_as: None,
        subscription: false,
    }
}

async fn registered() -> Result<Db, BoxError> {
    let db = Db::in_memory()?;
    Conversations::new(db.clone()).register_root(&root()).await?;

    Ok(db)
}

fn choice(id: &str, prompt: &str) -> UserChoice {
    UserChoice { id: ChoiceId::from(id), prompt: prompt.to_owned(), choices: Vec::new(), default: 0, response_url: String::new() }
}

enum Op {
    Add(&'static str, &'static str),
    Remove(&'static str),
}

/// Replacing a choice keeps its place; removing an unknown one is a no-op.
#[tokio::test]
async fn pending_choices_match_reference() -> Result<(), BoxError> {
    let reference = InMemoryStateStore::new();
    let sqlite = States::new(registered().await?);

    let ops = [
        Op::Add("a", "first"),
        Op::Add("b", "second"),
        Op::Add("a", "replaced"),
        Op::Add("c", "third"),
        Op::Remove("b"),
        Op::Remove("missing"),
    ];

    for (step, op) in ops.into_iter().enumerate() {
        match op {
            Op::Add(id, prompt) => {
                reference.add_pending_user_choice(&root(), choice(id, prompt)).await?;
                sqlite.add_pending_user_choice(&root(), choice(id, prompt)).await?;
            }
            Op::Remove(id) => {
                reference.remove_pending_user_choice(&root(), &ChoiceId::from(id)).await?;
                sqlite.remove_pending_user_choice(&root(), &ChoiceId::from(id)).await?;
            }
        }

        let expected = reference.get_pending_user_choices(&root()).await?;
        let actual = sqlite.get_pending_user_choices(&root()).await?;

        assert_eq!(actual, expected, "after step {step}");
    }

    Ok(())
}

/// A turn survives a restart: the same input ID is ignored and new input sees the old history.
#[tokio::test(flavor = "current_thread")]
async fn replay_after_restart_runs_once() -> Result<(), BoxError> {
    within(async {
        let db = registered().await?;

        let mut before = testing::start(&db).await?;
        let mut handle = attach(&mut before, &root()).await?;
        handle.send(InputMessage::user_text(root(), "hi"), "input-1").await?;
        before.model.next_request().await;
        reply(&mut before, &mut handle, "hello").await;
        before.system.shutdown().await;

        let mut after = testing::start(&db).await?;
        let handle = attach(&mut after, &root()).await?;
        handle.send(InputMessage::user_text(root(), "hi"), "input-1").await?;

        assert!(!reaches_model(&mut after, &root()).await, "a replayed input ran again after restart");

        handle.send(InputMessage::user_text(root(), "again"), "input-2").await?;
        let request = after.model.next_request().await;

        assert!(format!("{:?}", request.chat_history).contains("hello"), "the earlier turn is missing after restart");

        Ok(())
    })
    .await
}

/// Infinity writes history and processed IDs separately. A crash between the two
/// must not let the input run again: history alone marks it processed.
#[tokio::test(flavor = "current_thread")]
async fn input_in_history_counts_as_processed() -> Result<(), BoxError> {
    within(async {
        let db = registered().await?;
        let mut harness = testing::start(&db).await?;

        let assistant = InfinityMessage::Assistant { content: AssistantContent::text("hello".to_owned()) };
        let turn = vec![(user("hi"), "input-1".to_owned()), (assistant, "completion-1".to_owned())];
        harness.conversations.append_messages(&root(), turn).await?;

        let handle = attach(&mut harness, &root()).await?;
        handle.send(InputMessage::user_text(root(), "hi"), "input-1").await?;

        assert!(!reaches_model(&mut harness, &root()).await, "an input already in history ran again");

        Ok(())
    })
    .await
}

/// A processed input must not hide the work that continues it: a tool result
/// for a call made before the restart still reaches the model.
#[tokio::test(flavor = "current_thread")]
async fn processed_input_does_not_hide_its_tool_result() -> Result<(), BoxError> {
    within(async {
        let db = registered().await?;
        let mut harness = testing::start(&db).await?;

        let call = InfinityMessage::ToolCall { call: ToolCall::new("call-1", "probe", serde_json::json!({})), display_as: None };
        let turn = vec![(user("hi"), "input-1".to_owned()), (call, "completion-1".to_owned())];
        harness.conversations.append_messages(&root(), turn).await?;

        // A thread loaded with an unanswered call stays live awaiting its result, so there is no idle to wait for.
        let Some(handle) = harness.system.thread_handle(&root()).await? else {
            return Err("registered thread cannot be attached".into());
        };
        handle.send(tool_result("call-1"), "call-1").await?;

        // `within` fails the test if the result never reaches the model.
        harness.model.next_request().await;

        Ok(())
    })
    .await
}
