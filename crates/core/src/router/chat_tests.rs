//! Queue behavior through the real Infinity runtime, without network services.

use infinity_agent_core::ThreadId;
use infinity_agent_core::message::InfinityMessage;
use infinity_agent_core::traits::ConversationStore;
use infinity_provider_protocol::message::{AssistantContent, ToolCall, UserContent};
use shade_policy::{Cap, Limits};
use shade_proto::api::{SendMessage, TurnStatus};
use shade_store::chat::{Accepted, Chats, Enqueue};
use shade_store::conversation::Conversations;
use shade_store::db::Db;
use shade_store::operations::Operations;
use shade_store::roots::{Adapter, Target};
use tokio::sync::mpsc;

use super::{Outbound, Router};
use crate::client::{Command, Reply};
use crate::error::BoxError;
use crate::testing::{self, within};

async fn send(router: &Router, root: &str, text: &str, id: &str) -> Result<shade_proto::api::Turn, BoxError> {
    let request = SendMessage { text: text.to_owned(), client_message_id: id.to_owned() };
    let result = router.client_command(Command::Send { root: root.to_owned(), request }).await;

    match result {
        Ok(Reply::Turn(turn)) => Ok(turn),
        _ => Err("API submission failed".into()),
    }
}

async fn finished(chats: &Chats, id: &str) -> Result<TurnStatus, BoxError> {
    loop {
        let turn = chats.turn(id).await?.ok_or("missing turn")?;

        if !matches!(turn.status, TurnStatus::Queued | TurnStatus::Running) {
            return Ok(turn.status);
        }

        tokio::task::yield_now().await;
    }
}

async fn last(receiver: &mut mpsc::Receiver<shade_proto::Response>) -> Result<(), BoxError> {
    while let Some(response) = receiver.recv().await {
        match response {
            shade_proto::Response::Done => return Ok(()),
            shade_proto::Response::Error { message } => return Err(message.into()),
            _ => {}
        }
    }

    Err("reply ended without a terminal response".into())
}

#[tokio::test(flavor = "current_thread")]
async fn api_terminal_and_discord_wait_through_tool_rounds() -> Result<(), BoxError> {
    within(async {
        let db = Db::in_memory()?;
        let chats = Chats::new(db.clone());
        let root = chats.create(None).await?.id;
        let (router, mut model) = testing::router(&db, testing::policy(&[Cap::Now], Limits::default())).await?;
        let first = send(&router, &root, "same", "1").await?;
        model.next_request().await;
        let (terminal, mut terminal_reply) = mpsc::channel(64);
        let (discord, mut discord_reply) = mpsc::channel(64);
        router
            .prompt(
                Adapter::Terminal,
                Target::Existing(root.to_owned()),
                "same".to_owned(),
                None,
                Outbound::Client(terminal),
            )
            .await;
        router
            .prompt(
                Adapter::Discord,
                Target::Existing(root.to_owned()),
                "third".to_owned(),
                Some("discord:123".to_owned()),
                Outbound::Client(discord),
            )
            .await;
        assert_eq!(chats.detail(root.to_owned()).await?.queued_turns.len(), 2);

        model.send_tool_call("time", "now", serde_json::json!({}));
        model.finish();
        let continuation = model.next_request().await;
        assert!(!format!("{:?}", continuation.chat_history).contains("third"));
        router.start_chats().await;
        assert_eq!(
            chats.detail(root.to_owned()).await?.queued_turns.len(),
            2,
            "tool completion released the turn early"
        );
        model.send_text("first answer");
        model.finish();
        assert_eq!(finished(&chats, &first.id).await?, TurnStatus::Completed);

        router.start_chats().await;
        model.next_request().await;
        model.send_text("second answer");
        model.finish();
        last(&mut terminal_reply).await?;
        router.start_chats().await;
        let third = model.next_request().await;
        assert!(format!("{:?}", third.chat_history).contains("third"));
        model.send_text("third answer");
        model.finish();
        last(&mut discord_reply).await?;
        assert!(chats.detail(root).await?.queued_turns.is_empty());

        Ok(())
    })
    .await
}

#[tokio::test(flavor = "current_thread")]
async fn independent_conversations_reach_the_model_concurrently() -> Result<(), BoxError> {
    within(async {
        let db = Db::in_memory()?;
        let chats = Chats::new(db.clone());
        let first = chats.create(None).await?.id;
        let second = chats.create(None).await?.id;
        let (router, mut model) = testing::router(&db, testing::policy(&[], Limits::default())).await?;
        assert_eq!(send(&router, &first, "first", "1").await?.status, TurnStatus::Running);
        assert_eq!(send(&router, &second, "second", "1").await?.status, TurnStatus::Running);
        model.next_request().await;
        model.next_request().await;

        Ok(())
    })
    .await
}

#[tokio::test(flavor = "current_thread")]
async fn restart_runs_unprocessed_turns_in_order() -> Result<(), BoxError> {
    let db = Db::in_memory()?;
    let chats = Chats::new(db.clone());
    let root = chats.create(None).await?.id;
    let (first, second) = within(async {
        let (router, mut model) = testing::router(&db, testing::policy(&[], Limits::default())).await?;
        let first = send(&router, &root, "first", "1").await?;
        model.next_request().await;
        let second = send(&router, &root, "second", "2").await?;

        Ok((first.id, second.id))
    })
    .await?;

    within(async {
        let (router, mut model) = testing::router(&db, testing::policy(&[], Limits::default())).await?;
        router.resume().await?;
        let replay = model.next_request().await;
        assert!(!format!("{:?}", replay.chat_history).contains("second"));
        model.send_text("first answer");
        model.finish();
        assert_eq!(finished(&chats, &first).await?, TurnStatus::Completed);
        router.start_chats().await;
        model.next_request().await;
        model.send_text("second answer");
        model.finish();
        assert_eq!(finished(&chats, &second).await?, TurnStatus::Completed);

        Ok(())
    })
    .await
}

async fn stored_running(db: &Db, pending_tool: bool) -> Result<(String, String), BoxError> {
    let chats = Chats::new(db.clone());
    let root = chats.create(None).await?.id;
    let request = Enqueue {
        adapter: Adapter::Api,
        target: Target::Existing(root.to_owned()),
        text: "run".to_owned(),
        source_key: None,
        client_message_id: Some("1".to_owned()),
        delivery: "api".to_owned(),
        per_conversation: 32,
        total: 256,
    };
    let Accepted::New(turn) = chats.enqueue(request).await? else {
        return Err("input was not accepted".into());
    };
    chats.claim().await?.ok_or("input was not claimed")?;
    let mut history = vec![(InfinityMessage::User { content: UserContent::text("run") }, turn.id.to_owned())];

    if pending_tool {
        let call = ToolCall::new("time", "now", serde_json::json!({}));
        history.push((InfinityMessage::ToolCall { call, display_as: None }, "completion".to_owned()));
        let operations = Operations::new(db.clone());
        operations.start(&root, "time", "now").await?;
        operations.finish(&root, "time", "12:00").await?;
    } else {
        history.push((
            InfinityMessage::Assistant { content: AssistantContent::text("saved answer") },
            "completion".to_owned(),
        ));
    }

    Conversations::new(db.clone()).append_messages(&ThreadId::from(root.to_owned()), history).await?;

    Ok((root, turn.id))
}

#[tokio::test(flavor = "current_thread")]
async fn processed_turns_continue_only_when_a_tool_can_resume_them() -> Result<(), BoxError> {
    for pending_tool in [false, true] {
        let db = Db::in_memory()?;
        let (root, id) = stored_running(&db, pending_tool).await?;
        let chats = Chats::new(db.clone());

        within(async {
            let (router, mut model) = testing::router(&db, testing::policy(&[Cap::Now], Limits::default())).await?;
            router.resume().await?;

            if pending_tool {
                let replay = model.next_request().await;
                assert!(format!("{:?}", replay.chat_history).contains("12:00"));
                model.send_text("recovered");
                model.finish();
                assert_eq!(finished(&chats, &id).await?, TurnStatus::Completed);
            } else {
                assert!(model.try_next_request().is_none(), "a processed input was run again");
                assert_eq!(chats.turn(&id).await?.ok_or("missing interrupted turn")?.status, TurnStatus::Interrupted);
            }

            assert_eq!(chats.delivery(&id).await?.as_deref(), Some("api"));
            assert!(chats.detail(root).await?.active_turn.is_none());

            Ok(())
        })
        .await?;
    }

    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn a_timed_out_retry_releases_the_next_turn() -> Result<(), BoxError> {
    within(async {
        let db = Db::in_memory()?;
        let chats = Chats::new(db.clone());
        let root = chats.create(None).await?.id;
        let limits = Limits { max_run_seconds: 2, ..Limits::default() };
        let (router, mut model) = testing::router(&db, testing::policy(&[], limits)).await?;
        let first = send(&router, &root, "first", "1").await?;
        model.next_request().await;
        model.send_error(infinity_provider_protocol::CompletionError::provider(
            infinity_provider_protocol::ErrorClass::Throttled,
            "retry later",
        ));
        let second = send(&router, &root, "second", "2").await?;
        assert_eq!(second.status, TurnStatus::Queued);
        assert_eq!(finished(&chats, &first.id).await?, TurnStatus::Failed);
        router.start_chats().await;
        model.next_request().await;
        model.send_text("second answer");
        model.finish();
        assert_eq!(finished(&chats, &second.id).await?, TurnStatus::Completed);

        Ok(())
    })
    .await
}

#[tokio::test(flavor = "current_thread")]
async fn failed_turns_with_pending_tools_are_not_reopened() -> Result<(), BoxError> {
    let db = Db::in_memory()?;
    let (_root, id) = stored_running(&db, true).await?;
    let chats = Chats::new(db.clone());
    chats.finish(&id, TurnStatus::Failed, Some("reported failure".to_owned())).await?;

    within(async {
        let (router, mut model) = testing::router(&db, testing::policy(&[Cap::Now], Limits::default())).await?;
        router.resume().await?;
        assert!(model.try_next_request().is_none(), "an already failed turn ran after restart");
        assert_eq!(chats.turn(&id).await?.ok_or("missing failed turn")?.status, TurnStatus::Failed);

        Ok(())
    })
    .await
}

#[tokio::test(flavor = "current_thread")]
async fn a_healthy_client_can_drain_a_burst_larger_than_its_reply_queue() -> Result<(), BoxError> {
    within(async {
        let db = Db::in_memory()?;
        let (router, mut model) = testing::router(&db, testing::policy(&[], Limits::default())).await?;
        let (reply, mut replies) = mpsc::channel(super::REPLY_QUEUE);
        router.prompt(Adapter::Terminal, Target::Fresh, "burst".to_owned(), None, Outbound::Client(reply)).await;
        model.next_request().await;

        for _ in 0..256 {
            model.send_text("x");
        }

        model.finish();
        let mut text = String::new();

        while let Some(response) = replies.recv().await {
            match response {
                shade_proto::Response::Text { text: chunk } => text.push_str(&chunk),
                shade_proto::Response::Done => break,
                shade_proto::Response::Error { message } => return Err(message.into()),
            }
        }

        assert_eq!(text.len(), 256);

        Ok(())
    })
    .await
}

#[tokio::test(flavor = "current_thread")]
async fn a_stalled_client_does_not_prevent_the_turn_from_settling() -> Result<(), BoxError> {
    within(async {
        let db = Db::in_memory()?;
        let chats = Chats::new(db.clone());
        let root = chats.create(None).await?.id;
        let (router, mut model) = testing::router(&db, testing::policy(&[], Limits::default())).await?;
        let (reply, _replies) = mpsc::channel(super::REPLY_QUEUE);
        router
            .prompt(
                Adapter::Terminal,
                Target::Existing(root.to_owned()),
                "burst".to_owned(),
                None,
                Outbound::Client(reply),
            )
            .await;
        let id = chats.detail(root).await?.active_turn.ok_or("missing active turn")?.id;
        model.next_request().await;

        for _ in 0..256 {
            model.send_text("x");
        }

        model.finish();
        assert_eq!(finished(&chats, &id).await?, TurnStatus::Completed);

        Ok(())
    })
    .await
}
