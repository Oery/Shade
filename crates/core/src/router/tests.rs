//! Durable ingress through the real router on SQLite, with a scripted model.
//!
//! Each `within` block is one daemon. Leaving it drops its `LocalSet` and
//! every task on it, so the next block starts like a daemon after a crash.

use std::time::Duration;

use infinity_agent_core::ThreadId;
use infinity_agent_core::message::InfinityMessage;
use infinity_agent_core::traits::ConversationStore;
use shade_proto::Response;
use tokio::sync::mpsc;

use super::{Outbound, REPLY_QUEUE, Router};
use crate::error::BoxError;
use crate::policy::Limits;
use crate::store::conversation::Conversations;
use crate::store::db::Db;
use crate::store::inbox::Input;
use crate::store::roots::{Adapter, Target};
use crate::testing::{self, within};

const TEXT: &str = "remember the milk";

#[derive(Clone, Copy, Debug)]
enum CrashAfter {
    /// The input was recorded but never reached Infinity.
    Commit,
    /// Infinity had it and the model was answering; nothing was saved yet.
    Enqueue,
    /// The answer was saved with the input.
    Answer,
}

async fn accept(router: &Router) -> Result<Input, BoxError> {
    Ok(router.inbox.accept(Adapter::Terminal, Target::Named("default".to_owned()), TEXT.to_owned()).await?)
}

/// The final response of a turn.
async fn last(replies: &mut mpsc::Receiver<Response>) -> Option<Response> {
    while let Some(response) = replies.recv().await {
        if !matches!(response, Response::Text { .. }) {
            return Some(response);
        }
    }

    None
}

/// An accepted input is answered exactly once, in its own conversation,
/// wherever the daemon dies; once processed it is never sent again.
#[tokio::test(flavor = "current_thread")]
async fn accepted_input_is_answered_once_across_a_crash() -> Result<(), BoxError> {
    for crash in [CrashAfter::Commit, CrashAfter::Enqueue, CrashAfter::Answer] {
        let db = Db::in_memory()?;
        let root = within(first_daemon(&db, crash)).await?;
        within(second_daemon(&db, &root, !matches!(crash, CrashAfter::Answer))).await?;

        let history = Conversations::new(db).load_history_up_to(&root, None, None).await?;
        let inputs =
            history.iter().filter(|message| matches!(message, InfinityMessage::User { .. }) && format!("{message:?}").contains(TEXT));

        assert_eq!(inputs.count(), 1, "{crash:?}");
    }

    Ok(())
}

async fn first_daemon(db: &Db, crash: CrashAfter) -> Result<ThreadId, BoxError> {
    let (router, mut model) = testing::router(db, testing::policy(&[], Limits::default())).await?;
    let input = accept(&router).await?;
    let root = input.root.clone();

    if let CrashAfter::Commit = crash {
        return Ok(root);
    }

    let (reply, mut replies) = mpsc::channel(REPLY_QUEUE);
    router.dispatch(input, Outbound::Client(reply)).await;
    model.next_request().await;

    if let CrashAfter::Enqueue = crash {
        return Ok(root);
    }

    model.send_text("noted");
    model.finish();
    assert!(matches!(last(&mut replies).await, Some(Response::Done)));

    Ok(root)
}

async fn second_daemon(db: &Db, root: &ThreadId, resent: bool) -> Result<(), BoxError> {
    let (router, mut model) = testing::router(db, testing::policy(&[], Limits::default())).await?;
    assert_eq!(router.inbox.unsettled().await?.len(), usize::from(resent));

    router.resume().await?;

    if !resent {
        return Ok(());
    }

    // A replay under another ID or root would show the input twice, or not at all.
    let request = model.next_request().await;
    assert_eq!(format!("{:?}", request.chat_history).matches(TEXT).count(), 1, "root {root}");

    model.send_text("noted");
    model.finish();

    while !router.inbox.unsettled().await?.is_empty() {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    Ok(())
}

/// A turn that failed in front of its sender is not run again behind their back.
#[tokio::test(flavor = "current_thread")]
async fn failed_input_is_settled_before_its_sender_hears() -> Result<(), BoxError> {
    within(async {
        let db = Db::in_memory()?;
        let limits = Limits { max_completion_rounds: 0, ..Limits::default() };
        let (router, _model) = testing::router(&db, testing::policy(&[], limits)).await?;

        let (reply, mut replies) = mpsc::channel(REPLY_QUEUE);
        router.dispatch(accept(&router).await?, Outbound::Client(reply)).await;

        assert!(matches!(last(&mut replies).await, Some(Response::Error { .. })));
        assert!(router.inbox.unsettled().await?.is_empty(), "a failed input would run again after a restart");

        Ok(())
    })
    .await
}
