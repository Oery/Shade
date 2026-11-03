//! Durable ingress through the real router on SQLite, with a scripted model.
//!
//! Each `within` block is one daemon. Leaving it drops its `LocalSet` and
//! every task on it, so the next block starts like a daemon after a crash.

use std::time::Duration;

use chrono::{DateTime, Utc};
use infinity_agent_core::ThreadId;
use infinity_agent_core::message::InfinityMessage;
use infinity_agent_core::traits::ConversationStore;
use shade_proto::Response;
use tokio::sync::mpsc;

use super::{Outbound, REPLY_QUEUE, Router};
use crate::error::BoxError;
use crate::policy::{Cap, Limits};
use crate::store::conversation::Conversations;
use crate::store::db::Db;
use crate::store::inbox::Input;
use crate::store::roots::{Adapter, Target};
use crate::store::schedules::Schedules;
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
    let target = Target::Named("default".to_owned());

    match router.inbox.accept(Adapter::Terminal, target, TEXT.to_owned(), None).await? {
        Some(input) => Ok(input),
        None => Err("a terminal input was taken for a duplicate".into()),
    }
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

/// Discord may deliver a message twice: one input, one turn, one reply.
#[tokio::test(flavor = "current_thread")]
async fn message_delivered_twice_is_accepted_once() -> Result<(), BoxError> {
    within(async {
        let db = Db::in_memory()?;
        let (router, mut model) = testing::router(&db, testing::policy(&[], Limits::default())).await?;
        let mut replies = Vec::new();

        for _ in 0..2 {
            let (reply, receiver) = mpsc::channel(REPLY_QUEUE);
            let (target, source_key) = (Target::Named("42".to_owned()), Some("discord:7".to_owned()));
            router.prompt(Adapter::Discord, target, TEXT.to_owned(), source_key, Outbound::Client(reply)).await;
            replies.push(receiver);
        }

        model.next_request().await;
        model.send_text("noted");
        model.finish();

        assert!(matches!(last(&mut replies[0]).await, Some(Response::Done)));
        assert!(replies[1].recv().await.is_none(), "the duplicate got a reply of its own");

        let accepted: i64 = db.call(|connection| Ok(connection.query_row("SELECT COUNT(*) FROM inbox", [], |row| row.get(0))?)).await?;
        assert_eq!(accepted, 1);

        Ok(())
    })
    .await
}

#[derive(Clone, Copy, Debug)]
enum RunCrash {
    /// The occurrence was claimed; the run never started.
    Claimed,
    /// The run started and the model was answering.
    Started,
}

/// A job five hours overdue runs once, not five times, and a crash after
/// its claim or its start neither loses the run nor starts a second one.
#[tokio::test(flavor = "current_thread")]
async fn missed_occurrences_run_once_across_a_crash() -> Result<(), BoxError> {
    for crash in [RunCrash::Claimed, RunCrash::Started] {
        let db = Db::in_memory()?;
        let schedules = Schedules::new(db.clone());
        let now = Utc::now();
        schedules.add("weather", "cron", "0 * * * *", now.timestamp() - 5 * 3600).await?;

        within(first_scheduler(&db, now, crash)).await?;
        within(second_scheduler(&db, now)).await?;

        assert_eq!(schedules.statuses().await?, ["completed"], "{crash:?}");
        let next = schedules.list().await?.into_iter().find_map(|job| job.next_at);
        assert!(next > Some(now.timestamp()), "{crash:?}: the job did not move past now");
    }

    Ok(())
}

async fn first_scheduler(db: &Db, now: DateTime<Utc>, crash: RunCrash) -> Result<(), BoxError> {
    let (router, mut model) = testing::router(db, testing::policy(&[Cap::Now], Limits::default())).await?;

    match crash {
        RunCrash::Claimed => router.claim_due(now).await?,
        RunCrash::Started => {
            router.schedule(now).await;
            model.next_request().await;
        }
    }

    Ok(())
}

async fn second_scheduler(db: &Db, now: DateTime<Utc>) -> Result<(), BoxError> {
    let (router, mut model) = testing::router(db, testing::policy(&[Cap::Now], Limits::default())).await?;
    router.resume().await?;
    router.schedule(now).await;

    let request = model.next_request().await;
    assert_eq!(format!("{:?}", request.chat_history).matches("check the weather").count(), 1);
    model.send_text("sunny");
    model.finish();

    let schedules = Schedules::new(db.clone());

    while schedules.statuses().await? != ["completed"] {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    Ok(())
}
