//! Durable ingress through the real router on SQLite, with a scripted model.
//!
//! Each `within` block is one daemon. Leaving it drops its `LocalSet` and
//! every task on it, so the next block starts like a daemon after a crash.

use std::time::Duration;

use chrono::{DateTime, TimeDelta, Utc};
use infinity_agent_core::ThreadId;
use infinity_agent_core::message::InfinityMessage;
use infinity_agent_core::traits::ConversationStore;
use shade_policy::{Cap, Limits};
use shade_proto::Response;
use shade_store::conversation::Conversations;
use shade_store::db::Db;
use shade_store::inbox::{Inbox, Input};
use shade_store::roots::{Adapter, Target};
use shade_store::schedules::Schedules;
use tokio::sync::mpsc;

use super::{Outbound, REPLY_QUEUE, Router};
use crate::error::BoxError;
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

        assert_eq!(Inbox::new(db).count().await?, 1);

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
        let next = schedules.list().await?.into_iter().find_map(|job| job.definition.next_at);
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

/// An approved change applies once and only while fresh: a change drafted
/// before another one applied, or older than a day, does nothing. The
/// conversation that proposed it hears the decision and answers.
#[tokio::test(flavor = "current_thread")]
async fn approvals_apply_only_fresh_changes() -> Result<(), BoxError> {
    within(async {
        let db = Db::in_memory()?;
        let (router, mut model) = testing::router(&db, testing::policy(&[Cap::Now], Limits::default())).await?;
        let schedules = Schedules::new(db.clone());
        let now = Utc::now();
        schedules.add("tea", "cron", "0 16 * * *", now.timestamp() + 3600).await?;
        Conversations::new(db.clone()).register_root(&ThreadId::from("chat".to_owned())).await?;

        schedules.propose("first", "tea", Some(1), "Green tea", Some("chat")).await?;
        schedules.propose("second", "tea", Some(1), "Black tea", None).await?;

        assert!(router.approve("first".to_owned(), now).await.is_ok());
        let told = model.next_request().await;
        assert!(format!("{:?}", told.chat_history).contains("The owner approved"), "the proposer was not told");
        assert!(router.approve("first".to_owned(), now).await.is_err(), "a change applied twice");
        assert!(
            router.approve("second".to_owned(), now).await.is_err(),
            "a change drafted against an old version applied"
        );

        schedules.propose("late", "tea", Some(2), "Mint tea", None).await?;
        let tomorrow = now + TimeDelta::hours(25);
        assert!(router.approve("late".to_owned(), tomorrow).await.is_err(), "an expired change applied");

        let job = schedules.get("tea").await?.map(|job| (job.version, job.definition.title));
        assert_eq!(job, Some((2, "Green tea".to_owned())));

        Ok(())
    })
    .await
}

/// Job files win at startup without losing what SQLite keeps: runs and
/// pause state survive, changed timing counts from now, and a removed
/// file deletes its job.
#[tokio::test(flavor = "current_thread")]
async fn job_files_sync_at_startup() -> Result<(), BoxError> {
    within(async {
        let db = Db::in_memory()?;
        let (router, _model) = testing::router(&db, testing::policy(&[Cap::Now], Limits::default())).await?;
        let schedules = Schedules::new(db.clone());
        let path = router.job_files.path("brief");
        let now: DateTime<Utc> = "2026-11-03T12:00:00Z".parse()?;
        let file = |hour: u32| {
            format!("title = \"Brief\"\ncron = \"0 {hour} * * *\"\ntimezone = \"Europe/Paris\"\ncaps = [\"now\"]\nprompt = \"Hi\"\n")
        };

        if let Some(dir) = path.parent() {
            std::fs::create_dir_all(dir)?;
        }

        std::fs::write(&path, file(8))?;
        router.load_job_files(now).await?;

        let next = schedules.get("brief").await?.and_then(|job| job.definition.next_at);
        assert_eq!(next, Some("2026-11-04T07:00:00Z".parse::<DateTime<Utc>>()?.timestamp()));
        schedules.claim("brief".to_owned(), next.unwrap_or_default(), next.map(|next| next + 86_400)).await?;
        schedules.pause("brief".to_owned()).await?;

        // Unchanged file: nothing moves.
        router.load_job_files(now).await?;
        let job = schedules.get("brief").await?.map(|job| (job.version, job.definition.enabled));
        assert_eq!(job, Some((1, false)));

        // New timing counts from now and stays paused.
        std::fs::write(&path, file(9))?;
        router.load_job_files(now).await?;
        let job = schedules.get("brief").await?.map(|job| (job.version, job.definition.enabled, job.definition.next_at));
        let nine = "2026-11-04T08:00:00Z".parse::<DateTime<Utc>>()?.timestamp();
        assert_eq!(job, Some((2, false, Some(nine))));

        std::fs::remove_file(&path)?;
        router.load_job_files(now).await?;
        assert!(schedules.get("brief").await?.is_none(), "a job outlived its file");
        assert_eq!(schedules.statuses().await?, ["queued"], "the job's runs were lost");

        if let Some(dir) = path.parent() {
            std::fs::remove_dir_all(dir)?;
        }

        Ok(())
    })
    .await
}
