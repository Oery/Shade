//! What watches fired, through the real router and Infinity runtime.

use std::time::Duration;

use chrono::Utc;
use infinity_agent_core::ThreadId;
use shade_policy::{Cap, Limits, Origin};
use shade_proto::api::{SendMessage, Turn, TurnStatus};
use shade_store::chat::Chats;
use shade_store::db::Db;
use shade_store::grants::Grants;
use shade_store::watches::{Destination, Observation, Watches};

use super::Router;
use crate::client::{Command, Reply};
use crate::error::BoxError;
use crate::testing::{self, within};

fn fired(input: &str) -> Observation {
    Observation { watch: "w".to_owned(), version: 1, condition: true, value: "110".to_owned(), fired: Some(input.to_owned()), at: 1000 }
}

async fn send(router: &Router, root: &str, text: &str) -> Result<Turn, BoxError> {
    let request = SendMessage { text: text.to_owned(), client_message_id: text.to_owned() };

    match router.client_command(Command::Send { root: root.to_owned(), request }).await {
        Ok(Reply::Turn(turn)) => Ok(turn),
        _ => Err("cannot send".into()),
    }
}

async fn settled(chats: &Chats, turn: &Turn) -> Result<(), BoxError> {
    loop {
        let status = chats.turn(&turn.id).await?.ok_or("missing turn")?.status;

        if !matches!(status, TurnStatus::Queued | TurnStatus::Running) {
            return Ok(());
        }

        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

/// A fired run survives a crash before or after it started, runs once, and
/// holds the watch's caps and origin rather than the chat that created it.
#[tokio::test(flavor = "current_thread")]
async fn a_fired_run_runs_once_with_its_own_authority() -> Result<(), BoxError> {
    for started in [false, true] {
        let db = Db::in_memory()?;
        let root = Chats::new(db.clone()).create(None).await?.id;
        let watches = Watches::new(db.clone());
        let policy = || testing::policy(&[Cap::Now, Cap::Notify, Cap::Watches], Limits::default());

        watches.add("w", &root, Destination::Run).await?;
        watches.observe(fired("the price moved")).await?;

        within(async {
            let (router, mut model) = testing::router(&db, policy()).await?;

            if started {
                router.start_background(Utc::now()).await;
                model.next_request().await;
            }

            Ok(())
        })
        .await?;

        within(async {
            let (router, mut model) = testing::router(&db, policy()).await?;
            router.resume().await?;
            router.start_background(Utc::now()).await;

            let request = model.next_request().await;
            let tools: Vec<&str> = request.tools.iter().map(|tool| tool.name.as_str()).collect();

            assert_eq!(format!("{:?}", request.chat_history).matches("the price moved").count(), 1, "started: {started}");
            assert_eq!(tools, ["notify"]);

            model.send_text("noted");
            model.finish();

            while watches.statuses().await? != ["completed"] {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }

            Ok(())
        })
        .await?;

        let run = watches.run_root().await?.ok_or("no root")?;
        let grant = Grants::new(db.clone()).of(&ThreadId::from(run)).await?.ok_or("no grant")?;

        assert_eq!((grant.origin, grant.source.as_str()), (Origin::Watch, "w"));
    }

    Ok(())
}

/// A full conversation queue leaves a firing waiting, not lost, and it
/// arrives as a turn in that conversation once there is room.
#[tokio::test(flavor = "current_thread")]
async fn a_busy_conversation_gets_its_firing_later() -> Result<(), BoxError> {
    within(async {
        let db = Db::in_memory()?;
        let chats = Chats::new(db.clone());
        let root = chats.create(None).await?.id;
        let watches = Watches::new(db.clone());
        let limits = Limits { max_queued_chat_messages: 1, ..Limits::default() };
        let (router, mut model) = testing::router(&db, testing::policy(&[Cap::Now], limits)).await?;

        let first = send(&router, &root, "first").await?;
        model.next_request().await;
        let second = send(&router, &root, "second").await?;

        watches.add("w", &root, Destination::Conversation).await?;
        watches.observe(fired("the build finished")).await?;

        router.start_background(Utc::now()).await;
        assert_eq!(watches.statuses().await?, ["queued"]);

        model.send_text("one");
        model.finish();
        settled(&chats, &first).await?;
        router.start_chats().await;
        model.next_request().await;

        router.start_background(Utc::now()).await;
        assert_eq!(watches.statuses().await?, ["delivered"]);

        model.send_text("two");
        model.finish();
        settled(&chats, &second).await?;
        router.start_chats().await;

        let request = model.next_request().await;
        assert_eq!(format!("{:?}", request.chat_history).matches("the build finished").count(), 1);

        model.send_text("done");
        model.finish();

        Ok(())
    })
    .await
}

/// Delivering again, as after a crash before the delivery was marked, adds
/// no second turn: the firing's key makes it a repeat.
#[tokio::test(flavor = "current_thread")]
async fn delivering_twice_is_one_turn() -> Result<(), BoxError> {
    within(async {
        let db = Db::in_memory()?;
        let chats = Chats::new(db.clone());
        let root = chats.create(None).await?.id;
        let watches = Watches::new(db.clone());
        let (router, mut model) = testing::router(&db, testing::policy(&[Cap::Now], Limits::default())).await?;

        watches.add("w", &root, Destination::Conversation).await?;
        watches.observe(fired("the build finished")).await?;

        let again = watches.deliverable().await?;
        router.start_background(Utc::now()).await;

        for queued in again {
            router.deliver(queued, root.to_owned()).await?;
        }

        model.next_request().await;
        model.send_text("done");
        model.finish();

        let page = chats.turns(root, 0, 10).await?;
        assert_eq!(page.items.len(), 1);

        Ok(())
    })
    .await
}
