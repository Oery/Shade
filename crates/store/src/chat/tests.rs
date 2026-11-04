use shade_proto::api::TurnStatus;

use super::{Accepted, Chats, Enqueue};
use crate::BoxError;
use crate::db::Db;
use crate::grants::Grants;
use crate::roots::{Adapter, Roots, Target};

fn request(root: &str, text: &str, id: &str) -> Enqueue {
    Enqueue {
        adapter: Adapter::Api,
        target: Target::Existing(root.to_owned()),
        text: text.to_owned(),
        source_key: None,
        client_message_id: Some(id.to_owned()),
        delivery: "api".to_owned(),
        per_conversation: 32,
        total: 256,
    }
}

fn accepted(result: Accepted) -> Result<shade_proto::api::Turn, BoxError> {
    match result {
        Accepted::New(turn) => Ok(turn),
        _ => Err("input was not newly accepted".into()),
    }
}

#[tokio::test]
async fn queue_is_fifo_and_budgets_begin_at_dispatch() -> Result<(), BoxError> {
    let db = Db::in_memory()?;
    let chats = Chats::new(db.clone());
    let root = chats.create(Some("Work".to_owned())).await?.id;
    let other = chats.create(None).await?.id;
    let first = accepted(chats.enqueue(request(&root, "one", "1")).await?)?;
    let second = accepted(chats.enqueue(request(&root, "two", "2")).await?)?;
    let parallel = accepted(chats.enqueue(request(&other, "other", "3")).await?)?;
    let root_id = infinity_agent_core::ThreadId::from(root.to_owned());
    let grants = Grants::new(db);
    assert!(grants.budget(&root_id).await?.is_none());

    assert_eq!(chats.claim().await?.ok_or("missing first turn")?.input.id, first.id);
    let budget = grants.budget(&root_id).await?.ok_or("missing dispatched budget")?.id;
    assert_eq!(chats.claim().await?.ok_or("missing parallel turn")?.input.id, parallel.id);
    assert!(chats.claim().await?.is_none(), "two turns of one root ran together");
    assert_eq!(grants.budget(&root_id).await?.ok_or("missing budget")?.id, budget);

    chats.finish(&first.id, TurnStatus::Completed, None).await?;
    assert_eq!(chats.claim().await?.ok_or("missing second turn")?.input.id, second.id);
    assert_ne!(grants.budget(&root_id).await?.ok_or("missing second budget")?.id, budget);

    Ok(())
}

#[tokio::test]
async fn deduplication_precedes_limits_and_conflicts_do_not_enqueue() -> Result<(), BoxError> {
    let chats = Chats::new(Db::in_memory()?);
    let root = chats.create(None).await?.id;
    let mut first = request(&root, "one", "same");
    first.per_conversation = 1;
    let first = accepted(chats.enqueue(first).await?)?;
    let mut duplicate = request(&root, "one", "same");
    duplicate.per_conversation = 0;

    match chats.enqueue(duplicate).await? {
        Accepted::Repeated(turn) => assert_eq!(turn.id, first.id),
        _ => return Err("a retry did not return the original turn".into()),
    }

    assert!(matches!(chats.enqueue(request(&root, "changed", "same")).await?, Accepted::Conflict));
    let mut full = request(&root, "two", "new");
    full.per_conversation = 1;
    assert!(matches!(chats.enqueue(full).await?, Accepted::Full));
    assert_eq!(chats.detail(root).await?.queued_turns.len(), 1);

    Ok(())
}

#[tokio::test]
async fn catalog_includes_old_owner_roots_and_excludes_guests() -> Result<(), BoxError> {
    let db = Db::in_memory()?;
    let roots = Roots::new(db.clone());
    let old = roots.replace(Adapter::Terminal, "default".to_owned()).await?;
    let new = roots.replace(Adapter::Terminal, "default".to_owned()).await?;
    let guest = roots.replace(Adapter::DiscordGuest, "42".to_owned()).await?;
    let chats = Chats::new(db);
    assert!(chats.detail(guest.as_str().to_owned()).await.is_err());
    assert!(chats.enqueue(request(guest.as_str(), "no", "1")).await.is_err());

    let page = chats.list(None, 1).await?;
    assert_eq!(page.items.len(), 1);
    let cursor = page.next_cursor.ok_or("missing catalog cursor")?;
    let (at, id) = cursor.split_once('.').ok_or("invalid cursor")?;
    let next = chats.list(Some((at.parse()?, id.to_owned())), 1).await?;
    assert_eq!(next.items.len(), 1);
    assert_ne!(page.items[0].id, next.items[0].id);
    assert!(chats.detail(old.as_str().to_owned()).await?.conversation.name.is_none());
    assert_eq!(chats.detail(new.as_str().to_owned()).await?.conversation.name.as_deref(), Some("default"));

    Ok(())
}

#[tokio::test]
async fn restart_retains_the_budget_of_unprocessed_running_work() -> Result<(), BoxError> {
    let db = Db::in_memory()?;
    let chats = Chats::new(db.clone());
    let root = chats.create(None).await?.id;
    let turn = accepted(chats.enqueue(request(&root, "one", "1")).await?)?;
    chats.claim().await?.ok_or("missing turn")?;
    let root_id = infinity_agent_core::ThreadId::from(root.to_owned());
    let grants = Grants::new(db);
    let before = grants.budget(&root_id).await?.ok_or("missing budget")?.id;

    chats.recover(Vec::new()).await?;
    assert_eq!(chats.turn(&turn.id).await?.ok_or("missing recovered turn")?.status, TurnStatus::Queued);
    assert_eq!(chats.claim().await?.ok_or("missing resumed turn")?.input.id, turn.id);
    assert_eq!(grants.budget(&root_id).await?.ok_or("missing resumed budget")?.id, before);

    Ok(())
}
