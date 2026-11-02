//! Parity with Infinity's reference store, plus Shade's own guarantees.

use infinity_agent_core::ThreadId;
use infinity_agent_core::message::InfinityMessage;
use infinity_agent_core::stores::InMemoryConversationStore;
use infinity_agent_core::traits::{ConversationStore, SpawnContext};
use infinity_provider_protocol::message::{AssistantContent, ToolResult, UserContent};
use rap_protocol::ToolCallId;

use super::Conversations;
use crate::db::{Db, DbError};
use crate::error::BoxError;

fn user(text: &str) -> InfinityMessage {
    InfinityMessage::User { content: UserContent::text(text.to_owned()) }
}

fn assistant(text: &str) -> InfinityMessage {
    InfinityMessage::Assistant { content: AssistantContent::text(text.to_owned()) }
}

fn event(id: &str) -> InfinityMessage {
    let result = ToolResult { id: id.to_owned(), call_id: None, content: Vec::new() };

    InfinityMessage::SubscriptionEvent {
        result: Box::new(result),
        tool_call_id: ToolCallId::from(id),
        child_thread_id: None,
        invocation: None,
    }
}

fn root() -> ThreadId {
    ThreadId::from("root".to_owned())
}

/// Messages compare by their stored form; `InfinityMessage` has no `PartialEq`.
fn json(messages: &[InfinityMessage]) -> Result<Vec<String>, BoxError> {
    let mut texts = Vec::new();

    for message in messages {
        texts.push(serde_json::to_string(message)?);
    }

    Ok(texts)
}

/// Both stores with the same root holding five messages.
async fn stores() -> Result<(InMemoryConversationStore, Conversations, Db), BoxError> {
    let reference = InMemoryConversationStore::new();
    let db = Db::in_memory()?;
    let sqlite = Conversations::new(db.clone());

    reference.ensure_root_thread(&root()).await?;
    sqlite.register_root(&root()).await?;

    for (index, text) in ["m0", "m1", "m2", "m3", "m4"].into_iter().enumerate() {
        let message = vec![(assistant(text), format!("completion-{index}"))];
        reference.append_messages(&root(), message).await?;

        let message = vec![(assistant(text), format!("completion-{index}"))];
        sqlite.append_messages(&root(), message).await?;
    }

    Ok((reference, sqlite, db))
}

async fn count(db: &Db, sql: &'static str) -> Result<i64, DbError> {
    db.call(move |connection| Ok(connection.query_row(sql, [], |row| row.get(0))?)).await
}

#[tokio::test]
async fn history_bounds_match_reference() -> Result<(), BoxError> {
    let (reference, sqlite, _db) = stores().await?;

    let cases = [
        (None, None),
        (Some(2), None),
        (None, Some(3)),
        (Some(1), Some(4)),
        (Some(3), Some(3)),
        (Some(4), Some(2)),
        (Some(9), None),
        (None, Some(9)),
        (Some(0), Some(0)),
    ];

    for (start, end) in cases {
        let expected = json(&reference.load_history_up_to(&root(), start, end).await?)?;
        let actual = json(&sqlite.load_history_up_to(&root(), start, end).await?)?;

        assert_eq!(actual, expected, "start {start:?}, end {end:?}");
    }

    Ok(())
}

/// The latest summary wins by insertion, not by order, within the bound.
#[tokio::test]
async fn compaction_selection_matches_reference() -> Result<(), BoxError> {
    let (reference, sqlite, _db) = stores().await?;

    for (summary, order) in [("a", 2), ("b", 5), ("c", 3)] {
        reference.save_compaction_summary(&root(), summary, order).await?;
        sqlite.save_compaction_summary(&root(), summary, order).await?;
    }

    for bound in [None, Some(1), Some(2), Some(3), Some(4), Some(10)] {
        let expected = reference.load_latest_compaction_summary_up_to(&root(), bound).await?;
        let actual = sqlite.load_latest_compaction_summary_up_to(&root(), bound).await?;

        assert_eq!(actual, expected, "bound {bound:?}");
    }

    Ok(())
}

/// root → a (inherits 3) → b (Fresh) → c (inherits 1), with a summary on root.
async fn grow<C: ConversationStore>(store: &C) -> Result<Vec<(&'static str, ThreadId)>, BoxError> {
    let a = store.spawn_thread(&root(), &ToolCallId::from("call-a"), false, SpawnContext::InheritUpTo(3)).await?;
    store.append_messages(&a, vec![(user("a0"), "a0".to_owned()), (assistant("a1"), "a1".to_owned())]).await?;

    let b = store.spawn_thread(&a, &ToolCallId::from("call-b"), false, SpawnContext::Fresh).await?;
    store.append_messages(&b, vec![(user("b0"), "b0".to_owned()), (assistant("b1"), "b1".to_owned())]).await?;

    let c = store.spawn_thread(&b, &ToolCallId::from("call-c"), false, SpawnContext::InheritUpTo(1)).await?;
    store.append_messages(&c, vec![(user("c0"), "c0".to_owned())]).await?;

    store.save_compaction_summary(&root(), "root summary", 2).await?;

    Ok(vec![("root", root()), ("a", a), ("b", b), ("c", c)])
}

/// Replaces generated thread IDs with their names so both stores compare.
fn names(chain: Vec<(ThreadId, i64)>, threads: &[(&'static str, ThreadId)]) -> Vec<(&'static str, i64)> {
    let mut named = Vec::new();

    for (id, order) in chain {
        let name = threads.iter().find(|(_, thread)| *thread == id).map_or("unknown", |(name, _)| *name);
        named.push((name, order));
    }

    named
}

#[tokio::test]
async fn ancestry_through_fresh_children_matches_reference() -> Result<(), BoxError> {
    let (reference, sqlite, _db) = stores().await?;
    let reference_threads = grow(&reference).await?;
    let sqlite_threads = grow(&sqlite).await?;

    for ((name, reference_id), (_, sqlite_id)) in reference_threads.iter().zip(&sqlite_threads) {
        let (expected, expected_cut, expected_prefix) = reference.load_history_with_ancestors(reference_id).await?;
        let (actual, actual_cut, actual_prefix) = sqlite.load_history_with_ancestors(sqlite_id).await?;

        assert_eq!(json(&actual)?, json(&expected)?, "history of {name}");
        assert_eq!((actual_cut, actual_prefix), (expected_cut, expected_prefix), "bounds of {name}");

        let expected = names(reference.get_ancestor_chain(reference_id).await?, &reference_threads);
        let actual = names(sqlite.get_ancestor_chain(sqlite_id).await?, &sqlite_threads);

        assert_eq!(actual, expected, "ancestors of {name}");
    }

    // Grants are looked up on the structural root, which a Fresh boundary must not hide.
    let chain = names(sqlite.get_ancestor_chain(&sqlite_threads[3].1).await?, &sqlite_threads);
    assert_eq!(chain, [("root", 3), ("a", 0), ("b", 1)]);

    Ok(())
}

#[tokio::test]
async fn append_records_only_input_ids_as_processed() -> Result<(), BoxError> {
    let (_, sqlite, db) = stores().await?;

    let batch = vec![
        (user("hi"), "input-1".to_owned()),
        (event("event-1"), "event-1".to_owned()),
        (assistant("hello"), "completion-9".to_owned()),
        (assistant("again"), "completion-9".to_owned()),
    ];
    sqlite.append_messages(&root(), batch).await?;

    let processed = db
        .call(|connection| {
            let mut statement = connection.prepare("SELECT id FROM processed_ids ORDER BY id")?;
            let ids: Result<Vec<String>, _> = statement.query_map([], |row| row.get(0))?.collect();

            Ok(ids?)
        })
        .await?;

    assert_eq!(processed, ["event-1", "input-1"]);
    assert_eq!(sqlite.load_history_up_to(&root(), None, None).await?.len(), 9, "shared completion IDs are kept");

    Ok(())
}

#[tokio::test]
async fn failed_append_writes_nothing() -> Result<(), BoxError> {
    let (_, sqlite, db) = stores().await?;

    db.call(|connection| {
        let trigger = "CREATE TRIGGER fail BEFORE INSERT ON messages WHEN NEW.message_id = 'boom'
                       BEGIN SELECT RAISE(ABORT, 'boom'); END;";
        connection.execute_batch(trigger)?;

        Ok(())
    })
    .await?;

    let batch = vec![(user("hi"), "input-1".to_owned()), (assistant("hello"), "boom".to_owned())];

    assert!(sqlite.append_messages(&root(), batch).await.is_err());
    assert_eq!(count(&db, "SELECT COUNT(*) FROM messages").await?, 5);
    assert_eq!(count(&db, "SELECT COUNT(*) FROM processed_ids").await?, 0);

    Ok(())
}

#[tokio::test]
async fn unknown_threads_never_become_roots() -> Result<(), BoxError> {
    let (_, sqlite, db) = stores().await?;
    let ghost = ThreadId::from("ghost".to_owned());

    assert!(sqlite.ensure_root_thread(&ghost).await.is_err());
    assert!(sqlite.spawn_thread(&ghost, &ToolCallId::from("call"), false, SpawnContext::Fresh).await.is_err());
    assert!(sqlite.append_messages(&ghost, vec![(user("hi"), "input-1".to_owned())]).await.is_err());
    assert!(sqlite.save_compaction_summary(&ghost, "summary", 0).await.is_err());

    assert!(!sqlite.thread_exists(&ghost).await?);
    assert_eq!(count(&db, "SELECT COUNT(*) FROM threads").await?, 1);

    Ok(())
}
