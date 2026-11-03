//! The SQLite stores against Infinity's in-memory reference stores.

use infinity_agent_core::ThreadId;
use infinity_agent_core::stores::InMemoryStateStore;
use infinity_agent_core::system::UserChoice;
use infinity_agent_core::traits::StateStore;
use rap_protocol::ChoiceId;

use crate::BoxError;
use crate::conversation::Conversations;
use crate::db::Db;
use crate::state::States;

fn root() -> ThreadId {
    ThreadId::from("root".to_owned())
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
