//! Absolute run deadlines, reconstructed from persisted budgets after a restart.

use std::future::Future;
use std::time::Duration;

use infinity_agent_core::ThreadId;
use shade_store::grants::Grants;
use tokio::time::Instant;

use crate::authority::Denied;

pub struct Deadline {
    grants: Grants,
    budget: i64,
    at: Instant,
}

impl Deadline {
    pub async fn for_thread(grants: Grants, thread: &ThreadId<str>, seconds: u32) -> Result<Self, Denied> {
        let Some(budget) = grants.budget(thread).await? else {
            return Err(Denied::NoBudget(thread.as_str().to_owned()));
        };

        let expires = budget.started_at + i64::from(seconds);
        let millis = (expires * 1000 - chrono::Utc::now().timestamp_millis()).max(0) as u64;
        let at = Instant::now() + Duration::from_millis(millis);

        Ok(Self { grants, budget: budget.id, at })
    }

    /// Dropping the timed-out future cancels the local wait. An external write
    /// may already have taken effect, so tool callers must preserve that ambiguity.
    pub async fn run<F: Future>(&self, work: F) -> Result<F::Output, Denied> {
        if Instant::now() >= self.at {
            return Err(self.expire().await);
        }

        tokio::select! {
            biased;

            _ = tokio::time::sleep_until(self.at) => Err(self.expire().await),
            result = work => Ok(result),
        }
    }

    async fn expire(&self) -> Denied {
        match self.grants.expire(self.budget).await {
            Ok(()) => Denied::Limit("max_run_seconds".to_owned()),
            Err(error) => Denied::Db(error),
        }
    }
}

#[cfg(test)]
mod tests {
    use infinity_agent_core::ThreadId;
    use shade_policy::{Charge, Limits};
    use shade_store::conversation::Conversations;
    use shade_store::db::Db;
    use shade_store::grants::Grants;
    use shade_store::inbox::Inbox;

    use super::Deadline;
    use crate::error::BoxError;

    #[tokio::test]
    async fn an_old_deadline_never_expires_a_new_turn() -> Result<(), BoxError> {
        let db = Db::in_memory()?;
        let root = ThreadId::from("root".to_owned());
        Conversations::new(db.clone()).register_root(&root).await?;
        let grants = Grants::new(db.clone());
        let expired = Deadline::for_thread(grants.clone(), &root, 0).await?;

        let new = Inbox::new(db).accept_for_thread("root".to_owned(), "new turn".to_owned(), "new".to_owned()).await?;
        assert_eq!(new.ok_or("missing new input")?.root, root);
        assert!(expired.run(std::future::ready(())).await.is_err());
        assert_eq!(grants.charge(&root, Charge::Completion, Limits::default()).await?, None);

        Ok(())
    }

    #[tokio::test]
    async fn reconstructing_a_deadline_keeps_the_original_start_time() -> Result<(), BoxError> {
        let db = Db::in_memory()?;
        let root = ThreadId::from("root".to_owned());
        Conversations::new(db.clone()).register_root(&root).await?;
        let grants = Grants::new(db);
        let budget = grants.budget(&root).await?.ok_or("missing budget")?;
        let millis = ((budget.started_at + 1) * 1000 - chrono::Utc::now().timestamp_millis()).max(0) as u64;
        tokio::time::sleep(std::time::Duration::from_millis(millis + 20)).await;

        let resumed = Deadline::for_thread(grants, &root, 1).await?;
        assert!(resumed.run(std::future::ready(())).await.is_err());

        Ok(())
    }
}
