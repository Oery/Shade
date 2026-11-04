//! Recover one pump per thread, then dispatch legacy non-chat inputs.

use infinity_agent_core::ThreadId;

use super::Router;
use crate::error::BoxError;
use crate::recovery;
use crate::router::turn::Turn;
use crate::tools;

impl Router {
    pub(super) async fn resume_tools(&self, answers: Vec<recovery::Answer>, resumed: &mut Vec<String>) -> Result<(), BoxError> {
        let mut by_thread = std::collections::BTreeMap::<String, Vec<recovery::Answer>>::new();

        for answer in answers {
            by_thread.entry(answer.thread.as_str().to_owned()).or_default().push(answer);
        }

        for (thread, answers) in by_thread {
            if !self.chat.store.resume_allowed(thread.to_owned()).await? {
                continue;
            }

            let thread = ThreadId::from(thread);
            let handle = self.attach(&thread).await?;
            let outbound = self.resume_chat(&handle).await?;
            resumed.push(thread.as_str().to_owned());

            for answer in answers {
                tools::deliver(&self.system.sender(), answer.thread, answer.id, answer.call_id, answer.text).await?;
            }

            self.follow(handle, Turn::continuing(), outbound, None).await;
        }

        Ok(())
    }

    pub(super) async fn resume_inputs(&self, resumed: &mut Vec<String>) -> Result<(), BoxError> {
        for input in self.inbox.unsettled().await? {
            if self.chat.store.turn(&input.id).await?.is_some() {
                continue;
            }

            tracing::info!(root = %input.root, input = %input.id, "resending an input accepted before the restart");
            let outbound = self.outbound_for(&input.root).await;
            resumed.push(input.root.as_str().to_owned());
            self.dispatch(input, outbound).await;
        }

        Ok(())
    }
}
