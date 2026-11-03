//! Startup recovery: answer every tool call a previous daemon left waiting.
//!
//! A thread whose history ends with an unanswered call waits for its result
//! forever. Each such call gets its captured result if one was recorded, or
//! else an explicit unknown outcome. Nothing is run again.

use infinity_agent_core::ThreadId;

use crate::error::BoxError;
use crate::store::operations::Operations;

/// The result of a call whose outcome was lost.
pub const UNKNOWN_OUTCOME: &str = "Interrupted before the result was recorded. The action may have completed. Verify its outcome before retrying an action with side effects.";

/// The answer owed to one waiting call.
pub struct Answer {
    pub thread: ThreadId,
    pub id: String,
    pub call_id: Option<String>,
    pub text: String,
}

/// Settles every waiting call and returns the answers to deliver.
pub async fn answers(operations: &Operations) -> Result<Vec<Answer>, BoxError> {
    let mut answers = Vec::new();

    for (thread, calls) in operations.unanswered().await? {
        let thread = ThreadId::from(thread);

        for call in calls {
            let text = match operations.settle(thread.as_str(), &call.id, &call.function.name).await? {
                Some(result) => {
                    tracing::info!(%thread, call = %call.id, tool = %call.function.name, "redelivering a captured result");
                    result
                }
                None => {
                    tracing::warn!(%thread, call = %call.id, tool = %call.function.name, "tool call interrupted; outcome unknown");
                    UNKNOWN_OUTCOME.to_owned()
                }
            };

            answers.push(Answer { thread: thread.to_owned(), id: call.id, call_id: call.call_id, text });
        }
    }

    Ok(answers)
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::sync::Arc;
    use std::time::Duration;

    use infinity_agent_core::ThreadId;
    use infinity_agent_core::message::InputMessage;

    use super::{UNKNOWN_OUTCOME, answers};
    use crate::error::BoxError;
    use crate::policy::{Cap, Limits};
    use crate::store::conversation::Conversations;
    use crate::store::db::Db;
    use crate::store::operations::Operations;
    use crate::testing::{self, Dirs, attach, within};
    use crate::tools;

    /// Each run of the command appends a line here.
    const RUNS: &str = "runs";

    fn root() -> ThreadId {
        ThreadId::from("root".to_owned())
    }

    /// The daemon dies after dispatching a shell call. On restart the call
    /// gets its captured result, or an unknown outcome when none was
    /// captured, under the same call ID; the command never runs again.
    #[tokio::test(flavor = "current_thread")]
    async fn interrupted_shell_calls_resolve_without_running_again() -> Result<(), BoxError> {
        let cases = [
            // Died after the result was captured and delivered, before the model's reply saved it.
            ("echo run >> runs", true, "exit code 0"),
            // Died while the command ran.
            ("echo run >> runs; sleep 1; echo run >> runs", false, UNKNOWN_OUTCOME),
        ];

        for (command, wait_for_result, expected) in cases {
            let dirs = Dirs::new();
            let db = Db::in_memory()?;
            Conversations::new(db.clone()).register_root(&root()).await?;

            // Each `within` has its own `LocalSet`; dropping it stops the first daemon's tasks.
            within(first_daemon(&db, &dirs, command, wait_for_result)).await?;
            within(second_daemon(&db, expected)).await?;

            tokio::time::sleep(Duration::from_millis(1500)).await;
            let runs = fs::read_to_string(dirs.workspace.join(RUNS))?;
            assert_eq!(runs.lines().count(), 1, "`{command}` ran again or outlived the first daemon");
        }

        Ok(())
    }

    async fn first_daemon(db: &Db, dirs: &Dirs, command: &str, wait_for_result: bool) -> Result<(), BoxError> {
        let policy = testing::policy(&[Cap::Shell], Limits::default());
        let mut harness = testing::start_sandboxed(db, policy, Some(Arc::new(dirs.bubblewrap()))).await?;
        let handle = attach(&mut harness, &root()).await?;

        handle.send(InputMessage::user_text(root(), "run it"), "input-1").await?;
        harness.model.next_request().await;
        harness.model.send_tool_call("call-1", "shell", serde_json::json!({ "command": command }));
        harness.model.finish();

        if wait_for_result {
            harness.model.next_request().await;
            return Ok(());
        }

        while !dirs.workspace.join(RUNS).exists() {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }

        Ok(())
    }

    async fn second_daemon(db: &Db, expected: &str) -> Result<(), BoxError> {
        let mut harness = testing::start(db).await?;
        let sender = harness.system.sender();

        for answer in answers(&Operations::new(db.clone())).await? {
            tools::deliver(&sender, answer.thread, answer.id, answer.call_id, answer.text).await?;
        }

        // A result under any other call ID would be ignored, and this would time out.
        let request = harness.model.next_request().await;
        assert!(format!("{:?}", request.chat_history).contains(expected), "the model did not see `{expected}`");

        Ok(())
    }
}
