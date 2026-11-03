//! `shell`: one command in the sandbox, answered asynchronously.
//!
//! The call returns at once and the command runs on the core's `LocalSet`
//! (the sandbox must be started from a long-lived thread). Its result is
//! recorded before it is delivered, so a restart redelivers it instead of
//! running the command again. A user message that interrupts the call does
//! not stop the command; Infinity ignores its late result.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use infinity_agent_core::ThreadId;
use infinity_agent_core::tools::{Tool, ToolContext};
use infinity_agent_core::traits::InputSender;
use rap_protocol::{ProviderCallId, ToolCallId};
use serde::Deserialize;

use crate::authority::Authority;
use crate::error::BoxError;
use crate::policy::Cap;
use crate::recovery::UNKNOWN_OUTCOME;
use crate::sandbox::{Access, Exit, Finished, Sandbox};
use crate::store::operations::Operations;
use crate::tools;

/// Used when the call does not ask for a timeout.
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30);

pub struct Shell {
    work: Arc<Work>,
    description: String,
}

/// What a running call needs, shared with its task.
struct Work {
    sandbox: Arc<Sandbox>,
    authority: Authority,
    operations: Operations,
}

/// Where a result goes.
struct Call<M> {
    sender: M,
    thread: ThreadId,
    id: String,
    call_id: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Args {
    command: String,
    timeout_seconds: Option<u64>,
}

impl Shell {
    /// `network` only shapes the description; dispatch checks the grant again.
    pub fn new(sandbox: Arc<Sandbox>, authority: Authority, operations: Operations, network: bool) -> Self {
        let description = describe(&sandbox, network);

        Self { work: Arc::new(Work { sandbox, authority, operations }), description }
    }
}

#[async_trait]
impl<M: InputSender + 'static> Tool<M> for Shell {
    fn name(&self) -> &str {
        Cap::Shell.name()
    }

    fn description(&self) -> &str {
        &self.description
    }

    fn parameters(&self) -> serde_json::Value {
        let max = self.work.sandbox.max_timeout().as_secs();

        serde_json::json!({
            "type": "object",
            "properties": {
                "command": { "type": "string", "description": "Run with `sh -c` in /workspace." },
                "timeout_seconds": {
                    "type": "integer",
                    "minimum": 1,
                    "maximum": max,
                    "description": format!("Defaults to {}.", DEFAULT_TIMEOUT.as_secs()),
                },
            },
            "required": ["command"],
            "additionalProperties": false,
        })
    }

    async fn execute(
        &self, args: serde_json::Value, id: ToolCallId, call_id: Option<ProviderCallId>, context: &ToolContext<M>,
    ) -> Result<(), BoxError> {
        let call = Call {
            sender: context.message_sender.clone(),
            thread: context.group_id.clone(),
            id: id.into_inner(),
            call_id: call_id.map(ProviderCallId::into_inner),
        };

        let work = Arc::clone(&self.work);
        tokio::task::spawn_local(async move { work.answer(args, call).await });

        Ok(())
    }
}

impl Work {
    async fn answer<M: InputSender>(&self, args: serde_json::Value, call: Call<M>) {
        let text = self.run(args, &call).await;

        if let Err(error) = tools::deliver(&call.sender, call.thread, call.id, call.call_id, text).await {
            tracing::warn!(%error, "cannot deliver a shell result; restart recovery will");
        }
    }

    async fn run<M>(&self, args: serde_json::Value, call: &Call<M>) -> String {
        let access = match self.authority.admit_tool(&call.thread, Cap::Shell).await {
            Ok(authorized) => Access { network: authorized.caps.contains(&Cap::ShellNetwork), vault: false },
            Err(denied) => return format!("Error: {denied}"),
        };

        let args: Args = match serde_json::from_value(args) {
            Ok(args) => args,
            Err(error) => return format!("Error: invalid arguments: {error}"),
        };

        let timeout = match self.timeout(args.timeout_seconds) {
            Ok(timeout) => timeout,
            Err(message) => return message,
        };

        let thread = call.thread.as_str();

        if let Err(answer) = tools::record(&self.operations, thread, &call.id, Cap::Shell.name()).await {
            return answer;
        }

        let text = match self.sandbox.run(&args.command, access, timeout).await {
            Ok(finished) => render(&finished, timeout),
            Err(error) => format!("Error: {error}"),
        };

        if let Err(error) = self.operations.finish(thread, &call.id, &text).await {
            tracing::error!(%error, %thread, "cannot record a shell result");
            return UNKNOWN_OUTCOME.to_owned();
        }

        text
    }

    fn timeout(&self, requested: Option<u64>) -> Result<Duration, String> {
        let max = self.sandbox.max_timeout();

        let Some(seconds) = requested else {
            return Ok(DEFAULT_TIMEOUT.min(max));
        };

        if seconds == 0 || seconds > max.as_secs() {
            return Err(format!("Error: timeout_seconds must be between 1 and {}", max.as_secs()));
        }

        Ok(Duration::from_secs(seconds))
    }
}

fn describe(sandbox: &Sandbox, network: bool) -> String {
    let network = if network {
        "Network access is available."
    } else {
        "There is no network access."
    };

    format!(
        "Run a shell command in an isolated Linux sandbox. /workspace is the working directory and persists between calls; \
         /tmp and processes do not. {network} Runs for at most {} seconds unless timeout_seconds allows up to {}; \
         when the command ends, every process it started is killed. Returns the exit code and the start and end of \
         stdout and stderr. A pipeline's exit code is its last command's; start with `set -o pipefail;` to fail \
         on any stage.",
        DEFAULT_TIMEOUT.as_secs(),
        sandbox.max_timeout().as_secs()
    )
}

fn render(finished: &Finished, timeout: Duration) -> String {
    let mut text = match finished.exit {
        Exit::Code(code) => format!("exit code {code}"),
        Exit::Killed => "killed by a signal".to_owned(),
        Exit::TimedOut => format!("timed out after {} seconds; every process it started was killed", timeout.as_secs()),
    };

    for (name, output) in [("stdout", &finished.stdout), ("stderr", &finished.stderr)] {
        if !output.is_empty() {
            text.push_str(&format!("\n--- {name} ---\n{}", output.text()));
        }
    }

    text
}
