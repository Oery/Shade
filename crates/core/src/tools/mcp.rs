//! MCP tools: each tool of a granted server, offered as `<server>_<tool>`.
//!
//! A call is admitted against the server's capability, recorded before it
//! is sent, and its result recorded before the model sees it. A restart
//! replays a recorded result; a call whose answer was lost is reported as
//! unknown and never sent again. Whether a retry is safe is for the model
//! to check with the server's own reads.

use std::sync::Arc;

use async_trait::async_trait;
use infinity_agent_core::ThreadId;
use infinity_agent_core::tools::{Tool, ToolContext};
use infinity_agent_core::traits::InputSender;
use rap_protocol::{ProviderCallId, ToolCallId};
use shade_mcp::{Answer, CallError, Server};
use shade_policy::Cap;
use shade_store::operations::Operations;

use crate::authority::Authority;
use crate::error::BoxError;
use crate::recovery::UNKNOWN_OUTCOME;
use crate::tools;

/// Longest tool name model providers accept.
const MAX_NAME: usize = 64;

pub struct McpTool {
    name: String,
    description: String,
    parameters: serde_json::Value,
    work: Arc<Work>,
}

/// What a running call needs, shared with its task.
struct Work {
    authority: Authority,
    operations: Operations,
    server: Arc<Server>,
    /// The tool's own name on its server.
    remote: String,
    /// The name the model calls it by.
    name: String,
}

/// Where a result goes.
struct Call<M> {
    sender: M,
    thread: ThreadId,
    id: String,
    call_id: Option<String>,
}

impl McpTool {
    /// `None` when the tool's name cannot be offered: too long once prefixed.
    pub fn new(authority: Authority, operations: Operations, server: &Arc<Server>, tool: shade_mcp::Tool) -> Option<Self> {
        let Some(name) = tool_name(server.name(), &tool.name) else {
            tracing::warn!(server = %server.name(), tool = %tool.name, "MCP tool name too long to offer");
            return None;
        };

        let description = format!("[{}] {}", server.name(), tool.description);
        let work = Work { authority, operations, server: Arc::clone(server), remote: tool.name, name: name.to_owned() };

        Some(Self { name, description, parameters: tool.input_schema, work: Arc::new(work) })
    }
}

#[async_trait]
impl<M: InputSender + 'static> Tool<M> for McpTool {
    fn name(&self) -> &str {
        &self.name
    }

    fn description(&self) -> &str {
        &self.description
    }

    fn parameters(&self) -> serde_json::Value {
        self.parameters.clone()
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
        let text = self.authority.redact(self.run(args, &call).await);

        if let Err(error) = tools::deliver(&call.sender, call.thread, call.id, call.call_id, text).await {
            tracing::warn!(%error, tool = %self.name, "cannot deliver an MCP result; restart recovery will");
        }
    }

    async fn run<M>(&self, args: serde_json::Value, call: &Call<M>) -> String {
        let cap = Cap::Server(self.server.name().to_owned());

        if let Err(denied) = self.authority.admit_tool(&call.thread, &cap).await {
            return format!("Error: {denied}");
        }

        let thread = call.thread.as_str();

        if let Err(answer) = tools::record(&self.operations, thread, &call.id, &self.name).await {
            return answer;
        }

        let text = match self.server.call(&self.remote, args).await {
            Ok(Answer::Done(text)) => text,
            Ok(Answer::Failed(text)) => format!("Error: {text}"),
            Err(error @ CallError::Unavailable(..)) => format!("Error: {error}"),
            Err(error @ CallError::Unknown(..)) => format!("Error: {error}. Check its state before retrying a change."),
        };
        let text = self.authority.redact(text);

        if let Err(error) = self.operations.finish(thread, &call.id, &text).await {
            tracing::error!(%error, %thread, tool = %self.name, "cannot record an MCP result");
            return UNKNOWN_OUTCOME.to_owned();
        }

        text
    }
}

/// `<server>_<tool>`, with characters model providers refuse replaced by `_`.
fn tool_name(server: &str, tool: &str) -> Option<String> {
    let tool: String = tool
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '_' || c == '-' {
                c
            } else {
                '_'
            }
        })
        .collect();
    let name = format!("{server}_{tool}");

    (name.len() <= MAX_NAME).then_some(name)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use infinity_agent_core::ThreadId;
    use rmcp::{ServerHandler, ServiceExt, tool, tool_handler, tool_router};
    use shade_mcp::Server;
    use shade_policy::{Cap, Limits};
    use shade_store::conversation::Conversations;
    use shade_store::db::Db;
    use shade_store::grants::Grants;
    use shade_store::operations::Operations;

    use super::{Call, McpTool};
    use crate::authority::Authority;
    use crate::error::BoxError;
    use crate::redact::Redactor;
    use crate::testing;

    /// An MCP server that counts the calls it receives.
    #[derive(Clone)]
    struct Echo {
        calls: Arc<AtomicUsize>,
    }

    #[tool_router]
    impl Echo {
        #[tool(description = "Returns a secret to test redaction.")]
        async fn secret(&self) -> String {
            "token=private-test-key-123456".to_owned()
        }

        #[tool(description = "Counts its calls.")]
        async fn count(&self) -> String {
            format!("call {}", self.calls.fetch_add(1, Ordering::SeqCst) + 1)
        }

        #[tool(description = "Answers too late.")]
        async fn slow(&self) -> String {
            self.calls.fetch_add(1, Ordering::SeqCst);
            tokio::time::sleep(Duration::from_secs(5)).await;
            "late".to_owned()
        }
    }

    #[tool_handler]
    impl ServerHandler for Echo {}

    /// The `echo` server's tools for owner chat holding `caps`, and its call counter.
    async fn tools(caps: &[Cap]) -> Result<(Vec<McpTool>, Arc<AtomicUsize>), BoxError> {
        let (client, server) = tokio::io::duplex(64 * 1024);
        let calls = Arc::new(AtomicUsize::new(0));
        let echo = Echo { calls: Arc::clone(&calls) };

        tokio::spawn(async move {
            if let Ok(running) = echo.serve(server).await {
                let _ = running.waiting().await;
            }
        });

        let server = Arc::new(Server::connected("echo", client, Duration::from_millis(300)).await?);
        let db = Db::in_memory()?;
        Conversations::new(db.clone()).register_root(&ThreadId::from("chat".to_owned())).await?;

        let redactor = Arc::new(Redactor::new(["private-test-key-123456".to_owned()]));
        let authority = Authority::new(Grants::new(db.clone()), testing::policy(caps, Limits::default())).with_redactor(redactor);
        let operations = Operations::new(db);
        let tools = server.tools().await.into_iter();

        Ok((
            tools.filter_map(|tool| McpTool::new(authority.clone(), operations.clone(), &server, tool)).collect(),
            calls,
        ))
    }

    async fn call(tool: &McpTool, id: &str) -> String {
        let call = Call { sender: (), thread: ThreadId::from("chat".to_owned()), id: id.to_owned(), call_id: None };

        tool.work.run(serde_json::json!({}), &call).await
    }

    fn named<'a>(tools: &'a [McpTool], name: &str) -> Result<&'a McpTool, BoxError> {
        match tools.iter().find(|tool| tool.name == name) {
            Some(tool) => Ok(tool),
            None => Err(format!("no tool {name}").into()),
        }
    }

    /// A server's tools are refused, before anything is sent, to a root
    /// that does not hold the server's capability.
    #[tokio::test]
    async fn calls_need_the_servers_capability() -> Result<(), BoxError> {
        let (tools, calls) = tools(&[Cap::Now]).await?;

        let refused = call(named(&tools, "echo_count")?, "call-1").await;

        assert!(refused.contains("`echo` is not granted"), "{refused}");
        assert_eq!(calls.load(Ordering::SeqCst), 0);

        Ok(())
    }

    /// A call is sent once: dispatched again under its ID, it gets its
    /// recorded answer, and one whose answer never came stays unknown.
    #[tokio::test]
    async fn calls_are_never_sent_twice() -> Result<(), BoxError> {
        let (tools, calls) = tools(&[Cap::Server("echo".to_owned())]).await?;
        let (count, slow) = (named(&tools, "echo_count")?, named(&tools, "echo_slow")?);

        assert_eq!(call(count, "call-1").await, "call 1");
        assert_eq!(call(count, "call-1").await, "call 1");

        let unknown = call(slow, "call-2").await;
        assert!(unknown.contains("did not answer") && unknown.contains("may have taken effect"), "{unknown}");
        assert_eq!(call(slow, "call-2").await, unknown);

        assert_eq!(calls.load(Ordering::SeqCst), 2, "a call was sent twice");

        Ok(())
    }

    #[tokio::test]
    async fn secrets_are_removed_before_recording_and_delivery() -> Result<(), BoxError> {
        let (tools, _) = tools(&[Cap::Server("echo".to_owned())]).await?;
        let tool = named(&tools, "echo_secret")?;
        let text = call(tool, "call-secret").await;
        let stored = tool.work.operations.captured("chat", "call-secret").await?;

        assert_eq!(text, "token=[redacted: one of Shade's secrets]");
        assert_eq!(stored.as_deref(), Some(text.as_str()));
        assert_eq!(call(tool, "call-secret").await, text);

        Ok(())
    }
}
