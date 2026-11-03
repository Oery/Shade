//! `now`: the current time in the configured time zone.

use async_trait::async_trait;
use chrono::Utc;
use chrono_tz::Tz;
use infinity_agent_core::tools::{Tool, ToolContext};
use infinity_agent_core::traits::InputSender;
use infinity_provider_protocol::{Text, ToolResult, ToolResultContent};
use rap_protocol::{ProviderCallId, ToolCallId};
use shade_policy::Cap;

use crate::authority::Authority;
use crate::error::BoxError;

pub struct Now {
    authority: Authority,
    timezone: Tz,
}

impl Now {
    pub fn new(authority: Authority, timezone: Tz) -> Self {
        Self { authority, timezone }
    }
}

#[async_trait]
impl<M: InputSender> Tool<M> for Now {
    fn name(&self) -> &str {
        Cap::Now.name()
    }

    fn description(&self) -> &str {
        "Current date, time, and time zone of the owner."
    }

    fn parameters(&self) -> serde_json::Value {
        serde_json::json!({ "type": "object", "properties": {} })
    }

    /// Never called: the answer is immediate, so the tool runs synchronously.
    async fn execute(
        &self, _args: serde_json::Value, _id: ToolCallId, _call_id: Option<ProviderCallId>, _context: &ToolContext<M>,
    ) -> Result<(), BoxError> {
        Err("`now` only runs synchronously".into())
    }

    fn supports_sync(&self) -> bool {
        true
    }

    async fn execute_synchronous(
        &self, _args: &serde_json::Value, id: &ToolCallId<str>, call_id: Option<&ProviderCallId<str>>, context: &ToolContext<M>,
    ) -> Option<ToolResult> {
        let text = match self.authority.admit_tool(&context.group_id, &Cap::Now).await {
            Ok(_) => {
                let now = Utc::now().with_timezone(&self.timezone);
                format!("{} ({})", now.format("%A %Y-%m-%d %H:%M:%S %:z"), self.timezone.name())
            }
            Err(denied) => format!("Error: {denied}"),
        };

        let call_id = call_id.map(|call_id| call_id.as_str().to_owned());

        Some(ToolResult { id: id.as_str().to_owned(), call_id, content: vec![ToolResultContent::Text(Text { text })] })
    }
}
