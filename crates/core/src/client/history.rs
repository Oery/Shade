use infinity_agent_core::message::InfinityMessage;
use infinity_provider_protocol::message::{AssistantContent, UserContent};
use shade_proto::api::{HistoryContent, HistoryMessage, Page};
use shade_store::chat::StoredMessage;

pub(super) fn project(page: Page<StoredMessage>) -> Page<HistoryMessage> {
    let mut items = Vec::new();

    for row in page.items {
        if let Some(content) = content(row.message) {
            items.push(HistoryMessage { id: row.ordinal.to_string(), turn_id: row.turn_id, content });
        }
    }

    Page { items, next_cursor: page.next_cursor }
}

fn content(message: InfinityMessage) -> Option<HistoryContent> {
    match message {
        InfinityMessage::User { content: UserContent::Text(text) } => Some(HistoryContent::User { text: text.text }),
        InfinityMessage::Assistant { content: AssistantContent::Text(text) } => Some(HistoryContent::Assistant { text: text.text }),
        InfinityMessage::ToolCall { call, .. } => Some(HistoryContent::ToolStarted { name: call.function.name }),
        InfinityMessage::ToolResult { display_segments: Some(segments), .. } => match crate::tools::shown(segments) {
            Some(shown) => Some(HistoryContent::Approval { code: shown.approval, text: shown.card.text() }),
            None => Some(HistoryContent::ToolFinished),
        },
        InfinityMessage::ToolResult { .. } => Some(HistoryContent::ToolFinished),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use infinity_provider_protocol::message::{Reasoning, Text, ToolCall, ToolResult, ToolResultContent};
    use shade_schedule::receipt::{Card, Proposal, Verb};

    use super::*;
    use crate::error::BoxError;
    use crate::tools::Shown;

    #[test]
    fn history_omits_reasoning_and_tool_bodies_and_keeps_real_cards() -> Result<(), BoxError> {
        let card = Shown { card: Card::example(Verb::Proposed(Proposal::New)), approval: "approve-me".to_owned() };
        let mut segments = crate::tools::display(&card).ok_or("cannot encode card")?;
        segments.push(rap_protocol::DisplaySegment::Text("private tool result".to_owned()));
        let reasoning =
            InfinityMessage::Assistant { content: AssistantContent::Reasoning(Reasoning::new_with_signature("private reasoning", None)) };
        let tool = InfinityMessage::ToolCall {
            call: ToolCall::new("id", "shell", serde_json::json!({"secret": "private arguments"})),
            display_as: None,
        };
        let result = InfinityMessage::ToolResult {
            result: ToolResult {
                id: "id".to_owned(),
                call_id: None,
                content: vec![ToolResultContent::Text(Text { text: "private result".to_owned() })],
            },
            display_segments: Some(segments),
        };
        let page = Page {
            items: vec![
                StoredMessage { ordinal: 0, turn_id: None, message: reasoning },
                StoredMessage { ordinal: 1, turn_id: None, message: tool },
                StoredMessage { ordinal: 2, turn_id: None, message: result },
            ],
            next_cursor: Some("2".to_owned()),
        };
        let projected = project(page);
        let json = serde_json::to_string(&projected)?;
        assert!(!json.contains("private"), "{json}");
        assert!(json.contains("approve-me") && json.contains("shell"), "{json}");
        assert_eq!(projected.items[0].id, "1");
        assert_eq!(projected.next_cursor.as_deref(), Some("2"));

        Ok(())
    }
}
