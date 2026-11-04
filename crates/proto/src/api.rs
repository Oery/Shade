//! Version 1 client API. These types do not expose Infinity's wire format.

use serde::{Deserialize, Serialize};

pub const DEFAULT_PAGE: usize = 50;
pub const MAX_PAGE: usize = 100;
pub const MAX_TEXT_BYTES: usize = 64 * 1024;

#[derive(Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CreateConversation {
    pub name: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SendMessage {
    pub text: String,
    pub client_message_id: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TurnStatus {
    Queued,
    Running,
    Completed,
    Failed,
    Interrupted,
}

impl TurnStatus {
    pub fn name(self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Running => "running",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Interrupted => "interrupted",
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Turn {
    pub id: String,
    pub conversation_id: String,
    pub text: String,
    pub status: TurnStatus,
    pub received_at: i64,
    pub started_at: Option<i64>,
    pub finished_at: Option<i64>,
    pub error: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Conversation {
    pub id: String,
    pub source: String,
    pub name: Option<String>,
    pub created_at: i64,
    pub activity: i64,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Page<T> {
    pub items: Vec<T>,
    pub next_cursor: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Approval {
    pub code: String,
    pub text: String,
    pub created_at: i64,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct ConversationDetail {
    pub conversation: Conversation,
    pub active_turn: Option<Turn>,
    pub queued_turns: Vec<Turn>,
    pub approvals: Vec<Approval>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct HistoryMessage {
    /// The row's ordinal, not the provider completion ID.
    pub id: String,
    /// The accepted input this message belongs to, when tracked.
    pub turn_id: Option<String>,
    #[serde(flatten)]
    pub content: HistoryContent,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum HistoryContent {
    User { text: String },
    Assistant { text: String },
    ToolStarted { name: String },
    ToolFinished,
    Approval { code: String, text: String },
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Decision {
    Approve,
    Reject,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DecideApproval {
    pub decision: Decision,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct DecisionResult {
    pub text: String,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Event {
    /// Subscribe atomically, then send this snapshot before live events.
    Snapshot {
        active_turn: Option<String>,
        partial_text: String,
        pending_tools: usize,
    },
    TurnStatus {
        turn_id: String,
        status: TurnStatus,
        error: Option<String>,
    },
    TextDelta {
        turn_id: String,
        text: String,
    },
    ToolStarted {
        turn_id: String,
        name: String,
    },
    /// Infinity does not identify the call in its live result event.
    ToolFinished {
        turn_id: String,
    },
    Approval {
        turn_id: String,
        code: String,
        text: String,
    },
    ApprovalDecided {
        code: String,
        decision: Decision,
    },
}

#[derive(Debug, Deserialize, Serialize)]
pub struct ApiError {
    pub error: ErrorDetail,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct ErrorDetail {
    pub code: String,
    pub message: String,
}
