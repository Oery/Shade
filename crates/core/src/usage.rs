//! The `shade usage` report: what each conversation has cost in tokens.

use chrono::DateTime;

use crate::store::usage::ConversationUsage;

/// Conversations shown, most recently active first.
pub const SHOWN: i64 = 20;

const NOTE: &str = "
input counts every token sent, cached ones included. context is the latest
request's input: roughly what the next message resends. cached reads 0 while
rig 0.31 does not report it on streaming responses.
";

pub fn render(conversations: &[ConversationUsage]) -> String {
    if conversations.is_empty() {
        return "No model requests recorded yet.\n".to_owned();
    }

    let mut text = format!(
        "{:<32} {:>8} {:>12} {:>10} {:>10} {:>10}  {}\n",
        "conversation", "requests", "input", "cached", "output", "context", "last request (UTC)"
    );

    for conversation in conversations {
        text.push_str(&row(conversation));
    }

    text.push_str(NOTE);

    text
}

fn row(conversation: &ConversationUsage) -> String {
    let name = match &conversation.name {
        Some(name) => name.to_owned(),
        None => format!("unnamed {}", conversation.root.get(..8).unwrap_or(&conversation.root)),
    };

    let last = match DateTime::from_timestamp(conversation.last_at, 0) {
        Some(time) => time.format("%Y-%m-%d %H:%M").to_string(),
        None => String::new(),
    };

    format!(
        "{name:<32} {:>8} {:>12} {:>10} {:>10} {:>10}  {last}\n",
        conversation.requests,
        conversation.input_tokens,
        conversation.cached_input_tokens,
        conversation.output_tokens,
        conversation.last_input_tokens,
    )
}
