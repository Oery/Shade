//! Owner-facing summaries. Never display raw tool arguments or results.

use chrono::DateTime;
use shade_policy::{Grant, Origin, Policy};
use shade_store::audit::Root;
use shade_store::usage::ConversationUsage;

use crate::usage::money;

pub fn render(root: &Root, policy: &Policy, usage: &[ConversationUsage]) -> String {
    let mut text = format!(
        "root={} {}:{} run={} status={}\n  grant={} effective={}\n",
        safe(&root.id),
        safe(&root.origin),
        safe(&root.source),
        safe(root.run.as_deref().unwrap_or("-")),
        safe(&root.status),
        safe(root.caps.as_deref().unwrap_or("current policy")),
        effective(root, policy)
    );

    for usage in usage {
        text.push_str(&format!(
            "  all-time usage: requests={} input={} cached={} output={} USD~{}\n",
            usage.requests,
            usage.input_tokens,
            usage.cached_input_tokens,
            usage.output_tokens,
            money(usage.estimated_usd)
        ));
    }

    for entry in &root.entries {
        let at = DateTime::from_timestamp(entry.at, 0).map(|at| at.format("%Y-%m-%d %H:%M:%S").to_string()).unwrap_or_default();
        text.push_str(&format!(
            "  {at} UTC thread={} {}: {}\n",
            safe(&entry.thread),
            safe(&entry.kind),
            safe(&entry.summary)
        ));
    }

    text
}

fn effective(root: &Root, policy: &Policy) -> String {
    let Some(origin) = Origin::parse(&root.origin) else {
        return "invalid origin".to_owned();
    };

    let caps = match &root.caps {
        Some(json) => match serde_json::from_str(json) {
            Ok(caps) => Some(caps),
            Err(_) => return "invalid grant".to_owned(),
        },
        None => None,
    };

    let grant = Grant { origin, source: root.source.to_owned(), caps };

    match policy.effective(&grant) {
        Some(caps) => caps.iter().map(|cap| safe(cap.name())).collect::<Vec<_>>().join(", "),
        None => "source removed".to_owned(),
    }
}

/// One bounded terminal-safe line, including untrusted tool/server names.
pub fn safe(text: &str) -> String {
    let text = &text[..text.floor_char_boundary(512)];

    text.chars().map(|c| if c.is_control() { ' ' } else { c }).collect()
}
