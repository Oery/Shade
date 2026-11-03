//! What a server's tools and results look like to Shade and the model.

use rmcp::model::{CallToolResult, ContentBlock};

use crate::{Answer, Tool};

/// Most of a result the model is given, in bytes. The rest is cut, and the
/// model is told how much.
pub const MAX_RESULT: usize = 16 * 1024;

pub fn tool(tool: rmcp::model::Tool) -> Tool {
    let description = tool.description.map(|text| text.into_owned()).unwrap_or_default();
    let input_schema = serde_json::Value::Object(tool.input_schema.as_ref().to_owned());

    Tool { name: tool.name.into_owned(), description, input_schema }
}

/// A result as text: its text blocks, else its structured content. Media
/// and resources are named, not inlined.
pub fn answer(result: CallToolResult) -> Answer {
    let mut parts = Vec::new();

    for block in result.content {
        parts.push(match block {
            ContentBlock::Text(text) => text.text,
            ContentBlock::Image(_) => "[an image was left out]".to_owned(),
            ContentBlock::Audio(_) => "[audio was left out]".to_owned(),
            ContentBlock::Resource(_) => "[an embedded resource was left out]".to_owned(),
            ContentBlock::ResourceLink(link) => format!("[resource: {}]", link.uri),
            _ => "[content Shade cannot show was left out]".to_owned(),
        });
    }

    let text = match (parts.is_empty(), result.structured_content) {
        (false, _) => parts.join("\n"),
        (true, Some(structured)) => structured.to_string(),
        (true, None) => "(no output)".to_owned(),
    };

    match result.is_error {
        Some(true) => Answer::Failed(bounded(text)),
        _ => Answer::Done(bounded(text)),
    }
}

/// `text` cut to `MAX_RESULT` bytes on a character boundary, saying how much was left out.
fn bounded(mut text: String) -> String {
    if text.len() <= MAX_RESULT {
        return text;
    }

    let omitted = text.len() - text.floor_char_boundary(MAX_RESULT);
    text.truncate(text.floor_char_boundary(MAX_RESULT));
    text.push_str(&format!("\n[… {omitted} more bytes were left out]"));

    text
}

#[cfg(test)]
mod tests {
    use super::{MAX_RESULT, bounded};

    /// A long result is cut between characters, and says so.
    #[test]
    fn long_results_are_cut_whole() {
        let cases = [("é".repeat(MAX_RESULT), MAX_RESULT, MAX_RESULT), ("a".repeat(MAX_RESULT + 1), MAX_RESULT, 1)];

        for (text, kept, omitted) in cases {
            let cut = bounded(text);
            let (head, note) = cut.split_at(kept);

            assert!(head.chars().all(|c| c == 'é' || c == 'a'), "{note}");
            assert_eq!(note, format!("\n[… {omitted} more bytes were left out]"));
        }
    }
}
