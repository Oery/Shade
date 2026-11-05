//! What a server's tools and results look like to Shade and the model.

use rmcp::model::{CallToolResult, ContentBlock, Resource, ResourceContents, ResourceTemplate};
use serde_json::Value;

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
/// is left out, and linked resources are named for the model to read.
pub fn answer(result: CallToolResult) -> Answer {
    let mut parts = Vec::new();

    for block in result.content {
        parts.push(match block {
            ContentBlock::Text(text) => text.text,
            ContentBlock::Image(_) => "[an image was left out]".to_owned(),
            ContentBlock::Audio(_) => "[audio was left out]".to_owned(),
            ContentBlock::Resource(embedded) => content(embedded.resource),
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

/// What a server publishes, one line each, as the model reads it.
pub fn listing(resources: Vec<Resource>, templates: Vec<ResourceTemplate>) -> String {
    let mut lines = Vec::new();

    for resource in resources {
        lines.push(line(&resource.uri, &resource.name, resource.description.as_deref()));
    }

    for template in templates {
        lines.push(format!(
            "template {}",
            line(&template.uri_template, &template.name, template.description.as_deref())
        ));
    }

    if lines.is_empty() {
        return "(no resources)".to_owned();
    }

    bounded(lines.join("\n"))
}

fn line(uri: &str, name: &str, description: Option<&str>) -> String {
    match description {
        Some(description) => format!("{uri} ({name}): {description}"),
        None => format!("{uri} ({name})"),
    }
}

/// A resource as the model reads it: its text, with binary content named.
pub fn contents(contents: Vec<ResourceContents>) -> Answer {
    let parts: Vec<String> = contents.into_iter().map(content).collect();

    if parts.is_empty() {
        return Answer::Done("(empty resource)".to_owned());
    }

    Answer::Done(bounded(parts.join("\n")))
}

fn content(content: ResourceContents) -> String {
    match content {
        ResourceContents::TextResourceContents { text, .. } => text,
        ResourceContents::BlobResourceContents { uri, .. } => format!("[binary content of {uri} was left out]"),
        _ => "[content Shade cannot show was left out]".to_owned(),
    }
}

/// The JSON snapshot of `uri`: its one text entry. Anything else is an error,
/// and so is a snapshot too large to hand to a run whole.
pub fn snapshot(contents: Vec<ResourceContents>, uri: &str) -> Result<Value, String> {
    let mut texts = contents.into_iter().filter_map(|content| match content {
        ResourceContents::TextResourceContents { uri: found, text, .. } if found == uri => Some(text),
        _ => None,
    });

    let (Some(text), None) = (texts.next(), texts.next()) else {
        return Err(format!("`{uri}` must have exactly one text entry"));
    };

    if text.len() > MAX_RESULT {
        return Err(format!("`{uri}` is {} bytes, more than the {MAX_RESULT} a snapshot may be", text.len()));
    }

    match serde_json::from_str(&text) {
        Ok(value) => Ok(value),
        Err(error) => Err(format!("`{uri}` is not JSON: {error}")),
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
    use rmcp::model::ResourceContents;
    use serde_json::json;

    use super::{MAX_RESULT, bounded, snapshot};

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

    /// A snapshot is one whole JSON entry for that URI, or an error: a
    /// watch must never evaluate another resource, or JSON cut short.
    #[test]
    fn snapshots_are_one_whole_entry() {
        let text = |text: &str, uri: &str| ResourceContents::text(text, uri);
        let blob = ResourceContents::BlobResourceContents { uri: "a".to_owned(), mime_type: None, blob: "e30=".to_owned(), meta: None };
        let large = format!("\"{}\"", "a".repeat(MAX_RESULT));

        let cases = [
            (vec![text("{\"n\":1}", "b"), text("2", "a")], Ok(json!(2))),
            (vec![text("1", "a"), text("2", "a")], Err(())),
            (vec![text("1", "b")], Err(())),
            (vec![blob], Err(())),
            (vec![], Err(())),
            (vec![text("{", "a")], Err(())),
            (vec![text(&large, "a")], Err(())),
        ];

        for (contents, expected) in cases {
            assert_eq!(snapshot(contents, "a").map_err(drop), expected);
        }
    }
}
