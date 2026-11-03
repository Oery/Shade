//! Redaction of Shade's own secrets from what tools return.
//!
//! Shade knows every secret it loaded: the model's API key, the Discord
//! token, hook tokens, the variables MCP servers get. A shell command or
//! MCP server that prints one would otherwise put it in the conversation's
//! history, in front of the model, and in the provider's logs. Each
//! occurrence is replaced before the result is recorded or delivered.
//! Values shorter than `MIN_SECRET` are not treated as secrets: they would
//! match ordinary text. Only exact, whole values are known; a secret cut by
//! output truncation, or re-encoded, is not recognized.

/// Shorter values would match ordinary text.
const MIN_SECRET: usize = 8;

const MARK: &str = "[redacted: one of Shade's secrets]";

#[derive(Default)]
pub struct Redactor {
    /// Longest first, so a secret that contains another is replaced whole.
    secrets: Vec<String>,
}

impl Redactor {
    pub fn new(values: impl IntoIterator<Item = String>) -> Self {
        let mut secrets: Vec<String> = values.into_iter().filter(|value| value.len() >= MIN_SECRET).collect();
        secrets.sort_by(|left, right| right.len().cmp(&left.len()).then_with(|| left.cmp(right)));
        secrets.dedup();

        Self { secrets }
    }

    pub fn redact(&self, text: String) -> String {
        let mut rest = text.as_str();
        let mut clean = String::new();

        while let Some((start, end)) = self.next(rest) {
            clean.push_str(&rest[..start]);
            clean.push_str(MARK);
            rest = &rest[end..];
        }

        if clean.is_empty() {
            return text;
        }

        clean.push_str(rest);

        clean
    }

    /// Merge overlapping matches, so replacing one cannot leave a fragment
    /// of another known secret. Inserted markers are never searched again.
    fn next(&self, text: &str) -> Option<(usize, usize)> {
        let start = self.secrets.iter().filter_map(|secret| text.find(secret)).min()?;
        let mut end = start;

        for (offset, _) in text[start..].char_indices() {
            let at = start + offset;

            if at > end {
                break;
            }

            for secret in &self.secrets {
                if text[at..].starts_with(secret) {
                    end = end.max(at + secret.len());
                }
            }
        }

        Some((start, end))
    }
}

#[cfg(test)]
mod tests {
    use super::{MARK, Redactor};

    /// Whole known values are replaced wherever they appear, longest
    /// first; values too short to be secrets are left alone.
    #[test]
    fn known_secrets_are_replaced() {
        let redactor = Redactor::new(["sk-abcdefgh".to_owned(), "sk-abcdefgh-extra".to_owned(), "short".to_owned()]);

        let cases = [
            ("key=sk-abcdefgh", format!("key={MARK}")),
            ("a sk-abcdefgh-extra b sk-abcdefgh", format!("a {MARK} b {MARK}")),
            ("a short word", "a short word".to_owned()),
            ("sk-abcdefg", "sk-abcdefg".to_owned()),
        ];

        for (text, expected) in cases {
            assert_eq!(redactor.redact(text.to_owned()), expected, "{text}");
        }
    }

    #[test]
    fn overlapping_secrets_and_markers_are_not_rescanned() {
        let redactor = Redactor::new(["abcdefghij".to_owned(), "cdefghijkl".to_owned(), "redacted".to_owned()]);

        assert_eq!(redactor.redact("abcdefghijkl".to_owned()), MARK);
        assert_eq!(redactor.redact("abcdefghij redacted".to_owned()), format!("{MARK} {MARK}"));
    }
}
