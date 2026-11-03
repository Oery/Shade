//! The system prompt the model sees.
//!
//! Infinity 0.2.0 puts its own coding-agent prompt (thread spawning,
//! sandboxes, steering files: features Shade turns off) in front of every
//! request, then a line with thread IDs, then the instructions Shade gives
//! the thread. It offers no way to replace that prompt, so Shade rewrites
//! the preamble of each request: its own short identity, then its own
//! instructions. If the layout ever differs, the preamble is left as it is.

/// Who the model is, before the instructions for this conversation or job.
const IDENTITY: &str = "You are Shade, a personal assistant working for one owner. You act only through the tools you \
                        are given, and only within what they allow. Say plainly what you did and did not do.";

/// Starts the line Infinity puts between its prompt and Shade's instructions.
const THREAD_LINE: &str = "\n\nYour current thread ID is `";

/// Shade's preamble in place of Infinity's: the identity, then Shade's own
/// instructions for the thread, if any.
pub fn preamble(infinity: &str) -> Option<String> {
    let (_, rest) = infinity.split_once(THREAD_LINE)?;

    match rest.split_once("\n\n") {
        Some((_, instructions)) => Some(format!("{IDENTITY}\n\n{instructions}")),
        None => Some(IDENTITY.to_owned()),
    }
}

#[cfg(test)]
mod tests {
    use super::{IDENTITY, preamble};

    /// Infinity's prompt and thread line go; Shade's instructions stay, and a
    /// layout Shade does not recognize is left alone.
    #[test]
    fn infinity_prompt_is_replaced() {
        let thread = "Your current thread ID is `a`. The root thread ID is `b`.";
        let with = format!("You are Infinity.\n\n## Threads\n\n{thread}\n\nYou are chatting.\n\nBe brief.");
        let without = format!("You are Infinity.\n\n{thread}");

        let cases = [
            (with.as_str(), Some(format!("{IDENTITY}\n\nYou are chatting.\n\nBe brief."))),
            (without.as_str(), Some(IDENTITY.to_owned())),
            ("Some other layout.", None),
        ];

        for (infinity, expected) in cases {
            assert_eq!(preamble(infinity), expected);
        }
    }
}
