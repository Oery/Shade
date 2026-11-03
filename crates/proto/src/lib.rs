//! Control protocol between the `shade` client and the `shaded` daemon.
//!
//! Messages are newline-delimited JSON. A connection starts with a [`Hello`],
//! then the client sends one [`Request`] and the daemon answers with zero or
//! more [`Response::Text`] chunks and one final `Done` or `Error`. Terminal
//! chat opens one connection per line, so the daemon keeps no session state. This crate must stay free of runtime dependencies so the
//! client builds without Infinity, SQLite, or Tokio.

use std::ffi::OsString;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

/// Sent in the hello of every connection. Client and daemon must match.
pub const PROTOCOL_VERSION: u32 = 2;

/// Longest accepted message line in bytes, newline included.
pub const MAX_LINE_BYTES: u64 = 1 << 20;

/// Longest conversation name in bytes.
pub const MAX_CONVERSATION_BYTES: usize = 64;

/// The conversation `shade chat` uses without `--thread`.
pub const DEFAULT_CONVERSATION: &str = "default";

const SOCKET_FILE: &str = "shade.sock";

#[derive(Debug, Serialize, Deserialize)]
pub struct Hello {
    pub version: u32,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Request {
    /// One-shot prompt in a fresh conversation.
    Ask {
        prompt: String,
    },
    /// One line of terminal chat in a named, long-lived conversation.
    Chat {
        conversation: String,
        text: String,
    },
    /// Points `conversation` at a fresh one. The previous history is kept.
    NewConversation {
        conversation: String,
    },
    Schedule {
        action: Manage,
    },
    Watch {
        action: Manage,
    },
    ThreadsList,
    Audit,
    Usage,
    /// Absolute destination path, resolved by the client.
    Backup {
        path: PathBuf,
    },
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Manage {
    List,
    Pause {
        id: String,
    },
    Resume {
        id: String,
    },
    Rm {
        id: String,
    },
    /// Applies a change owner chat proposed, by its code.
    Approve {
        code: String,
    },
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Response {
    /// A streamed piece of the reply.
    Text {
        text: String,
    },
    Done,
    Error {
        message: String,
    },
}

/// Conversation names are short, non-empty, and free of control characters,
/// so they print safely and fit in logs.
pub fn valid_conversation(name: &str) -> bool {
    !name.is_empty() && name.len() <= MAX_CONVERSATION_BYTES && !name.chars().any(char::is_control)
}

/// `$XDG_DATA_HOME/shade`, else `~/.local/share/shade`.
///
/// Both binaries resolve it the same way, so they agree on the socket path.
pub fn data_dir() -> Option<PathBuf> {
    xdg_dir(std::env::var_os("XDG_DATA_HOME"), std::env::var_os("HOME"), ".local/share")
}

/// `$XDG_CONFIG_HOME/shade/config.toml`, else `~/.config/shade/config.toml`.
pub fn config_file() -> Option<PathBuf> {
    let dir = xdg_dir(std::env::var_os("XDG_CONFIG_HOME"), std::env::var_os("HOME"), ".config")?;

    Some(dir.join("config.toml"))
}

pub fn socket_path(data_dir: &Path) -> PathBuf {
    data_dir.join(SOCKET_FILE)
}

/// Relative or empty values are ignored, as the XDG specification requires.
fn xdg_dir(xdg_home: Option<OsString>, home: Option<OsString>, home_default: &str) -> Option<PathBuf> {
    if let Some(dir) = absolute(xdg_home) {
        return Some(dir.join("shade"));
    }

    let home = absolute(home)?;

    Some(home.join(home_default).join("shade"))
}

fn absolute(value: Option<OsString>) -> Option<PathBuf> {
    let path = PathBuf::from(value?);

    if path.is_absolute() { Some(path) } else { None }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn conversation_names_are_bounded() {
        let longest = "é".repeat(MAX_CONVERSATION_BYTES / 2);
        let too_long = format!("{longest}x");

        let cases = [
            ("default", true),
            ("", false),
            (longest.as_str(), true),
            (too_long.as_str(), false),
            ("line\nbreak", false),
            ("bell\u{7}", false),
            ("work notes", true),
        ];

        for (name, valid) in cases {
            assert_eq!(super::valid_conversation(name), valid, "{name:?}");
        }
    }

    #[test]
    fn xdg_dir_skips_unusable_values() {
        let fallback = Some(PathBuf::from("/home/me/.local/share/shade"));
        let cases = [
            (Some("relative"), Some("/home/me"), fallback.clone()),
            (Some(""), Some("/home/me"), fallback),
            (None, Some("relative"), None),
            (Some(""), Some(""), None),
            (None, None, None),
        ];

        for (xdg, home, expected) in cases {
            let actual = xdg_dir(xdg.map(OsString::from), home.map(OsString::from), ".local/share");

            assert_eq!(actual, expected, "XDG_DATA_HOME={xdg:?} HOME={home:?}");
        }
    }
}
