mod chat;
mod client;
mod error;

use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Parser, Subcommand};
use shade_proto::Request;

use crate::error::Error;

/// Shade client. Every command talks to the running `shaded` daemon.
#[derive(Parser)]
#[command(name = "shade", version)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Chat in the terminal.
    Chat {
        /// Conversation to resume or create.
        #[arg(long, default_value = shade_proto::DEFAULT_CONVERSATION)]
        thread: String,
    },
    /// Run one prompt in a fresh conversation.
    Ask { prompt: String },
    /// Manage scheduled jobs.
    #[command(subcommand)]
    Schedule(Manage),
    /// Manage resource watches.
    #[command(subcommand)]
    Watch(Manage),
    /// Inspect conversations.
    #[command(subcommand)]
    Threads(Threads),
    /// Show the audit log.
    Audit {
        /// Activity within this duration: seconds, minutes, hours or days.
        #[arg(long, default_value = "1d", value_parser = duration)]
        since: u64,
        /// Inspect one root ID, as printed by audit or usage.
        #[arg(long)]
        root: Option<String>,
    },
    /// Show model and tool usage.
    Usage,
    /// Back up the database to a new file.
    Backup { path: PathBuf },
}

#[derive(Subcommand)]
enum Manage {
    /// List definitions.
    List,
    /// Stop creating new runs.
    Pause { id: String },
    /// Resume creating new runs.
    Resume { id: String },
    /// Delete a definition.
    Rm { id: String },
    /// Apply a change owner chat proposed, by the code in its receipt.
    Approve { code: String },
    /// Drop a change owner chat proposed.
    Reject { code: String },
}

#[derive(Subcommand)]
enum Threads {
    /// List conversations.
    List,
}

impl From<Manage> for shade_proto::Manage {
    fn from(manage: Manage) -> Self {
        match manage {
            Manage::List => Self::List,
            Manage::Pause { id } => Self::Pause { id },
            Manage::Resume { id } => Self::Resume { id },
            Manage::Rm { id } => Self::Rm { id },
            Manage::Approve { code } => Self::Approve { code },
            Manage::Reject { code } => Self::Reject { code },
        }
    }
}

fn main() -> ExitCode {
    let cli = Cli::parse();

    match run(cli) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("error: {error}");
            ExitCode::FAILURE
        }
    }
}

fn run(cli: Cli) -> Result<(), Error> {
    let request = match cli.command {
        Command::Chat { thread } => return chat::run(thread),
        Command::Ask { prompt } => Request::Ask { prompt },
        Command::Schedule(action) => Request::Schedule { action: action.into() },
        Command::Watch(action) => Request::Watch { action: action.into() },
        Command::Threads(Threads::List) => Request::ThreadsList,
        Command::Audit { since, root } => Request::Audit { since: since_timestamp(since), root },
        Command::Usage => Request::Usage,
        Command::Backup { path } => Request::Backup { path: absolute(path)? },
    };

    client::request(&request)
}

fn duration(value: &str) -> Result<u64, String> {
    let Some((unit, number)) = value.as_bytes().split_last() else {
        return Err("use a duration such as 30m, 12h or 1d".to_owned());
    };

    let multiplier = match unit {
        b's' => 1,
        b'm' => 60,
        b'h' => 3600,
        b'd' => 86400,
        _ => return Err("use a duration such as 30m, 12h or 1d".to_owned()),
    };

    let number = match std::str::from_utf8(number).ok().and_then(|number| number.parse::<u64>().ok()) {
        Some(number) if number > 0 => number,
        _ => return Err("duration must be a positive integer followed by s, m, h or d".to_owned()),
    };

    match number.checked_mul(multiplier) {
        Some(seconds) if seconds <= i64::MAX as u64 => Ok(seconds),
        _ => Err("duration is too large".to_owned()),
    }
}

fn since_timestamp(seconds: u64) -> i64 {
    let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_secs();

    i64::try_from(now.saturating_sub(seconds)).unwrap_or(i64::MAX)
}

/// The daemon has a different working directory, so send it absolute paths.
fn absolute(path: PathBuf) -> Result<PathBuf, Error> {
    match std::path::absolute(&path) {
        Ok(absolute) => Ok(absolute),
        Err(source) => Err(Error::Path { path, source }),
    }
}

#[cfg(test)]
mod tests {
    use super::duration;

    #[test]
    fn audit_durations_are_checked_without_overflow() {
        for (input, expected) in [("1s", 1), ("30m", 1800), ("12h", 43200), ("1d", 86400)] {
            assert_eq!(duration(input), Ok(expected));
        }

        for input in ["", "1", "0h", "-1d", "1.5h", "10w", "18446744073709551615d", "é"] {
            assert!(duration(input).is_err(), "{input}");
        }
    }
}
