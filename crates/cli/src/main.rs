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
    Audit,
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
        Command::Audit => Request::Audit,
        Command::Usage => Request::Usage,
        Command::Backup { path } => Request::Backup { path: absolute(path)? },
    };

    client::request(&request)
}

/// The daemon has a different working directory, so send it absolute paths.
fn absolute(path: PathBuf) -> Result<PathBuf, Error> {
    match std::path::absolute(&path) {
        Ok(absolute) => Ok(absolute),
        Err(source) => Err(Error::Path { path, source }),
    }
}
