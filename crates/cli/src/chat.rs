//! Interactive terminal chat: one daemon request per line.
//!
//! Closing the client mid-reply does not cancel the turn; the daemon finishes
//! it and keeps it in the conversation.

use std::io::{BufRead, Write};

use shade_proto::Request;

use crate::client;
use crate::error::Error;

const NEW: &str = "/new";

pub fn run(conversation: String) -> Result<(), Error> {
    eprintln!("Conversation `{conversation}`. {NEW} starts a fresh one, Ctrl-D quits.");

    let mut stdin = std::io::stdin().lock();
    let mut line = String::new();

    loop {
        prompt()?;
        line.clear();

        if stdin.read_line(&mut line)? == 0 {
            println!();

            return Ok(());
        }

        let text = line.trim();

        if text.is_empty() {
            continue;
        }

        let request = match text {
            NEW => Request::NewConversation { conversation: conversation.clone() },
            _ => Request::Chat { conversation: conversation.clone(), text: text.to_owned() },
        };

        // A failed turn is reported and the chat goes on; losing the daemon ends it.
        match client::request(&request) {
            Ok(()) => {}
            Err(Error::Daemon(message)) => eprintln!("error: {message}"),
            Err(error) => return Err(error),
        }
    }
}

fn prompt() -> Result<(), Error> {
    let mut stdout = std::io::stdout();
    stdout.write_all(b"> ")?;
    stdout.flush()?;

    Ok(())
}
