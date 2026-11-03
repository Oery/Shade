//! Discord: owner chat from anywhere.
//!
//! Only allowlisted users are heard, in DMs and in server channels where they
//! mention the bot. Each channel is one conversation. Replies in a server
//! channel are visible to everyone who can read it, so private work belongs
//! in DMs. The bot never pings anyone: replies allow no mentions.
//!
//! Discord gives a bot the content of DMs and of messages that mention it
//! without the privileged Message Content intent, so the bot does not ask for it.

use std::num::NonZeroU64;
use std::sync::{Arc, OnceLock};

use serenity::all::{
    ChannelId, Client, Context, CreateAllowedMentions, CreateMessage, EventHandler, GatewayIntents, Http, Message, MessageId, Ready,
    ShardManager, Typing,
};
use serenity::async_trait;
use shade_proto::Response;
use thiserror::Error;
use tokio::sync::mpsc;

use crate::config::DiscordConfig;
use crate::router::turn::Step;
use crate::router::{Inbound, Outbound, REPLY_QUEUE};
use crate::secrets::Secrets;
use crate::store::roots::{Adapter, Target};

/// Discord's message limit is 2000 characters; counting UTF-16 units stays
/// under it however Discord counts.
const MESSAGE_LIMIT: usize = 2000;

/// Most reply text kept between posts. The rest of a runaway reply is dropped.
const MAX_BUFFER: usize = 16 * 1024;

/// Typed in Discord to start a fresh conversation in the channel.
const NEW_CONVERSATION: &str = "/new";

#[derive(Debug, Error)]
pub enum DiscordError {
    #[error("[discord] owners is empty; list the Discord user IDs allowed to chat")]
    NoOwners,

    #[error("{0} ([discord] token_env) is not set in the environment or the .env file next to the config")]
    MissingToken(String),

    #[error("cannot set up the Discord client: {0}")]
    Client(Box<serenity::Error>),
}

/// A connected client, not yet listening.
pub struct Discord {
    client: Client,
}

impl Discord {
    pub async fn connect(config: DiscordConfig, secrets: &mut Secrets, inbound: mpsc::Sender<Inbound>) -> Result<Self, DiscordError> {
        if config.owners.is_empty() {
            return Err(DiscordError::NoOwners);
        }

        let Some(token) = secrets.take(&config.token_env) else {
            return Err(DiscordError::MissingToken(config.token_env));
        };

        let handler = Handler { inbound, owners: config.owners, bot: OnceLock::new() };
        let intents = GatewayIntents::DIRECT_MESSAGES | GatewayIntents::GUILD_MESSAGES;
        match Client::builder(&token, intents).event_handler(handler).await {
            Ok(client) => Ok(Self { client }),
            Err(source) => Err(DiscordError::Client(Box::new(source))),
        }
    }

    /// For posting replies to resumed work before the gateway is up.
    pub fn http(&self) -> Arc<Http> {
        Arc::clone(&self.client.http)
    }

    pub fn shards(&self) -> Arc<ShardManager> {
        Arc::clone(&self.client.shard_manager)
    }

    /// Listens until the shards shut down. Serenity reconnects on its own.
    pub async fn run(mut self) {
        if let Err(error) = self.client.start().await {
            tracing::error!(%error, "the Discord adapter stopped");
        }
    }
}

struct Handler {
    inbound: mpsc::Sender<Inbound>,
    owners: Vec<u64>,
    /// The bot's own user ID, known once connected.
    bot: OnceLock<u64>,
}

#[async_trait]
impl EventHandler for Handler {
    async fn ready(&self, _context: Context, ready: Ready) {
        let _ = self.bot.set(ready.user.id.get());
        tracing::info!(user = %ready.user.name, "connected to Discord");
    }

    async fn message(&self, context: Context, message: Message) {
        let Some(&bot) = self.bot.get() else {
            return;
        };

        let incoming = Incoming {
            author: message.author.id.get(),
            from_bot: message.author.bot,
            in_guild: message.guild_id.is_some(),
            mentions_bot: message.mentions_user_id(bot),
            content: &message.content,
            attachments: message.attachments.iter().map(|attachment| attachment.url.as_str()).collect(),
        };

        let Some(text) = admit(&incoming, bot, &self.owners) else {
            return;
        };

        if text == NEW_CONVERSATION {
            self.new_conversation(&context.http, message.channel_id).await;
            return;
        }

        let answering = incoming.in_guild.then_some(message.id);
        let reply = Reply::new(Arc::clone(&context.http), message.channel_id, answering);
        let target = Target::Named(message.channel_id.to_string());
        let source_key = Some(format!("discord:{}", message.id));
        let prompt = Inbound::Prompt { adapter: Adapter::Discord, target, text, source_key, outbound: Outbound::Discord(reply) };

        if self.inbound.send(prompt).await.is_err() {
            tracing::warn!("dropping a Discord message: the daemon is shutting down");
        }
    }
}

impl Handler {
    async fn new_conversation(&self, http: &Arc<Http>, channel: ChannelId) {
        let (reply, mut replies) = mpsc::channel(REPLY_QUEUE);
        let input = Inbound::NewConversation { adapter: Adapter::Discord, key: channel.to_string(), reply };

        if self.inbound.send(input).await.is_err() {
            return;
        }

        while let Some(response) = replies.recv().await {
            let text = match response {
                Response::Text { text } => text,
                Response::Error { message } => format!("⚠️ {message}"),
                Response::Done => return,
            };

            post(http, channel, &text, None).await;
        }
    }
}

/// A Discord message, reduced to what decides whether it is heard.
struct Incoming<'a> {
    author: u64,
    from_bot: bool,
    in_guild: bool,
    mentions_bot: bool,
    content: &'a str,
    attachments: Vec<&'a str>,
}

/// The text to act on, or `None` when the message is not for Shade: from a
/// bot (this one included), from anyone not allowlisted, or in a server
/// channel without a mention. Attachments become their URLs.
fn admit(incoming: &Incoming<'_>, bot: u64, owners: &[u64]) -> Option<String> {
    if incoming.from_bot || !owners.contains(&incoming.author) {
        return None;
    }

    if incoming.in_guild && !incoming.mentions_bot {
        return None;
    }

    let content = incoming.content.replace(&format!("<@{bot}>"), "").replace(&format!("<@!{bot}>"), "");
    let mut text = content.trim().to_owned();

    for url in &incoming.attachments {
        if !text.is_empty() {
            text.push('\n');
        }

        text.push_str(url);
    }

    if text.is_empty() { None } else { Some(text) }
}

/// Where one turn's reply goes, and the reply so far.
pub struct Reply {
    http: Arc<Http>,
    channel: ChannelId,
    /// The message answered, in server channels, so the reply sits under it.
    answering: Option<MessageId>,
    buffer: String,
    truncated: bool,
    /// Shows "typing…" until dropped.
    typing: Option<Typing>,
}

impl Reply {
    pub fn new(http: Arc<Http>, channel: ChannelId, answering: Option<MessageId>) -> Self {
        let typing = Some(channel.start_typing(&http));

        Self { http, channel, answering, buffer: String::new(), truncated: false, typing }
    }

    /// The reply for resumed work in a Discord conversation, from its key.
    pub fn to_channel(http: Arc<Http>, key: &str) -> Option<Self> {
        let id: NonZeroU64 = key.parse().ok()?;

        Some(Self::new(http, ChannelId::from(id), None))
    }

    /// Text is posted when a completion ends, so a reply cut short by newer
    /// input is never presented as finished.
    pub async fn send(&mut self, step: Step) {
        let last = step.is_last();

        match step {
            Step::Text(text) => self.append(&text),
            Step::Paused | Step::Done => self.flush().await,
            Step::Failed(message) => {
                self.flush().await;
                post(&self.http, self.channel, &format!("⚠️ {message}"), self.answering.take()).await;
            }
            Step::Superseded => self.buffer.clear(),
        }

        if last {
            self.typing = None;
        }
    }

    fn append(&mut self, text: &str) {
        let room = MAX_BUFFER.saturating_sub(self.buffer.len());
        let kept = &text[..text.floor_char_boundary(room)];
        self.buffer.push_str(kept);
        self.truncated |= kept.len() < text.len();
    }

    async fn flush(&mut self) {
        if self.truncated {
            self.buffer.push_str("\n[… the rest of this reply was too long and was dropped]");
            self.truncated = false;
        }

        for chunk in chunks(&self.buffer, MESSAGE_LIMIT) {
            post(&self.http, self.channel, &chunk, self.answering.take()).await;
        }

        self.buffer.clear();
    }
}

async fn post(http: &Http, channel: ChannelId, text: &str, answering: Option<MessageId>) {
    let mut message = CreateMessage::new().content(text).allowed_mentions(CreateAllowedMentions::new().replied_user(false));

    if let Some(id) = answering {
        message = message.reference_message((channel, id));
    }

    if let Err(error) = channel.send_message(http, message).await {
        tracing::warn!(%error, %channel, "cannot post to Discord");
    }
}

/// Splits `text` into messages of at most `limit` UTF-16 units, at line
/// breaks where possible and between characters otherwise. Blank chunks are
/// dropped: Discord refuses empty messages.
fn chunks(text: &str, limit: usize) -> Vec<String> {
    let mut chunks = Vec::new();
    let mut current = String::new();
    let mut length = 0;

    for line in text.split_inclusive('\n') {
        let line_length: usize = line.encode_utf16().count();

        if length + line_length > limit && length > 0 {
            chunks.push(std::mem::take(&mut current));
            length = 0;
        }

        // A line longer than a whole message is cut between characters.
        for character in line.chars() {
            if length + character.len_utf16() > limit {
                chunks.push(std::mem::take(&mut current));
                length = 0;
            }

            current.push(character);
            length += character.len_utf16();
        }
    }

    chunks.push(current);
    chunks.retain(|chunk| !chunk.trim().is_empty());

    chunks
}

#[cfg(test)]
mod tests {
    use super::{Incoming, admit, chunks};

    #[test]
    fn chunks_respect_lines_and_characters() {
        let cases: [(&str, &[&str]); 7] = [
            ("ab\ncd\nef", &["ab\n", "cd\nef"]),
            ("abcdefghijkl", &["abcde", "fghij", "kl"]),
            // Multi-byte characters count once and are never cut.
            ("ééééééé", &["ééééé", "éé"]),
            // Characters outside the BMP take two UTF-16 units, as Discord may count them.
            ("😀😀😀", &["😀😀", "😀"]),
            // A long line after a short one starts its own message.
            ("ab\ncdefgh", &["ab\n", "cdefg", "h"]),
            ("abcde\n\n\n", &["abcde"]),
            ("", &[]),
        ];

        for (text, expected) in cases {
            assert_eq!(chunks(text, 5), expected, "{text:?}");
        }
    }

    #[test]
    fn only_owners_are_heard() {
        const BOT: u64 = 9;
        const OWNER: u64 = 1;

        let message = |author, from_bot, in_guild, mentions_bot, content| Incoming {
            author,
            from_bot,
            in_guild,
            mentions_bot,
            content,
            attachments: Vec::new(),
        };

        let cases = [
            (message(OWNER, false, false, false, "hi"), Some("hi")),
            (message(2, false, false, false, "hi"), None),
            // A bot, even one listed as an owner, never drives Shade (no loops with itself).
            (message(OWNER, true, false, false, "hi"), None),
            // In a server channel, only messages that mention the bot.
            (message(OWNER, false, true, false, "hi"), None),
            (message(OWNER, false, true, true, "<@9> hi"), Some("hi")),
            (message(OWNER, false, true, true, "<@!9>   "), None),
        ];

        for (incoming, expected) in cases {
            assert_eq!(admit(&incoming, BOT, &[OWNER]).as_deref(), expected, "{:?}", incoming.content);
        }

        let with_file = Incoming { attachments: vec!["https://cdn/x.png"], ..message(OWNER, false, true, true, "<@9>") };
        assert_eq!(admit(&with_file, BOT, &[OWNER]).as_deref(), Some("https://cdn/x.png"));
    }
}
