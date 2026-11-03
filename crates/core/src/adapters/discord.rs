//! Discord: owner chat from anywhere.
//!
//! Only allowlisted users are heard, in DMs and in server channels where they
//! mention the bot. Each channel is one conversation. Replies in a server
//! channel are visible to everyone who can read it, so private work belongs
//! in DMs. The bot never pings anyone: replies allow no mentions.
//!
//! Notifications go to the first owner's DMs, which are also that owner's
//! DM conversation with the bot.
//!
//! `/new` is a slash command, and a change waiting for approval carries
//! Approve and Reject buttons (see `interactions`).
//!
//! Discord gives a bot a role with its own name, and "@Shade" in autocomplete
//! may pick that role instead of the bot. A mention of the bot's role counts
//! as a mention of the bot. Discord withholds the text of such messages
//! unless the bot holds the privileged Message Content intent, so it does.

use std::num::NonZeroU64;
use std::sync::{Arc, OnceLock};

mod interactions;

use serenity::all::{
    ChannelId, Client, Context, CreateAllowedMentions, CreateEmbed, CreateEmbedAuthor, CreateEmbedFooter, CreateMessage, EventHandler,
    GatewayIntents, Http, HttpError, Interaction, Message, MessageId, Ready, ShardManager, Typing, UserId,
};
use serenity::async_trait;
use shade_policy::channels::Destination;
use shade_schedule::receipt::{Card, Verb};
use shade_store::roots::{Adapter, Target};

use thiserror::Error;
use tokio::sync::mpsc;

use crate::config::DiscordConfig;
use crate::notify::{Failure, Notice};
use crate::router::turn::Step;
use crate::router::{Inbound, Outbound};
use crate::secrets::Secrets;
use crate::tools::Shown;

/// Discord's message limit is 2000 characters; counting UTF-16 units stays
/// under it however Discord counts.
const MESSAGE_LIMIT: usize = 2000;

/// Most reply text kept between posts. The rest of a runaway reply is dropped.
const MAX_BUFFER: usize = 16 * 1024;

#[derive(Debug, Error)]
pub enum DiscordError {
    #[error("[discord] owners is empty; list the Discord user IDs allowed to chat")]
    NoOwners,

    #[error("[discord] owners starts with 0, which is not a Discord user ID; the first owner receives notifications")]
    InvalidOwner,

    #[error("{0} ([discord] token_env) is not set in the environment or the .env file next to the config")]
    MissingToken(String),

    #[error("cannot set up the Discord client: {0}")]
    Client(Box<serenity::Error>),
}

/// A connected client, not yet listening.
pub struct Discord {
    client: Client,
    /// Receives notifications.
    owner: UserId,
}

impl Discord {
    pub async fn connect(config: DiscordConfig, secrets: &mut Secrets, inbound: mpsc::Sender<Inbound>) -> Result<Self, DiscordError> {
        let Some(&first) = config.owners.first() else {
            return Err(DiscordError::NoOwners);
        };

        let Some(owner) = NonZeroU64::new(first) else {
            return Err(DiscordError::InvalidOwner);
        };

        let Some(token) = secrets.take(&config.token_env) else {
            return Err(DiscordError::MissingToken(config.token_env));
        };

        let handler = Handler { inbound, owners: config.owners, bot: OnceLock::new() };
        let intents = GatewayIntents::DIRECT_MESSAGES | GatewayIntents::GUILD_MESSAGES | GatewayIntents::MESSAGE_CONTENT;
        match Client::builder(&token, intents).event_handler(handler).await {
            Ok(client) => Ok(Self { client, owner: UserId::from(owner) }),
            Err(source) => Err(DiscordError::Client(Box::new(source))),
        }
    }

    /// For posting replies to resumed work before the gateway is up.
    pub fn http(&self) -> Arc<Http> {
        Arc::clone(&self.client.http)
    }

    /// Posts each notice to the owner's DMs until every notifier is gone.
    /// Works before the gateway is up, like replies to resumed work.
    pub fn notices(&self, queue: mpsc::Receiver<Notice>) -> impl Future<Output = ()> + Send + 'static {
        post_notices(self.http(), self.owner, queue)
    }

    pub fn shards(&self) -> Arc<ShardManager> {
        Arc::clone(&self.client.shard_manager)
    }

    /// Listens until the shards shut down. Serenity reconnects on its own.
    pub async fn run(mut self) {
        if let Err(error) = self.client.start().await {
            tracing::error!(%error, "the Discord adapter stopped; if Discord refused its intents, enable Message Content for the bot");
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
    /// Slash commands are registered on the first connection only, not on each reconnect.
    async fn ready(&self, context: Context, ready: Ready) {
        tracing::info!(user = %ready.user.name, "connected to Discord");

        if self.bot.set(ready.user.id.get()).is_ok() {
            interactions::register(&context.http).await;
        }
    }

    async fn interaction_create(&self, context: Context, interaction: Interaction) {
        self.interaction(context, interaction).await;
    }

    async fn message(&self, context: Context, message: Message) {
        let Some(&bot) = self.bot.get() else {
            return;
        };

        let bot_role = self.mentioned_bot_role(&context.http, &message, bot).await;

        let incoming = Incoming {
            author: message.author.id.get(),
            from_bot: message.author.bot,
            in_guild: message.guild_id.is_some(),
            mentions_bot: message.mentions_user_id(bot) || bot_role.is_some(),
            bot_role,
            content: &message.content,
            attachments: message.attachments.iter().map(|attachment| attachment.url.as_str()).collect(),
        };

        let Some(text) = admit(&incoming, bot, &self.owners) else {
            ignored(&message, incoming.mentions_bot);
            return;
        };

        let answering = incoming.in_guild.then_some(message.id);
        let reply = Reply::new(Arc::clone(&context.http), message.channel_id, answering);
        let target = match incoming.in_guild {
            true => Target::Named(message.channel_id.to_string()),
            false => Target::Direct(message.channel_id.to_string()),
        };
        let source_key = Some(format!("discord:{}", message.id));
        let prompt = Inbound::Prompt { adapter: Adapter::Discord, target, text, source_key, outbound: Outbound::Discord(reply) };

        if self.inbound.send(prompt).await.is_err() {
            tracing::warn!("dropping a Discord message: the daemon is shutting down");
        }
    }
}

impl Handler {
    /// The bot's own role, when an owner's message mentions it.
    async fn mentioned_bot_role(&self, http: &Http, message: &Message, bot: u64) -> Option<u64> {
        let guild = message.guild_id?;

        if message.mention_roles.is_empty() || !self.owners.contains(&message.author.id.get()) {
            return None;
        }

        let roles = match guild.roles(http).await {
            Ok(roles) => roles,
            Err(error) => {
                tracing::warn!(%error, %guild, "cannot look up the bot's role");
                return None;
            }
        };

        let role = roles.values().find(|role| role.tags.bot_id.is_some_and(|id| id.get() == bot))?;

        message.mention_roles.contains(&role.id).then(|| role.id.get())
    }
}

/// Says why nothing answered, without logging what was said.
fn ignored(message: &Message, mentions_bot: bool) {
    tracing::debug!(
        author = %message.author.id,
        channel = %message.channel_id,
        from_bot = message.author.bot,
        in_guild = message.guild_id.is_some(),
        mentions_bot,
        mentions_roles = message.mention_roles.len(),
        mentions_everyone = message.mention_everyone,
        length = message.content.len(),
        "ignoring a Discord message",
    );
}

/// A Discord message, reduced to what decides whether it is heard.
struct Incoming<'a> {
    author: u64,
    from_bot: bool,
    in_guild: bool,
    mentions_bot: bool,
    /// The bot's role, when the message mentions it.
    bot_role: Option<u64>,
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

    let mut content = incoming.content.replace(&format!("<@{bot}>"), "").replace(&format!("<@!{bot}>"), "");

    if let Some(role) = incoming.bot_role {
        content = content.replace(&format!("<@&{role}>"), "");
    }

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
            Step::Shown(shown) => {
                self.flush().await;
                self.show(shown).await;
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

    /// Posts a change waiting for approval: Shade's card, with the buttons under it.
    async fn show(&mut self, shown: Shown) {
        let Shown { card, approval } = shown;
        let message = CreateMessage::new().embed(embed(&card)).components(interactions::approval_buttons(&approval));

        send(&self.http, self.channel, addressed(message, self.channel, self.answering.take())).await;
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

/// Tells each notice's sender whether Discord accepted it. An error
/// response means nothing was posted; any other error may hide a message
/// that did arrive, and the sender says so.
async fn post_notices(http: Arc<Http>, owner: UserId, mut queue: mpsc::Receiver<Notice>) {
    while let Some(Notice { text, destination, ack }) = queue.recv().await {
        let message = CreateMessage::new().content(text).allowed_mentions(CreateAllowedMentions::new());

        let sent = match destination {
            Destination::Owner => owner.direct_message(http.as_ref(), message).await,
            Destination::Channel(id) => ChannelId::from(id).send_message(http.as_ref(), message).await,
        };

        let result = match sent {
            Ok(_) => Ok(()),
            Err(serenity::Error::Http(HttpError::UnsuccessfulRequest(response))) => Err(Failure::Refused(response.error.message)),
            Err(error) => Err(Failure::Unknown(error.to_string())),
        };

        let _ = ack.send(result);
    }
}

async fn post(http: &Http, channel: ChannelId, text: &str, answering: Option<MessageId>) {
    send(http, channel, message(text, channel, answering)).await;
}

/// A message that pings nobody, under the one it answers if any.
fn message(text: &str, channel: ChannelId, answering: Option<MessageId>) -> CreateMessage {
    addressed(CreateMessage::new().content(text), channel, answering)
}

fn addressed(message: CreateMessage, channel: ChannelId, answering: Option<MessageId>) -> CreateMessage {
    let message = message.allowed_mentions(CreateAllowedMentions::new().replied_user(false));

    match answering {
        Some(id) => message.reference_message((channel, id)),
        None => message,
    }
}

async fn send(http: &Http, channel: ChannelId, message: CreateMessage) {
    if let Err(error) = channel.send_message(http, message).await {
        tracing::warn!(%error, %channel, "cannot post to Discord");
    }
}

/// A job card: what changed above the title, the prompt as the description.
fn embed(card: &Card) -> CreateEmbed {
    let colour = match card.verb {
        Verb::Created => 0x57_F2_87,
        Verb::Updated => 0x58_65_F2,
        Verb::Deleted => 0x99_AA_B5,
        Verb::Proposed(_) => 0xFE_E7_5C,
    };

    let label = format!("{} {}", card.icon(), card.label().trim_end_matches(':'));
    let embed = CreateEmbed::new().author(CreateEmbedAuthor::new(label)).title(&card.title).colour(colour);

    if card.verb == Verb::Deleted {
        return embed;
    }

    let embed = embed
        .description(&card.prompt)
        .field("When", &card.cadence, true)
        .field("Next run", card.next_run(), true)
        .field("Posts to", card.posts_to(), true)
        .field("Can use", card.can_use(), true);

    match &card.file {
        Some(file) => embed.footer(CreateEmbedFooter::new(format!("Saved in {file}"))),
        None => embed,
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
            bot_role: None,
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

        // "@Shade" may resolve to the bot's role; it is stripped like the bot's own mention.
        let by_role = Incoming { bot_role: Some(5), ..message(OWNER, false, true, true, "<@&5> hi <@&6>") };
        assert_eq!(admit(&by_role, BOT, &[OWNER]).as_deref(), Some("hi <@&6>"));

        let with_file = Incoming { attachments: vec!["https://cdn/x.png"], ..message(OWNER, false, true, true, "<@9>") };
        assert_eq!(admit(&with_file, BOT, &[OWNER]).as_deref(), Some("https://cdn/x.png"));
    }
}
