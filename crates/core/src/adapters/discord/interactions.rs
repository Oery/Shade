//! Discord interactions: the `/new` slash command, and the buttons that
//! approve or reject a change owner chat proposed.
//!
//! Only owners may use them; anyone else gets a private refusal. They never
//! reach the model, so it cannot approve its own change.

use serenity::all::{
    ButtonStyle, Command, CommandInteraction, ComponentInteraction, Context, CreateActionRow, CreateButton, CreateCommand,
    CreateInteractionResponse, CreateInteractionResponseMessage, EditInteractionResponse, Http, Interaction, User,
};
use shade_proto::{Manage, Response};
use shade_store::roots::Adapter;
use tokio::sync::mpsc;

use super::{Handler, MESSAGE_LIMIT};
use crate::router::{Inbound, REPLY_QUEUE};

const NEW: &str = "new";
const APPROVE: &str = "approve:";
const REJECT: &str = "reject:";

/// The buttons under a change waiting for approval. They carry its code,
/// so they work across restarts for as long as the change waits.
pub fn approval_buttons(code: &str) -> Vec<CreateActionRow> {
    let approve = CreateButton::new(format!("{APPROVE}{code}")).label("Approve").style(ButtonStyle::Success);
    let reject = CreateButton::new(format!("{REJECT}{code}")).label("Reject").style(ButtonStyle::Danger);

    vec![CreateActionRow::Buttons(vec![approve, reject])]
}

/// Registers the slash commands, replacing any the bot had.
pub async fn register(http: &Http) {
    let new = CreateCommand::new(NEW).description("Start a fresh conversation in this channel. The previous one is kept.");

    if let Err(error) = Command::set_global_commands(http, vec![new]).await {
        tracing::warn!(%error, "cannot register Discord slash commands");
    }
}

impl Handler {
    pub(super) async fn interaction(&self, context: Context, interaction: Interaction) {
        match interaction {
            Interaction::Command(command) => self.slash_command(&context.http, command).await,
            Interaction::Component(component) => self.button(&context.http, component).await,
            _ => {}
        }
    }

    async fn slash_command(&self, http: &Http, command: CommandInteraction) {
        if command.data.name != NEW || !self.is_owner(&command.user, |response| command.create_response(http, response)).await {
            return;
        }

        let deferred = CreateInteractionResponse::Defer(CreateInteractionResponseMessage::new());

        if !done(command.create_response(http, deferred).await, "answer /new") {
            return;
        }

        let (reply, replies) = mpsc::channel(REPLY_QUEUE);
        let input = Inbound::NewConversation { adapter: Adapter::Discord, key: command.channel_id.to_string(), reply };
        let text = outcome(self.ask(input, replies).await, None);

        done(command.edit_response(http, EditInteractionResponse::new().content(text)).await, "answer /new");
    }

    /// Applies or drops the change, then shows the outcome in place of the buttons.
    async fn button(&self, http: &Http, button: ComponentInteraction) {
        let Some((action, verdict)) = choice(&button.data.custom_id) else {
            return;
        };

        if !self.is_owner(&button.user, |response| button.create_response(http, response)).await {
            return;
        }

        if !done(button.create_response(http, CreateInteractionResponse::Acknowledge).await, "answer a button") {
            return;
        }

        let (reply, replies) = mpsc::channel(REPLY_QUEUE);
        let result = outcome(self.ask(Inbound::Schedule { action, reply }, replies).await, Some(verdict));
        let edit = EditInteractionResponse::new().content(below(&button.message.content, &result)).components(Vec::new());

        done(button.edit_response(http, edit).await, "show a button's outcome");
    }

    /// Whether `user` is an owner. Anyone else is told privately and ignored.
    async fn is_owner<F, R>(&self, user: &User, respond: F) -> bool
    where
        F: FnOnce(CreateInteractionResponse) -> R,
        R: Future<Output = serenity::Result<()>>,
    {
        if self.owners.contains(&user.id.get()) && !user.bot {
            return true;
        }

        let refusal = CreateInteractionResponseMessage::new().content("Only Shade's owners can do this.").ephemeral(true);

        if let Err(error) = respond(CreateInteractionResponse::Message(refusal)).await {
            tracing::warn!(%error, "cannot refuse an interaction");
        }

        tracing::debug!(user = %user.id, "ignoring an interaction from someone who is not an owner");

        false
    }

    /// Sends a command to the router and collects its answer.
    async fn ask(&self, input: Inbound, mut replies: mpsc::Receiver<Response>) -> Result<String, String> {
        if self.inbound.send(input).await.is_err() {
            return Err("the daemon is shutting down".to_owned());
        }

        let mut text = String::new();

        while let Some(response) = replies.recv().await {
            match response {
                Response::Text { text: part } => text.push_str(&part),
                Response::Error { message } => return Err(message),
                Response::Done => return Ok(text),
            }
        }

        Err("no answer came back".to_owned())
    }
}

/// What a button asks for, and what to say once it is done.
fn choice(id: &str) -> Option<(Manage, &'static str)> {
    if let Some(code) = id.strip_prefix(APPROVE) {
        return Some((Manage::Approve { code: code.to_owned() }, "✅ Approved."));
    }

    let code = id.strip_prefix(REJECT)?;

    Some((Manage::Reject { code: code.to_owned() }, "❌ Rejected; nothing was changed."))
}

/// The text to show for a command's answer: `verdict` in place of the router's text when given.
fn outcome(answer: Result<String, String>, verdict: Option<&str>) -> String {
    match (answer, verdict) {
        (Ok(_), Some(verdict)) => verdict.to_owned(),
        (Ok(text), None) => text,
        (Err(message), _) => format!("⚠️ {message}"),
    }
}

/// Logs a failed Discord call. `true` when it worked.
fn done<T>(result: serenity::Result<T>, what: &str) -> bool {
    match result {
        Ok(_) => true,
        Err(error) => {
            tracing::warn!(%error, "cannot {what} on Discord");
            false
        }
    }
}

/// `original` with `outcome` below it, cut to fit in one Discord message.
/// A card has no text of its own, so the outcome stands alone above it.
fn below(original: &str, outcome: &str) -> String {
    if original.is_empty() {
        return outcome.to_owned();
    }

    let room = MESSAGE_LIMIT.saturating_sub(outcome.encode_utf16().count() + 2);
    let mut kept = String::new();
    let mut length = 0;

    for character in original.chars() {
        length += character.len_utf16();

        if length > room {
            break;
        }

        kept.push(character);
    }

    format!("{kept}\n\n{outcome}")
}
