//! Typed access to the runtime. Mutations and subscriptions go through the
//! router; reads use the same typed store and hide Infinity's history format.

mod history;

use shade_proto::api::{
    self, Conversation, ConversationDetail, CreateConversation, Decision, DecisionResult, HistoryMessage, Page, SendMessage, Turn,
};
use shade_store::chat::Chats;
use shade_store::db::DbError;
use thiserror::Error;
use tokio::sync::{mpsc, oneshot};

use crate::router::Inbound;

pub use crate::chat::Subscription;

#[derive(Debug, Error)]
pub enum ClientError {
    #[error("conversation or approval not found")]
    NotFound,
    #[error("{0}")]
    Invalid(String),
    #[error("{0}")]
    Conflict(String),
    #[error("the chat queue is full; retry later")]
    Full,
    #[error("the runtime could not confirm the request")]
    Unavailable,
}

impl From<DbError> for ClientError {
    fn from(error: DbError) -> Self {
        if matches!(error, DbError::UnknownThread(_)) {
            return Self::NotFound;
        }

        tracing::error!(%error, "client storage request failed");

        Self::Unavailable
    }
}

/// Limits supplied by the configured webhook adapter, never by its body.
#[derive(Clone, Copy)]
pub struct HookAdmission {
    pub queued: u32,
    pub per_minute: u32,
}

pub enum HookAccepted {
    New(String),
    Repeated(String),
    Busy(&'static str),
}

pub(crate) enum Command {
    Create(CreateConversation),
    Send { root: String, request: SendMessage },
    Subscribe { root: String },
    Decide { root: String, code: String, decision: Decision },
    AcceptHook { name: String, delivery: Option<String>, text: String, admission: HookAdmission },
}

pub(crate) enum Reply {
    Conversation(Conversation),
    Turn(Turn),
    Subscription(Subscription),
    Decision(DecisionResult),
    Hook(HookAccepted),
}

/// A cheap, Send handle. It never owns the local Infinity runtime.
#[derive(Clone)]
pub struct Client {
    inbound: mpsc::Sender<Inbound>,
    chats: Chats,
}

impl Client {
    pub(crate) fn new(inbound: mpsc::Sender<Inbound>, chats: Chats) -> Self {
        Self { inbound, chats }
    }

    pub async fn create_conversation(&self, request: CreateConversation) -> Result<Conversation, ClientError> {
        match self.command(Command::Create(request)).await? {
            Reply::Conversation(conversation) => Ok(conversation),
            _ => Err(ClientError::Unavailable),
        }
    }

    pub async fn conversations(&self, cursor: Option<(i64, String)>, limit: usize) -> Result<Page<Conversation>, ClientError> {
        page_limit(limit)?;

        Ok(self.chats.list(cursor, limit).await?)
    }

    pub async fn conversation(&self, root: String) -> Result<ConversationDetail, ClientError> {
        Ok(self.chats.detail(root).await?)
    }

    pub async fn messages(&self, root: String, after: i64, limit: usize) -> Result<Page<HistoryMessage>, ClientError> {
        page_limit(limit)?;
        let page = self.chats.history(root, after, limit).await?;

        Ok(history::project(page))
    }

    pub async fn turns(&self, root: String, after: i64, limit: usize) -> Result<Page<Turn>, ClientError> {
        page_limit(limit)?;

        Ok(self.chats.turns(root, after, limit).await?)
    }

    pub async fn turn(&self, root: String, id: &str) -> Result<Turn, ClientError> {
        self.chats.owner_root(root.to_owned()).await?;
        let turn = self.chats.turn(id).await?.ok_or(ClientError::NotFound)?;

        if turn.conversation_id != root {
            return Err(ClientError::NotFound);
        }

        Ok(turn)
    }

    pub async fn send_message(&self, root: String, request: SendMessage) -> Result<Turn, ClientError> {
        match self.command(Command::Send { root, request }).await? {
            Reply::Turn(turn) => Ok(turn),
            _ => Err(ClientError::Unavailable),
        }
    }

    pub async fn subscribe(&self, root: String) -> Result<Subscription, ClientError> {
        match self.command(Command::Subscribe { root }).await? {
            Reply::Subscription(subscription) => Ok(subscription),
            _ => Err(ClientError::Unavailable),
        }
    }

    pub async fn decide(&self, root: String, code: String, decision: Decision) -> Result<DecisionResult, ClientError> {
        match self.command(Command::Decide { root, code, decision }).await? {
            Reply::Decision(result) => Ok(result),
            _ => Err(ClientError::Unavailable),
        }
    }

    pub async fn accept_hook(
        &self, name: String, delivery: Option<String>, text: String, admission: HookAdmission,
    ) -> Result<HookAccepted, ClientError> {
        let result = self.command(Command::AcceptHook { name, delivery, text, admission }).await?;

        if let Reply::Hook(accepted) = result {
            // Durable acceptance survives a lost nudge; the scheduler also polls.
            let _ = self.inbound.try_send(Inbound::StartBackground);

            return Ok(accepted);
        }

        Err(ClientError::Unavailable)
    }

    async fn command(&self, command: Command) -> Result<Reply, ClientError> {
        let (reply, answer) = oneshot::channel();

        if self.inbound.try_send(Inbound::Client { command, reply }).is_err() {
            return Err(ClientError::Unavailable);
        }

        match answer.await {
            Ok(result) => result,
            Err(_) => Err(ClientError::Unavailable),
        }
    }
}

fn page_limit(limit: usize) -> Result<(), ClientError> {
    if limit == 0 || limit > api::MAX_PAGE {
        return Err(ClientError::Invalid("limit must be between 1 and 100".to_owned()));
    }

    Ok(())
}

pub(crate) fn validate_message(request: &SendMessage) -> Result<(), ClientError> {
    let id = &request.client_message_id;

    if id.is_empty() || id.len() > 128 || id.chars().any(char::is_control) {
        return Err(ClientError::Invalid(
            "client_message_id must be 1 to 128 bytes without control characters".to_owned(),
        ));
    }

    if request.text.trim().is_empty() || request.text.len() > api::MAX_TEXT_BYTES {
        return Err(ClientError::Invalid("text must be nonempty and at most 64 KiB".to_owned()));
    }

    Ok(())
}
