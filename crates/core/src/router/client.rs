use chrono::Utc;
use shade_proto::api::{Decision, Event, TurnStatus};
use shade_store::chat::Accepted;
use shade_store::roots::{Adapter, Target};

use super::Router;
use crate::client::{ClientError, Command, HookAccepted, Reply};

impl Router {
    pub(super) async fn client_command(&self, command: Command) -> Result<Reply, ClientError> {
        match command {
            Command::Create(request) => {
                if request.name.as_deref().is_some_and(|name| !shade_proto::valid_conversation(name)) {
                    return Err(ClientError::Invalid("name must be 1 to 64 bytes without control characters".to_owned()));
                }

                let conversation = self.chat.store.create(request.name).await?;

                Ok(Reply::Conversation(conversation))
            }
            Command::Send { root, request } => self.client_send(root, request).await,
            Command::Subscribe { root } => {
                // Check the root's authority before exposing live state.
                self.chat.store.detail(root.to_owned()).await?;

                Ok(Reply::Subscription(self.chat.subscribe(&root)))
            }
            Command::Decide { root, code, decision } => self.client_decide(root, code, decision).await,
            Command::AcceptHook { name, delivery, text, admission } => {
                if !self.policy.hooks.contains_key(&name) {
                    return Err(ClientError::NotFound);
                }

                let limits = shade_store::hooks::Admission { queued: admission.queued, per_minute: admission.per_minute };
                let accepted = match self.hooks.accept(name, delivery, text, limits).await? {
                    shade_store::hooks::Accepted::New(id) => HookAccepted::New(id),
                    shade_store::hooks::Accepted::Repeated(id) => HookAccepted::Repeated(id),
                    shade_store::hooks::Accepted::Busy(limit) => HookAccepted::Busy(limit),
                };

                Ok(Reply::Hook(accepted))
            }
        }
    }

    async fn client_send(&self, root: String, request: shade_proto::api::SendMessage) -> Result<Reply, ClientError> {
        crate::client::validate_message(&request)?;

        let mut queued = self.chat_request(Adapter::Api, Target::Existing(root), request.text, None, "api".to_owned());
        queued.client_message_id = Some(request.client_message_id);
        let accepted = self.chat.store.enqueue(queued).await?;
        let turn = match accepted {
            Accepted::New(turn) => {
                self.chat.publish(
                    &turn.conversation_id,
                    Event::TurnStatus { turn_id: turn.id.to_owned(), status: TurnStatus::Queued, error: None },
                );
                turn
            }
            Accepted::Repeated(turn) => return Ok(Reply::Turn(turn)),
            Accepted::Conflict => return Err(ClientError::Conflict("client_message_id was already used with different text".to_owned())),
            Accepted::Full => return Err(ClientError::Full),
        };

        self.start_chats().await;
        let current = self.chat.store.turn(&turn.id).await?.ok_or(ClientError::Unavailable)?;

        Ok(Reply::Turn(current))
    }

    async fn client_decide(&self, root: String, code: String, decision: Decision) -> Result<Reply, ClientError> {
        if !self.chat.store.proposal_belongs(root.to_owned(), code.to_owned()).await? {
            return Err(ClientError::NotFound);
        }

        let limits = self.policy.limits;

        if !self.chat.store.has_capacity(root.to_owned(), limits.max_queued_chat_messages, limits.max_queued_chat_messages_total).await? {
            return Err(ClientError::Full);
        }

        let result = match decision {
            Decision::Approve => self.approve_to(code.to_owned(), Utc::now(), true).await,
            Decision::Reject => self.reject_to(code.to_owned(), true).await,
        };
        let text = match result {
            Ok(text) => text,
            Err(message) => return Err(ClientError::Conflict(self.telemetry.redact(message))),
        };

        self.chat.publish(&root, Event::ApprovalDecided { code, decision });

        Ok(Reply::Decision(shade_proto::api::DecisionResult { text }))
    }
}
