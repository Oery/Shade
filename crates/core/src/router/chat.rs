use infinity_agent_core::system::local::ThreadHandle;
use shade_proto::api::{Event, TurnStatus};
use shade_store::chat::{Accepted, Enqueue};
use shade_store::roots::{Adapter, Target};

use super::{Outbound, Router};
use crate::adapters::discord::Reply;
use crate::error::BoxError;
use crate::router::turn::Step;

impl Router {
    pub(super) async fn enqueue_chat(
        &self, adapter: Adapter, target: Target, text: String, source_key: Option<String>, outbound: Outbound,
    ) {
        let delivery = outbound.delivery();
        let request = self.chat_request(adapter, target, text, source_key, delivery);

        self.accept_chat(request, outbound).await;
    }

    pub(super) async fn accept_chat(&self, request: Enqueue, mut outbound: Outbound) {
        match self.chat.store.enqueue(request).await {
            Ok(Accepted::New(turn)) => {
                self.chat.publish(
                    &turn.conversation_id,
                    Event::TurnStatus { turn_id: turn.id.to_owned(), status: TurnStatus::Queued, error: None },
                );
                self.chat.remember(turn.id, outbound);
                self.start_chats().await;
            }
            Ok(Accepted::Repeated(_)) => {}
            Ok(Accepted::Full) => outbound.send("", Step::Failed("the chat queue is full; retry later".to_owned())).await,
            Ok(Accepted::Conflict) => outbound.send("", Step::Failed("message ID already used".to_owned())).await,
            Err(error) => outbound.send("", Step::Failed(format!("cannot queue the message: {error}"))).await,
        }
    }

    pub(super) fn chat_request(
        &self, adapter: Adapter, target: Target, text: String, source_key: Option<String>, delivery: String,
    ) -> Enqueue {
        Enqueue {
            adapter,
            target,
            text,
            source_key,
            delivery,
            client_message_id: None,
            per_conversation: self.policy.limits.max_queued_chat_messages,
            total: self.policy.limits.max_queued_chat_messages_total,
        }
    }

    pub(super) async fn start_chats(&self) {
        loop {
            let claimed = match self.chat.store.claim().await {
                Ok(Some(claimed)) => claimed,
                Ok(None) => return,
                Err(error) => {
                    tracing::error!(%error, "cannot claim a queued chat turn");
                    return;
                }
            };

            let input = claimed.input;
            self.chat.started(input.root.as_str(), &input.id);
            let outbound = match self.chat.take_reply(&input.id) {
                Some(outbound) => outbound,
                None => self.chat_destination(&claimed.delivery),
            };

            self.dispatch(input, outbound).await;
        }
    }

    pub(super) fn chat_destination(&self, delivery: &str) -> Outbound {
        if let (Some(channel), Some(http)) = (delivery.strip_prefix("discord:"), &self.discord)
            && let Some(reply) = Reply::to_channel(std::sync::Arc::clone(http), channel)
        {
            return Outbound::Discord(reply);
        }

        // API work remains available through saved history and API events.
        let notifier = if delivery == "api" { None } else { self.notifier.clone() };

        Outbound::Detached(notifier)
    }

    pub(super) async fn resume_chat(&self, handle: &ThreadHandle) -> Result<Outbound, BoxError> {
        let root = handle.thread_id();
        let Some(turn) = self.chat.store.running(root.as_str()).await? else {
            return Ok(self.outbound_for(root).await);
        };

        self.chat.started(root.as_str(), &turn.id);
        self.chat.restore_text(root.as_str(), &handle.replay().history);
        let delivery = self.chat.store.delivery(&turn.id).await?;

        Ok(self.chat_destination(delivery.as_deref().unwrap_or("terminal")))
    }

    pub(super) async fn finish_chat(&self, root: &str, id: &str, step: &Step) {
        if self.chat.tracks(root, id)
            && let Err(error) = self.chat.finish(root, id, step).await
        {
            tracing::error!(%error, input = %id, "cannot finish a chat turn");
        }
    }
}
