//! Live owner-chat state. Durable turns live in shade-store; these buffers
//! exist only to give a newly connected client the current partial reply.

use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::Arc;

use infinity_agent_core::system::AgentEvent;
use shade_proto::api::{Event, TurnStatus};
use shade_store::chat::Chats;
use tokio::sync::{broadcast, mpsc};

use crate::router::Outbound;
use crate::router::turn::Step;

const EVENT_QUEUE: usize = 64;

pub struct Subscription {
    pub snapshot: Event,
    pub events: broadcast::Receiver<Arc<Event>>,
}

#[derive(Default)]
struct Live {
    active: Option<String>,
    text: String,
    pending_tools: usize,
    events: Option<broadcast::Sender<Arc<Event>>>,
}

pub struct ChatRuntime {
    pub store: Chats,
    live: RefCell<HashMap<String, Live>>,
    replies: RefCell<HashMap<String, Outbound>>,
    wakeup: mpsc::Sender<()>,
}

impl ChatRuntime {
    pub fn new(store: Chats, wakeup: mpsc::Sender<()>) -> Self {
        Self { store, live: RefCell::new(HashMap::new()), replies: RefCell::new(HashMap::new()), wakeup }
    }

    pub fn remember(&self, id: String, outbound: Outbound) {
        self.replies.borrow_mut().insert(id, outbound);
    }

    pub fn take_reply(&self, id: &str) -> Option<Outbound> {
        self.replies.borrow_mut().remove(id)
    }

    pub fn started(&self, root: &str, id: &str) {
        let mut live = self.live.borrow_mut();
        let state = live.entry(root.to_owned()).or_default();
        state.active = Some(id.to_owned());
        state.text.clear();
        state.pending_tools = 0;
        emit(state, Event::TurnStatus { turn_id: id.to_owned(), status: TurnStatus::Running, error: None });
    }

    pub fn restore_text(&self, root: &str, history: &[infinity_agent_core::message::InfinityMessage]) {
        use infinity_agent_core::message::InfinityMessage;
        use infinity_provider_protocol::message::{AssistantContent, UserContent};

        let mut live = self.live.borrow_mut();
        let Some(state) = live.get_mut(root) else {
            return;
        };
        let start = history.iter().rposition(|message| matches!(message, InfinityMessage::User { content: UserContent::Text(_) }));
        let Some(start) = start else {
            return;
        };

        for message in &history[start + 1..] {
            match message {
                InfinityMessage::Assistant { content: AssistantContent::Text(text) } => {
                    state.text.push_str(&text.text);
                    state.text.push_str("\n\n");
                }
                InfinityMessage::ToolCall { .. } => state.pending_tools += 1,
                InfinityMessage::ToolResult { .. } => state.pending_tools = state.pending_tools.saturating_sub(1),
                _ => {}
            }
        }
    }

    pub fn tracks(&self, root: &str, id: &str) -> bool {
        self.live.borrow().get(root).is_some_and(|state| state.active.as_deref() == Some(id))
    }

    pub fn publish(&self, root: &str, event: Event) {
        if let Some(state) = self.live.borrow_mut().get_mut(root) {
            emit(state, event);
        }
    }

    /// No awaits between taking the snapshot and registering the receiver.
    pub fn subscribe(&self, root: &str) -> Subscription {
        let mut live = self.live.borrow_mut();
        live.retain(|_, state| state.active.is_some() || state.events.as_ref().is_some_and(|events| events.receiver_count() > 0));
        let state = live.entry(root.to_owned()).or_default();
        let events = state.events.get_or_insert_with(|| broadcast::channel(EVENT_QUEUE).0).subscribe();
        let snapshot = Event::Snapshot {
            active_turn: state.active.to_owned(),
            partial_text: state.text.to_owned(),
            pending_tools: state.pending_tools,
        };

        Subscription { snapshot, events }
    }

    pub fn observe(&self, root: &str, id: &str, event: &AgentEvent) {
        let mut live = self.live.borrow_mut();
        let Some(state) = live.get_mut(root) else {
            return;
        };

        let event = match event {
            AgentEvent::ToolCall { name, .. } => {
                state.pending_tools += 1;
                Event::ToolStarted { turn_id: id.to_owned(), name: name.to_owned() }
            }
            AgentEvent::ToolResult { .. } => {
                state.pending_tools = state.pending_tools.saturating_sub(1);
                Event::ToolFinished { turn_id: id.to_owned() }
            }
            _ => return,
        };

        emit(state, event);
    }

    pub fn step(&self, root: &str, id: &str, step: &Step) {
        let mut live = self.live.borrow_mut();
        let Some(state) = live.get_mut(root) else {
            return;
        };

        let event = match step {
            Step::Text(text) => {
                state.text.push_str(text);
                Event::TextDelta { turn_id: id.to_owned(), text: text.to_owned() }
            }
            Step::Shown(shown) => Event::Approval { turn_id: id.to_owned(), code: shown.approval.to_owned(), text: shown.card.text() },
            _ => return,
        };

        emit(state, event);
    }

    /// Terminal status is saved before publishing it or waking the router.
    pub async fn finish(&self, root: &str, id: &str, step: &Step) -> Result<(), shade_store::db::DbError> {
        let (status, error) = match step {
            Step::Done => (TurnStatus::Completed, None),
            Step::Failed(error) => (TurnStatus::Failed, Some(error.to_owned())),
            Step::Superseded => (TurnStatus::Interrupted, Some("interrupted by newer input".to_owned())),
            _ => return Ok(()),
        };

        self.store.finish(id, status, error.to_owned()).await?;
        let mut live = self.live.borrow_mut();

        if let Some(state) = live.get_mut(root) {
            state.active = None;
            state.text.clear();
            state.pending_tools = 0;
            emit(state, Event::TurnStatus { turn_id: id.to_owned(), status, error });
        }

        live.retain(|_, state| state.active.is_some() || state.events.as_ref().is_some_and(|events| events.receiver_count() > 0));
        drop(live);
        let _ = self.wakeup.try_send(());

        Ok(())
    }
}

fn emit(state: &Live, event: Event) {
    if let Some(events) = &state.events {
        let _ = events.send(Arc::new(event));
    }
}
