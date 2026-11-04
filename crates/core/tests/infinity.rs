//! Contract probes for the Infinity 0.2.0 behavior Shade relies on.
//!
//! These test the dependency, not Shade. Rerun them when upgrading Infinity;
//! docs/infinity-notes.md explains what each one guards.

use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;

use async_trait::async_trait;
use infinity_agent_core::ThreadId;
use infinity_agent_core::message::InputMessage;
use infinity_agent_core::stores::{InMemoryConversationStore, InMemoryStateStore};
use infinity_agent_core::system::local::{ChannelSender, LaunchingSystem, ThreadHandle, ThreadLifecycleState};
use infinity_agent_core::system::{AgentEvent, AgentSystemBuilder, NoRapHttp, StaticModel, ThreadConfig, ThreadConfigSource};
use infinity_agent_core::traits::{ConversationStore, InputSender};
use infinity_provider_protocol::mock::{MockModelController, mock_model};
use infinity_provider_protocol::{ModelEntry, SingleModelProvider};
use tokio::task::LocalSet;

type BoxError = Box<dyn std::error::Error + Send + Sync>;
type System = LaunchingSystem<InMemoryConversationStore, InMemoryStateStore, NoRapHttp>;

/// Stands in for Shade's grant lookup: resolves only granted threads and records every call.
#[derive(Clone, Default)]
struct Grants {
    granted: Rc<RefCell<Vec<ThreadId>>>,
    resolved: Rc<RefCell<Vec<ThreadId>>>,
}

#[async_trait(?Send)]
impl ThreadConfigSource<ChannelSender, NoRapHttp> for Grants {
    async fn resolve(&self, thread_id: &ThreadId<str>) -> Result<ThreadConfig<ChannelSender, NoRapHttp>, BoxError> {
        self.resolved.borrow_mut().push(thread_id.to_owned());

        if !self.granted.borrow().iter().any(|granted| granted.as_str() == thread_id.as_str()) {
            return Err("no grant".into());
        }

        Ok(ThreadConfig { tools: Vec::new(), extra_system_prompt: None, rap_notifier: None })
    }
}

struct Harness {
    system: System,
    conversations: InMemoryConversationStore,
    grants: Grants,
    model: MockModelController,
}

async fn start() -> Result<Harness, BoxError> {
    let (mock, model) = mock_model();
    let entry = ModelEntry {
        model_id: "mock".to_owned(),
        display_name: "mock".to_owned(),
        context_window: 100_000,
        max_output_tokens: None,
        supports_image_input: false,
    };
    let provider = SingleModelProvider::new(entry, mock);
    let static_model = StaticModel::new(Arc::new(provider), "mock").await?;

    let conversations = InMemoryConversationStore::new();
    let grants = Grants::default();
    let builder = AgentSystemBuilder::new_local(conversations.clone(), InMemoryStateStore::new(), static_model);
    let system = builder.without_builtin_tools().thread_config(grants.clone()).start();

    Ok(Harness { system, conversations, grants, model })
}

/// Attaching spawns a driver that loads the thread and goes idle; this waits that out.
async fn register(harness: &mut Harness, name: &str, granted: bool) -> Result<(ThreadId, ThreadHandle), BoxError> {
    let thread_id = ThreadId::from(name.to_owned());
    harness.conversations.ensure_root_thread(&thread_id).await?;

    if granted {
        harness.grants.granted.borrow_mut().push(thread_id.to_owned());
    }

    let Some(handle) = harness.system.thread_handle(&thread_id).await? else {
        return Err("registered thread cannot be attached".into());
    };

    until_idle(&mut harness.system, &thread_id).await;

    Ok((thread_id, handle))
}

async fn until_finished(handle: &mut ThreadHandle) {
    while let Some(event) = handle.recv().await {
        if matches!(event, AgentEvent::CompletionFinished { .. }) {
            return;
        }
    }
}

/// Whether the model was called before the thread went idle.
async fn reaches_model(harness: &mut Harness, thread_id: &ThreadId) -> bool {
    tokio::select! {
        _ = harness.model.next_request() => true,
        () = until_idle(&mut harness.system, thread_id) => false,
    }
}

async fn until_idle(system: &mut System, thread_id: &ThreadId) {
    while let Some(event) = system.next_lifecycle_event().await {
        if event.state == ThreadLifecycleState::Idle && event.thread_id == *thread_id {
            return;
        }
    }
}

/// Shade registers a root and its grant before attaching. Resolution must wait for the first input.
#[tokio::test(flavor = "current_thread")]
async fn config_resolves_on_first_input_not_on_attach() -> Result<(), BoxError> {
    LocalSet::new()
        .run_until(async {
            let mut harness = start().await?;
            let (thread_id, mut handle) = register(&mut harness, "root", true).await?;

            assert!(harness.grants.resolved.borrow().is_empty(), "attach resolved configuration");

            handle.send(InputMessage::user_text(thread_id.to_owned(), "hi"), "input-1").await?;
            harness.model.next_request().await;

            assert_eq!(*harness.grants.resolved.borrow(), std::slice::from_ref(&thread_id));

            harness.model.send_text("hello");
            harness.model.finish();
            until_finished(&mut handle).await;
            until_idle(&mut harness.system, &thread_id).await;

            Ok(())
        })
        .await
}

/// A failed resolution must stop the step before any model call: this is how Shade fails closed.
#[tokio::test(flavor = "current_thread")]
async fn ungranted_thread_never_reaches_the_model() -> Result<(), BoxError> {
    LocalSet::new()
        .run_until(async {
            let mut harness = start().await?;
            let (thread_id, handle) = register(&mut harness, "root", false).await?;

            handle.send(InputMessage::user_text(thread_id.to_owned(), "hi"), "input-1").await?;

            assert!(!reaches_model(&mut harness, &thread_id).await, "ungranted thread called the model");
            assert_eq!(harness.grants.resolved.borrow().len(), 1);

            Ok(())
        })
        .await
}

/// Loading any thread ID calls `ensure_root_thread`, so input for an unknown ID creates a root.
/// Shade's store must refuse to create roots there; registration is the only path.
#[tokio::test(flavor = "current_thread")]
async fn input_for_unknown_thread_creates_a_root() -> Result<(), BoxError> {
    LocalSet::new()
        .run_until(async {
            let mut harness = start().await?;
            let thread_id = ThreadId::from("unknown".to_owned());

            assert!(harness.system.thread_handle(&thread_id).await?.is_none());

            let input = InputMessage::user_text(thread_id.to_owned(), "hi");
            harness.system.sender().send_to_input_queue(input, "input-1").await?;

            assert!(!reaches_model(&mut harness, &thread_id).await, "unknown thread called the model");
            assert!(harness.conversations.thread_exists(&thread_id).await?);

            Ok(())
        })
        .await
}

/// Replaying an accepted input with the same ID must not run another completion.
#[tokio::test(flavor = "current_thread")]
async fn repeated_input_id_is_processed_once() -> Result<(), BoxError> {
    LocalSet::new()
        .run_until(async {
            let mut harness = start().await?;
            let (thread_id, mut handle) = register(&mut harness, "root", true).await?;

            handle.send(InputMessage::user_text(thread_id.to_owned(), "hi"), "input-1").await?;
            harness.model.next_request().await;
            harness.model.send_text("hello");
            harness.model.finish();
            until_finished(&mut handle).await;
            until_idle(&mut harness.system, &thread_id).await;

            handle.send(InputMessage::user_text(thread_id.to_owned(), "hi"), "input-1").await?;

            assert!(!reaches_model(&mut harness, &thread_id).await, "replayed input ran a second completion");

            Ok(())
        })
        .await
}

/// Shade withholds queued input until the prior turn finishes. Equal text
/// in successive turns must still produce two distinct model responses.
#[tokio::test(flavor = "current_thread")]
async fn sequential_turns_with_identical_text_remain_distinct() -> Result<(), BoxError> {
    LocalSet::new()
        .run_until(async {
            let mut harness = start().await?;
            let (thread, mut handle) = register(&mut harness, "root", true).await?;
            handle.send(InputMessage::user_text(thread.to_owned(), "same"), "first").await?;
            harness.model.next_request().await;
            harness.model.send_text("first answer");
            harness.model.finish();
            until_finished(&mut handle).await;
            until_idle(&mut harness.system, &thread).await;
            handle.send(InputMessage::user_text(thread.to_owned(), "same"), "second").await?;
            let second = harness.model.next_request().await;
            let history = format!("{:?}", second.chat_history);
            assert!(history.contains("first answer"));
            assert_eq!(history.matches("same").count(), 2);
            harness.model.send_text("second answer");
            harness.model.finish();
            until_finished(&mut handle).await;

            Ok(())
        })
        .await
}
