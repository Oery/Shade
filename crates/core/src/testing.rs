//! A real Infinity system on Shade's SQLite stores with a scripted model.

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use infinity_agent_core::ThreadId;
use infinity_agent_core::system::local::{LaunchingSystem, ThreadHandle, ThreadLifecycleState};
use infinity_agent_core::system::{AgentEvent, AgentSystemBuilder, NoRapHttp, StaticModel};
use infinity_provider_protocol::mock::{MockModelController, mock_model};
use infinity_provider_protocol::{ModelEntry, SingleModelProvider};
use tokio::task::LocalSet;

use crate::authority::{Authority, Metered};
use crate::error::BoxError;
use crate::policy::{Cap, Limits, Policy};
use crate::store::conversation::Conversations;
use crate::store::db::Db;
use crate::store::grants::Grants;
use crate::store::state::States;
use crate::thread_config::ThreadTools;

pub type System = LaunchingSystem<Conversations, States, NoRapHttp>;

pub struct Harness {
    pub system: System,
    pub model: MockModelController,
    pub conversations: Conversations,
    pub authority: Authority,
}

/// Chat may use `caps`, under `limits`.
pub fn policy(caps: &[Cap], limits: Limits) -> Arc<Policy> {
    let chat = caps.iter().copied().collect();

    Arc::new(Policy { chat, chat_prompt: None, hooks: Default::default(), limits, timezone: chrono_tz::UTC })
}

/// Runs `test` on a `LocalSet`, failing it after ten seconds instead of hanging.
pub async fn within<F: Future<Output = Result<(), BoxError>>>(test: F) -> Result<(), BoxError> {
    let local = LocalSet::new();

    match tokio::time::timeout(Duration::from_secs(10), local.run_until(test)).await {
        Ok(result) => result,
        Err(_) => Err("the test timed out".into()),
    }
}

/// Starts a system on `db`. A second call on the same `db` stands in for a daemon restart.
pub async fn start(db: &Db) -> Result<Harness, BoxError> {
    start_with(db, policy(&[Cap::Now], Limits::default())).await
}

pub async fn start_with(db: &Db, policy: Arc<Policy>) -> Result<Harness, BoxError> {
    let (mock, model) = mock_model();
    let entry = ModelEntry {
        model_id: "mock".to_owned(),
        display_name: "mock".to_owned(),
        context_window: 100_000,
        max_output_tokens: None,
        supports_image_input: false,
    };
    let static_model = StaticModel::new(Arc::new(SingleModelProvider::new(entry, mock)), "mock").await?;

    let authority = Authority::new(Grants::new(db.clone()), policy);
    let conversations = Conversations::new(db.clone());
    let tools = ThreadTools::new(conversations.clone(), authority.clone());
    let metered = Metered::new(static_model, authority.clone());
    let builder = AgentSystemBuilder::new_local(conversations.clone(), States::new(db.clone()), metered);
    let system = builder.without_builtin_tools().thread_config(tools).start();

    Ok(Harness { system, model, conversations, authority })
}

/// Attaches to a registered thread and waits out the idle its attach-spawned driver emits.
pub async fn attach(harness: &mut Harness, thread_id: &ThreadId) -> Result<ThreadHandle, BoxError> {
    let Some(handle) = harness.system.thread_handle(thread_id).await? else {
        return Err("registered thread cannot be attached".into());
    };

    until_idle(&mut harness.system, thread_id).await;

    Ok(handle)
}

pub async fn until_idle(system: &mut System, thread_id: &ThreadId) {
    while let Some(event) = system.next_lifecycle_event().await {
        if event.state == ThreadLifecycleState::Idle && event.thread_id == *thread_id {
            return;
        }
    }
}

pub async fn until(handle: &mut ThreadHandle, wanted: fn(&AgentEvent) -> bool) {
    while let Some(event) = handle.recv().await {
        if wanted(&event) {
            return;
        }
    }
}

/// Answers the pending request with `text` and waits for the turn to finish.
pub async fn reply(harness: &mut Harness, handle: &mut ThreadHandle, text: &str) {
    harness.model.send_text(text);
    harness.model.finish();

    until(handle, |event| matches!(event, AgentEvent::CompletionFinished { .. })).await;
}

/// Whether the model was called before the thread went idle.
pub async fn reaches_model(harness: &mut Harness, thread_id: &ThreadId) -> bool {
    tokio::select! {
        _ = harness.model.next_request() => true,
        () = until_idle(&mut harness.system, thread_id) => false,
    }
}
