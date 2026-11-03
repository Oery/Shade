//! A real Infinity system on Shade's SQLite stores with a scripted model.

use std::fs;
use std::future::Future;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use infinity_agent_core::ThreadId;
use infinity_agent_core::system::local::{LaunchingSystem, ThreadHandle, ThreadLifecycleState};
use infinity_agent_core::system::{AgentEvent, AgentSystemBuilder, NoRapHttp, StaticModel};
use infinity_provider_protocol::mock::{MockModelController, mock_model};
use infinity_provider_protocol::{ModelEntry, SingleModelProvider};
use shade_policy::{Cap, Limits, Policy};
use shade_sandbox::paths::Protected;
use shade_sandbox::{Config, Isolation, Runner, Sandbox, find_on_path};
use shade_schedule::jobfiles::JobFiles;
use shade_store::conversation::Conversations;
use shade_store::db::Db;
use shade_store::grants::Grants;
use shade_store::state::States;
use tokio::task::LocalSet;

use crate::authority::{Authority, Metered};
use crate::error::BoxError;
use crate::notify::Notifier;
use crate::router::Router;
use crate::telemetry::Telemetry;
use crate::thread_config::{Services, ThreadTools};

pub type System = LaunchingSystem<Conversations, States, NoRapHttp>;

pub struct Harness {
    pub system: System,
    pub model: MockModelController,
    pub conversations: Conversations,
    pub authority: Authority,
    pub telemetry: Telemetry,
}

/// Chat may use `caps`, under `limits`.
pub fn policy(caps: &[Cap], limits: Limits) -> Arc<Policy> {
    let chat = caps.iter().cloned().collect();

    let channels = [("alerts".to_owned(), 42)].into_iter().collect();

    let servers = [("echo".to_owned(), "Echoes and counts its calls.".to_owned())].into_iter().collect();

    Arc::new(Policy {
        chat,
        chat_prompt: None,
        jobs_prompt: None,
        channels,
        servers,
        hooks: [("ci".to_owned(), [Cap::Now].into_iter().collect())].into_iter().collect(),
        limits,
        timezone: chrono_tz::UTC,
    })
}

/// Runs `test` on a `LocalSet`, failing it after ten seconds instead of hanging.
pub async fn within<T, F: Future<Output = Result<T, BoxError>>>(test: F) -> Result<T, BoxError> {
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
    start_with_tools(db, policy, services(None, None)).await
}

/// Services reaching `sandbox` and `notifier`, with a job file directory nobody writes.
pub fn services(sandbox: Option<Arc<Sandbox>>, notifier: Option<Notifier>) -> Services {
    let job_files = Arc::new(JobFiles::new(std::env::temp_dir().join(format!("shade-test-jobs-{}", uuid::Uuid::new_v4()))));

    Services { sandbox, notifier, job_files, mcp: Default::default(), memory: None, redactor: Default::default() }
}

/// A system whose tools reach `services`.
pub async fn start_with_tools(db: &Db, policy: Arc<Policy>, services: Services) -> Result<Harness, BoxError> {
    let (mock, model) = mock_model();
    let entry = ModelEntry {
        model_id: "mock".to_owned(),
        display_name: "mock".to_owned(),
        context_window: 100_000,
        max_output_tokens: None,
        supports_image_input: false,
    };
    let static_model = StaticModel::new(Arc::new(SingleModelProvider::new(entry, mock)), "mock").await?;

    let authority = Authority::new(Grants::new(db.clone()), policy).with_redactor(Arc::clone(&services.redactor));
    let telemetry = Telemetry::new(db.clone(), Arc::clone(&services.redactor));
    let conversations = Conversations::new(db.clone());
    let memory = services.memory.clone();
    let tools = ThreadTools::new(conversations.clone(), authority.clone(), db.clone(), services);
    let metered = Metered::new(static_model, authority.clone(), telemetry.clone(), memory, None);
    let builder = AgentSystemBuilder::new_local(conversations.clone(), States::new(db.clone()), metered);
    let system = builder.without_builtin_tools().thread_config(tools).start();

    Ok(Harness { system, model, conversations, authority, telemetry })
}

/// A router on `db` with a scripted model, as the daemon builds it.
pub async fn router(db: &Db, policy: Arc<Policy>) -> Result<(Router, MockModelController), BoxError> {
    let Harness { system, model, telemetry, .. } = start_with(db, Arc::clone(&policy)).await?;

    Ok((Router::new(db.clone(), system, policy, None, services(None, None), telemetry), model))
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

/// A workspace and a vault under the system temp dir, removed when dropped.
pub struct Dirs {
    pub root: PathBuf,
    pub workspace: PathBuf,
    pub vault: PathBuf,
}

impl Dirs {
    pub fn new() -> Self {
        let root = std::env::temp_dir().join(format!("shade-test-{}", uuid::Uuid::new_v4()));
        let workspace = root.join("workspace");
        let vault = root.join("vault");
        fs::create_dir_all(&workspace).unwrap();
        fs::create_dir_all(&vault).unwrap();

        Self { root, workspace, vault }
    }

    /// A real bubblewrap sandbox with the vault configured.
    pub fn bubblewrap(&self) -> Sandbox {
        let config = Config {
            workspace: self.workspace.clone(),
            vault: Some(self.vault.clone()),
            env: Vec::new(),
            isolation: Isolation::Bubblewrap,
            max_timeout_seconds: 60,
        };
        let runner = Runner::Bubblewrap(find_on_path("bwrap").unwrap());

        Sandbox::new(runner, config, &Protected { paths: Vec::new(), home: None }).unwrap()
    }
}

impl Drop for Dirs {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.root);
    }
}
