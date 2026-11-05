//! Opt-in mock runtime for integration tests in adapter crates.

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use infinity_agent_core::system::{AgentSystemBuilder, StaticModel};
use infinity_provider_protocol::mock::{MockModelController, mock_model};
use infinity_provider_protocol::{ModelEntry, SingleModelProvider};
use shade_policy::{Cap, Limits, Policy};
use shade_schedule::jobfiles::JobFiles;
use shade_store::chat::Chats;
use shade_store::conversation::Conversations;
use shade_store::db::Db;
use shade_store::grants::Grants;
use shade_store::state::States;
use tokio::sync::mpsc;
use tokio::task::LocalSet;

use crate::Client;
use crate::authority::{Authority, Metered};
use crate::router::Router;
use crate::telemetry::Telemetry;
use crate::thread_config::{Services, ThreadTools};

pub type BoxError = Box<dyn std::error::Error + Send + Sync>;

pub fn policy(caps: &[Cap], limits: Limits) -> Arc<Policy> {
    Arc::new(Policy {
        chat: caps.iter().cloned().collect(),
        discord_guests: Default::default(),
        chat_prompt: None,
        jobs_prompt: None,
        channels: Default::default(),
        servers: Default::default(),
        hooks: [("ci".to_owned(), [Cap::Now].into_iter().collect())].into_iter().collect(),
        limits,
        timezone: chrono_tz::UTC,
    })
}

pub async fn within<T, F: Future<Output = Result<T, BoxError>>>(test: F) -> Result<T, BoxError> {
    match tokio::time::timeout(Duration::from_secs(10), LocalSet::new().run_until(test)).await {
        Ok(result) => result,
        Err(_) => Err("the test timed out".into()),
    }
}

/// Spawn the real router inside the caller's LocalSet, exposing only its
/// public client and scripted provider. Dropping the LocalSet stops the test.
pub async fn client(db: &Db, policy: Arc<Policy>) -> Result<(Client, MockModelController), BoxError> {
    let (mock, model) = mock_model();
    let entry = ModelEntry {
        model_id: "mock".to_owned(),
        display_name: "mock".to_owned(),
        context_window: 100_000,
        max_output_tokens: None,
        supports_image_input: false,
    };
    let model_source = StaticModel::new(Arc::new(SingleModelProvider::new(entry, mock)), "mock").await?;
    let job_files = Arc::new(JobFiles::new(std::env::temp_dir().join(format!("shade-test-jobs-{}", uuid::Uuid::new_v4()))));
    let services = Services {
        sandbox: None,
        notifier: None,
        job_files,
        mcp: Default::default(),
        memory: None,
        redactor: Default::default(),
        watches_changed: Default::default(),
    };
    let authority = Authority::new(Grants::new(db.clone()), Arc::clone(&policy));
    let telemetry = Telemetry::new(db.clone(), Arc::clone(&services.redactor));
    let conversations = Conversations::new(db.clone());
    let tools = ThreadTools::new(conversations.clone(), authority.clone(), db.clone(), services.clone());
    let metered = Metered::new(model_source, authority, telemetry.clone(), None, None);
    let system =
        AgentSystemBuilder::new_local(conversations, States::new(db.clone()), metered).without_builtin_tools().thread_config(tools).start();
    let router = Router::new(db.clone(), system, policy, None, services, telemetry);
    let (inbound, queue) = mpsc::channel(64);
    let client = Client::new(inbound, Chats::new(db.clone()));
    tokio::task::spawn_local(async move {
        router.serve(queue).await;
        router.shutdown().await;
    });

    Ok((client, model))
}
