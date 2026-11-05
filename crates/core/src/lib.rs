//! Shade runtime and typed client access. The daemon owns process startup and HTTP.

mod adapters;
mod audit;
mod authority;
mod chat;
mod client;
mod config;
mod deadline;
mod error;
mod lock;
mod memory;
mod model;
mod notify;
mod prompt;
mod recovery;
mod redact;
#[cfg(test)]
mod replay;
mod router;
mod secrets;
mod telemetry;
#[cfg(test)]
mod testing;
mod thread_config;
mod tools;
mod usage;
mod watcher;

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;

use shade_sandbox::Sandbox;
use tokio::sync::{Notify, mpsc};

pub use client::{Client, ClientError, HookAccepted, HookAdmission, Subscription};
pub use config::{ChatConfig, Config, DiscordConfig, JobsConfig, ModelConfig};
pub use error::Error;
pub use memory::Config as MemoryConfig;
pub use secrets::{Secrets, path_for as secrets_path};

#[cfg(any(test, feature = "testing"))]
pub mod test_support;

use shade_policy::{Cap, Policy};
use shade_schedule::jobfiles::{self, JobFiles};
use shade_store::db::{self, Db};
use shade_store::grants::Grants;

use crate::adapters::discord::Discord;
use crate::adapters::socket::ControlSocket;
use crate::memory::Memory;
use crate::model::Model;
use crate::notify::Notifier;
use crate::router::{Inbound, Router};
use crate::thread_config::Services;
use shade_mcp::{Server, Servers};
use watcher::Watcher;

/// Pending input between the control socket and the router.
const INBOUND_QUEUE: usize = 16;

/// A prepared local runtime, created and served inside a Tokio LocalSet.
/// Call `resume` before serving any adapter input.
/// The daemon runs its `serve` future alongside the HTTP server and signals.
pub struct Runtime {
    socket: ControlSocket,
    router: Router,
    inbound: Option<mpsc::Receiver<Inbound>>,
    client: Client,
    discord: Option<Discord>,
    shards: Option<Arc<serenity::all::ShardManager>>,
    watcher: Arc<Watcher>,
    // Struct fields drop in order: the socket must disappear before the lock.
    _lock: lock::ExecutionLock,
}

impl Runtime {
    pub async fn start(config: Config, config_path: &Path, mut secrets: Secrets) -> Result<Self, Error> {
        let Config { model, timezone, chat, jobs, hooks, limits, sandbox, mut discord, mcp, memory } = config;
        let channels = discord.as_mut().map(|discord| std::mem::take(&mut discord.channels)).unwrap_or_default();
        let discord_guests = discord.as_mut().map(|discord| std::mem::take(&mut discord.guests.caps)).unwrap_or_default();
        let servers = mcp.iter().map(|(name, server)| (name.to_owned(), server.description.to_owned())).collect();
        let memory = start_memory(memory, &mcp, config_path)?;
        let sections = config::Sections { chat, discord_guests, jobs, hooks, limits, timezone, channels, servers };
        let policy = config::policy(config_path, sections)?;

        let Some(data_dir) = shade_proto::data_dir() else {
            return Err(Error::NoDataDir);
        };

        lock::create_data_dir(&data_dir)?;

        // Declared before the socket so the socket file is removed while the lock is still held.
        let _lock = lock::acquire(&data_dir)?;

        // Probed now, so a broken sandbox stops the daemon before it accepts work.
        let sandbox = start_sandbox(sandbox, &policy, &data_dir, config_path).await?;
        let model = Model::new(model, &mut secrets)?;
        let mcp = start_mcp(mcp, &mut secrets).await?;
        let db = Db::open(&data_dir.join(db::DB_FILE))?;

        let (inbound, inbound_queue) = mpsc::channel(INBOUND_QUEUE);

        let discord = match discord {
            Some(config) => Some(Discord::connect(config, &mut secrets, inbound.clone()).await?),
            None => None,
        };

        let wake = inbound.clone();
        let client = Client::new(inbound.clone(), shade_store::chat::Chats::new(db.clone()));
        let socket = ControlSocket::bind(shade_proto::socket_path(&data_dir), inbound)?;
        tracing::info!(path = %socket.path().display(), model = %model.id(), "listening");
        let http = discord.as_ref().map(Discord::http);
        let shards = discord.as_ref().map(Discord::shards);
        let notifier = discord.as_ref().map(|discord| notifications(discord, &db));
        let job_files = Arc::new(JobFiles::new(jobfiles::dir_for(config_path)));
        let redactor = Arc::new(secrets.into_redactor());
        let policy = Arc::new(policy);
        let watches_changed = Arc::new(Notify::new());
        let watcher = Watcher::new(
            db.clone(),
            Arc::clone(&mcp),
            Arc::clone(&policy),
            Arc::clone(&redactor),
            notifier.clone(),
            wake,
            Arc::clone(&watches_changed),
        );
        let services = Services { sandbox: sandbox.map(Arc::new), notifier, job_files, mcp, memory, redactor, watches_changed };
        let router = Router::start(model, db, policy, services, http);

        Ok(Self { socket, router, inbound: Some(inbound_queue), client, discord, shards, watcher: Arc::new(watcher), _lock })
    }

    pub fn client(&self) -> Client {
        self.client.clone()
    }

    pub async fn resume(&self) -> Result<(), Error> {
        if let Err(message) = self.router.load_job_files(chrono::Utc::now()).await {
            return Err(Error::JobFiles(message));
        }

        match self.router.resume().await {
            Ok(()) => Ok(()),
            Err(error) => Err(Error::Recovery(error)),
        }
    }

    pub async fn serve(&mut self) {
        let Some(inbound) = self.inbound.take() else {
            return;
        };

        if let Some(discord) = self.discord.take() {
            tokio::spawn(discord.run());
        }

        tokio::spawn(Arc::clone(&self.watcher).run());

        tokio::select! {
            () = self.socket.serve() => {},
            () = self.router.serve(inbound) => {},
        }
    }

    pub async fn shutdown(self) {
        if let Some(shards) = self.shards {
            shards.shutdown_all().await;
        }

        self.router.shutdown().await;
    }
}

/// The sandbox for shell commands, or `None` when no origin holds `shell`.
///
/// Fails closed: with `shell` granted, a missing section, an exposed path,
/// or a sandbox that cannot actually run a command stops the daemon.
async fn start_sandbox(
    config: Option<shade_sandbox::Config>, policy: &Policy, data_dir: &Path, config_path: &Path,
) -> Result<Option<Sandbox>, Error> {
    if !policy.grants_anywhere(Cap::Shell) {
        return Ok(None);
    }

    let Some(config) = config else {
        return Err(Error::SandboxNotConfigured);
    };

    let sandbox = shade_sandbox::start(config, data_dir, config_path).await?;

    Ok(Some(sandbox))
}

/// Checks `[memory]` against the configured servers. A bad section stops startup.
fn start_memory(
    config: Option<memory::Config>, servers: &BTreeMap<String, shade_mcp::Config>, path: &Path,
) -> Result<Option<Arc<Memory>>, Error> {
    let Some(config) = config else {
        return Ok(None);
    };

    let names: Vec<&str> = servers.keys().map(String::as_str).collect();

    match Memory::new(config, &names) {
        Ok(memory) => Ok(Some(Arc::new(memory))),
        Err(message) => Err(Error::ConfigInvalid { path: path.to_owned(), message }),
    }
}

/// Starts every MCP server, each within its startup timeout, so a server
/// that is down never holds up the daemon. Those that fail are retried in
/// the background. A variable a server lists but nobody set stops startup.
async fn start_mcp(configs: BTreeMap<String, shade_mcp::Config>, secrets: &mut Secrets) -> Result<Arc<Servers>, Error> {
    let mut servers = Vec::new();

    for (name, config) in configs {
        let mut env = Vec::new();

        for key in &config.env {
            let Some(value) = secrets.take(key) else {
                return Err(Error::McpEnv { server: name, name: key.to_owned() });
            };

            env.push((key.to_owned(), value));
        }

        match Server::new(name, &config, env) {
            Ok(server) => servers.push(server),
            Err(message) => return Err(Error::McpServer(message)),
        }
    }

    let servers = Arc::new(Servers::new(servers));
    servers.connect_all().await;
    tokio::spawn(Arc::clone(&servers).keep_connected());

    Ok(servers)
}

/// Notifications go to Discord, the one adapter that can reach the owner
/// anywhere. Started before resumed work, which may report a failure.
fn notifications(discord: &Discord, db: &Db) -> Notifier {
    let (notifier, queue) = Notifier::new(Grants::new(db.clone()));
    tokio::spawn(discord.notices(queue));

    notifier
}
