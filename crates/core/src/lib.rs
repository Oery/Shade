//! Shade runtime. The `shaded` binary is a thin wrapper around [`run`].

mod adapters;
mod authority;
mod config;
mod error;
mod lock;
mod model;
mod notify;
mod prompt;
mod recovery;
#[cfg(test)]
mod replay;
mod router;
mod secrets;
#[cfg(test)]
mod testing;
mod thread_config;
mod tools;
mod usage;

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use shade_sandbox::Sandbox;
use tokio::signal::unix::{SignalKind, signal};
use tokio::sync::mpsc;
use tokio::task::LocalSet;

pub use error::Error;

use shade_policy::{Cap, Policy};
use shade_schedule::jobfiles::{self, JobFiles};
use shade_store::db::{self, Db};
use shade_store::grants::Grants;

use crate::adapters::discord::Discord;
use crate::adapters::socket::ControlSocket;
use crate::config::Config;
use crate::model::Model;
use crate::notify::Notifier;
use crate::router::{Inbound, Router};
use crate::secrets::Secrets;
use crate::thread_config::Services;
use shade_mcp::{Server, Servers};

/// Pending input between the control socket and the router.
const INBOUND_QUEUE: usize = 16;

/// Runs the daemon until SIGINT or SIGTERM.
pub fn run(config: Option<PathBuf>) -> Result<(), Error> {
    let runtime = match tokio::runtime::Builder::new_current_thread().enable_all().build() {
        Ok(runtime) => runtime,
        Err(source) => return Err(Error::Runtime(source)),
    };

    // Infinity's local agents are `!Send`, so they live on a `LocalSet`.
    runtime.block_on(LocalSet::new().run_until(serve(config)))
}

async fn serve(config_path: Option<PathBuf>) -> Result<(), Error> {
    let config_path = config::path(config_path)?;
    let Config { model, timezone, chat, jobs, hooks, limits, sandbox, mut discord, mcp } = config::load(&config_path)?;
    let channels = discord.as_mut().map(|discord| std::mem::take(&mut discord.channels)).unwrap_or_default();
    let servers = mcp.iter().map(|(name, server)| (name.to_owned(), server.description.to_owned())).collect();
    let sections = config::Sections { chat, jobs, hooks, limits, timezone, channels, servers };
    let policy = config::policy(&config_path, sections)?;
    let mut secrets = Secrets::load(secrets::path_for(&config_path))?;

    let Some(data_dir) = shade_proto::data_dir() else {
        return Err(Error::NoDataDir);
    };

    lock::create_data_dir(&data_dir)?;

    // Declared before the socket so the socket file is removed while the lock is still held.
    let _lock = lock::acquire(&data_dir)?;

    // Probed now, so a broken sandbox stops the daemon before it accepts work.
    let sandbox = start_sandbox(sandbox, &policy, &data_dir, &config_path).await?;
    let model = Model::new(model, &mut secrets)?;
    let mcp = start_mcp(mcp, &mut secrets).await?;
    let db = Db::open(&data_dir.join(db::DB_FILE))?;

    let (inbound, inbound_queue) = mpsc::channel(INBOUND_QUEUE);

    let discord = match discord {
        Some(config) => Some(Discord::connect(config, &mut secrets, inbound.clone()).await?),
        None => None,
    };

    let socket = ControlSocket::bind(shade_proto::socket_path(&data_dir), inbound)?;
    tracing::info!(path = %socket.path().display(), model = %model.id(), "listening");

    let http = discord.as_ref().map(Discord::http);
    let notifier = discord.as_ref().map(|discord| notifications(discord, &db));
    let job_files = Arc::new(JobFiles::new(jobfiles::dir_for(&config_path)));
    let services = Services { sandbox: sandbox.map(Arc::new), notifier, job_files, mcp };
    let router = Router::start(model, db, Arc::new(policy), services, http);

    // Job files are config: a bad one stops the daemon before it runs anything.
    if let Err(message) = router.load_job_files(chrono::Utc::now()).await {
        router.shutdown().await;
        return Err(Error::JobFiles(message));
    }

    if let Err(source) = router.resume().await {
        router.shutdown().await;
        return Err(Error::Recovery(source));
    }

    let result = listen(&socket, &router, inbound_queue, discord).await;
    router.shutdown().await;

    result
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

/// Serves new input until a shutdown signal. The Discord gateway starts
/// here, after resumed work, so a new message cannot overtake it.
async fn listen(socket: &ControlSocket, router: &Router, inbound: mpsc::Receiver<Inbound>, discord: Option<Discord>) -> Result<(), Error> {
    let shards = discord.as_ref().map(Discord::shards);

    if let Some(discord) = discord {
        tokio::spawn(discord.run());
    }

    let result = tokio::select! {
        () = socket.serve() => Ok(()),
        () = router.serve(inbound) => Ok(()),
        result = shutdown_signal() => result,
    };

    if let Some(shards) = shards {
        shards.shutdown_all().await;
    }

    result
}

async fn shutdown_signal() -> Result<(), Error> {
    let mut terminate = match signal(SignalKind::terminate()) {
        Ok(stream) => stream,
        Err(source) => return Err(Error::Signal(source)),
    };

    let mut interrupt = match signal(SignalKind::interrupt()) {
        Ok(stream) => stream,
        Err(source) => return Err(Error::Signal(source)),
    };

    tokio::select! {
        _ = terminate.recv() => {}
        _ = interrupt.recv() => {}
    }

    tracing::info!("shutting down");

    Ok(())
}
