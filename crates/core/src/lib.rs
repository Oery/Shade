//! Shade runtime. The `shaded` binary is a thin wrapper around [`run`].

mod adapters;
mod authority;
mod config;
mod error;
mod lock;
mod model;
mod policy;
mod recovery;
mod router;
mod sandbox;
mod secrets;
mod store;
#[cfg(test)]
mod testing;
mod thread_config;
mod tools;

use std::path::PathBuf;
use std::sync::Arc;

use tokio::signal::unix::{SignalKind, signal};
use tokio::sync::mpsc;
use tokio::task::LocalSet;

pub use error::Error;

use crate::adapters::discord::Discord;
use crate::adapters::socket::ControlSocket;
use crate::config::Config;
use crate::model::Model;
use crate::router::{Inbound, Router};
use crate::secrets::Secrets;
use crate::store::db::{self, Db};

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
    let Config { model, timezone, chat, hooks, limits, sandbox, discord } = config::load(&config_path)?;
    let policy = config::policy(&config_path, chat, hooks, limits, timezone)?;
    let mut secrets = Secrets::load(secrets::path_for(&config_path))?;

    let Some(data_dir) = shade_proto::data_dir() else {
        return Err(Error::NoDataDir);
    };

    lock::create_data_dir(&data_dir)?;

    // Declared before the socket so the socket file is removed while the lock is still held.
    let _lock = lock::acquire(&data_dir)?;

    // Probed now, so a broken sandbox stops the daemon before it accepts work.
    let sandbox = sandbox::start(sandbox, &policy, &data_dir, &config_path).await?;
    let model = Model::new(model, &mut secrets)?;
    let db = Db::open(&data_dir.join(db::DB_FILE))?;

    let (inbound, inbound_queue) = mpsc::channel(INBOUND_QUEUE);

    let discord = match discord {
        Some(config) => Some(Discord::connect(config, &mut secrets, inbound.clone()).await?),
        None => None,
    };

    let socket = ControlSocket::bind(shade_proto::socket_path(&data_dir), inbound)?;
    tracing::info!(path = %socket.path().display(), model = %model.id(), "listening");

    let http = discord.as_ref().map(Discord::http);
    let router = Router::start(model, db, Arc::new(policy), sandbox.map(Arc::new), http);

    if let Err(source) = router.resume().await {
        router.shutdown().await;
        return Err(Error::Recovery(source));
    }

    let result = listen(&socket, &router, inbound_queue, discord).await;
    router.shutdown().await;

    result
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
