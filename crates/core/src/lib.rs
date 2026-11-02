//! Shade runtime. The `shaded` binary is a thin wrapper around [`run`].

mod adapters;
mod config;
mod error;
mod lock;
mod model;
mod router;
mod secrets;

use std::path::PathBuf;

use tokio::signal::unix::{SignalKind, signal};
use tokio::sync::mpsc;
use tokio::task::LocalSet;

pub use error::Error;

use crate::adapters::socket::ControlSocket;
use crate::model::Model;
use crate::router::Router;
use crate::secrets::Secrets;

/// Pending asks between the control socket and the router.
const ASK_QUEUE: usize = 16;

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
    let config = config::load(&config_path)?;
    let mut secrets = Secrets::load(secrets::path_for(&config_path))?;

    let Some(data_dir) = shade_proto::data_dir() else {
        return Err(Error::NoDataDir);
    };

    lock::create_data_dir(&data_dir)?;

    // Declared before the socket so the socket file is removed while the lock is still held.
    let _lock = lock::acquire(&data_dir)?;
    let model = Model::new(config.model, &mut secrets)?;

    let (asks, ask_queue) = mpsc::channel(ASK_QUEUE);
    let socket = ControlSocket::bind(shade_proto::socket_path(&data_dir), asks)?;
    tracing::info!(path = %socket.path().display(), model = %model.id(), "listening");

    let router = Router::start(model);

    let result = tokio::select! {
        () = socket.serve() => Ok(()),
        () = router.serve(ask_queue) => Ok(()),
        result = shutdown_signal() => result,
    };

    router.shutdown().await;

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
