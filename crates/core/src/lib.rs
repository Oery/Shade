//! Shade runtime. The `shaded` binary is a thin wrapper around [`run`].

mod adapters;
mod error;
mod lock;

use std::path::PathBuf;

use tokio::signal::unix::{SignalKind, signal};

pub use error::Error;

use crate::adapters::socket::ControlSocket;

/// Runs the daemon until SIGINT or SIGTERM.
pub fn run(config: Option<PathBuf>) -> Result<(), Error> {
    tracing::info!(?config, "starting shade daemon");
    let runtime = match tokio::runtime::Builder::new_current_thread().enable_all().build() {
        Ok(runtime) => runtime,
        Err(source) => return Err(Error::Runtime(source)),
    };
    runtime.block_on(serve())
}

async fn serve() -> Result<(), Error> {
    let Some(data_dir) = shade_proto::data_dir() else {
        return Err(Error::NoDataDir);
    };
    lock::create_data_dir(&data_dir)?;
    // Declared before the socket so the socket file is removed while the lock is still held.
    let _lock = lock::acquire(&data_dir)?;
    let socket = ControlSocket::bind(shade_proto::socket_path(&data_dir))?;
    tracing::info!(path = %socket.path().display(), "listening");
    tokio::select! {
        () = socket.serve() => Ok(()),
        result = shutdown_signal() => result,
    }
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
