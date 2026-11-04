//! Compose the local runtime, HTTP listener, and process shutdown.

use std::path::PathBuf;

use shade_core::{Runtime, Secrets};
use tokio::signal::unix::{SignalKind, signal};
use tokio::task::LocalSet;

use crate::config::{self, Config};
use crate::error::Error;
use crate::http;

pub fn run(config_path: Option<PathBuf>) -> Result<(), Error> {
    let runtime = match tokio::runtime::Builder::new_current_thread().enable_all().build() {
        Ok(runtime) => runtime,
        Err(source) => return Err(Error::Runtime(source)),
    };

    runtime.block_on(LocalSet::new().run_until(serve(config_path)))
}

async fn serve(config_path: Option<PathBuf>) -> Result<(), Error> {
    let config_path = config::path(config_path)?;
    let Config { model, timezone, chat, jobs, hooks, http: http_config, api, limits, sandbox, discord, mcp, memory } =
        config::load(&config_path)?;
    let hook_caps = hooks.iter().map(|(name, hook)| (name.to_owned(), hook.caps.clone())).collect();
    let mut secrets = Secrets::load(shade_core::secrets_path(&config_path))?;
    let http_settings = http::prepare(hooks, api, http_config, &mut secrets)?;
    let config = shade_core::Config { model, timezone, chat, jobs, hooks: hook_caps, limits, sandbox, discord, mcp, memory };
    let mut runtime = Runtime::start(config, &config_path, secrets).await?;
    let server = match http::bind(http_settings, runtime.client()).await {
        Ok(server) => server,
        Err(error) => {
            runtime.shutdown().await;
            return Err(error);
        }
    };

    if let Err(error) = runtime.resume().await {
        runtime.shutdown().await;
        return Err(Error::Core(error));
    }

    let result = tokio::select! {
        () = runtime.serve() => Ok(()),
        () = http::serve(server) => Ok(()),
        result = shutdown_signal() => result,
    };
    runtime.shutdown().await;

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
        _ = terminate.recv() => {},
        _ = interrupt.recv() => {},
    }

    tracing::info!("shutting down");

    Ok(())
}
