//! HTTP transport for Shade's personal API and webhooks. Runtime operations
//! use shade-core's typed client; this crate does not access stores or Infinity.

mod api;
mod webhook;

use std::collections::BTreeMap;
use std::sync::Arc;

use shade_core::Client;
use thiserror::Error;
use tokio::net::TcpListener;

pub use webhook::{Hook, MIN_TOKEN};

#[derive(Debug, Error)]
pub enum Error {
    #[error("{0}")]
    Listen(String),
    #[error("cannot serve HTTP on {listen} ([http] listen): {source}")]
    Bind { listen: String, source: std::io::Error },
    #[error("{0} token must contain at least 32 bytes")]
    Token(String),
    #[error("[hooks.{hook}] condition: {message}")]
    Condition { hook: String, message: String },
}

/// A bound HTTP server. The daemon owns its lifetime alongside the runtime.
pub struct Server {
    listener: TcpListener,
    handlers: Arc<webhook::Webhooks>,
}

impl Server {
    pub async fn bind(listen: &str, hooks: BTreeMap<String, Hook>, api_token: Option<String>, client: Client) -> Result<Self, Error> {
        validate(&hooks, api_token.as_deref())?;
        let address = match webhook::listen_address(listen) {
            Ok(address) => address,
            Err(message) => return Err(Error::Listen(message)),
        };
        let listener = match TcpListener::bind(address).await {
            Ok(listener) => listener,
            Err(source) => return Err(Error::Bind { listen: listen.to_owned(), source }),
        };
        let api = api_token.map(|token| api::Api::new(token, client.clone()));
        tracing::info!(%listen, hooks = hooks.len(), api = api.is_some(), "serving HTTP");
        let handlers = Arc::new(webhook::Webhooks::new(hooks, client).with_api(api));

        Ok(Self { listener, handlers })
    }

    pub async fn serve(self) {
        self.handlers.serve(self.listener).await;
    }
}

fn validate(hooks: &BTreeMap<String, Hook>, api_token: Option<&str>) -> Result<(), Error> {
    if api_token.is_some_and(|token| token.len() < MIN_TOKEN) {
        return Err(Error::Token("[api]".to_owned()));
    }

    for (name, hook) in hooks {
        if hook.token.len() < MIN_TOKEN {
            return Err(Error::Token(format!("[hooks.{name}]")));
        }

        if let Some(Err(message)) = hook.condition.as_ref().map(shade_condition::Condition::validate) {
            return Err(Error::Condition { hook: name.to_owned(), message });
        }
    }

    Ok(())
}
