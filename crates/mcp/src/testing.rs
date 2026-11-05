//! An in-process MCP server publishing JSON resources, for tests here and in
//! other crates.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use rmcp::model::{
    ErrorData, Implementation, ReadResourceRequestParams, ReadResourceResponse, ReadResourceResult, ResourceContents, ServerCapabilities,
    ServerConfig, SubscriptionFilter,
};
use rmcp::service::{RequestContext, SubscriptionContext, SubscriptionSink};
use rmcp::{RoleServer, ServerHandler, ServiceExt};
use tokio::sync::{Mutex, Semaphore};

use crate::Server;

const TIMEOUT: Duration = Duration::from_secs(5);

/// A resource server tests change by hand. Clones share one state.
#[derive(Clone, Default)]
pub struct Feed {
    state: Arc<State>,
}

#[derive(Default)]
struct State {
    resources: Mutex<BTreeMap<String, String>>,
    sinks: Mutex<Vec<SubscriptionSink>>,
    listens: AtomicUsize,
    reads: AtomicUsize,
    /// While present, a read waits here for a permit after taking its value.
    gate: Mutex<Option<Arc<Semaphore>>>,
}

impl Feed {
    /// Publishes `text` at `uri`, and tells whoever listens.
    pub async fn set(&self, uri: &str, text: &str) {
        self.state.resources.lock().await.insert(uri.to_owned(), text.to_owned());

        for sink in self.state.sinks.lock().await.iter() {
            let listening = sink.accepted().resource_subscriptions.as_ref().is_some_and(|uris| uris.iter().any(|found| found == uri));

            if listening {
                let _ = sink.notify_resource_updated(uri).await;
            }
        }
    }

    /// A Shade server connected to this feed, named `name`.
    pub async fn connect(&self, name: &str) -> Result<Server, String> {
        let (client, server) = tokio::io::duplex(64 * 1024);
        let feed = self.to_owned();

        tokio::spawn(async move {
            if let Ok(running) = feed.serve(server).await {
                let _ = running.waiting().await;
            }
        });

        Server::connected(name, client, TIMEOUT).await
    }

    /// Reads answered so far.
    pub fn reads(&self) -> usize {
        self.state.reads.load(Ordering::SeqCst)
    }

    /// Subscriptions opened so far.
    pub fn listens(&self) -> usize {
        self.state.listens.load(Ordering::SeqCst)
    }

    /// From now on each read takes its value, then waits for a permit from
    /// the returned gate, so a test can change the resource during a read.
    pub async fn hold_reads(&self) -> Arc<Semaphore> {
        let gate = Arc::new(Semaphore::new(0));
        *self.state.gate.lock().await = Some(Arc::clone(&gate));

        gate
    }
}

impl ServerHandler for Feed {
    fn get_info(&self) -> ServerConfig {
        let capabilities = ServerCapabilities::builder().enable_resources().enable_resources_subscribe().build();

        ServerConfig::new(capabilities).with_server_info(Implementation::new("feed", "1.0.0"))
    }

    fn accepted_subscription_filter(&self, requested: &SubscriptionFilter) -> Option<SubscriptionFilter> {
        Some(requested.to_owned())
    }

    async fn listen(&self, context: SubscriptionContext) -> Result<(), ErrorData> {
        self.state.sinks.lock().await.push(context.sink().to_owned());
        self.state.listens.fetch_add(1, Ordering::SeqCst);
        context.cancelled().await;

        Ok(())
    }

    async fn read_resource(
        &self, request: ReadResourceRequestParams, _context: RequestContext<RoleServer>,
    ) -> Result<ReadResourceResponse, ErrorData> {
        let found = self.state.resources.lock().await.get(&request.uri).map(String::to_owned);

        let Some(text) = found else {
            return Err(ErrorData::resource_not_found(format!("no {}", request.uri), None));
        };

        let gate = self.state.gate.lock().await.as_ref().map(Arc::clone);

        if let Some(gate) = gate
            && let Ok(permit) = gate.acquire().await
        {
            permit.forget();
        }

        self.state.reads.fetch_add(1, Ordering::SeqCst);

        // Offered for a minute: the client must not take it up on that.
        let result = ReadResourceResult::new(vec![ResourceContents::text(text, request.uri)]).with_ttl_ms(60_000);

        Ok(result.into())
    }
}
