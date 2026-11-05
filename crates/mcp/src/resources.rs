//! Resources: what a server publishes, read on demand or listened to.
//!
//! A change notification only says a URI changed, so it is an invalidation,
//! not an event: whoever listens reads the resource again. A subscription
//! does not survive the connection, and ending for any reason means listen
//! again and reread.

use std::time::Duration;

use rmcp::model::{ReadResourceRequestParams, ResourceContents, SubscriptionFilter};
use rmcp::service::{ServiceError, Subscription, SubscriptionEnd};
use rmcp::{Peer, RoleClient};
use serde_json::Value;

use crate::{Answer, CallError, Features, Server, render};

/// What the peer of a server that just connected says it can do.
pub fn features(peer: &Peer<RoleClient>) -> Features {
    let Some(info) = peer.peer_info() else {
        return Features::default();
    };

    let Some(resources) = &info.capabilities.resources else {
        return Features::default();
    };

    Features { resources: true, subscribe: resources.subscribe == Some(true) }
}

impl Server {
    /// Every resource and template it publishes, as text for the model.
    pub async fn resources(&self) -> Result<Answer, CallError> {
        let peer = self.running().await?;

        let resources = match self.request(peer.list_all_resources()).await {
            Ok(resources) => resources,
            Err(reason) => return Ok(Answer::Failed(reason)),
        };

        match self.request(peer.list_all_resource_templates()).await {
            Ok(templates) => Ok(Answer::Done(render::listing(resources, templates))),
            Err(reason) => Ok(Answer::Failed(reason)),
        }
    }

    /// A resource as text for the model. A read changes nothing, so one
    /// that fails has no unknown outcome: it can simply be tried again.
    pub async fn read(&self, uri: &str) -> Result<Answer, CallError> {
        let peer = self.running().await?;

        match self.fetch(&peer, uri).await {
            Ok(contents) => Ok(render::contents(contents)),
            Err(reason) => Ok(Answer::Failed(reason)),
        }
    }

    /// The current JSON snapshot of a resource, starting the server if it is down.
    pub async fn snapshot(&self, uri: &str) -> Result<Value, String> {
        let peer = self.peer().await?;

        render::snapshot(self.fetch(&peer, uri).await?, uri)
    }

    /// Starts listening for changes to `uri`, starting the server if it is
    /// down. Changes after this returns are seen, so read after listening.
    pub async fn listen(&self, uri: &str) -> Result<Updates, String> {
        let peer = self.peer().await?;
        let filter = SubscriptionFilter::builder().resource_subscription(uri).build();

        let subscription = self.request(peer.listen(filter)).await?;
        let accepted = subscription.acknowledged().resource_subscriptions.as_ref();

        if !accepted.is_some_and(|uris| uris.iter().any(|found| found == uri)) {
            return Err(format!("the {} server does not accept subscriptions to `{uri}`", self.name));
        }

        Ok(Updates { subscription })
    }

    async fn running(&self) -> Result<Peer<RoleClient>, CallError> {
        match self.peer().await {
            Ok(peer) => Ok(peer),
            Err(reason) => Err(CallError::Unavailable(self.name.to_owned(), reason)),
        }
    }

    async fn fetch(&self, peer: &Peer<RoleClient>, uri: &str) -> Result<Vec<ResourceContents>, String> {
        let params = ReadResourceRequestParams::new(uri);
        let result = self.request(peer.read_resource(params)).await?;

        Ok(result.contents)
    }
}

/// Changes to one resource.
pub struct Updates {
    subscription: Subscription,
}

impl Updates {
    /// Waits until the resource has changed, then takes every change already
    /// waiting: a burst means one reread. A change arriving later, even
    /// during that read, is left for the next wait. An error means the
    /// subscription ended.
    pub async fn changed(&mut self) -> Result<(), String> {
        next(&mut self.subscription).await?;

        // Polled once: whatever is not ready yet belongs to the next wait.
        while let Ok(Ok(Some(_))) = tokio::time::timeout(Duration::ZERO, self.subscription.next()).await {}

        Ok(())
    }
}

async fn next(subscription: &mut Subscription) -> Result<(), String> {
    let error: ServiceError = match subscription.next().await {
        Ok(Some(_)) => return Ok(()),
        Ok(None) => return Err(ended(subscription.end())),
        Err(error) => error,
    };

    Err(error.to_string())
}

fn ended(end: Option<&SubscriptionEnd>) -> String {
    match end {
        Some(SubscriptionEnd::Lagged { capacity }) => format!("more than {capacity} changes were left waiting"),
        Some(SubscriptionEnd::Graceful(_)) => "the server ended the subscription".to_owned(),
        _ => "the connection closed".to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use crate::testing::Feed;
    use crate::{Answer, Features};

    type Result = std::result::Result<(), Box<dyn std::error::Error + Send + Sync>>;

    /// A refused read leaves the connection up, or a mistyped URI would
    /// restart a server its tools share. What comes next is the fresh value,
    /// even though the server offered the last one as good for a minute.
    #[tokio::test]
    async fn reads_are_fresh_and_a_refusal_breaks_nothing() -> Result {
        let feed = Feed::default();
        feed.set("feed://a", "1").await;
        let server = feed.connect("feed").await?;

        assert_eq!(server.features().await, Features { resources: true, subscribe: true });
        assert_eq!(server.read("feed://a").await?, Answer::Done("1".to_owned()));

        assert!(matches!(server.read("feed://missing").await?, Answer::Failed(_)));

        feed.set("feed://a", "2").await;
        assert_eq!(server.read("feed://a").await?, Answer::Done("2".to_owned()));

        Ok(())
    }

    /// A burst already waiting is one change, and nothing is left behind to
    /// wake the next wait.
    #[tokio::test]
    async fn a_burst_is_one_change() -> Result {
        let feed = Feed::default();
        feed.set("feed://a", "0").await;
        let server = feed.connect("feed").await?;
        let mut updates = server.listen("feed://a").await?;

        for value in ["1", "2", "3"] {
            feed.set("feed://a", value).await;
        }

        tokio::time::sleep(Duration::from_millis(100)).await;
        updates.changed().await?;

        assert!(tokio::time::timeout(Duration::from_millis(100), updates.changed()).await.is_err());

        Ok(())
    }

    /// Changes to another resource do not wake a listener.
    #[tokio::test]
    async fn listening_is_per_resource() -> Result {
        let feed = Feed::default();
        let server = feed.connect("feed").await?;
        let mut updates = server.listen("feed://a").await?;

        feed.set("feed://b", "1").await;

        assert!(tokio::time::timeout(Duration::from_millis(100), updates.changed()).await.is_err());

        Ok(())
    }
}
