//! Follows the resources active watches observe, without any model call.
//!
//! One pipeline per `(server, uri)` serves every watch on that resource. It
//! listens before it reads, so no change slips between the two, and reads
//! one at a time; a change arriving during a read is waiting when it is done.
//! A failed read or a lost subscription means listen and read again, after a
//! backoff. The persisted state is what stops a reconnect firing again: a
//! condition's last result, or the items a `new_items` watch has seen.

use std::collections::btree_map::Entry;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use chrono::{DateTime, Utc};
use serde_json::Value;
use shade_mcp::{Server, Servers};
use shade_policy::{Cap, Policy};
use shade_store::db::Db;
use shade_store::watches::{Mode, Observation, Watch, Watches};
use shade_watch::Prior;
use tokio::sync::{Notify, mpsc};
use tokio::task::AbortHandle;

use crate::notify::Notifier;
use crate::redact::Redactor;
use crate::router::Inbound;

type Source = (String, String);

pub struct Watcher {
    watches: Watches,
    servers: Arc<Servers>,
    policy: Arc<Policy>,
    redactor: Arc<Redactor>,
    notifier: Option<Notifier>,
    /// Tells the router something fired.
    inbound: mpsc::Sender<Inbound>,
    /// Woken when the set of watched resources may have changed.
    changed: Arc<Notify>,
}

impl Watcher {
    pub fn new(
        db: Db, servers: Arc<Servers>, policy: Arc<Policy>, redactor: Arc<Redactor>, notifier: Option<Notifier>,
        inbound: mpsc::Sender<Inbound>, changed: Arc<Notify>,
    ) -> Self {
        Self { watches: Watches::new(db), servers, policy, redactor, notifier, inbound, changed }
    }

    /// Keeps one pipeline per watched resource, for as long as the daemon runs.
    pub async fn run(self: Arc<Self>) {
        let mut running = BTreeMap::new();

        loop {
            self.reconcile(&mut running).await;
            self.changed.notified().await;
        }
    }

    async fn reconcile(self: &Arc<Self>, running: &mut BTreeMap<Source, AbortHandle>) {
        let wanted = match self.watches.sources().await {
            Ok(sources) => self.allowed(sources),
            Err(error) => {
                tracing::error!(%error, "cannot read which resources are watched");
                return;
            }
        };

        running.retain(|source, task| {
            let keep = wanted.contains(source) && !task.is_finished();

            if !keep {
                task.abort();
            }

            keep
        });

        for source in wanted {
            let Some(server) = self.servers.get(&source.0) else {
                continue;
            };

            if let Entry::Vacant(slot) = running.entry(source) {
                let pipeline = Arc::clone(self).pipeline(Arc::clone(server), slot.key().1.to_owned());
                slot.insert(tokio::spawn(pipeline).abort_handle());
            }
        }
    }

    /// A watch reads with the authority of owner chat, which created it. A
    /// server chat may no longer use is not read, whatever the watch says.
    fn allowed(&self, sources: Vec<Source>) -> BTreeSet<Source> {
        let held = |source: &Source| self.policy.chat.contains(&Cap::Server(source.0.to_owned()));

        sources.into_iter().filter(held).collect()
    }

    async fn pipeline(self: Arc<Self>, server: Arc<Server>, uri: String) {
        let mut attempts = 0;

        loop {
            let reason = self.follow(&server, &uri, &mut attempts).await;

            tracing::debug!(server = %server.name(), %uri, %reason, "lost a watched resource");
            self.report(&server, &uri, reason).await;

            tokio::time::sleep(shade_watch::backoff(attempts)).await;
            attempts = attempts.saturating_add(1);
        }
    }

    /// Follows the resource until that fails, returning why.
    async fn follow(&self, server: &Server, uri: &str, attempts: &mut u32) -> String {
        let mut updates = match server.listen(uri).await {
            Ok(updates) => updates,
            Err(reason) => return reason,
        };

        loop {
            if let Err(reason) = self.observe(server, uri).await {
                return reason;
            }

            *attempts = 0;

            if let Err(reason) = updates.changed().await {
                return reason;
            }
        }
    }

    /// Reads the resource once, for every watch on it.
    async fn observe(&self, server: &Server, uri: &str) -> Result<(), String> {
        let document = server.snapshot(uri).await?;
        let value = self.redactor.redact(document.to_string());
        let now = Utc::now();

        let watches = match self.watches.of_source(server.name(), uri).await {
            Ok(watches) => watches,
            Err(error) => return Err(format!("cannot read watches: {error}")),
        };

        let mut fired = false;

        for watch in &watches {
            fired |= self.judge(watch, &document, &value, now).await;
        }

        if fired {
            // Durable already; the scheduler tick covers a full queue.
            let _ = self.inbound.try_send(Inbound::StartBackground);
            self.changed.notify_one();
        }

        Ok(())
    }

    /// Evaluates one watch, recording the outcome. `true` when it fired.
    async fn judge(&self, watch: &Watch, document: &Value, value: &str, now: DateTime<Utc>) -> bool {
        let observation = match self.evaluate(watch, document, value, now).await {
            Ok(observation) => observation,
            Err(reason) => {
                self.record_failure(watch, &reason).await;
                return false;
            }
        };

        let fired = observation.fired.is_some();

        match self.watches.observe(observation).await {
            Ok(recorded) => recorded && fired,
            Err(error) => {
                tracing::error!(%error, watch = %watch.id, "cannot record an observation");
                false
            }
        }
    }

    /// What one reading means for a watch. A failure is the watch's alone and
    /// keeps the subscription: a resource may only recover while listened to.
    async fn evaluate(&self, watch: &Watch, document: &Value, value: &str, now: DateTime<Utc>) -> Result<Observation, String> {
        shade_watch::available(document)?;

        match watch.definition.mode {
            Mode::NewItems => self.new_items(watch, document, value, now).await,
            Mode::OnChange | Mode::EveryMatch => condition(watch, document, value, now),
        }
    }

    /// Compares what is listed with what the watch remembers. The first
    /// observation only remembers, as a baseline never fires.
    async fn new_items(&self, watch: &Watch, document: &Value, value: &str, now: DateTime<Utc>) -> Result<Observation, String> {
        let mut items = shade_watch::items(document, &watch.definition.condition)?;

        for item in &mut items {
            item.id = self.redactor.redact(std::mem::take(&mut item.id));
        }

        let seen = match self.watches.seen(&watch.id).await {
            Ok(seen) => seen,
            Err(error) => return Err(format!("cannot read what it has seen: {error}")),
        };

        let new: Vec<&Value> = items.iter().filter(|item| !seen.contains(&item.id)).map(|item| item.value).collect();

        let fired = match (watch.last_condition, new.is_empty()) {
            (Some(_), false) => Some(shade_watch::input(watch, &self.redactor.redact(serde_json::json!(new).to_string()), now)),
            _ => None,
        };

        Ok(Observation {
            watch: watch.id.to_owned(),
            version: watch.version,
            condition: !items.is_empty(),
            value: value.to_owned(),
            items: items.into_iter().map(|item| item.id).collect(),
            fired,
            at: now.timestamp(),
        })
    }

    /// The resource itself failed: every watch on it failed.
    async fn report(&self, server: &Server, uri: &str, reason: String) {
        let reason = self.redactor.redact(reason);

        let watches = match self.watches.of_source(server.name(), uri).await {
            Ok(watches) => watches,
            Err(error) => {
                tracing::error!(%error, "cannot read watches");
                return;
            }
        };

        for watch in &watches {
            self.record_failure(watch, &reason).await;
        }
    }

    async fn record_failure(&self, watch: &Watch, reason: &str) {
        let alert = match self.watches.fail(&watch.id, watch.version, reason).await {
            Ok(alert) => alert,
            Err(error) => {
                tracing::error!(%error, watch = %watch.id, "cannot record a failed observation");
                return;
            }
        };

        if let (true, Some(notifier)) = (alert, &self.notifier) {
            let text = format!("⚠️ The watch \"{}\" keeps failing, so it may miss changes: {reason}", watch.definition.title);
            notifier.alert(text).await;
        }
    }
}

/// Decides a watch with a condition. An error leaves its state as it was.
fn condition(watch: &Watch, document: &Value, value: &str, now: DateTime<Utc>) -> Result<Observation, String> {
    let definition = &watch.definition;
    let matched = shade_watch::evaluate(document, &definition.condition, definition.max_age_seconds, now.timestamp())?;

    let prior = Prior { condition: watch.last_condition, last_fired: watch.last_fired_at, cooldown: definition.cooldown_seconds };
    let decision = shade_watch::step(&prior, matched, definition.mode, now.timestamp());

    Ok(Observation {
        watch: watch.id.to_owned(),
        version: watch.version,
        condition: decision.condition,
        value: value.to_owned(),
        items: Vec::new(),
        fired: decision.fire.then(|| shade_watch::input(watch, value, now)),
        at: now.timestamp(),
    })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use infinity_agent_core::ThreadId;
    use shade_mcp::Servers;
    use shade_mcp::testing::Feed;
    use shade_policy::{Cap, Limits};
    use shade_store::conversation::Conversations;
    use shade_store::db::Db;
    use shade_store::schedules::Receipt;
    use shade_store::watches::{Change, Definition, Destination, Watch, Watches};
    use tokio::sync::{Notify, mpsc};
    use tokio::task::JoinHandle;

    use super::Watcher;
    use crate::error::BoxError;
    use crate::redact::Redactor;
    use crate::router::Inbound;
    use crate::testing;

    const URI: &str = "feed://price";

    struct Rig {
        db: Db,
        watches: Watches,
        // Kept open, so the watcher's messages have somewhere to go.
        _inbound: mpsc::Receiver<Inbound>,
        sender: mpsc::Sender<Inbound>,
    }

    impl Rig {
        async fn new() -> Result<Self, BoxError> {
            let db = Db::in_memory()?;
            Conversations::new(db.clone()).register_root(&ThreadId::from("chat".to_owned())).await?;

            let (sender, _inbound) = mpsc::channel(8);

            Ok(Self { watches: Watches::new(db.clone()), db, _inbound, sender })
        }

        async fn watch(&self, id: &str) -> Result<(), BoxError> {
            self.watches.add(id, "chat", Destination::Run).await?;

            Ok(())
        }

        /// A watcher connected to `feed`, for a chat holding `caps`.
        async fn start(&self, feed: &Feed, caps: &[Cap]) -> Result<JoinHandle<()>, BoxError> {
            let servers = Arc::new(Servers::new(vec![feed.connect("feed").await?]));
            let policy = testing::policy(caps, Limits::default());
            let redactor = Arc::new(Redactor::new(["private-test-key-123456".to_owned()]));
            let changed = Arc::new(Notify::new());
            let watcher = Watcher::new(self.db.clone(), servers, policy, redactor, None, self.sender.to_owned(), changed);

            Ok(tokio::spawn(Arc::new(watcher).run()))
        }

        /// Makes `new_items` watch `id` follow `/items`.
        async fn point_at_items(&self, id: &str) -> Result<(), BoxError> {
            let watch = self.watches.get(id).await?.ok_or("watch")?;
            let definition = Definition { condition: "/items".to_owned(), ..watch.definition };
            let update = Change::Update { id: id.to_owned(), version: watch.version, enabled: true, definition };
            let receipt = Receipt { thread: "chat".to_owned(), call: "point".to_owned(), tool: "watch_update", text: String::new() };
            self.watches.apply(update, receipt).await?;

            Ok(())
        }

        /// The value section of each run that may start now.
        async fn items_fired(&self) -> Result<Vec<String>, BoxError> {
            let queued = self.watches.startable(4).await?;
            let sections =
                queued.iter().filter_map(|run| run.input.split_once("<value>\n")).map(|(_, rest)| rest.trim_end_matches("\n</value>"));

            Ok(sections.map(str::to_owned).collect())
        }

        /// Starts and ends every run that may start now.
        async fn finish_runs(&self) -> Result<(), BoxError> {
            for queued in self.watches.startable(4).await? {
                self.watches.start(queued.run.to_owned()).await?;
                self.watches.finish(&queued.run, None).await?;
            }

            Ok(())
        }

        /// Waits until watch `id` satisfies `ready`.
        async fn until(&self, id: &str, ready: impl Fn(&Watch) -> bool) -> Result<(), BoxError> {
            for _ in 0..500 {
                if self.watches.get(id).await?.is_some_and(|watch| ready(&watch)) {
                    return Ok(());
                }

                tokio::time::sleep(Duration::from_millis(10)).await;
            }

            Err(format!("watch {id} never got there: {:?}", self.watches.get(id).await?).into())
        }
    }

    fn held() -> [Cap; 1] {
        [Cap::Server("feed".to_owned())]
    }

    fn valued(value: &str) -> impl Fn(&Watch) -> bool {
        move |watch| watch.last_value.as_deref() == Some(value)
    }

    /// Every watch on a resource shares its subscription and its reads.
    #[tokio::test]
    async fn watches_on_one_resource_share_a_pipeline() -> Result<(), BoxError> {
        let rig = Rig::new().await?;
        let feed = Feed::default();
        feed.set(URI, "90").await;
        rig.watch("a").await?;
        rig.watch("b").await?;

        let watcher = rig.start(&feed, &held()).await?;
        rig.until("b", valued("90")).await?;

        feed.set(URI, "110").await;
        rig.until("a", valued("110")).await?;
        rig.until("b", valued("110")).await?;

        assert_eq!(feed.listens(), 1);
        assert_eq!(feed.reads(), 2);
        assert_eq!(rig.watches.startable(4).await?.len(), 2);

        watcher.abort();

        Ok(())
    }

    /// A change landing during a read is not lost to the value that read saw.
    #[tokio::test]
    async fn a_change_during_a_read_is_read_again() -> Result<(), BoxError> {
        let rig = Rig::new().await?;
        let feed = Feed::default();
        feed.set(URI, "90").await;
        rig.watch("a").await?;

        let gate = feed.hold_reads().await;
        gate.add_permits(1);

        let watcher = rig.start(&feed, &held()).await?;
        rig.until("a", valued("90")).await?;

        feed.set(URI, "110").await;
        tokio::time::sleep(Duration::from_millis(100)).await;

        feed.set(URI, "95").await;
        gate.add_permits(2);

        rig.until("a", valued("95")).await?;
        assert_eq!(feed.reads(), 3);
        assert_eq!(rig.watches.startable(4).await?.len(), 1, "the read in between saw a crossing");

        watcher.abort();

        Ok(())
    }

    /// A new connection finding the value still true is not a transition.
    #[tokio::test]
    async fn reconnecting_while_true_does_not_fire_again() -> Result<(), BoxError> {
        let rig = Rig::new().await?;
        let feed = Feed::default();
        feed.set(URI, "90").await;
        rig.watch("a").await?;

        let first = rig.start(&feed, &held()).await?;
        rig.until("a", valued("90")).await?;
        feed.set(URI, "110").await;
        rig.until("a", valued("110")).await?;
        first.abort();

        for queued in rig.watches.startable(4).await? {
            rig.watches.start(queued.run.to_owned()).await?;
            rig.watches.finish(&queued.run, None).await?;
        }

        let again = Feed::default();
        again.set(URI, "120").await;

        let second = rig.start(&again, &held()).await?;
        rig.until("a", valued("120")).await?;

        assert!(rig.watches.startable(4).await?.is_empty());

        second.abort();

        Ok(())
    }

    /// A resource that stops being readable fails its watches and leaves
    /// their state alone, and they pick up where they were once it recovers.
    #[tokio::test]
    async fn a_bad_read_preserves_the_condition() -> Result<(), BoxError> {
        let rig = Rig::new().await?;
        let feed = Feed::default();
        feed.set(URI, "110").await;
        rig.watch("a").await?;

        let watcher = rig.start(&feed, &held()).await?;
        rig.until("a", valued("110")).await?;

        feed.set(URI, "not json").await;
        rig.until("a", |watch| watch.failures > 0).await?;

        let failing = rig.watches.get("a").await?.ok_or("watch")?;
        assert_eq!(failing.last_condition, Some(true));
        assert_eq!(failing.last_value.as_deref(), Some("110"));

        feed.set(URI, "120").await;
        rig.until("a", |watch| watch.failures == 0 && watch.last_value.as_deref() == Some("120")).await?;

        assert!(rig.watches.startable(4).await?.is_empty(), "true to true across a failure is no crossing");

        watcher.abort();

        Ok(())
    }

    /// What fired a live check: a restart finds the old list, and the first
    /// poll adds to it seconds later. Only the new item fires, at once.
    #[tokio::test]
    async fn only_unseen_items_fire_however_soon() -> Result<(), BoxError> {
        let rig = Rig::new().await?;
        let feed = Feed::default();
        feed.set(URI, r#"[{"id":1}]"#).await;
        rig.watches.add_items("a", "chat").await?;

        let first = rig.start(&feed, &held()).await?;
        rig.until("a", valued(r#"[{"id":1}]"#)).await?;
        assert!(rig.watches.startable(4).await?.is_empty(), "the baseline never fires");

        feed.set(URI, r#"[{"id":1},{"id":2}]"#).await;
        rig.until("a", valued(r#"[{"id":1},{"id":2}]"#)).await?;
        first.abort();

        assert_eq!(rig.items_fired().await?, ["[{\"id\":2}]"]);
        rig.finish_runs().await?;

        let again = Feed::default();
        again.set(URI, r#"[{"id":1},{"id":2}]"#).await;

        let second = rig.start(&again, &held()).await?;
        rig.until("a", |_| again.reads() > 0).await?;
        assert!(rig.watches.startable(4).await?.is_empty(), "a reconnect finding the same list fires nothing");

        again.set(URI, r#"[{"id":2},{"id":3}]"#).await;
        rig.until("a", valued(r#"[{"id":2},{"id":3}]"#)).await?;

        assert_eq!(rig.items_fired().await?, ["[{\"id\":3}]"]);

        second.abort();

        Ok(())
    }

    /// Firings waiting for capacity each keep their items, and a watch still
    /// runs one at a time.
    #[tokio::test]
    async fn new_items_are_not_lost_while_a_run_waits() -> Result<(), BoxError> {
        let rig = Rig::new().await?;
        let feed = Feed::default();
        feed.set(URI, "[]").await;
        rig.watches.add_items("a", "chat").await?;

        let watcher = rig.start(&feed, &held()).await?;
        rig.until("a", valued("[]")).await?;

        feed.set(URI, r#"[{"id":1}]"#).await;
        rig.until("a", valued(r#"[{"id":1}]"#)).await?;
        feed.set(URI, r#"[{"id":1},{"id":2}]"#).await;
        rig.until("a", valued(r#"[{"id":1},{"id":2}]"#)).await?;

        assert_eq!(rig.watches.statuses().await?, ["queued", "queued"]);
        assert_eq!(rig.items_fired().await?, ["[{\"id\":1}]"], "one at a time");

        rig.finish_runs().await?;
        assert_eq!(rig.items_fired().await?, ["[{\"id\":2}]"]);

        watcher.abort();

        Ok(())
    }

    /// An entry without an ID stops the watch from following, loudly.
    #[tokio::test]
    async fn a_list_that_cannot_be_followed_fails() -> Result<(), BoxError> {
        let rig = Rig::new().await?;
        let feed = Feed::default();
        feed.set(URI, r#"[{"name":"no id"}]"#).await;
        rig.watches.add_items("a", "chat").await?;

        let watcher = rig.start(&feed, &held()).await?;
        rig.until("a", |watch| watch.failures > 0).await?;

        assert_eq!(rig.watches.get("a").await?.ok_or("watch")?.last_condition, None, "no baseline from a bad list");

        watcher.abort();

        Ok(())
    }

    /// A resource saying it is unavailable fails its watch, however healthy
    /// its value looks, and each report counts towards the alert. It stays
    /// subscribed, and picks up where it was once the resource recovers.
    #[tokio::test]
    async fn an_unavailable_resource_fails_without_losing_its_place() -> Result<(), BoxError> {
        let rig = Rig::new().await?;
        let feed = Feed::default();
        let document = |availability: &str, items: &str| format!(r#"{{"availability":"{availability}","items":{items}}}"#);
        feed.set(URI, &document("available", r#"[{"id":1}]"#)).await;
        rig.watches.add_items("a", "chat").await?;
        rig.point_at_items("a").await?;

        let watcher = rig.start(&feed, &held()).await?;
        rig.until("a", |watch| watch.last_condition == Some(true)).await?;

        for failures in 1..=3 {
            feed.set(URI, &document("unavailable", &format!(r#"[{{"id":{failures}}},{{"id":9}}]"#))).await;
            rig.until("a", |watch| watch.failures == failures).await?;
        }

        assert!(rig.watches.startable(4).await?.is_empty(), "nothing it said while unavailable counts");
        assert_eq!(feed.listens(), 1, "the subscription is what lets it recover");

        feed.set(URI, &document("available", r#"[{"id":1},{"id":2}]"#)).await;
        rig.until("a", |watch| watch.failures == 0).await?;

        assert_eq!(rig.items_fired().await?, ["[{\"id\":2}]"]);

        watcher.abort();

        Ok(())
    }

    /// A watch cannot keep reading a server owner chat lost.
    #[tokio::test]
    async fn a_server_chat_no_longer_holds_is_not_read() -> Result<(), BoxError> {
        let rig = Rig::new().await?;
        let feed = Feed::default();
        feed.set(URI, "110").await;
        rig.watch("a").await?;

        let watcher = rig.start(&feed, &[Cap::Now]).await?;
        tokio::time::sleep(Duration::from_millis(100)).await;

        assert_eq!((feed.listens(), feed.reads()), (0, 0));

        watcher.abort();

        Ok(())
    }

    /// A secret in a value reaches neither the database nor the run.
    #[tokio::test]
    async fn secrets_are_removed_from_stored_values() -> Result<(), BoxError> {
        let rig = Rig::new().await?;
        let feed = Feed::default();
        let document = |price: i64| format!(r#"{{"price":{price},"token":"private-test-key-123456"}}"#);
        feed.set(URI, &document(90)).await;

        let definition = r#"{"pointer":"/price","op":">","value":100}"#;
        rig.watch("a").await?;
        let watch = rig.watches.get("a").await?.ok_or("watch")?;
        let condition = Definition { condition: definition.to_owned(), ..watch.definition };
        let update = Change::Update { id: "a".to_owned(), version: 1, enabled: true, definition: condition };
        let receipt = Receipt { thread: "chat".to_owned(), call: "update".to_owned(), tool: "watch_update", text: String::new() };
        rig.watches.apply(update, receipt).await?;

        let watcher = rig.start(&feed, &held()).await?;
        rig.until("a", |watch| watch.last_condition == Some(false)).await?;

        feed.set(URI, &document(110)).await;
        rig.until("a", |watch| watch.last_condition == Some(true)).await?;

        let stored = rig.watches.get("a").await?.ok_or("watch")?.last_value.unwrap_or_default();
        let queued = rig.watches.startable(4).await?;

        assert!(!stored.contains("private-test-key"), "{stored}");
        assert!(queued.iter().all(|run| !run.input.contains("private-test-key")));
        assert_eq!(queued.len(), 1);

        watcher.abort();

        Ok(())
    }
}
