//! Best-effort observations, never the acknowledgment of a durable operation.
//! One bounded queue and one writer serve the whole runtime, including usage.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use infinity_agent_core::system::AgentEvent;
use infinity_provider_protocol::Usage;
use shade_store::audit::{Audit, Event};
use shade_store::db::Db;
use shade_store::usage::Usages;
use tokio::sync::{mpsc, oneshot};

use crate::redact::Redactor;

const CAPACITY: usize = 128;

enum Record {
    Event(Event),
    Usage { thread: String, model: String, usage: Usage, estimated_usd: Option<f64> },
    Flush(oneshot::Sender<()>),
}

#[derive(Clone)]
pub struct Telemetry {
    queue: mpsc::Sender<Record>,
    dropped: Arc<AtomicU64>,
    redactor: Arc<Redactor>,
}

impl Telemetry {
    pub fn new(db: Db, redactor: Arc<Redactor>) -> Self {
        let (queue, receiver) = mpsc::channel(CAPACITY);
        let dropped = Arc::new(AtomicU64::new(0));
        tokio::spawn(write(db, receiver, Arc::clone(&dropped)));

        Self { queue, dropped, redactor }
    }

    fn send(&self, record: Record) {
        if self.queue.try_send(record).is_err() {
            let previous = self.dropped.fetch_add(1, Ordering::Relaxed);

            if previous == 0 || previous.is_power_of_two() {
                tracing::warn!(dropped = previous + 1, "audit/usage queue unavailable; observations were lost");
            }
        }
    }

    pub fn event(&self, thread: &str, kind: &str, summary: String) {
        let mut summary = self.redactor.redact(summary);
        summary.truncate(summary.floor_char_boundary(512));

        self.send(Record::Event(Event {
            thread: thread.to_owned(),
            at: chrono::Utc::now().timestamp(),
            kind: kind.to_owned(),
            summary,
        }));
    }

    /// Observe Infinity's handle stream without retaining text, reasoning,
    /// arguments or result bodies. Durable tool outcomes come from Operations.
    pub fn observe(&self, thread: &str, event: &AgentEvent) {
        let (kind, summary) = match event {
            AgentEvent::ToolCall { name, .. } => ("tool", format!("requested {name}; arguments omitted")),
            AgentEvent::ToolResult { segments } => {
                let failed = segments
                    .iter()
                    .any(|segment| matches!(segment, rap_protocol::DisplaySegment::Text(text) if text.starts_with("Error:")));
                let summary = if failed {
                    "tool returned an error; content omitted"
                } else {
                    "tool result received; content omitted"
                };

                ("result", summary.to_owned())
            }
            AgentEvent::CompactionApplied => ("compaction", "summary applied".to_owned()),
            AgentEvent::Info { text } => {
                let summary = if text.contains("limit") {
                    "runtime reported a work limit"
                } else {
                    "runtime diagnostic; content omitted"
                };

                ("diagnostic", summary.to_owned())
            }
            _ => return,
        };

        self.event(thread, kind, summary);
    }

    pub fn usage(&self, thread: &str, model: &str, usage: Usage, estimated_usd: Option<f64>) {
        self.send(Record::Usage { thread: thread.to_owned(), model: model.to_owned(), usage, estimated_usd });
    }

    /// A report and clean shutdown wait for accepted observations. A crash
    /// or pressure can still lose them; no caller uses this to admit work.
    pub async fn flush(&self) {
        let (reply, done) = oneshot::channel();

        if self.queue.send(Record::Flush(reply)).await.is_ok() {
            let _ = done.await;
        }
    }

    pub fn dropped(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }

    pub fn redact(&self, text: String) -> String {
        self.redactor.redact(text)
    }
}

async fn write(db: Db, mut queue: mpsc::Receiver<Record>, dropped: Arc<AtomicU64>) {
    let audit = Audit::new(db.clone());
    let usages = Usages::new(db);

    while let Some(record) = queue.recv().await {
        let result = match record {
            Record::Event(event) => audit.record(event).await,
            Record::Usage { thread, model, usage, estimated_usd } => usages.record_priced(thread, model, usage, estimated_usd).await,
            Record::Flush(reply) => {
                let _ = reply.send(());
                continue;
            }
        };

        if let Err(error) = result {
            dropped.fetch_add(1, Ordering::Relaxed);
            tracing::warn!(%error, "cannot persist an audit/usage observation; durable tool records are separate");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use infinity_agent_core::ThreadId;
    use shade_store::conversation::Conversations;
    use shade_store::operations::{Operations, Started};

    use crate::error::BoxError;

    const SECRET: &str = "private-test-key-123456";

    async fn registered() -> Result<Db, BoxError> {
        let db = Db::in_memory()?;
        Conversations::new(db.clone()).register_root(&ThreadId::from("root".to_owned())).await?;

        Ok(db)
    }

    /// Redaction happens before truncation and writing; arbitrary argument,
    /// result and diagnostic bodies never enter observer persistence.
    #[tokio::test]
    async fn only_safe_summaries_reach_storage() -> Result<(), BoxError> {
        let db = registered().await?;
        let redactor = Arc::new(Redactor::new([SECRET.to_owned()]));
        let telemetry = Telemetry::new(db.clone(), redactor);
        let event = AgentEvent::ToolCall {
            name: "mail_send".to_owned(),
            args: serde_json::json!({"password": "unknown-password", "nested": {"token": SECRET}}),
            display_as: Some("sensitive display text".to_owned()),
        };
        telemetry.observe("root", &event);
        telemetry.observe("root", &AgentEvent::Info { text: "password=unknown-password".to_owned() });
        telemetry.event("root", "test", format!("{}{SECRET}tail", "a".repeat(500)));
        telemetry.flush().await;

        let roots = Audit::new(db).recent(0, None).await?;
        let entries = &roots[0].entries;
        assert_eq!(entries.len(), 3);

        for entry in entries {
            assert!(entry.summary.len() <= 512);
            assert!(!entry.summary.contains(SECRET));
            assert!(!entry.summary.contains("unknown-password"));
            assert!(!entry.summary.contains("sensitive display text"));
        }

        assert!(entries.iter().any(|entry| entry.summary.contains("mail_send")));
        assert!(entries.iter().any(|entry| entry.summary.ends_with("[redacted: o")));

        Ok(())
    }

    /// A stalled observer writer cannot spawn waiting tasks or admit/finish
    /// a tool. The separately awaited operation path still works under pressure.
    #[tokio::test]
    async fn observer_pressure_is_bounded_and_never_acknowledges_work() -> Result<(), BoxError> {
        let db = registered().await?;
        let (queue, receiver) = mpsc::channel(CAPACITY);
        let telemetry = Telemetry { queue, dropped: Default::default(), redactor: Default::default() };

        for _ in 0..(CAPACITY * 10) {
            telemetry.event("root", "completion", "observed".to_owned());
            telemetry.usage("root", "mock", Usage::default(), None);
        }

        assert_eq!(receiver.len(), CAPACITY);
        assert_eq!(telemetry.dropped(), (CAPACITY * 19) as u64);
        let operations = Operations::new(db.clone());
        assert_eq!(operations.start("root", "call", "shell").await?, Started::New);
        assert_eq!(operations.captured("root", "call").await?, None);

        tokio::spawn(write(db.clone(), receiver, Arc::clone(&telemetry.dropped)));
        telemetry.flush().await;

        assert_eq!(operations.start("root", "call", "shell").await?, Started::Again(None));
        assert_eq!(operations.captured("root", "call").await?, None);
        assert!(!Audit::new(db).recent(0, None).await?[0].entries.iter().any(|entry| entry.kind == "outcome"));

        Ok(())
    }
}
