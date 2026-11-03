//! Checks a thread's authority and charges its budget before any work runs.
//!
//! Three points ask: thread configuration (which tools exist), every provider
//! request, and every Shade tool call. Infinity calls the provider again
//! inside one step after synchronous tools and unknown-tool errors, without
//! resolving the model again, so the provider request is where completion
//! rounds are counted.

use std::sync::Arc;

use async_trait::async_trait;
use futures_util::StreamExt;
use infinity_agent_core::ThreadId;
use infinity_agent_core::system::{ModelSource, ResolvedModel};
use infinity_provider_protocol::{
    CompletionError, CompletionRequest, ErrorClass, FinalResponse, ModelEntry, ModelProvider, ModelStream, StreamChunk, Usage,
};
use thiserror::Error;

use crate::error::BoxError;
use crate::policy::{Cap, Caps, Charge, Origin, Policy};
use crate::store::db::DbError;
use crate::store::grants::{Grants, Refusal};
use crate::store::usage::Usages;

#[derive(Debug, Error)]
pub enum Denied {
    #[error("thread {0} has no grant")]
    NoGrant(String),

    #[error("the {0} that started this conversation is no longer configured")]
    SourceRemoved(&'static str),

    #[error("`{0}` is not granted to this conversation")]
    NotGranted(&'static str),

    #[error("thread {0} has no work budget")]
    NoBudget(String),

    #[error("this turn reached its {0} limit (see [limits] in the config)")]
    Limit(String),

    #[error(transparent)]
    Db(#[from] DbError),
}

/// A thread's authority under the current policy.
#[derive(Debug)]
pub struct Authorized {
    pub origin: Origin,
    pub caps: Caps,
}

#[derive(Clone)]
pub struct Authority {
    grants: Grants,
    policy: Arc<Policy>,
}

impl Authority {
    pub fn new(grants: Grants, policy: Arc<Policy>) -> Self {
        Self { grants, policy }
    }

    pub fn policy(&self) -> &Policy {
        &self.policy
    }

    /// The authority of the root that owns `thread`. Fails closed.
    pub async fn authorize(&self, thread: &ThreadId<str>) -> Result<Authorized, Denied> {
        let Some(grant) = self.grants.of(thread).await? else {
            return Err(Denied::NoGrant(thread.as_str().to_owned()));
        };

        let Some(caps) = self.policy.effective(&grant) else {
            return Err(Denied::SourceRemoved(grant.origin.name()));
        };

        Ok(Authorized { origin: grant.origin, caps })
    }

    /// Admits one call of a tool that needs `cap`, returning the caller's authority.
    pub async fn admit_tool(&self, thread: &ThreadId<str>, cap: Cap) -> Result<Authorized, Denied> {
        let authorized = self.authorize(thread).await?;

        if !authorized.caps.contains(&cap) {
            return Err(Denied::NotGranted(cap.name()));
        }

        self.charge(thread, Charge::ToolCall).await?;

        Ok(authorized)
    }

    /// Admits one request to the model provider.
    pub async fn admit_completion(&self, thread: &ThreadId<str>) -> Result<(), Denied> {
        self.authorize(thread).await?;

        self.charge(thread, Charge::Completion).await
    }

    async fn charge(&self, thread: &ThreadId<str>, charge: Charge) -> Result<(), Denied> {
        match self.grants.charge(thread, charge, self.policy.limits).await? {
            None => Ok(()),
            Some(Refusal::NoBudget) => Err(Denied::NoBudget(thread.as_str().to_owned())),
            Some(Refusal::Limit(limit)) => Err(Denied::Limit(limit)),
        }
    }
}

/// A model source whose every provider request is admitted first, and
/// whose reported token usage is recorded.
pub struct Metered<S> {
    inner: S,
    authority: Authority,
    usages: Usages,
}

impl<S> Metered<S> {
    pub fn new(inner: S, authority: Authority, usages: Usages) -> Self {
        Self { inner, authority, usages }
    }
}

#[async_trait(?Send)]
impl<S: ModelSource> ModelSource for Metered<S> {
    async fn resolve(&self, thread_id: &ThreadId<str>) -> Result<ResolvedModel, BoxError> {
        let mut model = self.inner.resolve(thread_id).await?;

        let provider = MeteredProvider {
            inner: model.provider,
            thread: thread_id.to_owned(),
            authority: self.authority.clone(),
            usages: self.usages.clone(),
        };
        model.provider = Arc::new(provider);

        Ok(model)
    }
}

struct MeteredProvider {
    inner: Arc<dyn ModelProvider>,
    thread: ThreadId,
    authority: Authority,
    usages: Usages,
}

#[async_trait]
impl ModelProvider for MeteredProvider {
    async fn list_models(&self) -> Result<Vec<ModelEntry>, BoxError> {
        self.inner.list_models().await
    }

    /// A refusal is fatal, so Infinity ends the step instead of retrying.
    async fn invoke_model(&self, model_id: &str, request: CompletionRequest) -> Result<ModelStream, CompletionError> {
        if let Err(denied) = self.authority.admit_completion(&self.thread).await {
            return Err(CompletionError::provider(ErrorClass::Fatal, denied.to_string()));
        }

        let stream = self.inner.invoke_model(model_id, request).await?;
        let (usages, thread, model) = (self.usages.clone(), self.thread.as_str().to_owned(), model_id.to_owned());

        let recorded = stream.inspect(move |chunk| {
            if let Ok(StreamChunk::Final(FinalResponse { usage: Some(usage) })) = chunk {
                record(usages.clone(), thread.to_owned(), model.to_owned(), *usage);
            }
        });

        Ok(Box::pin(recorded))
    }
}

/// Usage is a record for the owner, not a gate: writing it must not hold up
/// the stream, and losing it to a storage error only costs the record.
fn record(usages: Usages, thread: String, model: String, usage: Usage) {
    tokio::spawn(async move {
        if let Err(error) = usages.record(thread, model, usage).await {
            tracing::warn!(%error, "cannot record token usage");
        }
    });
}

#[cfg(test)]
mod tests {
    use infinity_agent_core::ThreadId;
    use infinity_agent_core::message::InputMessage;
    use infinity_agent_core::system::AgentEvent;
    use infinity_agent_core::traits::InputSender;
    use infinity_provider_protocol::Usage;
    use rusqlite::OptionalExtension;

    use super::Denied;
    use crate::error::BoxError;
    use crate::policy::{Cap, Limits};
    use crate::store::db::Db;
    use crate::testing::{self, attach, reaches_model, until, within};

    fn root() -> ThreadId {
        ThreadId::from("root".to_owned())
    }

    /// A root that exists but has no grant is as dead as an unknown thread.
    #[tokio::test(flavor = "current_thread")]
    async fn ungranted_root_never_reaches_the_model() -> Result<(), BoxError> {
        within(async {
            let db = Db::in_memory()?;
            let mut harness = testing::start(&db).await?;
            db.call(|connection| Ok(connection.execute("INSERT INTO threads (id, root_id) VALUES ('root', 'root')", [])?)).await?;

            let input = InputMessage::user_text(root(), "hi");
            harness.system.sender().send_to_input_queue(input, "input-1").await?;

            assert!(!reaches_model(&mut harness, &root()).await, "an ungranted root called the model");

            Ok(())
        })
        .await
    }

    /// A tool outside the grant is not offered, and dispatching it anyway is refused.
    #[tokio::test(flavor = "current_thread")]
    async fn omitted_tool_is_hidden_and_refused() -> Result<(), BoxError> {
        within(async {
            let db = Db::in_memory()?;
            let mut harness = testing::start_with(&db, testing::policy(&[], Limits::default())).await?;
            harness.conversations.register_root(&root()).await?;
            let handle = attach(&mut harness, &root()).await?;

            handle.send(InputMessage::user_text(root(), "what time is it?"), "input-1").await?;
            assert!(harness.model.next_request().await.tools.is_empty(), "`now` was offered without its cap");

            let direct = harness.authority.admit_tool(&root(), Cap::Now).await;
            assert!(matches!(direct, Err(Denied::NotGranted("now"))), "{direct:?}");

            Ok(())
        })
        .await
    }

    /// A model that never stops calling tools hits a visible limit. Synchronous
    /// tools loop inside one Infinity step, so every provider request counts.
    #[tokio::test(flavor = "current_thread")]
    async fn looping_run_stops_at_its_limit() -> Result<(), BoxError> {
        within(async {
            let db = Db::in_memory()?;
            let limits = Limits { max_completion_rounds: 3, ..Limits::default() };
            let mut harness = testing::start_with(&db, testing::policy(&[Cap::Now], limits)).await?;
            harness.conversations.register_root(&root()).await?;
            let mut handle = attach(&mut harness, &root()).await?;

            handle.send(InputMessage::user_text(root(), "loop"), "input-1").await?;

            for round in 0..3 {
                harness.model.next_request().await;
                harness.model.send_tool_call(&format!("call-{round}"), "now", serde_json::json!({}));
                harness.model.finish();
            }

            until(
                &mut handle,
                |event| matches!(event, AgentEvent::Info { text } if text.contains("max_completion_rounds limit")),
            )
            .await;

            Ok(())
        })
        .await
    }

    /// Every provider request's usage is recorded, and a turn past 75% of the
    /// window Infinity is given compacts: the budget Shade sets there holds.
    #[tokio::test(flavor = "current_thread")]
    async fn usage_is_recorded_and_large_turns_compact() -> Result<(), BoxError> {
        within(async {
            let db = Db::in_memory()?;
            let mut harness = testing::start(&db).await?;
            harness.conversations.register_root(&root()).await?;
            let handle = attach(&mut harness, &root()).await?;

            handle.send(InputMessage::user_text(root(), "hi"), "input-1").await?;
            harness.model.next_request().await;
            harness.model.send_text("hello");

            // The harness window is 100k tokens.
            let usage = Usage { input_tokens: 76_000, output_tokens: 10, total_tokens: 76_010, ..Usage::default() };
            harness.model.finish_with_usage(Some(usage));

            let request = harness.model.next_request().await;
            assert!(
                request.tools.iter().any(|tool| tool.name == "close_thread"),
                "a turn past the budget did not compact"
            );

            let sql = "SELECT input_tokens FROM usage";

            while db.call(move |connection| Ok(connection.query_row(sql, [], |row| row.get::<_, i64>(0)).optional()?)).await?
                != Some(76_000)
            {
                tokio::task::yield_now().await;
            }

            Ok(())
        })
        .await
    }
}
