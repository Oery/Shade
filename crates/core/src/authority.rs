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
    CompletionError, CompletionRequest, ErrorClass, FinalResponse, ModelEntry, ModelProvider, ModelStream, StreamChunk,
};
use shade_policy::{Cap, Caps, Charge, Origin, Policy};
use shade_store::db::DbError;
use shade_store::grants::{Grants, Refusal};
use thiserror::Error;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::deadline::Deadline;
use crate::error::BoxError;
use crate::memory::Memory;
use crate::prompt;
use crate::redact::Redactor;
use crate::telemetry::Telemetry;
use crate::usage::Prices;

#[derive(Debug, Error)]
pub enum Denied {
    #[error("thread {0} has no grant")]
    NoGrant(String),

    #[error("the {0} that started this conversation is no longer configured")]
    SourceRemoved(&'static str),

    #[error("`{0}` is not granted to this conversation")]
    NotGranted(String),

    #[error("`{0}` can only be used in owner chat")]
    NotChat(String),

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
    redactor: Arc<Redactor>,
}

impl Authority {
    pub fn new(grants: Grants, policy: Arc<Policy>) -> Self {
        Self { grants, policy, redactor: Arc::new(Redactor::default()) }
    }

    pub fn with_redactor(mut self, redactor: Arc<Redactor>) -> Self {
        self.redactor = redactor;

        self
    }

    pub fn redact(&self, text: String) -> String {
        self.redactor.redact(text)
    }

    pub fn policy(&self) -> &Policy {
        &self.policy
    }

    pub async fn deadline(&self, thread: &ThreadId<str>) -> Result<Deadline, Denied> {
        Deadline::for_thread(self.grants.clone(), thread, self.policy.limits.max_run_seconds).await
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
    pub async fn admit_tool(&self, thread: &ThreadId<str>, cap: &Cap) -> Result<Authorized, Denied> {
        let authorized = self.authorize(thread).await?;

        if !authorized.caps.contains(cap) {
            return Err(Denied::NotGranted(cap.name().to_owned()));
        }

        self.charge(thread, Charge::ToolCall).await?;

        Ok(authorized)
    }

    /// Admits one call of a management tool: owner chat only, whatever the grant says.
    pub async fn admit_management(&self, thread: &ThreadId<str>, cap: &Cap) -> Result<Authorized, Denied> {
        let authorized = self.admit_tool(thread, cap).await?;

        if authorized.origin != Origin::Chat {
            return Err(Denied::NotChat(cap.name().to_owned()));
        }

        Ok(authorized)
    }

    /// Admits one message to the owner. The `notify` call was admitted first.
    pub async fn admit_notification(&self, thread: &ThreadId<str>) -> Result<(), Denied> {
        self.charge(thread, Charge::Notification).await
    }

    /// Admits one request to the model provider.
    pub async fn admit_completion(&self, thread: &ThreadId<str>) -> Result<Authorized, Denied> {
        let authorized = self.authorize(thread).await?;
        self.charge(thread, Charge::Completion).await?;

        Ok(authorized)
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
    telemetry: Telemetry,
    prices: Option<Prices>,
    memory: Option<Arc<Memory>>,
    completions: Arc<Semaphore>,
}

impl<S> Metered<S> {
    pub fn new(inner: S, authority: Authority, telemetry: Telemetry, memory: Option<Arc<Memory>>, prices: Option<Prices>) -> Self {
        let completions = Arc::new(Semaphore::new(usize::from(authority.policy().limits.max_concurrent_completions)));

        Self { inner, authority, telemetry, memory, prices, completions }
    }
}

#[async_trait(?Send)]
impl<S: ModelSource> ModelSource for Metered<S> {
    async fn resolve(&self, thread_id: &ThreadId<str>) -> Result<ResolvedModel, BoxError> {
        let mut model = self.inner.resolve(thread_id).await?;
        let deadline = Arc::new(self.authority.deadline(thread_id).await?);

        let provider = MeteredProvider {
            inner: model.provider,
            thread: thread_id.to_owned(),
            authority: self.authority.clone(),
            telemetry: self.telemetry.clone(),
            prices: self.prices,
            memory: self.memory.clone(),
            completions: Arc::clone(&self.completions),
            deadline,
        };
        model.provider = Arc::new(provider);

        Ok(model)
    }
}

struct MeteredProvider {
    inner: Arc<dyn ModelProvider>,
    thread: ThreadId,
    authority: Authority,
    telemetry: Telemetry,
    prices: Option<Prices>,
    memory: Option<Arc<Memory>>,
    completions: Arc<Semaphore>,
    /// Retries of an old completion must never borrow a newer turn's deadline.
    deadline: Arc<Deadline>,
}

#[async_trait]
impl ModelProvider for MeteredProvider {
    async fn list_models(&self) -> Result<Vec<ModelEntry>, BoxError> {
        self.inner.list_models().await
    }

    /// A refusal is fatal, so Infinity ends the step instead of retrying.
    /// Also replaces Infinity's coding-agent prompt with Shade's (see `prompt`).
    async fn invoke_model(&self, model_id: &str, request: CompletionRequest) -> Result<ModelStream, CompletionError> {
        let deadline = Arc::clone(&self.deadline);

        let (stream, permit) = match deadline.run(self.start(model_id, request)).await {
            Ok(result) => result?,
            Err(denied) => return Err(self.refused(denied)),
        };

        let (telemetry, thread, model, prices) =
            (self.telemetry.clone(), self.thread.as_str().to_owned(), model_id.to_owned(), self.prices);

        let recorded = bounded_stream(stream, deadline, permit).inspect(move |chunk| match chunk {
            Ok(StreamChunk::Final(FinalResponse { usage })) => {
                telemetry.event(&thread, "completion", format!("finished {model}; usage reported={}", usage.is_some()));

                if let Some(usage) = usage {
                    telemetry.usage(&thread, &model, *usage, prices.map(|price| price.estimate(usage)));
                }
            }
            Err(_) => telemetry.event(&thread, "error", "provider stream failed".to_owned()),
            _ => {}
        });

        Ok(Box::pin(recorded))
    }
}

impl MeteredProvider {
    fn refused(&self, denied: Denied) -> CompletionError {
        self.telemetry.event(self.thread.as_str(), "refused", denied.to_string());

        CompletionError::provider(ErrorClass::Fatal, denied.to_string())
    }

    async fn start(&self, model_id: &str, mut request: CompletionRequest) -> Result<(ModelStream, OwnedSemaphorePermit), CompletionError> {
        let permit = match Arc::clone(&self.completions).acquire_owned().await {
            Ok(permit) => permit,
            Err(_) => return Err(CompletionError::provider(ErrorClass::Fatal, "the model request queue is closed")),
        };

        let authorized = match self.authority.admit_completion(&self.thread).await {
            Ok(authorized) => authorized,
            Err(denied) => return Err(self.refused(denied)),
        };

        let memory = match &self.memory {
            Some(memory) => memory.section(&authorized.caps).await,
            None => None,
        };

        match request.preamble.as_deref().map(|infinity| prompt::preamble(infinity, memory.as_deref())) {
            Some(Some(preamble)) => request.preamble = Some(preamble),
            Some(None) => tracing::warn!("Infinity's system prompt has an unknown layout; sending it unchanged"),
            None => {}
        }

        self.telemetry.event(self.thread.as_str(), "completion", format!("request {model_id}"));

        let stream = match self.inner.invoke_model(model_id, request).await {
            Ok(stream) => stream,
            Err(error) => {
                self.telemetry.event(self.thread.as_str(), "error", "provider request failed".to_owned());
                return Err(error);
            }
        };

        Ok((stream, permit))
    }
}

/// The permit covers the entire response. Cancellation, errors and the final
/// chunk release it, even if the consumer never polls the stream again.
fn bounded_stream(stream: ModelStream, deadline: Arc<Deadline>, permit: OwnedSemaphorePermit) -> ModelStream {
    let state = Some((stream, deadline, permit));
    let stream = futures_util::stream::unfold(state, |state| async move {
        let (mut stream, deadline, permit) = state?;

        let chunk = match deadline.run(stream.next()).await {
            Ok(chunk) => chunk?,
            Err(denied) => Err(CompletionError::provider(ErrorClass::Fatal, denied.to_string())),
        };

        let state = match &chunk {
            Err(_) | Ok(StreamChunk::Final(_)) => None,
            _ => Some((stream, deadline, permit)),
        };

        Some((chunk, state))
    });

    Box::pin(stream)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use infinity_agent_core::ThreadId;
    use infinity_agent_core::message::InputMessage;
    use infinity_agent_core::system::AgentEvent;
    use infinity_agent_core::traits::InputSender;
    use infinity_provider_protocol::Usage;
    use shade_policy::{Cap, Limits};
    use shade_store::db::Db;
    use shade_store::inbox::{Inbox, Input};
    use shade_store::roots::{Adapter, Roots, Target};
    use shade_store::usage::Usages;

    use super::Denied;
    use crate::error::BoxError;
    use crate::testing::{self, attach, reaches_model, reply, until, within};

    /// Stalls either before the HTTP response or while reading its stream.
    struct Stalled {
        before_stream: bool,
    }

    #[async_trait::async_trait]
    impl infinity_provider_protocol::ModelProvider for Stalled {
        async fn list_models(&self) -> Result<Vec<infinity_provider_protocol::ModelEntry>, BoxError> {
            Ok(Vec::new())
        }

        async fn invoke_model(
            &self, _model: &str, _request: infinity_provider_protocol::CompletionRequest,
        ) -> Result<infinity_provider_protocol::ModelStream, infinity_provider_protocol::CompletionError> {
            if self.before_stream {
                std::future::pending::<()>().await;
            }

            Ok(Box::pin(futures_util::stream::pending()))
        }
    }

    async fn stalled(
        db: &Db, thread: ThreadId, before_stream: bool, completions: Arc<tokio::sync::Semaphore>,
    ) -> Result<super::MeteredProvider, BoxError> {
        let limits = Limits { max_run_seconds: 2, ..Limits::default() };
        let authority = super::Authority::new(shade_store::grants::Grants::new(db.clone()), testing::policy(&[Cap::Now], limits));
        let telemetry = crate::telemetry::Telemetry::new(db.clone(), Arc::default());

        let deadline = Arc::new(authority.deadline(&thread).await?);

        Ok(super::MeteredProvider {
            inner: Arc::new(Stalled { before_stream }),
            thread,
            authority,
            telemetry,
            prices: None,
            memory: None,
            completions,
            deadline,
        })
    }

    #[tokio::test]
    async fn stalled_requests_and_streams_expire_and_release_capacity() -> Result<(), BoxError> {
        use futures_util::StreamExt;
        use infinity_provider_protocol::{CompletionRequest, ErrorClass, ModelProvider};

        for before_stream in [true, false] {
            let db = Db::in_memory()?;
            shade_store::conversation::Conversations::new(db.clone()).register_root(&root()).await?;
            let permits = Arc::new(tokio::sync::Semaphore::new(1));
            let provider = stalled(&db, root(), before_stream, Arc::clone(&permits)).await?;

            let result = tokio::time::timeout(std::time::Duration::from_secs(3), async {
                match provider.invoke_model("mock", CompletionRequest::default()).await {
                    Ok(mut stream) => stream.next().await.ok_or("missing stream error"),
                    Err(error) => Ok(Err(error)),
                }
            })
            .await??;
            let error = result.err().ok_or("a stalled provider succeeded")?;

            assert_eq!(error.class(), ErrorClass::Fatal);
            assert!(error.to_string().contains("max_run_seconds"));
            assert_eq!(permits.available_permits(), 1, "the expired call kept its slot");
            assert!(matches!(provider.authority.admit_tool(&root(), &Cap::Now).await, Err(Denied::Limit(_))));
        }

        Ok(())
    }

    #[tokio::test]
    async fn model_capacity_covers_streams_and_cancellation() -> Result<(), BoxError> {
        use infinity_provider_protocol::{CompletionRequest, ModelProvider};

        let db = Db::in_memory()?;
        let conversations = shade_store::conversation::Conversations::new(db.clone());
        let other = ThreadId::from("other".to_owned());
        conversations.register_root(&root()).await?;
        conversations.register_root(&other).await?;
        let permits = Arc::new(tokio::sync::Semaphore::new(1));
        let first = stalled(&db, root(), false, Arc::clone(&permits)).await?;
        let second = stalled(&db, other, false, Arc::clone(&permits)).await?;

        let stream = first.invoke_model("mock", CompletionRequest::default()).await?;
        let waiting = second.invoke_model("mock", CompletionRequest::default());
        tokio::pin!(waiting);
        assert!(tokio::time::timeout(std::time::Duration::from_millis(20), &mut waiting).await.is_err());

        drop(stream);
        let stream = waiting.await?;
        assert_eq!(permits.available_permits(), 0);
        drop(stream);
        assert_eq!(permits.available_permits(), 1);

        Ok(())
    }

    #[tokio::test]
    async fn waiting_for_model_capacity_uses_the_run_deadline() -> Result<(), BoxError> {
        use infinity_provider_protocol::{CompletionRequest, ModelProvider};

        let db = Db::in_memory()?;
        shade_store::conversation::Conversations::new(db.clone()).register_root(&root()).await?;
        let permits = Arc::new(tokio::sync::Semaphore::new(1));
        let held = Arc::clone(&permits).acquire_owned().await?;
        let provider = stalled(&db, root(), false, Arc::clone(&permits)).await?;

        let result =
            tokio::time::timeout(std::time::Duration::from_secs(3), provider.invoke_model("mock", CompletionRequest::default())).await?;
        assert!(result.err().ok_or("a queued call succeeded")?.to_string().contains("max_run_seconds"));

        drop(held);
        assert_eq!(permits.available_permits(), 1);

        Ok(())
    }

    fn root() -> ThreadId {
        ThreadId::from("root".to_owned())
    }

    async fn discord_input(db: &Db, adapter: Adapter) -> Result<Input, BoxError> {
        let target = Target::Named("42".to_owned());

        match Inbox::new(db.clone()).accept(adapter, target, "hi".to_owned(), None).await? {
            Some(input) => Ok(input),
            None => Err("a new Discord input was treated as a duplicate".into()),
        }
    }

    /// A guest can get an answer without owner tools or owner history. Its
    /// prompt also excludes the owner's custom instructions.
    #[tokio::test(flavor = "current_thread")]
    async fn guest_answers_are_isolated_from_owner_chat() -> Result<(), BoxError> {
        within(async {
            let db = Db::in_memory()?;
            let mut policy = testing::policy(&[Cap::Now, Cap::Schedules], Limits::default());
            Arc::get_mut(&mut policy).ok_or("policy is shared")?.chat_prompt = Some("owner secret".to_owned());
            let mut harness = testing::start_with(&db, policy).await?;
            let owner = discord_input(&db, Adapter::Discord).await?;
            let mut handle = attach(&mut harness, &owner.root).await?;
            handle.send(InputMessage::user_text(owner.root, owner.text), &owner.id).await?;

            let request = harness.model.next_request().await;
            assert!(request.tools.iter().any(|tool| tool.name == "now"));
            assert!(request.tools.iter().any(|tool| tool.name == "schedule_create"));
            reply(&mut harness, &mut handle, "private owner history").await;

            let guest = discord_input(&db, Adapter::DiscordGuest).await?;
            let mut handle = attach(&mut harness, &guest.root).await?;
            handle.send(InputMessage::user_text(guest.root.to_owned(), guest.text), &guest.id).await?;

            let request = harness.model.next_request().await;
            assert!(request.tools.is_empty());
            assert!(!format!("{:?}", request.chat_history).contains("private owner history"));
            let preamble = request.preamble.as_deref().ok_or("missing guest prompt")?;
            assert!(preamble.contains("guest"));
            assert!(!preamble.contains("owner secret"));

            let management = harness.authority.admit_management(&guest.root, &Cap::Schedules).await;
            assert!(matches!(management, Err(Denied::NotGranted(_))));
            reply(&mut harness, &mut handle, "hello guest").await;
            harness.system.shutdown().await;

            Ok(())
        })
        .await
    }

    /// Stored guest grants keep following guest caps after a restart;
    /// removed tools are both hidden and refused at dispatch.
    #[tokio::test(flavor = "current_thread")]
    async fn guest_tools_follow_current_guest_policy() -> Result<(), BoxError> {
        within(async {
            let db = Db::in_memory()?;
            let mut policy = testing::policy(&[Cap::Schedules], Limits::default());
            Arc::get_mut(&mut policy).ok_or("policy is shared")?.discord_guests.insert(Cap::Now);
            let mut harness = testing::start_with(&db, policy).await?;
            let guest = discord_input(&db, Adapter::DiscordGuest).await?;
            let mut handle = attach(&mut harness, &guest.root).await?;
            handle.send(InputMessage::user_text(guest.root.to_owned(), guest.text), &guest.id).await?;

            let request = harness.model.next_request().await;
            assert_eq!(request.tools.len(), 1);
            assert_eq!(request.tools[0].name, "now");
            assert!(harness.authority.admit_tool(&guest.root, &Cap::Now).await.is_ok());
            reply(&mut harness, &mut handle, "hello guest").await;
            harness.system.shutdown().await;

            let mut after = testing::start(&db).await?;
            let input = discord_input(&db, Adapter::DiscordGuest).await?;
            assert_eq!(input.root, guest.root);
            let mut handle = attach(&mut after, &input.root).await?;
            handle.send(InputMessage::user_text(input.root.to_owned(), input.text), &input.id).await?;
            assert!(after.model.next_request().await.tools.is_empty());

            let direct = after.authority.admit_tool(&input.root, &Cap::Now).await;
            assert!(matches!(direct, Err(Denied::NotGranted(_))));
            reply(&mut after, &mut handle, "hello again").await;
            after.system.shutdown().await;

            Ok(())
        })
        .await
    }

    /// A root that exists but has no grant is as dead as an unknown thread.
    #[tokio::test(flavor = "current_thread")]
    async fn ungranted_root_never_reaches_the_model() -> Result<(), BoxError> {
        within(async {
            let db = Db::in_memory()?;
            let mut harness = testing::start(&db).await?;
            Roots::new(db.clone()).insert_ungranted("root").await?;

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

            let direct = harness.authority.admit_tool(&root(), &Cap::Now).await;
            assert!(matches!(&direct, Err(Denied::NotGranted(cap)) if cap == "now"), "{direct:?}");

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

            let usages = Usages::new(db);

            while usages.first_input_tokens().await? != Some(76_000) {
                tokio::task::yield_now().await;
            }

            Ok(())
        })
        .await
    }
}
