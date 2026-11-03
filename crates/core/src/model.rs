//! Model provider setup.
//!
//! Infinity resolves the model at the start of every completion round, with the
//! thread ID. Shade builds the rig client there so each request can carry
//! per-conversation headers. Building one is cheap: the HTTP connection pool is shared.

use std::sync::Arc;

use async_trait::async_trait;
use http::header::{HeaderMap, HeaderName, HeaderValue, USER_AGENT};
use infinity_agent_core::ThreadId;
use infinity_agent_core::system::{ModelSource, ResolvedModel};
use infinity_provider_protocol::{
    CompletionError, CompletionModel, CompletionRequest, ModelEntry, ModelProvider, ModelStream, SingleModelProvider,
};
use infinity_provider_rig::RigCompletionModel;
use rig::client::CompletionClient;
use rig::http_client::ReqwestClient;
use rig::providers::{anthropic, openai};

use crate::Error;
use crate::config::{ModelConfig, Provider};
use crate::error::BoxError;
use crate::secrets::Secrets;

/// Gateways ask clients to identify themselves rather than as a generic HTTP library.
const AGENT: &str = concat!("shade/", env!("CARGO_PKG_VERSION"));

/// Where conversations are summarized unless the config says otherwise.
const DEFAULT_COMPACT_AT: usize = 100_000;

pub struct Model {
    config: ModelConfig,
    api_key: String,
    session_header: Option<HeaderName>,
    http: ReqwestClient,
    /// The window Infinity is told about. It is used only to trigger
    /// compaction, at 75% of it, so it encodes the compaction budget.
    compaction_window: usize,
}

impl Model {
    /// Reads the API key and checks the configuration, so mistakes surface at startup.
    pub fn new(config: ModelConfig, secrets: &mut Secrets) -> Result<Self, Error> {
        let Some(api_key) = secrets.take(&config.api_key_env) else {
            return Err(Error::MissingApiKey(config.api_key_env));
        };

        let session_header = match &config.session_header {
            Some(name) => match HeaderName::from_bytes(name.as_bytes()) {
                Ok(name) => Some(name),
                Err(source) => return Err(Error::Model(Box::new(source))),
            },
            None => None,
        };

        let compaction_window = compaction_window(config.context_window, config.compact_at_tokens)?;

        Ok(Self { config, api_key, session_header, http: ReqwestClient::new(), compaction_window })
    }

    pub fn id(&self) -> &str {
        &self.config.id
    }

    fn headers(&self, thread_id: &str) -> Result<HeaderMap, BoxError> {
        let mut headers = HeaderMap::new();
        headers.insert(USER_AGENT, HeaderValue::from_static(AGENT));

        if let Some(name) = &self.session_header {
            headers.insert(name, HeaderValue::from_str(thread_id)?);
        }

        Ok(headers)
    }

    fn anthropic(&self, headers: HeaderMap) -> Result<Arc<dyn ModelProvider>, BoxError> {
        let mut builder = anthropic::Client::<ReqwestClient>::builder().api_key(self.api_key.as_str());

        if let Some(url) = &self.config.base_url {
            builder = builder.base_url(url);
        }

        let client = builder.http_client(self.http.clone()).http_headers(headers).build()?;

        Ok(self.provider(client.completion_model(&self.config.id)))
    }

    fn openai_compatible(&self, headers: HeaderMap) -> Result<Arc<dyn ModelProvider>, BoxError> {
        let mut builder = openai::CompletionsClient::<ReqwestClient>::builder().api_key(self.api_key.as_str());

        if let Some(url) = &self.config.base_url {
            builder = builder.base_url(url);
        }

        let client = builder.http_client(self.http.clone()).http_headers(headers).build()?;

        Ok(self.provider(client.completion_model(&self.config.id)))
    }

    fn provider<M>(&self, model: M) -> Arc<dyn ModelProvider>
    where
        M: rig::completion::CompletionModel + Send + Sync + 'static,
        M::StreamingResponse: Send + 'static,
    {
        let entry = ModelEntry {
            model_id: self.config.id.to_owned(),
            display_name: self.config.id.to_owned(),
            context_window: self.config.context_window,
            max_output_tokens: Some(self.config.max_output_tokens),
            supports_image_input: true,
        };
        let model = MaxTokens { inner: RigCompletionModel::new(model), max_tokens: self.config.max_output_tokens };

        Arc::new(SingleModelProvider::new(entry, model))
    }
}

#[async_trait(?Send)]
impl ModelSource for Model {
    async fn resolve(&self, thread_id: &ThreadId<str>) -> Result<ResolvedModel, BoxError> {
        let headers = self.headers(thread_id.as_str())?;

        let provider = match self.config.provider {
            Provider::Anthropic => self.anthropic(headers)?,
            Provider::OpenaiCompatible => self.openai_compatible(headers)?,
        };

        Ok(ResolvedModel {
            provider,
            model_id: self.config.id.to_owned(),
            context_window: self.compaction_window,
            supports_image_input: true,
        })
    }
}

/// The window to report to Infinity so that it compacts at `compact_at`
/// tokens: it compacts once a request passes 75% of the window.
fn compaction_window(context_window: usize, compact_at: Option<usize>) -> Result<usize, Error> {
    let limit = context_window / 4 * 3;
    let compact_at = compact_at.unwrap_or(DEFAULT_COMPACT_AT.min(limit));

    if compact_at == 0 || compact_at > limit {
        return Err(Error::CompactionBudget { compact_at, limit });
    }

    Ok(compact_at / 3 * 4)
}

/// Sets `max_tokens` on every request.
///
/// Infinity 0.2.0 always sends `max_tokens: None`, and the rig bridge does not
/// fill it from the model entry, so Anthropic would reject every request.
/// rig's OpenAI Chat Completions client ignores the field.
struct MaxTokens<M> {
    inner: M,
    max_tokens: u64,
}

#[async_trait]
impl<M: CompletionModel> CompletionModel for MaxTokens<M> {
    async fn stream(&self, mut request: CompletionRequest) -> Result<ModelStream, CompletionError> {
        request.max_tokens = Some(self.max_tokens);

        self.inner.stream(request).await
    }
}

#[cfg(test)]
mod tests {
    use super::compaction_window;

    /// Infinity compacts past 75% of the window it is given.
    #[test]
    fn compaction_budget_stays_inside_the_model_window() {
        let cases = [
            (1_000_000, None, Some(133_332)),
            // A small model compacts at 75% of its own window, as before.
            (100_000, None, Some(100_000)),
            (1_000_000, Some(30_000), Some(40_000)),
            (1_000_000, Some(750_000), Some(1_000_000)),
            (1_000_000, Some(750_001), None),
            (1_000_000, Some(0), None),
        ];

        for (window, compact_at, expected) in cases {
            assert_eq!(compaction_window(window, compact_at).ok(), expected, "window {window}, compact_at {compact_at:?}");
        }
    }
}
