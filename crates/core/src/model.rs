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
        if let Some(prices) = config.prices {
            prices.validate()?;
        }

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

    pub fn prices(&self) -> Option<crate::usage::Prices> {
        self.config.prices
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
        let model = MaxTokens {
            inner: RigCompletionModel::new(model),
            max_tokens: self.config.max_output_tokens,
            openai: matches!(self.config.provider, Provider::OpenaiCompatible),
        };

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
/// rig's OpenAI Chat Completions client needs it in `additional_params` instead.
struct MaxTokens<M> {
    inner: M,
    max_tokens: u64,
    openai: bool,
}

#[async_trait]
impl<M: CompletionModel> CompletionModel for MaxTokens<M> {
    async fn stream(&self, mut request: CompletionRequest) -> Result<ModelStream, CompletionError> {
        request.max_tokens = Some(self.max_tokens);

        if self.openai {
            let params = request.additional_params.get_or_insert_with(|| serde_json::json!({}));
            let Some(params) = params.as_object_mut() else {
                return Err(CompletionError::provider(
                    infinity_provider_protocol::ErrorClass::Fatal,
                    "model parameters must be an object",
                ));
            };

            params.insert("max_tokens".to_owned(), self.max_tokens.into());
        }

        self.inner.stream(request).await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use bytes::Bytes;
    use futures_util::StreamExt;
    use http::{Request, Response};
    use http_body_util::{BodyExt, Full};
    use hyper::body::Incoming;
    use hyper::server::conn::http1;
    use hyper::service::service_fn;
    use hyper_util::rt::TokioIo;
    use infinity_agent_core::ThreadId;
    use infinity_agent_core::system::ModelSource;
    use infinity_provider_protocol::{CompletionRequest, Message};
    use tokio::net::TcpListener;
    use tokio::sync::mpsc;

    use super::{Model, compaction_window};
    use crate::config::{ModelConfig, Provider};
    use crate::error::BoxError;

    /// Captures the real HTTP body after both Infinity and rig have converted it.
    /// Authentication is intentionally refused; no real credentials or services are used.
    #[tokio::test]
    async fn output_caps_reach_both_providers_on_the_wire() -> Result<(), BoxError> {
        for provider in [Provider::OpenaiCompatible, Provider::Anthropic] {
            let (url, mut captured, server) = capture_request().await?;
            let config = ModelConfig {
                provider,
                id: "fake".to_owned(),
                api_key_env: String::new(),
                base_url: Some(url),
                session_header: None,
                context_window: 10000,
                max_output_tokens: 7,
                compact_at_tokens: None,
                prices: None,
            };
            let model = Model {
                config,
                api_key: "local-test-only".to_owned(),
                session_header: None,
                http: rig::http_client::ReqwestClient::new(),
                compaction_window: 10000,
            };
            let resolved = model.resolve(&ThreadId::from("test".to_owned())).await?;
            let request = CompletionRequest { chat_history: vec![Message::user("hello")], ..CompletionRequest::default() };
            if let Ok(mut stream) = resolved.provider.invoke_model("fake", request).await {
                let _ = stream.next().await;
            }

            let body = tokio::time::timeout(std::time::Duration::from_secs(5), captured.recv()).await?.ok_or("no HTTP request captured")?;
            assert_eq!(body["max_tokens"], 7, "{body}");
            server.abort();
        }

        Ok(())
    }

    async fn capture_request()
    -> Result<(String, mpsc::Receiver<serde_json::Value>, tokio::task::JoinHandle<Result<(), BoxError>>), BoxError> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let url = format!("http://{}", listener.local_addr()?);
        let (captured, receiver) = mpsc::channel(1);

        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await?;
            let captured = Arc::new(captured);
            let service = service_fn(move |request: Request<Incoming>| {
                let captured = Arc::clone(&captured);

                async move {
                    let body = match request.into_body().collect().await {
                        Ok(body) => serde_json::from_slice(&body.to_bytes()).unwrap_or_default(),
                        Err(_) => serde_json::Value::Null,
                    };
                    let _ = captured.send(body).await;
                    let body = Bytes::from_static(br#"{"error":{"type":"authentication_error","message":"local test"}}"#);
                    let mut response = Response::new(Full::new(body));
                    *response.status_mut() = http::StatusCode::UNAUTHORIZED;

                    Ok::<_, std::convert::Infallible>(response)
                }
            });
            http1::Builder::new().serve_connection(TokioIo::new(stream), service).await?;

            Ok(())
        });

        Ok((url, receiver, server))
    }

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
