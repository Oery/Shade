//! Model provider setup.

use std::sync::Arc;

use async_trait::async_trait;
use infinity_agent_core::system::StaticModel;
use infinity_provider_protocol::{CompletionError, CompletionModel, CompletionRequest, ModelEntry, ModelStream, SingleModelProvider};
use infinity_provider_rig::RigCompletionModel;
use rig::client::CompletionClient;
use rig::http_client::ReqwestClient;
use rig::providers::anthropic;

use crate::Error;
use crate::config::{ModelConfig, Provider};

pub async fn static_model(config: &ModelConfig) -> Result<StaticModel, Error> {
    let Ok(api_key) = std::env::var(&config.api_key_env) else {
        return Err(Error::MissingApiKey(config.api_key_env.to_owned()));
    };

    let client = match config.provider {
        Provider::Anthropic => anthropic::Client::<ReqwestClient>::new(api_key),
    };
    let client = match client {
        Ok(client) => client,
        Err(source) => return Err(Error::Model(Box::new(source))),
    };

    let entry = ModelEntry {
        model_id: config.id.to_owned(),
        display_name: config.id.to_owned(),
        context_window: config.context_window,
        max_output_tokens: Some(config.max_output_tokens),
        supports_image_input: true,
    };
    let model = MaxTokens { inner: RigCompletionModel::new(client.completion_model(&config.id)), max_tokens: config.max_output_tokens };
    let provider = SingleModelProvider::new(entry, model);

    match StaticModel::new(Arc::new(provider), &config.id).await {
        Ok(model) => Ok(model),
        Err(source) => Err(Error::Model(source)),
    }
}

/// Sets `max_tokens` on every request.
///
/// Infinity 0.2.0 always sends `max_tokens: None`, and the rig bridge does not
/// fill it from the model entry, so Anthropic would reject every request.
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
