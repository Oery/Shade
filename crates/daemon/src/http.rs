//! Resolve transport secrets before core builds its redactor.

use std::collections::BTreeMap;

use shade_core::{Client, HookAdmission, Secrets};
use shade_http::{Hook, Server};

use crate::config::{ApiConfig, HookConfig, HttpConfig};
use crate::error::Error;

pub struct Settings {
    listen: String,
    hooks: BTreeMap<String, Hook>,
    api_token: Option<String>,
}

pub fn prepare(
    hooks: BTreeMap<String, HookConfig>, api: Option<ApiConfig>, http: HttpConfig, secrets: &mut Secrets,
) -> Result<Option<Settings>, Error> {
    if hooks.is_empty() && api.is_none() {
        return Ok(None);
    }

    let api_token = match api {
        Some(config) => match secrets.take(&config.token_env) {
            Some(token) if token.len() >= shade_http::MIN_TOKEN => Some(token),
            _ => return Err(Error::ApiToken(config.token_env)),
        },
        None => None,
    };
    let mut served = BTreeMap::new();

    for (name, hook) in hooks {
        let token = match secrets.take(&hook.token_env) {
            Some(token) if token.len() >= shade_http::MIN_TOKEN => token,
            _ => return Err(Error::HookToken { hook: name, name: hook.token_env }),
        };
        let admission = HookAdmission { queued: hook.max_queued, per_minute: hook.max_per_minute };
        let hook = Hook { token, template: hook.template, condition: hook.condition, delivery_header: hook.delivery_header, admission };
        served.insert(name, hook);
    }

    Ok(Some(Settings { listen: http.listen, hooks: served, api_token }))
}

pub async fn bind(settings: Option<Settings>, client: Client) -> Result<Option<Server>, Error> {
    let Some(settings) = settings else {
        return Ok(None);
    };
    let server = Server::bind(&settings.listen, settings.hooks, settings.api_token, client).await?;

    Ok(Some(server))
}

pub async fn serve(server: Option<Server>) {
    match server {
        Some(server) => server.serve().await,
        None => std::future::pending().await,
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write;
    use std::os::unix::fs::OpenOptionsExt;

    use shade_core::test_support::{self, BoxError, within};
    use shade_store::db::Db;

    use super::*;

    #[tokio::test(flavor = "current_thread")]
    async fn api_only_startup_requires_a_strong_secret_and_redacts_it() -> Result<(), BoxError> {
        within(async {
            let token = "0123456789abcdef0123456789abcdef";
            let path = std::env::temp_dir().join(format!("shade-api-env-{}", uuid::Uuid::new_v4()));
            let mut file = std::fs::OpenOptions::new().write(true).create_new(true).mode(0o600).open(&path)?;
            writeln!(file, "SHADE_API_TEST_SECRET={token}\nSHADE_API_TEST_SHORT=short")?;
            drop(file);
            let mut secrets = Secrets::load(path.to_owned())?;
            std::fs::remove_file(&path)?;
            let db = Db::in_memory()?;
            let (client, _model) = test_support::client(&db, test_support::policy(&[], Default::default())).await?;
            let config = ApiConfig { token_env: "SHADE_API_TEST_SECRET".to_owned() };
            let http = HttpConfig { listen: "127.0.0.1:0".to_owned() };
            let settings = prepare(BTreeMap::new(), Some(config), http, &mut secrets)?;
            assert!(bind(settings, client).await?.is_some(), "API did not start without hooks");

            let short = ApiConfig { token_env: "SHADE_API_TEST_SHORT".to_owned() };
            let result = prepare(BTreeMap::new(), Some(short), HttpConfig::default(), &mut secrets);
            assert!(matches!(result, Err(Error::ApiToken(_))));
            assert!(!secrets.into_redactor().redact(token.to_owned()).contains(token));
            let missing = ApiConfig { token_env: "SHADE_API_TEST_MISSING".to_owned() };
            let mut secrets = Secrets::load(path)?;
            let result = prepare(BTreeMap::new(), Some(missing), HttpConfig::default(), &mut secrets);
            assert!(matches!(result, Err(Error::ApiToken(_))));

            Ok(())
        })
        .await
    }
}
