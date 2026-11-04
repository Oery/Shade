//! Webhooks: `POST /hook/<name>` with the hook's bearer token starts a run.
//!
//! A request is authenticated (constant-time token comparison), read up to
//! 64 KiB, filtered by the hook's condition if it has one, and recorded
//! before it is answered `202`. A body that does not match is answered
//! `202` too, with nothing to run. Anything Shade cannot decide is a client
//! error, never a silent skip, and a storage failure is `503`, so the sender
//! retries rather than losing the event. The body never chooses the run's
//! grant, origin, or destination: those come from the hook's config.
//!
//! Webhooks are for this machine and the owner's tailnet: the listener only
//! binds loopback or Tailscale addresses (`tailscale serve` in front of
//! localhost adds HTTPS). An unknown hook and a wrong token get the same
//! answer after the same comparison, so hook names cannot be probed.
//! Connections are capped, and a client that does not send its request in
//! time is dropped.

use std::collections::BTreeMap;
use std::convert::Infallible;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use http::{Method, Request, Response, StatusCode, header};
use http_body_util::combinators::UnsyncBoxBody;
use http_body_util::{BodyExt, Full, Limited};
use hyper::body::Incoming;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper_util::rt::{TokioIo, TokioTimer};
use shade_condition::Condition;
use shade_core::{Client, HookAccepted as Accepted, HookAdmission as Admission};
use subtle::ConstantTimeEq;
use tokio::net::TcpListener;
use tokio::sync::Semaphore;

/// Largest body accepted.
const MAX_BODY: usize = 64 * 1024;

/// Shortest hook token accepted at startup.
pub const MIN_TOKEN: usize = 32;

/// Connections served at once; more wait to be accepted.
const MAX_CONNECTIONS: usize = 32;

/// How long a client has to send a request's headers, and then its body.
const HEADER_TIMEOUT: Duration = Duration::from_secs(10);
const BODY_TIMEOUT: Duration = Duration::from_secs(30);

/// Compared against for an unknown hook, so it costs what a wrong token does.
const UNKNOWN_HOOK: &str = "no hook has this name; this token is only ever compared, never accepted";

/// One configured hook, ready to serve.
pub struct Hook {
    pub token: String,
    pub template: String,
    pub condition: Option<Condition>,
    pub delivery_header: Option<String>,
    pub admission: Admission,
}

pub struct Webhooks {
    api: Option<super::api::Api>,
    hooks: BTreeMap<String, Hook>,
    client: Client,
    header_timeout: Duration,
    body_timeout: Duration,
}

/// An answer: a status and a small JSON body.
pub(crate) type Answer = Response<UnsyncBoxBody<Bytes, Infallible>>;

impl Webhooks {
    pub fn new(hooks: BTreeMap<String, Hook>, client: Client) -> Self {
        Self { api: None, hooks, client, header_timeout: HEADER_TIMEOUT, body_timeout: BODY_TIMEOUT }
    }

    pub fn with_api(mut self, api: Option<super::api::Api>) -> Self {
        self.api = api;

        self
    }

    /// Serves requests until the task is dropped, at most `MAX_CONNECTIONS` at a time.
    pub async fn serve(self: Arc<Self>, listener: TcpListener) {
        let permits = Arc::new(Semaphore::new(MAX_CONNECTIONS));

        loop {
            let Ok(permit) = Arc::clone(&permits).acquire_owned().await else {
                return; // The semaphore is never closed.
            };

            let stream = match listener.accept().await {
                Ok((stream, _)) => stream,
                Err(error) => {
                    tracing::warn!(%error, "webhook accept failed");
                    continue;
                }
            };

            let webhooks = Arc::clone(&self);
            let service = service_fn(move |request| {
                let webhooks = Arc::clone(&webhooks);
                async move { Ok::<_, Infallible>(webhooks.handle(request).await) }
            });

            // The header timeout also covers a kept-alive connection waiting for its next request.
            let mut connection = http1::Builder::new();
            connection.timer(TokioTimer::new()).header_read_timeout(self.header_timeout);

            tokio::spawn(async move {
                if let Err(error) = connection.serve_connection(TokioIo::new(stream), service).await {
                    tracing::debug!(%error, "webhook connection ended");
                }

                drop(permit);
            });
        }
    }

    async fn handle(&self, request: Request<Incoming>) -> Answer {
        if request.uri().path() == "/v1" || request.uri().path().starts_with("/v1/") {
            return match &self.api {
                Some(api) => api.handle(request).await,
                None => answer(
                    StatusCode::NOT_FOUND,
                    serde_json::json!({"error": {"code": "not_found", "message": "API is disabled"}}),
                ),
            };
        }

        let Some(name) = request.uri().path().strip_prefix("/hook/").filter(|_| request.method() == Method::POST) else {
            return answer(StatusCode::NOT_FOUND, serde_json::json!({ "error": "only POST /hook/<name> is served" }));
        };

        let name = name.to_owned();
        let hook = self.hooks.get(&name);

        // An unknown hook is refused like a wrong token, after the same comparison.
        let valid = authorized(&request, hook.map_or(UNKNOWN_HOOK, |hook| hook.token.as_str()));

        let (Some(hook), true) = (hook, valid) else {
            tracing::info!(
                hook = if hook.is_some() { name.as_str() } else { "(unknown)" },
                "webhook request refused: no valid token"
            );
            return answer(StatusCode::UNAUTHORIZED, serde_json::json!({ "error": "missing or wrong bearer token" }));
        };

        let response = self.receive(&name, hook, request).await;
        tracing::info!(hook = %name, status = response.status().as_u16(), "webhook request");

        response
    }

    /// An authenticated request: read, filter, and accept.
    async fn receive(&self, name: &str, hook: &Hook, request: Request<Incoming>) -> Answer {
        let delivery = hook.delivery_header.as_deref().and_then(|header| request.headers().get(header)?.to_str().ok().map(str::to_owned));

        let body = match tokio::time::timeout(self.body_timeout, read(request)).await {
            Ok(Ok(body)) => body,
            Ok(Err(status)) => return answer(status, serde_json::json!({ "error": "the body is too large or unreadable" })),
            Err(_) => return answer(StatusCode::REQUEST_TIMEOUT, serde_json::json!({ "error": "the body did not arrive in time" })),
        };

        if let Some(refusal) = filter(hook.condition.as_ref(), &body) {
            return refusal;
        }

        let input = hook.template.replace("{hook}", name).replace("{body}", &body);
        self.accept(name, delivery, input, hook.admission).await
    }

    /// Recorded before it is answered, so `202` always means durable.
    async fn accept(&self, name: &str, delivery: Option<String>, input: String, admission: Admission) -> Answer {
        match self.client.accept_hook(name.to_owned(), delivery, input, admission).await {
            Ok(Accepted::New(event)) => answer(StatusCode::ACCEPTED, serde_json::json!({ "accepted": true, "event": event })),
            Ok(Accepted::Repeated(event)) => {
                answer(StatusCode::ACCEPTED, serde_json::json!({ "accepted": true, "event": event, "repeated": true }))
            }
            Ok(Accepted::Busy(limit)) => retry(StatusCode::TOO_MANY_REQUESTS, &format!("too many {limit}; retry later"), 60),
            Err(error) => {
                tracing::error!(%error, hook = %name, "cannot record a webhook event");
                retry(StatusCode::SERVICE_UNAVAILABLE, "the event could not be recorded; retry later", 30)
            }
        }
    }
}

/// The address webhooks are served on, if it reaches only this machine or
/// the owner's tailnet: loopback, or a Tailscale address (`100.64.0.0/10`,
/// `fd7a:115c:a1e0::/48`). Anything else would expose Shade on whatever
/// network the machine joins.
pub fn listen_address(listen: &str) -> Result<SocketAddr, String> {
    let Ok(address) = listen.parse::<SocketAddr>() else {
        return Err(format!("[http] listen `{listen}` must be an IP address and a port, such as 127.0.0.1:8787"));
    };

    if !loopback_or_tailnet(address.ip()) {
        return Err(format!(
            "[http] listen `{listen}` would serve HTTP beyond this machine and your tailnet; \
             use 127.0.0.1 (with `tailscale serve` in front) or this machine's Tailscale address"
        ));
    }

    Ok(address)
}

fn loopback_or_tailnet(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(ip) => ip.is_loopback() || (ip.octets()[0] == 100 && ip.octets()[1] & 0b1100_0000 == 0b0100_0000),
        IpAddr::V6(ip) => ip.is_loopback() || ip.segments()[..3] == [0xfd7a, 0x115c, 0xa1e0],
    }
}

/// The hook's token as `Authorization: Bearer <token>`, compared in constant time.
pub(super) fn authorized(request: &Request<Incoming>, token: &str) -> bool {
    let Some(sent) = request.headers().get(header::AUTHORIZATION).and_then(|value| value.to_str().ok()) else {
        return false;
    };

    let Some(sent) = sent.strip_prefix("Bearer ") else {
        return false;
    };

    bool::from(sent.as_bytes().ct_eq(token.as_bytes()))
}

/// The body as text, at most `MAX_BODY` bytes.
pub(super) async fn read(request: Request<Incoming>) -> Result<String, StatusCode> {
    let body = match Limited::new(request.into_body(), MAX_BODY).collect().await {
        Ok(collected) => collected.to_bytes(),
        Err(_) => return Err(StatusCode::PAYLOAD_TOO_LARGE),
    };

    match String::from_utf8(body.to_vec()) {
        Ok(text) => Ok(text),
        Err(_) => Err(StatusCode::BAD_REQUEST),
    }
}

/// `None` when the event should run. A body that does not match is answered
/// `202` with nothing to run; one the condition cannot read or decide is a
/// client error the sender sees.
fn filter(condition: Option<&Condition>, body: &str) -> Option<Answer> {
    let condition = condition?;

    let document: serde_json::Value = match serde_json::from_str(body) {
        Ok(document) => document,
        Err(error) => {
            return Some(answer(
                StatusCode::BAD_REQUEST,
                serde_json::json!({ "error": format!("the body is not JSON: {error}") }),
            ));
        }
    };

    match condition.matches(&document) {
        Ok(true) => None,
        Ok(false) => Some(answer(
            StatusCode::ACCEPTED,
            serde_json::json!({ "accepted": false, "reason": "the condition did not match" }),
        )),
        Err(reason) => Some(answer(
            StatusCode::UNPROCESSABLE_ENTITY,
            serde_json::json!({ "error": format!("the condition cannot be decided: {reason}") }),
        )),
    }
}

pub(super) fn answer(status: StatusCode, body: serde_json::Value) -> Answer {
    let mut response = Response::new(Full::new(Bytes::from(body.to_string())).boxed_unsync());
    *response.status_mut() = status;
    response.headers_mut().insert(header::CONTENT_TYPE, header::HeaderValue::from_static("application/json"));

    response
}

/// A refusal the sender should retry after `seconds`.
fn retry(status: StatusCode, message: &str, seconds: u32) -> Answer {
    let mut response = answer(status, serde_json::json!({ "error": message }));
    response.headers_mut().insert(header::RETRY_AFTER, header::HeaderValue::from(seconds));

    response
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::net::SocketAddr;
    use std::sync::Arc;
    use std::time::Duration;

    use shade_core::HookAdmission as Admission;
    use shade_store::db::Db;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};

    use super::{Hook, MAX_BODY, Webhooks, listen_address};
    use shade_core::test_support::{self as testing, BoxError, within};
    use shade_store::hooks::HookEvents;

    async fn serve(webhooks: Webhooks) -> Result<SocketAddr, BoxError> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        tokio::spawn(Arc::new(webhooks).serve(listener));

        Ok(address)
    }

    /// A client that connects and never sends its request is dropped.
    #[tokio::test]
    async fn silent_clients_are_dropped() -> Result<(), BoxError> {
        within(async {
            let db = Db::in_memory()?;
            let (client, _model) = testing::client(&db, testing::policy(&[], Default::default())).await?;
            let mut webhooks = Webhooks::new(BTreeMap::new(), client);
            webhooks.header_timeout = Duration::from_millis(200);
            let address = serve(webhooks).await?;

            let mut stream = TcpStream::connect(address).await?;
            let mut rest = Vec::new();
            let closed = tokio::time::timeout(Duration::from_secs(5), stream.read_to_end(&mut rest)).await;

            assert!(closed.is_ok(), "a silent connection was kept open");

            Ok(())
        })
        .await
    }

    /// Only loopback and Tailscale addresses are served.
    #[test]
    fn webhooks_stay_on_this_machine_and_the_tailnet() {
        let cases = [
            ("127.0.0.1:8787", true),
            ("[::1]:8787", true),
            ("100.64.0.1:8787", true),
            ("100.127.255.254:8787", true),
            ("[fd7a:115c:a1e0::1]:8787", true),
            ("100.63.255.255:8787", false),
            ("100.128.0.1:8787", false),
            ("0.0.0.0:8787", false),
            ("[::]:8787", false),
            ("192.168.1.10:8787", false),
            ("localhost:8787", false),
        ];

        for (listen, served) in cases {
            assert_eq!(listen_address(listen).is_ok(), served, "{listen}");
        }
    }

    /// Path, token, delivery ID, body, and the expected status.
    type Case<'a> = (&'a str, Option<&'a str>, Option<&'a str>, &'a str, u16);

    /// Sends one request and returns the status code.
    async fn post(address: SocketAddr, path: &str, token: Option<&str>, delivery: Option<&str>, body: &str) -> Result<u16, BoxError> {
        let mut stream = TcpStream::connect(address).await?;
        let mut request = format!("POST {path} HTTP/1.1\r\nHost: shade\r\nConnection: close\r\nContent-Length: {}\r\n", body.len());

        if let Some(token) = token {
            request.push_str(&format!("Authorization: Bearer {token}\r\n"));
        }

        if let Some(delivery) = delivery {
            request.push_str(&format!("X-Delivery: {delivery}\r\n"));
        }

        request.push_str("\r\n");
        stream.write_all(request.as_bytes()).await?;
        // A body the server refuses part way may be cut short; the answer still comes.
        let _ = stream.write_all(body.as_bytes()).await;

        let mut response = String::new();
        stream.read_to_string(&mut response).await?;

        match response.get(9..12).and_then(|code| code.parse().ok()) {
            Some(status) => Ok(status),
            None => Err(format!("not an HTTP response: {response:?}").into()),
        }
    }

    /// Wrong requests get their documented status; a matching event is
    /// recorded before `202`, once per delivery ID; anything else is not recorded.
    #[tokio::test]
    async fn requests_are_answered_as_documented() -> Result<(), BoxError> {
        within(async {
            let db = Db::in_memory()?;
            let events = HookEvents::new(db.clone());
            let condition = serde_json::from_value(serde_json::json!({ "pointer": "/conclusion", "op": "==", "value": "failure" }))?;
            let hook = Hook {
                token: "secret".to_owned(),
                template: "{body}".to_owned(),
                condition: Some(condition),
                delivery_header: Some("X-Delivery".to_owned()),
                admission: Admission { queued: 10, per_minute: 10 },
            };

            let limits = shade_policy::Limits { max_concurrent_background_runs: 0, ..Default::default() };
            let (client, _model) = testing::client(&db, testing::policy(&[], limits)).await?;
            let webhooks = Webhooks::new(BTreeMap::from([("ci".to_owned(), hook)]), client);
            let address = serve(webhooks).await?;

            let failure = r#"{"conclusion": "failure"}"#;
            let too_large = "x".repeat(MAX_BODY + 1);

            let cases: [Case<'_>; 10] = [
                ("/other", Some("secret"), None, failure, 404),
                // An unknown hook looks exactly like a wrong token.
                ("/hook/other", Some("secret"), None, failure, 401),
                ("/hook/ci", None, None, failure, 401),
                ("/hook/ci", Some("secreT"), None, failure, 401),
                ("/hook/ci", Some("secret"), None, &too_large, 413),
                ("/hook/ci", Some("secret"), None, "not json", 400),
                ("/hook/ci", Some("secret"), None, r#"{"conclusion": 3}"#, 422),
                ("/hook/ci", Some("secret"), None, r#"{"conclusion": "success"}"#, 202),
                ("/hook/ci", Some("secret"), Some("d1"), failure, 202),
                ("/hook/ci", Some("secret"), Some("d1"), failure, 202),
            ];

            for (path, token, delivery, body, expected) in cases {
                let status = post(address, path, token, delivery, body).await?;
                assert_eq!(status, expected, "{path} token={token:?} delivery={delivery:?} body={:.40}", body);
            }

            // Only the matching event is recorded, once, and it was there when `202` came back.
            assert_eq!(events.startable(10).await?.len(), 1);

            Ok(())
        })
        .await
    }
}
