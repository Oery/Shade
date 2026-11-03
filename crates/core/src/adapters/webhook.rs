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
//! The listener binds to localhost by default; exposing it is for a reverse
//! proxy or tunnel the owner chooses.

use std::collections::BTreeMap;
use std::convert::Infallible;
use std::sync::Arc;

use bytes::Bytes;
use http::{Method, Request, Response, StatusCode, header};
use http_body_util::{BodyExt, Full, Limited};
use hyper::body::Incoming;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper_util::rt::TokioIo;
use shade_condition::Condition;
use shade_store::hooks::{Accepted, Admission, HookEvents};
use subtle::ConstantTimeEq;
use tokio::net::TcpListener;
use tokio::sync::mpsc;

use crate::router::Inbound;

/// Largest body accepted.
const MAX_BODY: usize = 64 * 1024;

/// One configured hook, ready to serve.
pub struct Hook {
    pub token: String,
    pub template: String,
    pub condition: Option<Condition>,
    pub delivery_header: Option<String>,
    pub admission: Admission,
}

pub struct Webhooks {
    hooks: BTreeMap<String, Hook>,
    events: HookEvents,
    /// Tells the router a new event can start.
    inbound: mpsc::Sender<Inbound>,
}

/// An answer: a status and a small JSON body.
type Answer = Response<Full<Bytes>>;

impl Webhooks {
    pub fn new(hooks: BTreeMap<String, Hook>, events: HookEvents, inbound: mpsc::Sender<Inbound>) -> Self {
        Self { hooks, events, inbound }
    }

    /// Serves requests until the task is dropped.
    pub async fn serve(self: Arc<Self>, listener: TcpListener) {
        loop {
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

            tokio::spawn(async move {
                if let Err(error) = http1::Builder::new().serve_connection(TokioIo::new(stream), service).await {
                    tracing::debug!(%error, "webhook connection ended");
                }
            });
        }
    }

    async fn handle(&self, request: Request<Incoming>) -> Answer {
        let name = request.uri().path().strip_prefix("/hook/").unwrap_or_default().to_owned();

        let Some(hook) = self.hooks.get(&name).filter(|_| request.method() == Method::POST) else {
            return answer(StatusCode::NOT_FOUND, serde_json::json!({ "error": "no such hook" }));
        };

        let response = self.receive(&name, hook, request).await;
        tracing::info!(hook = %name, status = response.status().as_u16(), "webhook request");

        response
    }

    async fn receive(&self, name: &str, hook: &Hook, request: Request<Incoming>) -> Answer {
        if !authorized(&request, &hook.token) {
            return answer(StatusCode::UNAUTHORIZED, serde_json::json!({ "error": "missing or wrong bearer token" }));
        }

        let delivery = hook.delivery_header.as_deref().and_then(|header| request.headers().get(header)?.to_str().ok().map(str::to_owned));

        let body = match read(request).await {
            Ok(body) => body,
            Err(status) => return answer(status, serde_json::json!({ "error": "the body is too large or unreadable" })),
        };

        if let Some(refusal) = filter(hook.condition.as_ref(), &body) {
            return refusal;
        }

        let input = hook.template.replace("{hook}", name).replace("{body}", &body);
        self.accept(name, delivery, input, hook.admission).await
    }

    /// Recorded before it is answered, so `202` always means durable.
    async fn accept(&self, name: &str, delivery: Option<String>, input: String, admission: Admission) -> Answer {
        match self.events.accept(name.to_owned(), delivery, input, admission).await {
            Ok(Accepted::New(event)) => {
                // The router also starts it on its next tick if this nudge is lost.
                let _ = self.inbound.try_send(Inbound::StartBackground);
                answer(StatusCode::ACCEPTED, serde_json::json!({ "accepted": true, "event": event }))
            }
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

/// The hook's token as `Authorization: Bearer <token>`, compared in constant time.
fn authorized(request: &Request<Incoming>, token: &str) -> bool {
    let Some(sent) = request.headers().get(header::AUTHORIZATION).and_then(|value| value.to_str().ok()) else {
        return false;
    };

    let Some(sent) = sent.strip_prefix("Bearer ") else {
        return false;
    };

    bool::from(sent.as_bytes().ct_eq(token.as_bytes()))
}

/// The body as text, at most `MAX_BODY` bytes.
async fn read(request: Request<Incoming>) -> Result<String, StatusCode> {
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

fn answer(status: StatusCode, body: serde_json::Value) -> Answer {
    let mut response = Response::new(Full::new(Bytes::from(body.to_string())));
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

    use shade_store::db::Db;
    use shade_store::hooks::{Admission, HookEvents};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};
    use tokio::sync::mpsc;

    use super::{Hook, MAX_BODY, Webhooks};
    use crate::error::BoxError;

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
        let events = HookEvents::new(Db::in_memory()?);
        let condition = serde_json::from_value(serde_json::json!({ "pointer": "/conclusion", "op": "==", "value": "failure" }))?;
        let hook = Hook {
            token: "secret".to_owned(),
            template: "{body}".to_owned(),
            condition: Some(condition),
            delivery_header: Some("X-Delivery".to_owned()),
            admission: Admission { queued: 10, per_minute: 10 },
        };

        let (inbound, _queue) = mpsc::channel(4);
        let webhooks = Arc::new(Webhooks::new(BTreeMap::from([("ci".to_owned(), hook)]), events.clone(), inbound));
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        tokio::spawn(webhooks.serve(listener));

        let failure = r#"{"conclusion": "failure"}"#;
        let too_large = "x".repeat(MAX_BODY + 1);

        let cases: [Case<'_>; 9] = [
            ("/hook/other", Some("secret"), None, failure, 404),
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
    }
}
