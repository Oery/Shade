//! Personal HTTP client API, sharing the webhook listener and router.

mod events;

#[cfg(test)]
mod tests;

use std::time::Duration;

use http::{Method, Request, StatusCode, header};
use hyper::body::Incoming;
use serde::Serialize;
use serde::de::DeserializeOwned;
use shade_core::{Client, ClientError};
use shade_proto::api::{CreateConversation, DecideApproval, SendMessage};

use super::webhook::{Answer, answer, authorized, read};

const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

pub struct Failure {
    status: StatusCode,
    code: &'static str,
    message: String,
}

impl Failure {
    fn new(status: StatusCode, code: &'static str, message: &str) -> Self {
        Self { status, code, message: message.to_owned() }
    }

    pub fn unavailable() -> Self {
        Self::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "unavailable",
            "Shade could not confirm the request; retry messages with the same client_message_id",
        )
    }

    pub fn not_found() -> Self {
        Self::new(StatusCode::NOT_FOUND, "not_found", "conversation or approval not found")
    }

    pub fn conflict(message: &str) -> Self {
        Self::new(StatusCode::CONFLICT, "conflict", message)
    }

    pub fn full() -> Self {
        Self::new(StatusCode::TOO_MANY_REQUESTS, "queue_full", "the chat queue is full; retry later")
    }

    fn invalid(message: &str) -> Self {
        Self::new(StatusCode::BAD_REQUEST, "invalid_request", message)
    }

    fn response(self) -> Answer {
        let error =
            shade_proto::api::ApiError { error: shade_proto::api::ErrorDetail { code: self.code.to_owned(), message: self.message } };
        let mut response = json(self.status, &error);

        if self.status == StatusCode::TOO_MANY_REQUESTS || self.status == StatusCode::SERVICE_UNAVAILABLE {
            response.headers_mut().insert(header::RETRY_AFTER, http::HeaderValue::from_static("5"));
        }

        response
    }
}

impl From<ClientError> for Failure {
    fn from(error: ClientError) -> Self {
        match error {
            ClientError::NotFound => Self::not_found(),
            ClientError::Invalid(message) => Self::invalid(&message),
            ClientError::Conflict(message) => Self::conflict(&message),
            ClientError::Full => Self::full(),
            ClientError::Unavailable => Self::unavailable(),
        }
    }
}

pub struct Api {
    token: String,
    client: Client,
}

impl Api {
    pub fn new(token: String, client: Client) -> Self {
        Self { token, client }
    }

    pub async fn handle(&self, request: Request<Incoming>) -> Answer {
        if !authorized(&request, &self.token) {
            return Failure::new(StatusCode::UNAUTHORIZED, "unauthorized", "missing or wrong bearer token").response();
        }

        let result = tokio::time::timeout(REQUEST_TIMEOUT, self.route(request)).await;

        match result {
            Ok(Ok(response)) => response,
            Ok(Err(failure)) => failure.response(),
            Err(_) => Failure::unavailable().response(),
        }
    }

    async fn route(&self, request: Request<Incoming>) -> Result<Answer, Failure> {
        let path = request.uri().path().to_owned();
        let parts: Vec<&str> = path.trim_start_matches('/').split('/').collect();
        let query = Query::parse(request.uri().query())?;
        let method = request.method();

        match (method, parts.as_slice()) {
            (&Method::GET, ["v1", "conversations"]) => {
                let cursor = query.conversation_cursor()?;
                let page = self.client.conversations(cursor, query.limit).await?;

                Ok(json(StatusCode::OK, &page))
            }
            (&Method::POST, ["v1", "conversations"]) => self.create(request).await,
            (&Method::GET, ["v1", "conversations", root]) => {
                let detail = self.client.conversation((*root).to_owned()).await?;

                Ok(json(StatusCode::OK, &detail))
            }
            (&Method::GET, ["v1", "conversations", root, "messages"]) => {
                let page = self.client.messages((*root).to_owned(), query.ordinal(-1)?, query.limit).await?;

                Ok(json(StatusCode::OK, &page))
            }
            (&Method::GET, ["v1", "conversations", root, "turns"]) => {
                let page = self.client.turns((*root).to_owned(), query.ordinal(0)?, query.limit).await?;

                Ok(json(StatusCode::OK, &page))
            }
            (&Method::GET, ["v1", "conversations", root, "turns", id]) => {
                let turn = self.client.turn((*root).to_owned(), id).await?;

                Ok(json(StatusCode::OK, &turn))
            }
            (&Method::POST, ["v1", "conversations", root, "messages"]) => self.send((*root).to_owned(), request).await,
            (&Method::GET, ["v1", "conversations", root, "events"]) => self.subscribe((*root).to_owned()).await,
            (&Method::POST, ["v1", "conversations", root, "approvals", code]) => {
                let decision: DecideApproval = body(request).await?;
                let result = self.client.decide((*root).to_owned(), (*code).to_owned(), decision.decision).await?;

                Ok(json(StatusCode::OK, &result))
            }
            _ => Err(Failure::not_found()),
        }
    }

    async fn create(&self, request: Request<Incoming>) -> Result<Answer, Failure> {
        let create: CreateConversation = body(request).await?;
        let conversation = self.client.create_conversation(create).await?;

        Ok(json(StatusCode::CREATED, &conversation))
    }

    async fn send(&self, root: String, request: Request<Incoming>) -> Result<Answer, Failure> {
        let request: SendMessage = body(request).await?;
        let turn = self.client.send_message(root, request).await?;

        Ok(json(StatusCode::ACCEPTED, &turn))
    }

    async fn subscribe(&self, root: String) -> Result<Answer, Failure> {
        let subscription = self.client.subscribe(root).await?;

        events::response(subscription)
    }
}

async fn body<T: DeserializeOwned>(request: Request<Incoming>) -> Result<T, Failure> {
    let content_type = request.headers().get(header::CONTENT_TYPE).and_then(|value| value.to_str().ok());

    if !content_type.is_some_and(|value| value.split(';').next().is_some_and(|mime| mime.trim().eq_ignore_ascii_case("application/json"))) {
        return Err(Failure::new(
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "unsupported_media_type",
            "send Content-Type: application/json",
        ));
    }

    let text = match read(request).await {
        Ok(text) => text,
        Err(status) => return Err(Failure::new(status, "invalid_body", "body exceeds 64 KiB or is not UTF-8")),
    };

    match serde_json::from_str(&text) {
        Ok(value) => Ok(value),
        Err(_) => Err(Failure::invalid("body must match the endpoint's JSON schema")),
    }
}

fn json(status: StatusCode, value: &impl Serialize) -> Answer {
    match serde_json::to_value(value) {
        Ok(value) => answer(status, value),
        Err(error) => {
            tracing::error!(%error, "cannot encode API response");
            answer(
                StatusCode::INTERNAL_SERVER_ERROR,
                serde_json::json!({"error": {"code": "encoding_error", "message": "cannot encode response"}}),
            )
        }
    }
}

struct Query {
    limit: usize,
    cursor: Option<String>,
}

impl Query {
    fn parse(query: Option<&str>) -> Result<Self, Failure> {
        let mut result = Self { limit: shade_proto::api::DEFAULT_PAGE, cursor: None };
        let mut limit_seen = false;

        for pair in query.into_iter().flat_map(|query| query.split('&')) {
            let Some((key, value)) = pair.split_once('=') else {
                return Err(Failure::invalid("query parameters need values"));
            };

            match key {
                "limit" if !limit_seen => {
                    let Ok(limit) = value.parse::<usize>() else {
                        return Err(Failure::invalid("limit must be an integer"));
                    };

                    if limit == 0 || limit > shade_proto::api::MAX_PAGE {
                        return Err(Failure::invalid("limit must be between 1 and 100"));
                    }

                    result.limit = limit;
                    limit_seen = true;
                }
                "cursor" if result.cursor.is_none() && value.len() <= 128 => result.cursor = Some(value.to_owned()),
                _ => return Err(Failure::invalid("unknown or repeated query parameter")),
            }
        }

        Ok(result)
    }

    fn ordinal(&self, default: i64) -> Result<i64, Failure> {
        let Some(cursor) = &self.cursor else {
            return Ok(default);
        };
        let Ok(cursor) = cursor.parse::<i64>() else {
            return Err(Failure::invalid("invalid cursor"));
        };

        if cursor < 0 {
            return Err(Failure::invalid("invalid cursor"));
        }

        Ok(cursor)
    }

    fn conversation_cursor(&self) -> Result<Option<(i64, String)>, Failure> {
        let Some(cursor) = &self.cursor else {
            return Ok(None);
        };
        let Some((at, id)) = cursor.split_once('.') else {
            return Err(Failure::invalid("invalid cursor"));
        };
        let Ok(at) = at.parse::<i64>() else {
            return Err(Failure::invalid("invalid cursor"));
        };

        if id.is_empty() || id.len() > 64 || !id.bytes().all(|byte| byte.is_ascii_alphanumeric() || byte == b'-') {
            return Err(Failure::invalid("invalid cursor"));
        }

        Ok(Some((at, id.to_owned())))
    }
}
