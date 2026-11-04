//! Exercise the real HTTP listener, router and SQLite stores with a mock model.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use serde_json::{Value, json};
use shade_core::{Client, HookAdmission as Admission};
use shade_policy::Limits;
use shade_proto::api::TurnStatus;
use shade_store::chat::Chats;
use shade_store::db::Db;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use super::{Api, Query};
use crate::webhook::{Hook, Webhooks};
use shade_core::test_support::{self as testing, BoxError, within};

const TOKEN: &str = "0123456789abcdef0123456789abcdef";

async fn serve(client: Client) -> Result<SocketAddr, BoxError> {
    let api = Api::new(TOKEN.to_owned(), client.clone());
    let hook = Hook {
        token: "hook-secret".to_owned(),
        template: "{body}".to_owned(),
        condition: None,
        delivery_header: None,
        admission: Admission { queued: 10, per_minute: 10 },
    };
    let hooks = BTreeMap::from([("ci".to_owned(), hook)]);
    let server = Webhooks::new(hooks, client).with_api(Some(api));
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    tokio::spawn(Arc::new(server).serve(listener));

    Ok(address)
}

async fn connect(address: SocketAddr, method: &str, path: &str, token: Option<&str>, body: &str) -> Result<TcpStream, BoxError> {
    let mut stream = TcpStream::connect(address).await?;
    let mut request = format!(
        "{method} {path} HTTP/1.1\r\nHost: shade\r\nConnection: close\r\nContent-Type: application/json\r\nContent-Length: {}\r\n",
        body.len()
    );

    if let Some(token) = token {
        request.push_str(&format!("Authorization: Bearer {token}\r\n"));
    }

    request.push_str("\r\n");
    request.push_str(body);
    stream.write_all(request.as_bytes()).await?;

    Ok(stream)
}

async fn request(address: SocketAddr, method: &str, path: &str, token: Option<&str>, body: &str) -> Result<(u16, Value), BoxError> {
    let mut stream = connect(address, method, path, token, body).await?;
    let mut response = String::new();
    tokio::time::timeout(Duration::from_secs(3), stream.read_to_string(&mut response)).await??;
    let (headers, body) = response.split_once("\r\n\r\n").ok_or("missing HTTP headers")?;
    let status = headers.get(9..12).ok_or("missing status")?.parse()?;

    Ok((status, serde_json::from_str(body)?))
}

async fn authenticated(address: SocketAddr, method: &str, path: &str, body: Value) -> Result<(u16, Value), BoxError> {
    request(address, method, path, Some(TOKEN), &body.to_string()).await
}

async fn until_status(chats: &Chats, id: &str, expected: TurnStatus) -> Result<(), BoxError> {
    loop {
        let turn = chats.turn(id).await?.ok_or("missing accepted turn")?;

        if turn.status == expected {
            return Ok(());
        }

        tokio::task::yield_now().await;
    }
}

async fn read_until(stream: &mut TcpStream, needle: &str) -> Result<String, BoxError> {
    let read = async {
        let mut text = String::new();
        let mut buffer = [0u8; 4096];

        loop {
            let count = stream.read(&mut buffer).await?;

            if count == 0 {
                return Err("event stream ended".into());
            }

            text.push_str(&String::from_utf8_lossy(&buffer[..count]));

            if text.contains(needle) {
                return Ok(text);
            }
        }
    };

    tokio::time::timeout(Duration::from_secs(3), read).await?
}

#[tokio::test(flavor = "current_thread")]
async fn authentication_and_webhooks_are_independent() -> Result<(), BoxError> {
    within(async {
        let db = Db::in_memory()?;
        let limits = Limits { max_concurrent_background_runs: 0, ..Limits::default() };
        let (client, _model) = testing::client(&db, testing::policy(&[], limits)).await?;
        let address = serve(client).await?;

        for token in [None, Some("wrong"), Some("hook-secret")] {
            let (status, body) = request(address, "GET", "/v1/conversations", token, "").await?;
            assert_eq!(status, 401);
            assert_eq!(body["error"]["code"], "unauthorized");
        }

        assert_eq!(request(address, "POST", "/hook/ci", Some(TOKEN), "{}").await?.0, 401);
        assert_eq!(request(address, "POST", "/hook/ci", Some("hook-secret"), "{}").await?.0, 202);
        assert_eq!(authenticated(address, "GET", "/v1/conversations", Value::Null).await?.0, 200);
        assert_eq!(authenticated(address, "GET", "/v1/conversations?limit=0", Value::Null).await?.0, 400);
        assert_eq!(authenticated(address, "GET", "/v1/conversations/missing", Value::Null).await?.0, 404);

        Ok(())
    })
    .await
}

#[tokio::test(flavor = "current_thread")]
async fn http_chat_streams_queues_and_deduplicates() -> Result<(), BoxError> {
    within(async {
        let db = Db::in_memory()?;
        let chats = Chats::new(db.clone());
        let (client, mut model) = testing::client(&db, testing::policy(&[], Limits::default())).await?;
        let address = serve(client).await?;

        let (status, created) = authenticated(address, "POST", "/v1/conversations", json!({"name": "Work"})).await?;
        assert_eq!(status, 201);
        let root = created["id"].as_str().ok_or("missing conversation ID")?;
        let path = format!("/v1/conversations/{root}/messages");
        let (_, first) = authenticated(address, "POST", &path, json!({"text": "first", "client_message_id": "1"})).await?;
        let id = first["id"].as_str().ok_or("missing turn ID")?;
        model.next_request().await;
        model.send_text("partial");
        tokio::task::yield_now().await;

        let (_, second) = authenticated(address, "POST", &path, json!({"text": "second", "client_message_id": "2"})).await?;
        assert_eq!(second["status"], "queued");
        let (_, duplicate) = authenticated(address, "POST", &path, json!({"text": "first", "client_message_id": "1"})).await?;
        assert_eq!(duplicate["id"], id);
        assert_eq!(
            authenticated(address, "POST", &path, json!({"text": "changed", "client_message_id": "1"})).await?.0,
            409
        );
        assert!(model.try_next_request().is_none(), "queued input interrupted the first turn");

        let mut events = connect(address, "GET", &format!("/v1/conversations/{root}/events"), Some(TOKEN), "").await?;
        let snapshot = read_until(&mut events, "snapshot").await?;
        assert!(snapshot.contains("partial"), "snapshot missed the existing partial text: {snapshot}");
        model.send_text(" answer");
        model.finish();
        let updates = read_until(&mut events, "answer").await?;
        assert!(updates.contains("text_delta"));
        until_status(&chats, id, TurnStatus::Completed).await?;

        let next = model.next_request().await;
        assert!(format!("{:?}", next.chat_history).contains("second"));
        drop(events);
        model.send_text("second answer");
        model.finish();
        let second_id = second["id"].as_str().ok_or("missing second ID")?;
        until_status(&chats, second_id, TurnStatus::Completed).await?;

        let (_, history) = authenticated(address, "GET", &path, Value::Null).await?;
        assert!(history.to_string().contains("second answer"));
        let (_, turns) = authenticated(address, "GET", &format!("/v1/conversations/{root}/turns"), Value::Null).await?;
        assert_eq!(turns["items"].as_array().ok_or("missing turn list")?.len(), 2);
        assert_eq!(turns["items"][1]["status"], "completed");
        let (_, status) = authenticated(address, "GET", &format!("/v1/conversations/{root}/turns/{second_id}"), Value::Null).await?;
        assert_eq!(status["status"], "completed");
        assert_eq!(history["items"][0]["turn_id"], id);

        Ok(())
    })
    .await
}

#[test]
fn query_validation_rejects_ambiguous_parameters() {
    for query in ["limit=0", "limit=101", "limit=1&limit=2", "cursor=1&cursor=2", "other=1", "limit=x"] {
        assert!(Query::parse(Some(query)).is_err(), "accepted {query}");
    }
}

#[tokio::test(flavor = "current_thread")]
async fn approvals_are_scoped_and_decisions_queue_a_followup() -> Result<(), BoxError> {
    within(async {
        let db = Db::in_memory()?;
        let conversations = shade_store::conversation::Conversations::new(db.clone());
        conversations.register_root(&infinity_agent_core::ThreadId::from("chat".to_owned())).await?;
        conversations.register_root(&infinity_agent_core::ThreadId::from("other".to_owned())).await?;
        let schedules = shade_store::schedules::Schedules::new(db.clone());
        schedules.add("tea", "cron", "0 16 * * *", chrono::Utc::now().timestamp() + 3600).await?;
        schedules.propose("approve", "tea", Some(1), "Green tea", Some("chat")).await?;
        let (client, mut model) = testing::client(&db, testing::policy(&[], Limits::default())).await?;
        let address = serve(client).await?;

        let (_, detail) = authenticated(address, "GET", "/v1/conversations/chat", Value::Null).await?;
        assert_eq!(detail["approvals"][0]["code"], "approve");
        let wrong = "/v1/conversations/other/approvals/approve";
        assert_eq!(authenticated(address, "POST", wrong, json!({"decision": "approve"})).await?.0, 404);
        assert!(schedules.pending("approve").await?.is_some());
        let path = "/v1/conversations/chat/approvals/approve";
        assert_eq!(authenticated(address, "POST", path, json!({"decision": "approve"})).await?.0, 200);
        assert_eq!(authenticated(address, "POST", path, json!({"decision": "approve"})).await?.0, 404);
        let followup = model.next_request().await;
        assert!(format!("{:?}", followup.chat_history).contains("The owner approved"));
        model.send_text("approved");
        model.finish();
        let chats = Chats::new(db.clone());
        let turns = chats.turns("chat".to_owned(), 0, 50).await?;
        let id = &turns.items.first().ok_or("missing approval follow-up")?.id;
        until_status(&chats, id, TurnStatus::Completed).await?;
        assert_eq!(chats.delivery(id).await?.as_deref(), Some("api"));

        schedules.propose("reject", "tea", Some(2), "Black tea", Some("chat")).await?;
        assert_eq!(
            authenticated(address, "POST", "/v1/conversations/chat/approvals/reject", json!({"decision": "reject"})).await?.0,
            200
        );
        assert!(schedules.pending("reject").await?.is_none());
        assert_eq!(schedules.get("tea").await?.ok_or("missing job")?.definition.title, "Green tea");
        model.next_request().await;
        model.send_text("rejected");
        model.finish();

        Ok(())
    })
    .await
}

#[tokio::test(flavor = "current_thread")]
async fn queue_limits_reject_before_acceptance_and_retries_still_work() -> Result<(), BoxError> {
    within(async {
        let db = Db::in_memory()?;
        let limits = Limits { max_queued_chat_messages: 1, max_queued_chat_messages_total: 1, ..Limits::default() };
        let (client, mut model) = testing::client(&db, testing::policy(&[], limits)).await?;
        let root = Chats::new(db.clone()).create(None).await?.id;
        let address = serve(client).await?;
        let path = format!("/v1/conversations/{root}/messages");
        let first = json!({"text": "first", "client_message_id": "1"});
        let second = json!({"text": "second", "client_message_id": "2"});
        assert_eq!(authenticated(address, "POST", &path, first).await?.0, 202);
        model.next_request().await;
        assert_eq!(authenticated(address, "POST", &path, second.to_owned()).await?.0, 202);
        let third = json!({"text": "third", "client_message_id": "3"});
        let (status, body) = authenticated(address, "POST", &path, third).await?;
        assert_eq!(status, 429);
        assert_eq!(body["error"]["code"], "queue_full");
        assert_eq!(authenticated(address, "POST", &path, second).await?.0, 202);
        assert_eq!(shade_store::inbox::Inbox::new(db).count().await?, 2);
        assert_eq!(
            authenticated(address, "POST", &path, json!({"text": "bad", "client_message_id": "4", "caps": ["shell"]})).await?.0,
            400
        );

        Ok(())
    })
    .await
}
