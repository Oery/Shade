use std::convert::Infallible;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use http::{Response, header};
use http_body_util::{BodyExt, StreamBody};
use hyper::body::Frame;
use shade_proto::api::Event;
use tokio::sync::broadcast;

use super::Failure;
use crate::webhook::Answer;
use shade_core::Subscription;

pub(super) fn response(subscription: Subscription) -> Result<Answer, Failure> {
    let initial = encode(&subscription.snapshot)?;
    let state = (Some(initial), subscription.events);
    let stream = futures_util::stream::unfold(state, |(initial, mut events)| async move {
        let bytes = match initial {
            Some(initial) => initial,
            None => next(&mut events).await?,
        };

        Some((Ok::<_, Infallible>(Frame::data(bytes)), (None, events)))
    });
    let body = StreamBody::new(stream).boxed_unsync();
    let mut response = Response::new(body);
    response.headers_mut().insert(header::CONTENT_TYPE, http::HeaderValue::from_static("text/event-stream"));
    response.headers_mut().insert(header::CACHE_CONTROL, http::HeaderValue::from_static("no-cache"));
    response.headers_mut().insert("x-accel-buffering", http::HeaderValue::from_static("no"));

    Ok(response)
}

async fn next(events: &mut broadcast::Receiver<Arc<Event>>) -> Option<Bytes> {
    match tokio::time::timeout(Duration::from_secs(15), events.recv()).await {
        Ok(Ok(event)) => encode(&event).ok(),
        // A lagged client must reload state. Never hold up a running turn.
        Ok(Err(_)) => None,
        Err(_) => Some(Bytes::from_static(b": keep-alive\n\n")),
    }
}

fn encode(event: &Event) -> Result<Bytes, Failure> {
    let json = match serde_json::to_string(event) {
        Ok(json) => json,
        Err(_) => return Err(Failure::unavailable()),
    };

    Ok(Bytes::from(format!("data: {json}\n\n")))
}

#[cfg(test)]
mod tests {
    use super::*;
    type BoxError = Box<dyn std::error::Error + Send + Sync>;

    #[tokio::test]
    async fn slow_subscribers_disconnect_without_blocking_work() -> Result<(), BoxError> {
        let (sender, mut receiver) = broadcast::channel(64);

        for _ in 0..100 {
            sender.send(Arc::new(Event::TextDelta { turn_id: "turn".to_owned(), text: "chunk".to_owned() }))?;
        }

        assert!(next(&mut receiver).await.is_none());

        Ok(())
    }
}
