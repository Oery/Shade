//! Control socket: the `shade` client's way into the daemon.

use std::fs::{self, Permissions};
use std::io::ErrorKind;
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use serde::Serialize;
use serde::de::DeserializeOwned;
use shade_proto::{Hello, MAX_LINE_BYTES, PROTOCOL_VERSION, Request, Response};
use shade_store::roots::{Adapter, Target};
use thiserror::Error;
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::{Semaphore, mpsc};

use crate::Error;
use crate::router::{Inbound, Outbound, REPLY_QUEUE};

const MAX_CONNECTIONS: usize = 16;

#[derive(Debug, Error)]
enum ConnectionError {
    #[error("connection I/O failed: {0}")]
    Io(#[from] std::io::Error),

    #[error("invalid message: {0}")]
    Json(#[from] serde_json::Error),

    #[error("message line too long")]
    LineTooLong,

    #[error("peer closed the connection mid-exchange")]
    Closed,

    #[error("rejected peer with uid {0}")]
    ForeignPeer(u32),
}

type Writer = tokio::net::unix::OwnedWriteHalf;

pub struct ControlSocket {
    listener: UnixListener,
    path: PathBuf,
    owner_uid: u32,
    inbound: mpsc::Sender<Inbound>,
}

impl ControlSocket {
    /// The caller must hold the execution lock: it proves a leftover socket file is stale.
    pub fn bind(path: PathBuf, inbound: mpsc::Sender<Inbound>) -> Result<Self, Error> {
        if let Err(source) = remove_if_present(&path) {
            return Err(Error::Socket { path, source });
        }

        let listener = match UnixListener::bind(&path) {
            Ok(listener) => listener,
            Err(source) => return Err(Error::Socket { path, source }),
        };

        let owner_uid = match restrict_to_owner(&path) {
            Ok(uid) => uid,
            Err(source) => return Err(Error::Socket { path, source }),
        };

        Ok(Self { listener, path, owner_uid, inbound })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Accepts connections until the task is dropped, at most `MAX_CONNECTIONS` at a time.
    pub async fn serve(&self) {
        let permits = Arc::new(Semaphore::new(MAX_CONNECTIONS));

        loop {
            let Ok(permit) = Arc::clone(&permits).acquire_owned().await else {
                return; // The semaphore is never closed.
            };

            let stream = match self.listener.accept().await {
                Ok((stream, _)) => stream,
                Err(error) => {
                    tracing::warn!(%error, "control socket accept failed");
                    continue;
                }
            };

            let owner_uid = self.owner_uid;
            let inbound = self.inbound.clone();

            tokio::spawn(async move {
                if let Err(error) = handle(stream, owner_uid, inbound).await {
                    tracing::warn!(%error, "control connection failed");
                }

                drop(permit);
            });
        }
    }
}

impl Drop for ControlSocket {
    fn drop(&mut self) {
        if let Err(error) = remove_if_present(&self.path) {
            tracing::warn!(%error, path = %self.path.display(), "cannot remove control socket");
        }
    }
}

fn remove_if_present(path: &Path) -> std::io::Result<()> {
    match fs::remove_file(path) {
        Err(error) if error.kind() != ErrorKind::NotFound => Err(error),
        _ => Ok(()),
    }
}

/// Sets mode `0600` and returns the socket owner, which is this process's user.
fn restrict_to_owner(path: &Path) -> std::io::Result<u32> {
    fs::set_permissions(path, Permissions::from_mode(0o600))?;
    let metadata = fs::metadata(path)?;

    Ok(metadata.uid())
}

async fn handle(stream: UnixStream, owner_uid: u32, inbound: mpsc::Sender<Inbound>) -> Result<(), ConnectionError> {
    let peer_uid = stream.peer_cred()?.uid();

    if peer_uid != owner_uid {
        return Err(ConnectionError::ForeignPeer(peer_uid));
    }

    let (read, mut write) = stream.into_split();
    let mut reader = BufReader::new(read);
    let hello: Hello = read_message(&mut reader).await?;

    if hello.version != PROTOCOL_VERSION {
        let message = format!(
            "protocol mismatch: shade speaks version {}, shaded speaks {PROTOCOL_VERSION}; upgrade both together",
            hello.version
        );
        return write_message(&mut write, &Response::Error { message }).await;
    }

    let request: Request = read_message(&mut reader).await?;

    match request {
        Request::Ask { prompt } => {
            let prompt = |reply| Inbound::Prompt {
                adapter: Adapter::Terminal,
                target: Target::Fresh,
                text: prompt,
                source_key: None,
                outbound: Outbound::Client(reply),
            };
            forward(prompt, &inbound, &mut write).await
        }
        Request::Chat { conversation, text } => {
            if !shade_proto::valid_conversation(&conversation) {
                return write_message(&mut write, &invalid_conversation()).await;
            }

            let target = Target::Named(conversation);
            let prompt =
                |reply| Inbound::Prompt { adapter: Adapter::Terminal, target, text, source_key: None, outbound: Outbound::Client(reply) };
            forward(prompt, &inbound, &mut write).await
        }
        Request::NewConversation { conversation } => {
            if !shade_proto::valid_conversation(&conversation) {
                return write_message(&mut write, &invalid_conversation()).await;
            }

            let new = |reply| Inbound::NewConversation { adapter: Adapter::Terminal, key: conversation, reply };
            forward(new, &inbound, &mut write).await
        }
        Request::Usage => forward(|reply| Inbound::Usage { reply }, &inbound, &mut write).await,
        Request::Audit { since, root } => forward(|reply| Inbound::Audit { since, root, reply }, &inbound, &mut write).await,
        Request::Schedule { action } => forward(|reply| Inbound::Schedule { action, reply }, &inbound, &mut write).await,
        Request::Watch { action } => forward(|reply| Inbound::Watch { action, reply }, &inbound, &mut write).await,
        Request::Backup { path } => forward(|reply| Inbound::Backup { path, reply }, &inbound, &mut write).await,
        other => write_message(&mut write, &not_implemented(other)).await,
    }
}

/// Sends input to the router and streams its replies until the final one.
async fn forward<F>(input: F, inbound: &mpsc::Sender<Inbound>, write: &mut Writer) -> Result<(), ConnectionError>
where
    F: FnOnce(mpsc::Sender<Response>) -> Inbound,
{
    let (reply, mut replies) = mpsc::channel(REPLY_QUEUE);

    if inbound.send(input(reply)).await.is_err() {
        let message = "the daemon is shutting down".to_owned();
        return write_message(write, &Response::Error { message }).await;
    }

    while let Some(response) = replies.recv().await {
        let terminal = !matches!(response, Response::Text { .. });

        // Leaving mid-turn is normal: the turn finishes without this client.
        if let Err(error) = write_message(write, &response).await {
            tracing::debug!(%error, "client left before the reply ended");
            return Ok(());
        }

        if terminal {
            return Ok(());
        }
    }

    let message = "the turn ended without a reply".to_owned();

    write_message(write, &Response::Error { message }).await
}

fn invalid_conversation() -> Response {
    let message = format!(
        "conversation names are 1 to {} bytes without control characters",
        shade_proto::MAX_CONVERSATION_BYTES
    );

    Response::Error { message }
}

fn not_implemented(request: Request) -> Response {
    let name = match request {
        Request::Ask { .. } => "ask",
        Request::Chat { .. } => "chat",
        Request::NewConversation { .. } => "chat",
        Request::Schedule { .. } => "schedule",
        Request::Watch { .. } => "watch",
        Request::ThreadsList => "threads",
        Request::Audit { .. } => "audit",
        Request::Usage => "usage",
        Request::Backup { .. } => "backup",
    };

    Response::Error { message: format!("`{name}` is not implemented yet") }
}

async fn read_message<T, R>(reader: &mut R) -> Result<T, ConnectionError>
where
    T: DeserializeOwned,
    R: AsyncBufRead + Unpin,
{
    let line = read_line(reader, MAX_LINE_BYTES).await?;
    let message = serde_json::from_str(&line)?;

    Ok(message)
}

/// Reads one newline-terminated line of at most `limit` bytes.
async fn read_line<R: AsyncBufRead + Unpin>(reader: &mut R, limit: u64) -> Result<String, ConnectionError> {
    let mut line = String::new();
    let read = (&mut *reader).take(limit).read_line(&mut line).await?;

    if line.ends_with('\n') {
        return Ok(line);
    }

    if read as u64 == limit {
        return Err(ConnectionError::LineTooLong);
    }

    Err(ConnectionError::Closed)
}

async fn write_message<W, T>(writer: &mut W, message: &T) -> Result<(), ConnectionError>
where
    W: AsyncWrite + Unpin,
    T: Serialize,
{
    let mut line = serde_json::to_vec(message)?;
    line.push(b'\n');
    writer.write_all(&line).await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn read_line_never_returns_partial_lines() {
        let cases: [(&[u8], &str); 4] = [(b"abc\n", "abc\n"), (b"abcd\n", "too long"), (b"abc", "closed"), (b"", "closed")];

        for (input, expected) in cases {
            let mut reader = input;
            let actual = match read_line(&mut reader, 4).await {
                Ok(line) => line,
                Err(ConnectionError::LineTooLong) => "too long".to_string(),
                Err(ConnectionError::Closed) => "closed".to_string(),
                Err(error) => error.to_string(),
            };

            assert_eq!(actual, expected, "input {input:?}");
        }
    }
}
