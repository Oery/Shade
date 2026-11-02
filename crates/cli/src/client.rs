//! Blocking connection to the daemon's control socket.

use std::io::{BufRead, BufReader, ErrorKind, Read, Write};
use std::os::unix::net::UnixStream;

use serde::Serialize;
use serde::de::DeserializeOwned;
use shade_proto::{Hello, MAX_LINE_BYTES, PROTOCOL_VERSION, Request, Response};

use crate::error::Error;

/// Sends one request and waits for its final response.
pub fn request(request: &Request) -> Result<(), Error> {
    let stream = connect()?;
    let mut writer = &stream;
    write_message(&mut writer, &Hello { version: PROTOCOL_VERSION })?;
    write_message(&mut writer, request)?;
    let mut reader = BufReader::new(&stream);
    match read_message(&mut reader)? {
        Response::Done => Ok(()),
        Response::Error { message } => Err(Error::Daemon(message)),
    }
}

fn connect() -> Result<UnixStream, Error> {
    let Some(data_dir) = shade_proto::data_dir() else {
        return Err(Error::NoDataDir);
    };
    let path = shade_proto::socket_path(&data_dir);
    match UnixStream::connect(&path) {
        Ok(stream) => Ok(stream),
        Err(source) if is_not_running(&source) => Err(Error::NotRunning { path }),
        Err(source) => Err(Error::Connect { path, source }),
    }
}

/// No socket file, or a stale one left by a daemon that died.
fn is_not_running(error: &std::io::Error) -> bool {
    matches!(error.kind(), ErrorKind::NotFound | ErrorKind::ConnectionRefused)
}

fn write_message<W: Write, T: Serialize>(writer: &mut W, message: &T) -> Result<(), Error> {
    let mut line = serde_json::to_vec(message)?;
    line.push(b'\n');
    writer.write_all(&line)?;
    Ok(())
}

fn read_message<R: BufRead, T: DeserializeOwned>(reader: &mut R) -> Result<T, Error> {
    let mut line = String::new();
    let read = reader.take(MAX_LINE_BYTES).read_line(&mut line)?;
    if line.ends_with('\n') {
        return Ok(serde_json::from_str(&line)?);
    }
    if read as u64 == MAX_LINE_BYTES {
        return Err(Error::LineTooLong);
    }
    Err(Error::Closed)
}
