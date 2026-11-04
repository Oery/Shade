//! Verify the composition root with isolated state and no external services.

use std::fs::{self, OpenOptions};
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::os::unix::net::UnixStream;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use serde_json::Value;
use shade_proto::{Hello, PROTOCOL_VERSION, Request, Response};

type BoxError = Box<dyn std::error::Error + Send + Sync>;
const TOKEN: &str = "0123456789abcdef0123456789abcdef";

struct Daemon {
    process: Child,
    root: PathBuf,
    address: SocketAddr,
}

impl Daemon {
    fn start() -> Result<Self, BoxError> {
        let root = std::env::temp_dir().join(format!("shade-daemon-startup-{}", uuid::Uuid::new_v4()));
        fs::create_dir(&root)?;
        let reserved = TcpListener::bind("127.0.0.1:0")?;
        let address = reserved.local_addr()?;
        drop(reserved);
        let config = format!(
            "[model]\nprovider = 'openai_compatible'\nid = 'mock'\napi_key_env = 'SHADE_STARTUP_MODEL_KEY'\n\
             base_url = 'http://127.0.0.1:1'\ncontext_window = 100000\nmax_output_tokens = 1000\n\
             [api]\ntoken_env = 'SHADE_STARTUP_API_TOKEN'\n[http]\nlisten = '{address}'\n"
        );
        let config_path = root.join("config.toml");
        fs::write(&config_path, config)?;
        let process = Command::new(env!("CARGO_BIN_EXE_shaded"))
            .arg("--config")
            .arg(config_path)
            .env("XDG_DATA_HOME", &root)
            .env("SHADE_STARTUP_MODEL_KEY", "unused-local-test-key")
            .env("SHADE_STARTUP_API_TOKEN", TOKEN)
            .env("RUST_LOG", "error")
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()?;

        Ok(Self { process, root, address })
    }

    fn ready(&mut self) -> Result<(), BoxError> {
        let until = Instant::now() + Duration::from_secs(5);

        while Instant::now() < until {
            if let Some(status) = self.process.try_wait()? {
                let mut error = String::new();

                if let Some(mut stderr) = self.process.stderr.take() {
                    stderr.read_to_string(&mut error)?;
                }

                return Err(format!("daemon exited {status}: {error}").into());
            }

            if self.request("GET", "").is_ok() {
                return Ok(());
            }

            thread::sleep(Duration::from_millis(10));
        }

        Err("daemon did not start its HTTP server".into())
    }

    fn request(&self, method: &str, body: &str) -> Result<(u16, Value), BoxError> {
        let mut stream = TcpStream::connect_timeout(&self.address, Duration::from_millis(200))?;
        stream.set_read_timeout(Some(Duration::from_secs(2)))?;
        let request = format!(
            "{method} /v1/conversations HTTP/1.1\r\nHost: shade\r\nConnection: close\r\n\
             Authorization: Bearer {TOKEN}\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}",
            body.len()
        );
        stream.write_all(request.as_bytes())?;
        let mut response = String::new();
        stream.read_to_string(&mut response)?;
        let (headers, body) = response.split_once("\r\n\r\n").ok_or("missing HTTP response")?;
        let status = headers.get(9..12).ok_or("missing HTTP status")?.parse()?;

        Ok((status, serde_json::from_str(body)?))
    }

    fn terminal_request(&self) -> Result<(), BoxError> {
        let mut stream = UnixStream::connect(self.root.join("shade/shade.sock"))?;
        stream.set_read_timeout(Some(Duration::from_secs(2)))?;
        serde_json::to_writer(&mut stream, &Hello { version: PROTOCOL_VERSION })?;
        stream.write_all(b"\n")?;
        serde_json::to_writer(&mut stream, &Request::NewConversation { conversation: "terminal".to_owned() })?;
        stream.write_all(b"\n")?;
        let mut reader = BufReader::new(stream);
        let mut line = String::new();

        while reader.read_line(&mut line)? > 0 {
            match serde_json::from_str::<Response>(&line)? {
                Response::Done => return Ok(()),
                Response::Error { message } => return Err(message.into()),
                _ => line.clear(),
            }
        }

        Err("terminal request ended without a reply".into())
    }

    fn stop(&mut self) -> Result<(), BoxError> {
        let signal = Command::new("kill").arg("-TERM").arg(self.process.id().to_string()).status()?;
        assert!(signal.success());
        let until = Instant::now() + Duration::from_secs(5);

        while Instant::now() < until {
            if let Some(status) = self.process.try_wait()? {
                assert!(status.success(), "daemon shutdown failed: {status}");

                return Ok(());
            }

            thread::sleep(Duration::from_millis(10));
        }

        Err("daemon did not shut down after SIGTERM".into())
    }
}

impl Drop for Daemon {
    fn drop(&mut self) {
        let _ = self.process.kill();
        let _ = self.process.wait();
        let _ = fs::remove_dir_all(&self.root);
    }
}

#[test]
fn daemon_serves_both_transports_and_releases_its_lock_on_shutdown() -> Result<(), BoxError> {
    let mut daemon = Daemon::start()?;
    daemon.ready()?;
    let (status, conversation) = daemon.request("POST", r#"{"name":"Smoke"}"#)?;
    assert_eq!(status, 201);
    assert!(conversation["id"].is_string());
    daemon.terminal_request()?;
    let (status, page) = daemon.request("GET", "")?;
    assert_eq!(status, 200);
    assert_eq!(page["items"].as_array().ok_or("missing conversations")?.len(), 2);
    daemon.stop()?;
    assert!(!daemon.root.join("shade/shade.sock").exists());
    let lock = OpenOptions::new().read(true).write(true).open(daemon.root.join("shade/shade.lock"))?;
    assert!(lock.try_lock().is_ok(), "the daemon kept its execution lock");

    Ok(())
}
