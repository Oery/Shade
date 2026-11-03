//! MCP servers: Shade as a client of the owner's tool servers.
//!
//! Each configured server runs as a child process over stdio, with a
//! cleared environment holding only what it needs to find its own files and
//! the variables its config lists. Its tools are listed when it starts. A
//! server that cannot start is tried again in the background, so it never
//! holds up the daemon; one that stops is restarted on its next call. A call
//! that was sent but got no answer has an unknown outcome, and is never sent
//! again by Shade on its own.

mod render;

use std::collections::BTreeMap;
use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;

use rmcp::model::CallToolRequestParams;
use rmcp::service::RunningService;
use rmcp::transport::TokioChildProcess;
use rmcp::{Peer, RoleClient, ServiceExt};
use serde::Deserialize;
use thiserror::Error;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::{ChildStderr, Command};
use tokio::sync::Mutex;

pub use render::MAX_RESULT;

/// Taken from the daemon's environment for every server: enough to find its own files.
const INHERITED: [&str; 6] = ["PATH", "HOME", "XDG_CONFIG_HOME", "XDG_DATA_HOME", "XDG_STATE_HOME", "XDG_CACHE_HOME"];

/// How often a server that is down is tried again.
const RETRY: Duration = Duration::from_secs(60);

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// The server's program and its arguments.
    pub command: Vec<String>,
    /// Secrets passed to the server, by name: read from Shade's environment or the `.env` next to its config.
    #[serde(default)]
    pub env: Vec<String>,
    /// Settings passed to the server as they are written, such as a path.
    #[serde(default)]
    pub vars: BTreeMap<String, String>,
    /// What the server is for, shown to the model when it chooses a job's caps.
    pub description: String,
    /// Seconds the server has to start and list its tools.
    #[serde(default = "default_startup")]
    pub startup_timeout_seconds: u64,
    /// Seconds a call may take; after that its outcome is unknown.
    #[serde(default = "default_call")]
    pub call_timeout_seconds: u64,
}

fn default_startup() -> u64 {
    20
}

fn default_call() -> u64 {
    60
}

/// A tool a server offers.
#[derive(Clone, Debug)]
pub struct Tool {
    pub name: String,
    pub description: String,
    pub input_schema: serde_json::Value,
}

/// How a call ended, as the model reads it.
#[derive(Debug, PartialEq)]
pub enum Answer {
    Done(String),
    /// The tool itself reported an error.
    Failed(String),
}

#[derive(Debug, Error)]
pub enum CallError {
    #[error("the {0} server is not running ({1}); nothing was sent")]
    Unavailable(String, String),

    #[error("the {0} server did not answer ({1}); the call may have taken effect")]
    Unknown(String, String),
}

type Client = RunningService<RoleClient, ()>;

/// How a server is started.
struct Launch {
    program: String,
    args: Vec<String>,
    env: Vec<(String, String)>,
    startup: Duration,
}

pub struct Server {
    name: String,
    /// `None` for a server connected some other way, which cannot be restarted.
    launch: Option<Launch>,
    call_timeout: Duration,
    client: Mutex<Option<Client>>,
    tools: Mutex<Vec<Tool>>,
}

impl Server {
    /// A server started from `config`. `env` holds the values of the variables it lists.
    pub fn new(name: String, config: &Config, env: Vec<(String, String)>) -> Result<Self, String> {
        let Some((program, args)) = config.command.split_first() else {
            return Err(format!("[mcp.{name}] command is empty"));
        };

        // Later entries win: what the config names overrides what the daemon inherited.
        let mut all: Vec<(String, String)> =
            INHERITED.into_iter().filter_map(|key| Some((key.to_owned(), std::env::var(key).ok()?))).collect();
        all.extend(config.vars.iter().map(|(key, value)| (key.to_owned(), value.to_owned())));
        all.extend(env);
        let env = all;

        let launch =
            Launch { program: program.to_owned(), args: args.to_vec(), env, startup: Duration::from_secs(config.startup_timeout_seconds) };

        Ok(Self::with(name, Some(launch), Duration::from_secs(config.call_timeout_seconds)))
    }

    fn with(name: String, launch: Option<Launch>, call_timeout: Duration) -> Self {
        Self { name, launch, call_timeout, client: Mutex::new(None), tools: Mutex::new(Vec::new()) }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    /// The tools it listed when it last started; none while it never has.
    pub async fn tools(&self) -> Vec<Tool> {
        self.tools.lock().await.to_vec()
    }

    /// Starts the server and lists its tools, unless it is running already.
    pub async fn connect(&self) -> Result<(), String> {
        self.peer().await.map(drop)
    }

    /// Calls one of its tools, starting the server first if it is down.
    pub async fn call(&self, tool: &str, arguments: serde_json::Value) -> Result<Answer, CallError> {
        let serde_json::Value::Object(arguments) = arguments else {
            return Ok(Answer::Failed("the arguments must be a JSON object".to_owned()));
        };

        let peer = match self.peer().await {
            Ok(peer) => peer,
            Err(reason) => return Err(CallError::Unavailable(self.name.to_owned(), reason)),
        };

        let params = CallToolRequestParams::new(tool.to_owned()).with_arguments(arguments);

        match tokio::time::timeout(self.call_timeout, peer.call_tool(params)).await {
            Ok(Ok(result)) => Ok(render::answer(result)),
            Ok(Err(error)) => {
                // A broken connection is replaced on the next call.
                *self.client.lock().await = None;
                Err(CallError::Unknown(self.name.to_owned(), error.to_string()))
            }
            Err(_) => Err(CallError::Unknown(
                self.name.to_owned(),
                format!("no answer within {} s", self.call_timeout.as_secs()),
            )),
        }
    }

    /// The running server's peer, starting it if needed.
    async fn peer(&self) -> Result<Peer<RoleClient>, String> {
        let mut client = self.client.lock().await;

        if let Some(running) = client.as_ref().filter(|running| !running.is_closed()) {
            return Ok(running.peer().clone());
        }

        let Some(launch) = &self.launch else {
            return Err("it stopped and cannot be restarted".to_owned());
        };

        let (running, tools) = match tokio::time::timeout(launch.startup, start(&self.name, launch)).await {
            Ok(started) => started?,
            Err(_) => return Err(format!("it did not start within {} s", launch.startup.as_secs())),
        };

        tracing::info!(server = %self.name, tools = tools.len(), "MCP server started");
        *self.tools.lock().await = tools;
        let peer = running.peer().clone();
        *client = Some(running);

        Ok(peer)
    }
}

/// Spawns the server, connects, and lists its tools.
async fn start(name: &str, launch: &Launch) -> Result<(Client, Vec<Tool>), String> {
    let mut command = Command::new(&launch.program);
    command.args(&launch.args).env_clear().envs(launch.env.iter().map(|(key, value)| (key, value))).kill_on_drop(true);

    let (transport, stderr) = match TokioChildProcess::builder(command).stderr(Stdio::piped()).spawn() {
        Ok(spawned) => spawned,
        Err(error) => return Err(format!("cannot run {}: {error}", launch.program)),
    };

    if let Some(stderr) = stderr {
        tokio::spawn(log_stderr(name.to_owned(), stderr));
    }

    let client = match ().serve(transport).await {
        Ok(client) => client,
        Err(error) => return Err(format!("it did not complete the MCP handshake: {error}")),
    };

    match client.list_all_tools().await {
        Ok(tools) => Ok((client, tools.into_iter().map(render::tool).collect())),
        Err(error) => Err(format!("it did not list its tools: {error}")),
    }
}

/// A server's diagnostics go to Shade's log, line by line.
async fn log_stderr(server: String, stderr: ChildStderr) {
    let mut lines = BufReader::new(stderr).lines();

    while let Ok(Some(line)) = lines.next_line().await {
        tracing::debug!(%server, "{line}");
    }
}

/// Every configured server.
#[derive(Default)]
pub struct Servers {
    servers: Vec<Arc<Server>>,
}

impl Servers {
    pub fn new(servers: Vec<Server>) -> Self {
        Self { servers: servers.into_iter().map(Arc::new).collect() }
    }

    pub fn get(&self, name: &str) -> Option<&Arc<Server>> {
        self.servers.iter().find(|server| server.name == name)
    }

    /// Starts every server at once. One that fails is logged, not fatal.
    pub async fn connect_all(&self) {
        let mut starting = tokio::task::JoinSet::new();

        for server in &self.servers {
            let server = Arc::clone(server);
            starting.spawn(async move { (server.connect().await, server) });
        }

        while let Some(Ok((result, server))) = starting.join_next().await {
            if let Err(reason) = result {
                tracing::warn!(server = %server.name, %reason, "MCP server unavailable; its tools are not offered until it starts");
            }
        }
    }

    /// Tries servers that are down again, every minute, for as long as the daemon runs.
    pub async fn keep_connected(self: Arc<Self>) {
        loop {
            tokio::time::sleep(RETRY).await;

            for server in &self.servers {
                if server.client.lock().await.is_none()
                    && let Err(reason) = server.connect().await
                {
                    tracing::debug!(server = %server.name, %reason, "MCP server still unavailable");
                }
            }
        }
    }
}

#[cfg(feature = "testing")]
impl Server {
    /// A server already connected over `stream`, such as one end of a
    /// `tokio::io::duplex` whose other end an in-process server serves.
    pub async fn connected(name: &str, stream: tokio::io::DuplexStream, call_timeout: Duration) -> Result<Self, String> {
        let client = match ().serve(stream).await {
            Ok(client) => client,
            Err(error) => return Err(error.to_string()),
        };

        let tools = match client.list_all_tools().await {
            Ok(tools) => tools.into_iter().map(render::tool).collect(),
            Err(error) => return Err(error.to_string()),
        };

        let server = Self::with(name.to_owned(), None, call_timeout);
        *server.client.lock().await = Some(client);
        *server.tools.lock().await = tools;

        Ok(server)
    }
}
