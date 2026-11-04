//! Bubblewrap sandbox for shell commands.
//!
//! Each command starts in an empty filesystem with the system read-only, the
//! workspace read-write at [`WORKSPACE`], and the vault at [`VAULT`] only
//! when the caller's grant allows it. Nothing else from the host is mounted:
//! no home, no Shade config or data directory, no `/run/user` sockets. The
//! environment is cleared, and the network is unshared unless asked for.
//!
//! Every command gets its own PID namespace. When the command exits, times
//! out, or the daemon dies, the namespace's init dies and the kernel kills
//! every process left in it, including ones that called `setsid`. The
//! kernel tracks the parent per thread, so commands must be started from a
//! thread that lives as long as the daemon, such as the one running Shade's core.

pub mod output;
pub mod paths;
#[cfg(test)]
mod tests;

use std::ffi::{OsStr, OsString};
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::process::{ExitStatus, Stdio};
use std::time::Duration;

use serde::Deserialize;
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio::process::{Child, Command};

use crate::output::{KEEP, Output};
use crate::paths::Protected;

/// Where the workspace appears inside the sandbox.
pub const WORKSPACE: &str = "/workspace";

/// Where the vault appears inside the sandbox.
pub const VAULT: &str = "/vault";

/// System fallback for `PATH`. Resolved Nix entries from the daemon come first.
const PATH: &str = "/run/current-system/sw/bin:/usr/local/bin:/usr/bin:/bin";

/// System locations mounted read-only. A symlink is recreated as one, so
/// merged-`/usr` layouts (`/bin -> usr/bin`) and NixOS's
/// `/run/current-system` resolve inside the sandbox as they do outside.
const SYSTEM: [&str; 8] = ["/usr", "/bin", "/sbin", "/lib", "/lib32", "/lib64", "/nix/store", "/run/current-system"];

/// Host configuration mounted read-only, following symlinks, so that
/// `/etc/resolv.conf` pointing into `/run` still works.
const ETC: [&str; 13] = [
    "/etc/resolv.conf",
    "/etc/hosts",
    "/etc/nsswitch.conf",
    "/etc/passwd",
    "/etc/group",
    "/etc/localtime",
    "/etc/ld.so.cache",
    "/etc/ld.so.conf",
    "/etc/ld.so.conf.d",
    "/etc/ssl",
    "/etc/ca-certificates",
    "/etc/pki",
    "/etc/static",
];

/// Written by the startup probe to prove the workspace mount works.
const PROBE_FILE: &str = ".shade-probe";

/// Longest time the startup probe may take.
const PROBE_TIMEOUT: Duration = Duration::from_secs(10);

/// The `[sandbox]` config section.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// Host directory mounted read-write at `/workspace`, the shell's working directory.
    pub workspace: PathBuf,
    /// Host directory mounted read-write at `/vault`, only for roots granted `vault`.
    pub vault: Option<PathBuf>,
    /// Variables copied from the daemon's environment. Everything else is cleared.
    #[serde(default)]
    pub env: Vec<String>,
    #[serde(default)]
    pub isolation: Isolation,
    /// The longest a command may run when it asks for more than the default 30 seconds.
    #[serde(default = "default_max_timeout")]
    pub max_timeout_seconds: u64,
}

fn default_max_timeout() -> u64 {
    300
}

#[derive(Clone, Copy, Debug, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Isolation {
    #[default]
    Bubblewrap,
    /// Development only: shell commands run on the host with the owner's full authority.
    Off,
}

#[derive(Debug, Error)]
pub enum SandboxError {
    #[error("[sandbox] path {path}: {source}")]
    Path { path: PathBuf, source: io::Error },

    #[error("{0}")]
    Exposed(String),

    #[error("bubblewrap (`bwrap`) is not on PATH; install it, or set `isolation = \"off\"` in [sandbox] for development only")]
    NoBubblewrap,

    #[error("cannot start the sandbox: {0}")]
    Spawn(io::Error),

    #[error("cannot wait for the sandbox: {0}")]
    Wait(io::Error),

    #[error("the sandbox does not work on this system: {0}")]
    Broken(String),
}

/// How commands are started.
pub enum Runner {
    /// Inside bubblewrap, at this executable.
    Bubblewrap(PathBuf),
    /// Directly on the host, with the owner's full authority. A timeout
    /// kills only `sh`; descendants it detached keep running.
    Host,
}

/// What one command may reach beyond the workspace.
#[derive(Clone, Copy)]
pub struct Access {
    /// Host networking, from `shell_network`.
    pub network: bool,
    /// The vault mount, from the `vault` capability.
    pub vault: bool,
}

#[derive(Debug, PartialEq)]
pub enum Exit {
    Code(i32),
    /// Ended by a signal without an exit code.
    Killed,
    TimedOut,
}

/// A finished command and the ends of its output.
pub struct Finished {
    pub exit: Exit,
    pub stdout: Output,
    pub stderr: Output,
}

pub struct Sandbox {
    runner: Runner,
    workspace: PathBuf,
    vault: Option<PathBuf>,
    max_timeout: Duration,
    /// Arguments shared by every bubblewrap command: namespaces, system mounts, environment.
    base: Vec<OsString>,
    /// Variables copied from the daemon's environment when the sandbox was built.
    env: Vec<(String, OsString)>,
    /// Nix executables already visible through `/nix/store`, then system tools.
    path: OsString,
}

/// The sandbox for the daemon using `data_dir` and `config_path`, probed.
///
/// Fails closed: an exposed path or a sandbox that cannot actually run a
/// command is an error.
pub async fn start(config: Config, data_dir: &Path, config_path: &Path) -> Result<Sandbox, SandboxError> {
    let runner = match config.isolation {
        Isolation::Bubblewrap => Runner::Bubblewrap(find_bubblewrap()?),
        Isolation::Off => {
            tracing::warn!("[sandbox] isolation is off: shell commands run with your full authority");
            Runner::Host
        }
    };

    let protected = Protected::for_daemon(data_dir, config_path)?;
    let sandbox = Sandbox::new(runner, config, &protected)?;
    sandbox.probe().await?;

    Ok(sandbox)
}

impl Sandbox {
    /// Resolves and checks the configured paths against `protected`.
    pub fn new(runner: Runner, config: Config, protected: &Protected) -> Result<Self, SandboxError> {
        let workspace = paths::directory(config.workspace)?;

        let vault = match config.vault {
            Some(vault) => Some(paths::directory(vault)?),
            None => None,
        };

        if let Err(message) = paths::check(&workspace, vault.as_deref(), protected) {
            return Err(SandboxError::Exposed(message));
        }

        let env = copy_env(&config.env);
        let host_path = std::env::var_os("PATH").unwrap_or_default();
        let path = shell_path(&host_path);
        let base = base_args(&env, &path);

        let max_timeout = Duration::from_secs(config.max_timeout_seconds);

        Ok(Self { runner, workspace, vault, max_timeout, base, env, path })
    }

    /// The longest timeout a command may ask for.
    pub fn max_timeout(&self) -> Duration {
        self.max_timeout
    }

    /// Runs `script` with `sh -c`, killing every process it started once it
    /// exits or `timeout` passes.
    pub async fn run(&self, script: &str, access: Access, timeout: Duration) -> Result<Finished, SandboxError> {
        let mut child = match self.command(script, access).spawn() {
            Ok(child) => child,
            Err(source) => return Err(SandboxError::Spawn(source)),
        };

        let mut stdout = Output::new(KEEP);
        let mut stderr = Output::new(KEEP);
        let streams = (child.stdout.take(), child.stderr.take());

        let finished = tokio::time::timeout(timeout, async {
            let (_, _, status) = tokio::join!(retain(streams.0, &mut stdout), retain(streams.1, &mut stderr), child.wait());
            status
        })
        .await;

        let exit = match finished {
            Ok(Ok(status)) => exit_of(status),
            Ok(Err(source)) => return Err(SandboxError::Wait(source)),
            Err(_) => kill(&mut child).await?,
        };

        Ok(Finished { exit, stdout, stderr })
    }

    /// Runs a command that writes the workspace (and checks the vault is
    /// writable when one is configured), then checks the write on the host.
    /// Exit status alone would accept a `bwrap` that never ran anything.
    async fn probe(&self) -> Result<(), SandboxError> {
        let Runner::Bubblewrap(_) = self.runner else {
            return Ok(());
        };

        let mut script = format!("echo shade > {WORKSPACE}/{PROBE_FILE}");

        if self.vault.is_some() {
            script.push_str(&format!(" && test -w {VAULT}"));
        }

        let finished = self.run(&script, Access { network: true, vault: true }, PROBE_TIMEOUT).await?;
        let written = self.workspace.join(PROBE_FILE);
        let content = fs::read_to_string(&written).unwrap_or_default();
        let _ = fs::remove_file(&written);

        if finished.exit != Exit::Code(0) || content != "shade\n" {
            return Err(SandboxError::Broken(probe_failure(&finished)));
        }

        Ok(())
    }

    fn command(&self, script: &str, access: Access) -> Command {
        let mut command = match &self.runner {
            Runner::Bubblewrap(bwrap) => self.bubblewrap(bwrap, script, access),
            Runner::Host => self.host(script),
        };

        command.stdin(Stdio::null()).stdout(Stdio::piped()).stderr(Stdio::piped()).kill_on_drop(true);

        command
    }

    fn bubblewrap(&self, bwrap: &Path, script: &str, access: Access) -> Command {
        let mut command = Command::new(bwrap);
        command.env_clear().args(&self.base);

        if access.network {
            command.arg("--share-net");
        }

        command.arg("--bind").arg(&self.workspace).arg(WORKSPACE);

        if let (true, Some(vault)) = (access.vault, &self.vault) {
            command.arg("--bind").arg(vault).arg(VAULT);
        }

        command.args(["--chdir", WORKSPACE, "--", "/bin/sh", "-c", script]);

        command
    }

    /// No isolation: the opt-out runs `sh` in the workspace with the same clean environment.
    fn host(&self, script: &str) -> Command {
        let mut command = Command::new("/bin/sh");
        command.env_clear();

        for (name, value) in &self.env {
            command.env(name, value);
        }

        command.env("PATH", &self.path).env("HOME", &self.workspace).current_dir(&self.workspace);
        command.args(["-c", script]);

        command
    }
}

/// Namespaces, lifetime, system mounts, and environment. Mounts reflect the
/// host layout, which does not change while the daemon runs.
fn base_args(env: &[(String, OsString)], path: &OsStr) -> Vec<OsString> {
    let mut args = Vec::new();
    push(&mut args, ["--unshare-all", "--die-with-parent", "--new-session", "--clearenv"]);

    for path in SYSTEM {
        match fs::read_link(path) {
            Ok(target) => push(&mut args, [OsStr::new("--symlink"), target.as_os_str(), OsStr::new(path)]),
            Err(_) if Path::new(path).exists() => push(&mut args, ["--ro-bind", path, path]),
            Err(_) => {}
        }
    }

    for path in ETC {
        if Path::new(path).exists() {
            push(&mut args, ["--ro-bind", path, path]);
        }
    }

    push(&mut args, ["--dev", "/dev", "--proc", "/proc", "--tmpfs", "/tmp"]);

    for (name, value) in env {
        push(&mut args, [OsStr::new("--setenv"), OsStr::new(name), value.as_os_str()]);
    }

    // Set last so the configured variables cannot replace them.
    push(&mut args, [OsStr::new("--setenv"), OsStr::new("PATH"), path]);
    push(&mut args, ["--setenv", "HOME", WORKSPACE, "--setenv", "TMPDIR", "/tmp"]);

    args
}

/// Nix profiles can live outside the sandbox; their resolved store paths do not.
fn shell_path(host_path: &OsStr) -> OsString {
    let mut path = OsString::new();

    for entry in std::env::split_paths(host_path) {
        let Ok(resolved) = entry.canonicalize() else {
            continue;
        };

        if resolved.starts_with("/nix/store") && resolved.is_dir() {
            path.push(resolved);
            path.push(":");
        }
    }

    path.push(PATH);

    path
}

fn push<S: AsRef<OsStr>, const N: usize>(args: &mut Vec<OsString>, items: [S; N]) {
    for item in items {
        args.push(item.as_ref().to_owned());
    }
}

/// The allowed variables that are set in the daemon's environment.
fn copy_env(names: &[String]) -> Vec<(String, OsString)> {
    let mut env = Vec::new();

    for name in names {
        if let Some(value) = std::env::var_os(name) {
            env.push((name.to_owned(), value));
        }
    }

    env
}

fn find_bubblewrap() -> Result<PathBuf, SandboxError> {
    match find_on_path("bwrap") {
        Some(bwrap) => Ok(bwrap),
        None => Err(SandboxError::NoBubblewrap),
    }
}

pub fn find_on_path(name: &str) -> Option<PathBuf> {
    let path = std::env::var_os("PATH")?;

    for dir in std::env::split_paths(&path) {
        let candidate = dir.join(name);

        if candidate.is_file() {
            return Some(candidate);
        }
    }

    None
}

/// Reads `stream` to the end into a bounded capture.
async fn retain<R: AsyncRead + Unpin>(stream: Option<R>, kept: &mut Output) {
    let Some(mut stream) = stream else {
        return;
    };

    let mut chunk = [0; 8192];

    loop {
        let read = match stream.read(&mut chunk).await {
            Ok(0) | Err(_) => return,
            Ok(read) => read,
        };

        kept.push(&chunk[..read]);
    }
}

/// Kills bubblewrap, which takes the whole sandbox with it, and reaps it.
async fn kill(child: &mut Child) -> Result<Exit, SandboxError> {
    if let Err(source) = child.kill().await {
        return Err(SandboxError::Wait(source));
    }

    Ok(Exit::TimedOut)
}

fn exit_of(status: ExitStatus) -> Exit {
    match status.code() {
        Some(code) => Exit::Code(code),
        None => Exit::Killed,
    }
}

fn probe_failure(finished: &Finished) -> String {
    let output = format!("{}{}", finished.stderr.text().trim(), finished.stdout.text().trim());

    if output.is_empty() {
        format!("a test command ended with {:?} without writing the workspace", finished.exit)
    } else {
        output
    }
}
