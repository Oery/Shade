//! Real bubblewrap runs. Each test gets its own directories under the system temp dir.

use std::fs;
use std::net::TcpListener;
use std::os::unix::net::UnixListener;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use crate::paths::Protected;
use crate::{Access, Config, Exit, Finished, Isolation, Runner, Sandbox, SandboxError, find_on_path};

const NONE: Access = Access { network: false, vault: false };
const TIMEOUT: Duration = Duration::from_secs(10);

/// Starts a detached process that writes `escaped` unless it is killed within a second.
const DETACH: &str = "setsid sh -c 'touch /workspace/started; sleep 1; touch /workspace/escaped' & sleep 0.3";

/// Set for the helper process of `daemon_death_kills_the_sandbox`: the workspace to use.
const HOLDER_ENV: &str = "SHADE_TEST_SANDBOX_HOLDER";

/// A workspace and a vault under the system temp dir, removed when dropped.
struct Dirs {
    root: PathBuf,
    workspace: PathBuf,
    vault: PathBuf,
}

impl Dirs {
    fn new() -> Self {
        let root = std::env::temp_dir().join(format!("shade-test-{}", uuid::Uuid::new_v4()));
        let workspace = root.join("workspace");
        let vault = root.join("vault");
        fs::create_dir_all(&workspace).unwrap();
        fs::create_dir_all(&vault).unwrap();

        Self { root, workspace, vault }
    }

    fn sandbox(&self, runner: Runner) -> Result<Sandbox, SandboxError> {
        let config = Config {
            workspace: self.workspace.clone(),
            vault: Some(self.vault.clone()),
            env: Vec::new(),
            isolation: Isolation::Bubblewrap,
            max_timeout_seconds: 60,
        };

        Sandbox::new(runner, config, &Protected { paths: Vec::new(), home: None })
    }

    /// A real bubblewrap sandbox with the vault configured.
    fn bubblewrap(&self) -> Sandbox {
        self.sandbox(Runner::Bubblewrap(find_on_path("bwrap").unwrap())).unwrap()
    }
}

impl Drop for Dirs {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.root);
    }
}

fn stdout(finished: &Finished) -> String {
    finished.stdout.text()
}

#[test]
fn host_paths_outside_the_nix_store_are_not_inherited() -> Result<(), Box<dyn std::error::Error>> {
    let dirs = Dirs::new();
    let paths = std::env::join_paths([dirs.workspace.as_path(), Path::new("/usr/bin"), Path::new("/nonexistent/bin")])?;

    assert_eq!(super::shell_path(&paths), super::PATH);

    Ok(())
}

#[tokio::test]
async fn nix_profile_paths_resolve_without_exposing_the_profile_directory() -> Result<(), Box<dyn std::error::Error>> {
    // Only Nix hosts have the store mounted by the sandbox.
    if !Path::new("/nix/store").is_dir() {
        return Ok(());
    }

    let dirs = Dirs::new();
    let profile = dirs.root.join("profile");
    std::os::unix::fs::symlink("/nix/store", &profile)?;

    let mut sandbox = dirs.bubblewrap();
    sandbox.path = super::shell_path(profile.as_os_str());
    sandbox.base = super::base_args(&sandbox.env, &sandbox.path);

    let finished = sandbox.run("printf '%s' \"$PATH\"", NONE, TIMEOUT).await?;
    assert_eq!(finished.exit, Exit::Code(0), "{}", finished.stderr.text());
    assert_eq!(stdout(&finished), format!("/nix/store:{}", super::PATH));

    let script = format!("test ! -e '{}' && test -d /nix/store", profile.display());
    let finished = sandbox.run(&script, NONE, TIMEOUT).await?;
    assert_eq!(finished.exit, Exit::Code(0), "{}", finished.stderr.text());

    Ok(())
}

#[tokio::test]
async fn host_secrets_sockets_and_environment_are_hidden() {
    let dirs = Dirs::new();
    let data = dirs.root.join("data");
    fs::create_dir(&data).unwrap();
    fs::write(data.join(".env"), "API_KEY=secret").unwrap();
    let socket = shade_proto::socket_path(&data);
    let _listener = UnixListener::bind(&socket).unwrap();

    let mut hidden = vec![socket, data.join(".env"), PathBuf::from("/run/user"), PathBuf::from("/etc/shadow")];
    hidden.extend(shade_proto::data_dir());
    hidden.extend(std::env::var_os("HOME").map(PathBuf::from));

    let mut script = String::new();

    for path in &hidden {
        script.push_str(&format!("test -e '{}' && echo '{}'; ", path.display(), path.display()));
    }

    let finished = dirs.bubblewrap().run(&format!("{script}true"), NONE, TIMEOUT).await.unwrap();
    assert_eq!(finished.exit, Exit::Code(0), "{}", finished.stderr.text());
    assert_eq!(stdout(&finished), "", "visible inside the sandbox");

    let finished = dirs.bubblewrap().run("env | cut -d= -f1", NONE, TIMEOUT).await.unwrap();
    let allowed = ["PATH", "HOME", "TMPDIR", "PWD", "OLDPWD", "SHLVL", "_"];

    for name in stdout(&finished).lines() {
        assert!(allowed.contains(&name), "`{name}` leaked into the sandbox");
    }
}

#[tokio::test]
async fn vault_is_mounted_only_when_granted() {
    for granted in [false, true] {
        let dirs = Dirs::new();
        let access = Access { network: false, vault: granted };
        let finished = dirs.bubblewrap().run("echo note > /vault/note", access, TIMEOUT).await.unwrap();

        assert_eq!(finished.exit == Exit::Code(0), granted, "vault granted: {granted}");
        assert_eq!(dirs.vault.join("note").exists(), granted, "vault granted: {granted}");
    }
}

#[tokio::test]
async fn host_processes_are_invisible() {
    let dirs = Dirs::new();
    let finished = dirs.bubblewrap().run("ls /proc | grep -c '^[0-9]'", NONE, TIMEOUT).await.unwrap();
    let count: u32 = stdout(&finished).trim().parse().unwrap();

    // Bubblewrap's init, sh, ls, and grep.
    assert!(count <= 4, "{count} processes visible");
}

#[tokio::test]
async fn network_needs_shell_network() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let script = format!("exec bash -c 'echo > /dev/tcp/127.0.0.1/{port}'");

    for network in [false, true] {
        let dirs = Dirs::new();
        let access = Access { network, vault: false };
        let finished = dirs.bubblewrap().run(&script, access, TIMEOUT).await.unwrap();

        assert_eq!(finished.exit == Exit::Code(0), network, "network: {network}");
    }
}

#[tokio::test]
async fn detached_descendants_die_with_the_command() {
    let cases = [
        (format!("{DETACH}; exit 3"), TIMEOUT, Exit::Code(3)),
        (format!("{DETACH}; sleep 30"), Duration::from_millis(600), Exit::TimedOut),
    ];

    for (script, timeout, exit) in cases {
        let dirs = Dirs::new();
        let finished = dirs.bubblewrap().run(&script, NONE, timeout).await.unwrap();
        assert_eq!(finished.exit, exit);

        tokio::time::sleep(Duration::from_millis(1500)).await;
        assert!(dirs.workspace.join("started").exists(), "the detached process never ran");
        assert!(!dirs.workspace.join("escaped").exists(), "a detached process outlived {exit:?}");
    }
}

#[tokio::test]
async fn broken_sandbox_fails_the_probe() {
    // `true` exits 0 without running anything, so exit status alone is not enough.
    let runners = [PathBuf::from("/nonexistent/bwrap"), find_on_path("false").unwrap(), find_on_path("true").unwrap()];

    for bwrap in runners {
        let dirs = Dirs::new();
        let sandbox = dirs.sandbox(Runner::Bubblewrap(bwrap.clone())).unwrap();

        assert!(sandbox.probe().await.is_err(), "{} passed the probe", bwrap.display());
    }

    let dirs = Dirs::new();
    dirs.bubblewrap().probe().await.unwrap();
    assert!(!dirs.workspace.join(super::PROBE_FILE).exists(), "the probe left its file behind");
}

#[test]
fn symlinked_workspace_is_resolved() {
    let dirs = Dirs::new();
    let link = dirs.root.join("link");
    std::os::unix::fs::symlink(&dirs.vault, &link).unwrap();

    let config = Config { workspace: link, vault: None, env: Vec::new(), isolation: Isolation::Bubblewrap, max_timeout_seconds: 60 };
    let protected = Protected { paths: vec![dirs.vault.canonicalize().unwrap()], home: None };
    let result = Sandbox::new(Runner::Host, config, &protected);

    assert!(matches!(result, Err(SandboxError::Exposed(_))), "a link into a protected path was accepted");
}

/// The daemon dies abruptly (SIGKILL) while a command runs: nothing it started survives.
#[test]
fn daemon_death_kills_the_sandbox() {
    let dirs = Dirs::new();

    let mut holder = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "tests::sandbox_holder", "--ignored"])
        .env(HOLDER_ENV, &dirs.workspace)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();

    let deadline = Instant::now() + TIMEOUT;

    while !dirs.workspace.join("started").exists() {
        assert!(Instant::now() < deadline, "the holder never started its command");
        std::thread::sleep(Duration::from_millis(20));
    }

    holder.kill().unwrap();
    holder.wait().unwrap();
    std::thread::sleep(Duration::from_millis(1500));

    assert!(!dirs.workspace.join("escaped").exists(), "a sandboxed process outlived the daemon");
}

/// Stands in for the daemon in `daemon_death_kills_the_sandbox`; does nothing when run directly.
#[tokio::test]
#[ignore = "helper process for daemon_death_kills_the_sandbox"]
async fn sandbox_holder() {
    let Some(workspace) = std::env::var_os(HOLDER_ENV) else {
        return;
    };

    let config = Config {
        workspace: PathBuf::from(workspace),
        vault: None,
        env: Vec::new(),
        isolation: Isolation::Bubblewrap,
        max_timeout_seconds: 60,
    };
    let runner = Runner::Bubblewrap(find_on_path("bwrap").unwrap());
    let sandbox = Sandbox::new(runner, config, &Protected { paths: Vec::new(), home: None }).unwrap();

    let _ = sandbox.run(&format!("{DETACH}; sleep 30"), NONE, Duration::from_secs(60)).await;
}
