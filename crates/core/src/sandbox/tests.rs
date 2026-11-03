//! Real bubblewrap runs. Each test gets its own directories under the system temp dir.

use std::fs;
use std::net::TcpListener;
use std::os::unix::net::UnixListener;
use std::path::PathBuf;
use std::time::Duration;

use crate::config::{Isolation, SandboxConfig};
use crate::sandbox::paths::Protected;
use crate::sandbox::{Access, Exit, Finished, Runner, Sandbox, SandboxError, find_on_path};

const NONE: Access = Access { network: false, vault: false };
const TIMEOUT: Duration = Duration::from_secs(10);

/// A workspace and a vault, removed when dropped.
struct Dirs {
    root: PathBuf,
    workspace: PathBuf,
    vault: PathBuf,
}

impl Dirs {
    fn new() -> Self {
        let root = std::env::temp_dir().join(format!("shade-sandbox-{}", uuid::Uuid::new_v4()));
        let workspace = root.join("workspace");
        let vault = root.join("vault");
        fs::create_dir_all(&workspace).unwrap();
        fs::create_dir_all(&vault).unwrap();

        Self { root, workspace, vault }
    }

    fn sandbox(&self, runner: Runner) -> Result<Sandbox, SandboxError> {
        let config = SandboxConfig {
            workspace: self.workspace.clone(),
            vault: Some(self.vault.clone()),
            env: Vec::new(),
            isolation: Isolation::Bubblewrap,
        };

        Sandbox::new(runner, config, &Protected { paths: Vec::new(), home: None })
    }

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
    String::from_utf8_lossy(&finished.stdout).into_owned()
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
    assert_eq!(finished.exit, Exit::Code(0), "{}", String::from_utf8_lossy(&finished.stderr));
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
    let detach = "setsid sh -c 'touch /workspace/started; sleep 1; touch /workspace/escaped' & sleep 0.3";

    let cases = [
        (format!("{detach}; exit 3"), TIMEOUT, Exit::Code(3)),
        (format!("{detach}; sleep 30"), Duration::from_millis(600), Exit::TimedOut),
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

    let config = SandboxConfig { workspace: link, vault: None, env: Vec::new(), isolation: Isolation::Bubblewrap };
    let protected = Protected { paths: vec![dirs.vault.canonicalize().unwrap()], home: None };
    let result = Sandbox::new(Runner::Host, config, &protected);

    assert!(matches!(result, Err(SandboxError::Exposed(_))), "a link into a protected path was accepted");
}
