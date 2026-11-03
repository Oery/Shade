//! Which host directories the shell may be given.
//!
//! The workspace and vault are mounted read-write, so they must not reach
//! anything that controls Shade: its data directory (database and control
//! socket), its config directory (config and `.env`), or the directory
//! holding its executables. Neither may contain the home directory, which
//! holds SSH keys and browser profiles.

use std::io;
use std::path::{Path, PathBuf};

use crate::SandboxError;

/// Host paths the shell must never reach, all canonical.
pub struct Protected {
    /// Off limits in both directions: no mount inside them, none containing them.
    pub paths: Vec<PathBuf>,
    /// May contain a mount, but no mount may contain it.
    pub home: Option<PathBuf>,
}

impl Protected {
    pub fn for_daemon(data_dir: &Path, config_path: &Path) -> Result<Self, SandboxError> {
        let config = canonical(config_path)?;
        let exe = match std::env::current_exe() {
            Ok(exe) => canonical(&exe)?,
            Err(source) => return Err(SandboxError::Path { path: PathBuf::from("/proc/self/exe"), source }),
        };

        let mut paths = vec![canonical(data_dir)?];
        paths.extend(config.parent().map(Path::to_path_buf));
        paths.extend(exe.parent().map(Path::to_path_buf));

        let home = match std::env::var_os("HOME") {
            Some(home) => Some(canonical(Path::new(&home))?),
            None => None,
        };

        Ok(Self { paths, home })
    }
}

/// The canonical form of a configured directory. Symlinks are resolved so a
/// link cannot point the workspace at a protected path.
pub fn directory(path: PathBuf) -> Result<PathBuf, SandboxError> {
    let resolved = canonical(&path)?;

    if !resolved.is_dir() {
        return Err(SandboxError::Path { path, source: io::Error::from(io::ErrorKind::NotADirectory) });
    }

    Ok(resolved)
}

fn canonical(path: &Path) -> Result<PathBuf, SandboxError> {
    match path.canonicalize() {
        Ok(path) => Ok(path),
        Err(source) => Err(SandboxError::Path { path: path.to_owned(), source }),
    }
}

/// Checks canonical mount sources against `protected` and each other.
pub fn check(workspace: &Path, vault: Option<&Path>, protected: &Protected) -> Result<(), String> {
    check_one("workspace", workspace, protected)?;

    let Some(vault) = vault else {
        return Ok(());
    };

    check_one("vault", vault, protected)?;

    // A vault inside the workspace would be reachable without `vault`.
    if overlaps(workspace, vault) {
        return Err(format!(
            "[sandbox] workspace {} and vault {} overlap; they must be separate directories",
            workspace.display(),
            vault.display()
        ));
    }

    Ok(())
}

fn check_one(name: &str, path: &Path, protected: &Protected) -> Result<(), String> {
    for protected in &protected.paths {
        if overlaps(path, protected) {
            return Err(format!(
                "[sandbox] {name} {} overlaps {}, which the shell must not reach",
                path.display(),
                protected.display()
            ));
        }
    }

    if let Some(home) = &protected.home
        && home.starts_with(path)
    {
        return Err(format!("[sandbox] {name} {} contains the home directory {}", path.display(), home.display()));
    }

    Ok(())
}

/// Whole path components, so `/data/shade-work` does not overlap `/data/shade`.
fn overlaps(a: &Path, b: &Path) -> bool {
    a.starts_with(b) || b.starts_with(a)
}

#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};

    use super::{Protected, check};

    #[test]
    fn mounts_never_reach_protected_paths() {
        let protected = Protected {
            paths: ["/home/me/.local/share/shade", "/home/me/.config/shade", "/home/me/.cargo/bin"].map(PathBuf::from).to_vec(),
            home: Some(PathBuf::from("/home/me")),
        };

        let cases = [
            ("/home/me/work", None, true),
            ("/home/me/.local/share/shade/work", None, false),
            ("/home/me/.config/shade", None, false),
            ("/home/me/.local", None, false),
            ("/home/me/.cargo", None, false),
            // A shared name prefix is not containment.
            ("/home/me/.local/share/shade-work", None, true),
            ("/home/me", None, false),
            ("/home", None, false),
            ("/home/me/work", Some("/home/me/notes"), true),
            ("/home/me/work", Some("/home/me/work/notes"), false),
            ("/home/me/work/inner", Some("/home/me/work"), false),
            ("/home/me/work", Some("/home/me/.config/shade/vault"), false),
            ("/home/me/work", Some("/"), false),
        ];

        for (workspace, vault, allowed) in cases {
            let result = check(Path::new(workspace), vault.map(Path::new), &protected);

            assert_eq!(result.is_ok(), allowed, "workspace {workspace}, vault {vault:?}: {result:?}");
        }
    }
}
