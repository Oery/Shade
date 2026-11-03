//! Secrets from an optional `.env` file next to the config file.
//!
//! The file is parsed into a map, not into the process environment, so child
//! processes never inherit it. Variables already in the environment win.

use std::collections::HashMap;

use std::fs::File;
use std::io::ErrorKind;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};

use crate::Error;
use crate::redact::Redactor;

pub const FILE_NAME: &str = ".env";

pub struct Secrets {
    file: HashMap<String, String>,
    /// Every value handed out, so tool results can be cleaned of them.
    taken: Vec<String>,
}

impl Secrets {
    /// A missing file is fine. A file that group or others can access is refused.
    pub fn load(path: PathBuf) -> Result<Self, Error> {
        let file = match File::open(&path) {
            Ok(file) => file,
            Err(source) if source.kind() == ErrorKind::NotFound => return Ok(Self { file: HashMap::new(), taken: Vec::new() }),
            Err(source) => return Err(Error::SecretsRead { path, source: Box::new(source) }),
        };

        let mode = match file.metadata() {
            Ok(metadata) => metadata.permissions().mode(),
            Err(source) => return Err(Error::SecretsRead { path, source: Box::new(source) }),
        };

        if mode & 0o077 != 0 {
            return Err(Error::SecretsExposed { path });
        }

        let mut secrets = HashMap::new();

        for entry in dotenvy::from_read_iter(file) {
            match entry {
                Ok((name, value)) => secrets.insert(name, value),
                Err(source) => return Err(Error::SecretsRead { path, source: Box::new(source) }),
            };
        }

        Ok(Self { file: secrets, taken: Vec::new() })
    }

    /// Takes `name` from the environment, or else from the file.
    pub fn take(&mut self, name: &str) -> Option<String> {
        let value = match std::env::var(name) {
            Ok(value) => Some(value),
            Err(_) => self.file.remove(name),
        };

        self.taken.extend(value.iter().cloned());

        value
    }

    /// A redactor for every secret handed out so far: call it once all are taken.
    pub fn into_redactor(self) -> Redactor {
        Redactor::new(self.taken)
    }
}

/// The `.env` file that belongs to the config file at `config`.
pub fn path_for(config: &Path) -> PathBuf {
    config.with_file_name(FILE_NAME)
}
