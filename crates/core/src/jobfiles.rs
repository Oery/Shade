//! Job files: recurring jobs as TOML files in `jobs/` next to the config.
//!
//! The file stem is the job's ID. Files are read at startup, validated like
//! the config (a bad file stops the daemon), and copied into SQLite, which
//! keeps each job's runs, next occurrence and pause state. Owner chat edits
//! them through the schedule tools: the file is written first, atomically,
//! then SQLite; at startup the files win, so a crash between the two
//! resolves to the file. One lock covers every change to a job file.
//!
//! The config directory is hidden from the sandbox, so only Shade's tools
//! and the owner change these files.

use std::fs;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex, MutexGuard};

use crate::notify;
use crate::policy::{self, Cap, Caps, Policy};
use crate::schedule::{self, TriggerArgs};
use crate::store::schedules::{Definition, Job};

/// The longest job file name, without `.toml`.
const MAX_NAME: usize = 40;

/// One job, as the owner writes it.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct JobFile {
    title: String,
    /// Exactly one of `cron` and `at`.
    #[serde(skip_serializing_if = "Option::is_none")]
    cron: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    timezone: Option<String>,
    caps: Vec<String>,
    /// Where runs may post, the first by default. The owner's DMs when absent.
    #[serde(default)]
    channels: Vec<String>,
    prompt: String,
}

pub struct JobFiles {
    dir: PathBuf,
    lock: Mutex<()>,
}

impl JobFiles {
    pub fn new(dir: PathBuf) -> Self {
        Self { dir, lock: Mutex::new(()) }
    }

    /// Held across a file write and its SQLite write.
    pub async fn lock(&self) -> MutexGuard<'_, ()> {
        self.lock.lock().await
    }

    pub fn path(&self, id: &str) -> PathBuf {
        self.dir.join(format!("{id}.toml"))
    }

    /// Writes the job's file through a temporary file and a rename, so it is never half written.
    pub fn write(&self, id: &str, definition: &Definition) -> Result<(), String> {
        let text = match toml::to_string(&to_file(definition)) {
            Ok(text) => text,
            Err(error) => return Err(format!("cannot write job {id} as TOML: {error}")),
        };

        let partial = self.dir.join(format!(".{id}.toml.partial"));

        if let Err(error) = write_atomically(&self.dir, &partial, &self.path(id), &text) {
            return Err(format!("cannot write {}: {error}", self.path(id).display()));
        }

        Ok(())
    }

    pub fn remove(&self, id: &str) -> Result<(), String> {
        match fs::remove_file(self.path(id)) {
            Err(error) if error.kind() != ErrorKind::NotFound => Err(format!("cannot remove {}: {error}", self.path(id).display())),
            _ => Ok(()),
        }
    }

    /// Every job file, validated, as the definitions SQLite should hold.
    /// A job whose timing is unchanged keeps its next occurrence and pause
    /// state from `stored`; changed or new timing counts from `now`.
    pub fn load(&self, policy: &Policy, stored: &[Job], now: DateTime<Utc>) -> Result<Vec<(String, Definition)>, String> {
        let entries = match fs::read_dir(&self.dir) {
            Ok(entries) => entries,
            Err(error) if error.kind() == ErrorKind::NotFound => return Ok(Vec::new()),
            Err(error) => return Err(format!("cannot read {}: {error}", self.dir.display())),
        };

        let mut jobs = Vec::new();

        for entry in entries {
            let path = match entry {
                Ok(entry) => entry.path(),
                Err(error) => return Err(format!("cannot read {}: {error}", self.dir.display())),
            };

            if let Some(job) = load_file(&path, policy, stored, now)? {
                jobs.push(job);
            }
        }

        Ok(jobs)
    }
}

/// `jobs/` next to the config file.
pub fn dir_for(config: &Path) -> PathBuf {
    match config.parent() {
        Some(dir) => dir.join("jobs"),
        None => PathBuf::from("jobs"),
    }
}

/// A job file name: a short lowercase word, which becomes the job's ID.
pub fn check_name(name: &str) -> Result<(), String> {
    let word = name.chars().all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-' || c == '_');

    if name.is_empty() || name.len() > MAX_NAME || !word || name.starts_with(['-', '_']) {
        return Err(format!("`{name}` is not a job file name: use up to {MAX_NAME} lowercase letters, digits, - and _"));
    }

    Ok(())
}

/// `None` for anything that is not a `.toml` job file (editor leftovers, partial writes).
fn load_file(path: &Path, policy: &Policy, stored: &[Job], now: DateTime<Utc>) -> Result<Option<(String, Definition)>, String> {
    let (Some(id), Some("toml")) = (path.file_stem().and_then(|stem| stem.to_str()), path.extension().and_then(|ext| ext.to_str())) else {
        return Ok(None);
    };

    if id.starts_with('.') {
        return Ok(None);
    }

    match read_job(path, id, policy, stored, now) {
        Ok(definition) => Ok(Some((id.to_owned(), definition))),
        Err(message) => Err(format!("{}: {message}", path.display())),
    }
}

fn read_job(path: &Path, id: &str, policy: &Policy, stored: &[Job], now: DateTime<Utc>) -> Result<Definition, String> {
    check_name(id)?;

    let text = match fs::read_to_string(path) {
        Ok(text) => text,
        Err(error) => return Err(error.to_string()),
    };

    let file: JobFile = match toml::from_str(&text) {
        Ok(file) => file,
        Err(error) => return Err(error.to_string()),
    };

    let previous = stored.iter().find(|job| job.id == id);

    if previous.is_some_and(|job| !job.definition.file) {
        return Err(format!("a job created in chat already has the ID {id}; rename the file"));
    }

    definition(file, policy, previous.map(|job| &job.definition), now)
}

fn definition(file: JobFile, policy: &Policy, previous: Option<&Definition>, now: DateTime<Utc>) -> Result<Definition, String> {
    let trigger = match (file.cron, file.at) {
        (Some(expression), None) => TriggerArgs::Cron { expression, timezone: file.timezone },
        (None, Some(at)) => TriggerArgs::Once { at, timezone: file.timezone },
        _ => return Err("give exactly one of `cron` and `at`".to_owned()),
    };

    let mut channels = Vec::new();

    for written in if file.channels.is_empty() {
        vec!["dm".to_owned()]
    } else {
        file.channels
    } {
        let (stored, _) = notify::channel(&written, &policy.channels)?;

        if !channels.contains(&stored) {
            channels.push(stored);
        }
    }

    let mut definition = Definition {
        title: file.title,
        kind: String::new(),
        spec: String::new(),
        timezone: String::new(),
        prompt: file.prompt,
        caps: file_caps(file.caps, &policy.chat)?,
        channels,
        file: true,
        enabled: true,
        next_at: None,
    };

    let (kind, spec, timezone) = trigger_parts(&trigger, policy);

    // Unchanged timing keeps the job's state; a one-shot job that already ran stays done.
    if let Some(previous) =
        previous.filter(|previous| (previous.kind.as_str(), &previous.spec, &previous.timezone) == (kind, &spec, &timezone))
    {
        definition.kind = previous.kind.to_owned();
        definition.spec = spec;
        definition.timezone = timezone;
        definition.enabled = previous.enabled;
        definition.next_at = previous.next_at;

        return Ok(definition);
    }

    let timing = schedule::timing(trigger, policy.timezone, now)?;
    definition.kind = timing.kind.to_owned();
    definition.spec = timing.spec;
    definition.timezone = timing.timezone.name().to_owned();
    definition.next_at = Some(timing.next_at.timestamp());

    // New timing counts from now, but a job the owner paused stays paused.
    // (A one-shot job that ran has no next occurrence: it is done, not paused.)
    definition.enabled = !previous.is_some_and(|previous| !previous.enabled && previous.next_at.is_some());

    Ok(definition)
}

/// How a trigger will be stored, to compare with the stored job without re-validating it.
fn trigger_parts(trigger: &TriggerArgs, policy: &Policy) -> (&'static str, String, String) {
    let default = policy.timezone.name().to_owned();

    match trigger {
        TriggerArgs::Cron { expression, timezone } => ("cron", expression.to_owned(), timezone.as_deref().map_or(default, str::to_owned)),
        TriggerArgs::Once { at, timezone } => ("once", at.to_owned(), timezone.as_deref().map_or(default, str::to_owned)),
    }
}

/// A file may give a job anything owner chat holds, except management.
fn file_caps(names: Vec<String>, chat: &Caps) -> Result<Vec<String>, String> {
    let allowed = policy::delegable(chat);
    let mut caps = Caps::new();

    for name in names {
        let cap = Cap::try_from(name)?;

        if !allowed.contains(&cap) {
            return Err(format!("`{}` is not in [chat] caps, or cannot be given to a job", cap.name()));
        }

        caps.insert(cap);
    }

    if caps.contains(&Cap::ShellNetwork) && !caps.contains(&Cap::Shell) {
        return Err("shell_network requires shell".to_owned());
    }

    Ok(caps.into_iter().map(|cap| cap.name().to_owned()).collect())
}

fn write_atomically(dir: &Path, partial: &Path, path: &Path, text: &str) -> std::io::Result<()> {
    fs::create_dir_all(dir)?;
    fs::write(partial, text)?;

    fs::rename(partial, path)
}

fn to_file(definition: &Definition) -> JobFile {
    let (cron, at) = match definition.kind.as_str() {
        "cron" => (Some(definition.spec.to_owned()), None),
        _ => (None, Some(definition.spec.to_owned())),
    };

    JobFile {
        title: definition.title.to_owned(),
        cron,
        at,
        timezone: Some(definition.timezone.to_owned()),
        caps: definition.caps.to_owned(),
        channels: definition.channels.to_owned(),
        prompt: definition.prompt.to_owned(),
    }
}
