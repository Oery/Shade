//! Receipts: what Shade itself shows the owner when a job changes.
//!
//! A receipt is written by Shade, not the model, so it is short and exact.
//! The model only learns that the owner has seen it, which keeps its reply
//! from repeating the details. Adapters render the same card: an embed in
//! Discord, a few lines in the terminal.

use chrono::{DateTime, Datelike, Utc};
use chrono_tz::Tz;
use serde::{Deserialize, Serialize};

use crate::store::schedules::Definition;

/// Longest prompt excerpt on a card, in characters.
const PROMPT_EXCERPT: usize = 300;

#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
pub enum Verb {
    Created,
    Updated,
    Deleted,
    /// Waits for the owner: a new job, or a change to an existing one.
    Proposed {
        new: bool,
    },
}

/// One job change, as the owner sees it.
#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct Card {
    pub verb: Verb,
    pub title: String,
    /// "Every Wednesday at 09:00", or the cron expression when it has no plain wording.
    pub cadence: String,
    /// "tomorrow, 09:00"; `None` when the job will not run (paused, done, deleted).
    pub next: Option<String>,
    /// `dm`, channel names, or channel IDs.
    pub channels: Vec<String>,
    pub caps: Vec<String>,
    /// An excerpt of what each run is asked to do.
    pub prompt: String,
    pub paused: bool,
    /// Where a file job lives, such as `jobs/daily-brief.toml`.
    pub file: Option<String>,
}

impl Card {
    /// The card for `definition` at `now`. Times are shown in the job's zone,
    /// which is named only when it differs from `home`.
    pub fn new(verb: Verb, id: &str, definition: &Definition, home: Tz, now: DateTime<Utc>) -> Self {
        let zone = definition.timezone.parse::<Tz>().unwrap_or(home);
        let next = definition.next_at.and_then(|at| DateTime::from_timestamp(at, 0));

        Self {
            verb,
            title: definition.title.to_owned(),
            cadence: cadence(definition, zone, home),
            next: next.filter(|_| definition.enabled).map(|next| friendly(next, zone, home, now)),
            channels: definition.channels.to_owned(),
            caps: definition.caps.to_owned(),
            prompt: excerpt(&definition.prompt),
            paused: !definition.enabled && definition.next_at.is_some(),
            file: definition.file.then(|| format!("jobs/{id}.toml")),
        }
    }

    /// The card as a few lines of text, for the terminal and the CLI.
    pub fn text(&self) -> String {
        let mut text = format!("{} \"{}\"", self.heading(), self.title);

        if self.verb == Verb::Deleted {
            return text;
        }

        text.push_str(&format!("\n   {} · next run: {}", self.cadence, self.next_run()));
        text.push_str(&format!("\n   posts to {} · can use {}", self.posts_to(), self.can_use()));

        if matches!(self.verb, Verb::Proposed { .. }) {
            text.push_str(&format!("\n   asks: {}", self.prompt));
        }

        if let Some(file) = &self.file {
            text.push_str(&format!("\n   saved in {file}"));
        }

        text
    }

    fn heading(&self) -> String {
        format!("{} {}", self.icon(), self.label())
    }

    pub fn icon(&self) -> &'static str {
        match self.verb {
            Verb::Created => "📅",
            Verb::Updated => "📝",
            Verb::Deleted => "🗑️",
            Verb::Proposed { .. } => "⏳",
        }
    }

    pub fn label(&self) -> &'static str {
        match self.verb {
            Verb::Created => "Scheduled",
            Verb::Updated => "Changed",
            Verb::Deleted => "Deleted",
            Verb::Proposed { new: true } => "New job, waiting for your approval:",
            Verb::Proposed { new: false } => "Change waiting for your approval:",
        }
    }

    /// "tomorrow, 09:00", "paused" or "done".
    pub fn next_run(&self) -> String {
        match (&self.next, self.paused) {
            (Some(next), _) => next.to_owned(),
            (None, true) => "paused".to_owned(),
            (None, false) => "done".to_owned(),
        }
    }

    pub fn posts_to(&self) -> String {
        let places: Vec<String> = self.channels.iter().map(|channel| place(channel)).collect();

        places.join(", ")
    }

    pub fn can_use(&self) -> String {
        if self.caps.is_empty() {
            "nothing".to_owned()
        } else {
            self.caps.join(", ")
        }
    }
}

fn place(channel: &str) -> String {
    match channel {
        "dm" => "your DMs".to_owned(),
        id if id.chars().all(|c| c.is_ascii_digit()) => format!("<#{id}>"),
        name => format!("#{name}"),
    }
}

fn excerpt(prompt: &str) -> String {
    let flat = prompt.split_whitespace().collect::<Vec<_>>().join(" ");

    if flat.chars().count() <= PROMPT_EXCERPT {
        return flat;
    }

    let cut: String = flat.chars().take(PROMPT_EXCERPT).collect();

    format!("{}…", cut.trim_end())
}

/// When a job runs, in words where the pattern has common wording.
fn cadence(definition: &Definition, zone: Tz, home: Tz) -> String {
    let suffix = if zone == home {
        String::new()
    } else {
        format!(" ({})", zone.name())
    };

    if definition.kind == "once" {
        return match definition.next_at.and_then(|at| DateTime::from_timestamp(at, 0)) {
            Some(at) => format!("Once, {}{suffix}", at.with_timezone(&zone).format("%a %-d %b at %H:%M")),
            None => format!("Once, {}", definition.spec),
        };
    }

    match plain_cron(&definition.spec) {
        Some(words) => format!("{words}{suffix}"),
        None => format!("cron `{}`{suffix}", definition.spec),
    }
}

/// Common cron patterns in words. `None` for anything else, which is shown as it is.
pub fn plain_cron(expression: &str) -> Option<String> {
    let fields: Vec<&str> = expression.split_whitespace().collect();
    let [minute, hour, day, month, weekday] = fields.as_slice() else {
        return None;
    };

    if *month != "*" {
        return None;
    }

    if let (Some(step), "*", "*", "*") = (minute.strip_prefix("*/"), *hour, *day, *weekday) {
        return Some(format!("Every {step} minutes"));
    }

    let minute: u32 = minute.parse().ok()?;

    if *hour == "*" && *day == "*" && *weekday == "*" {
        return Some(format!("Every hour at :{minute:02}"));
    }

    let hour: u32 = hour.parse().ok()?;
    let at = format!("{hour:02}:{minute:02}");

    match (*day, *weekday) {
        ("*", "*") => Some(format!("Every day at {at}")),
        ("*", weekdays) => Some(format!("{} at {at}", days(weekdays)?)),
        (day, "*") => Some(format!("Every month on the {} at {at}", ordinal(day.parse().ok()?))),
        _ => None,
    }
}

/// "Weekdays", "Every Wednesday", "Mondays and Thursdays".
fn days(weekdays: &str) -> Option<String> {
    match weekdays.to_ascii_uppercase().as_str() {
        "1-5" | "MON-FRI" => return Some("Weekdays".to_owned()),
        "0,6" | "6,0" | "6,7" | "SAT,SUN" | "SUN,SAT" => return Some("Weekends".to_owned()),
        _ => {}
    }

    let mut names = Vec::new();

    for day in weekdays.split(',') {
        names.push(day_name(day)?);
    }

    match names.as_slice() {
        [one] => Some(format!("Every {one}")),
        [rest @ .., last] => Some(format!("{}s and {last}s", rest.join("s, "))),
        [] => None,
    }
}

fn day_name(day: &str) -> Option<&'static str> {
    let names = ["Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"];

    let index = match day.parse::<usize>() {
        Ok(7) => 0,
        Ok(number) => number,
        Err(_) => names.iter().position(|name| name[..3].eq_ignore_ascii_case(day))?,
    };

    names.get(index).copied()
}

fn ordinal(day: u32) -> String {
    let suffix = match (day % 10, day % 100) {
        (1, 11) | (2, 12) | (3, 13) => "th",
        (1, _) => "st",
        (2, _) => "nd",
        (3, _) => "rd",
        _ => "th",
    };

    format!("{day}{suffix}")
}

/// "today, 09:00", "tomorrow, 09:00", or "Wed 11 Nov, 09:00", in `zone`.
fn friendly(instant: DateTime<Utc>, zone: Tz, home: Tz, now: DateTime<Utc>) -> String {
    let local = instant.with_timezone(&zone);
    let today = now.with_timezone(&zone).date_naive();
    let suffix = if zone == home {
        String::new()
    } else {
        format!(" {}", local.format("%Z"))
    };

    let day = match local.date_naive().num_days_from_ce() - today.num_days_from_ce() {
        0 => "today".to_owned(),
        1 => "tomorrow".to_owned(),
        _ => local.format("%a %-d %b").to_string(),
    };

    format!("{day}, {}{suffix}", local.format("%H:%M"))
}

#[cfg(test)]
mod tests {
    use super::plain_cron;

    /// Common patterns read as words; anything else stays a cron expression.
    #[test]
    fn cron_patterns_in_words() {
        let cases = [
            ("0 9 * * 3", Some("Every Wednesday at 09:00")),
            ("30 8 * * 1-5", Some("Weekdays at 08:30")),
            ("0 10 * * SAT,SUN", Some("Weekends at 10:00")),
            ("0 18 * * 1,4", Some("Mondays and Thursdays at 18:00")),
            ("0 7 * * 7", Some("Every Sunday at 07:00")),
            ("5 * * * *", Some("Every hour at :05")),
            ("*/15 * * * *", Some("Every 15 minutes")),
            ("0 9 1 * *", Some("Every month on the 1st at 09:00")),
            ("0 9 22 * *", Some("Every month on the 22nd at 09:00")),
            // Months, ranges of hours and mixed day rules keep their cron form.
            ("0 9 * 6 *", None),
            ("0 9-17 * * *", None),
            ("0 9 1 * 1", None),
        ];

        for (expression, expected) in cases {
            assert_eq!(plain_cron(expression).as_deref(), expected, "{expression}");
        }
    }
}
