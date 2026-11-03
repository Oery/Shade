//! Schedule timing and the text a scheduled run starts with.
//!
//! Instants are stored in UTC; a cron job keeps its IANA time zone for the
//! next occurrence. `croner` moves a time inside a spring-forward gap to
//! after the gap and runs wildcard patterns in both passes of a fall-back
//! hour. Shade's policy differs, so its candidates are filtered: a time
//! that no longer matches the pattern is skipped, and so is the second pass
//! of a repeated local time.

use chrono::{DateTime, LocalResult, NaiveDateTime, TimeZone, Utc};
use chrono_tz::Tz;
use croner::Cron;
use croner::parser::{CronParser, Seconds, Year};
use serde::Deserialize;

use crate::store::schedules::{Definition, Job, Waiting};

/// How long a proposed change may wait for approval, in seconds.
pub const APPROVAL_WINDOW: i64 = 24 * 3600;

/// Candidates examined before deciding a pattern never runs (it only
/// matches inside DST gaps, or on a date that does not exist).
const MAX_CANDIDATES: usize = 1000;

/// When a stored job runs.
pub enum Trigger {
    Cron {
        cron: Box<Cron>,
        timezone: Tz,
    },
    /// A one-shot job: its single instant is its `next_at`.
    Once,
}

impl Trigger {
    /// Reads a stored definition. Rows are written by Shade, so an error means a damaged row.
    pub fn parse(kind: &str, spec: &str, timezone: &str) -> Result<Self, String> {
        let Ok(timezone) = timezone.parse::<Tz>() else {
            return Err(format!("unknown time zone `{timezone}`"));
        };

        match kind {
            "cron" => match parse_cron(spec) {
                Ok(cron) => Ok(Self::Cron { cron: Box::new(cron), timezone }),
                Err(error) => Err(format!("invalid cron expression `{spec}`: {error}")),
            },
            "once" => Ok(Self::Once),
            _ => Err(format!("unknown schedule kind `{kind}`")),
        }
    }

    /// The first occurrence strictly after `after`. `None` when the job
    /// never runs again: one-shot jobs run once.
    pub fn next_after(&self, after: DateTime<Utc>) -> Option<DateTime<Utc>> {
        match self {
            Self::Cron { cron, timezone } => next_cron(cron, *timezone, after),
            Self::Once => None,
        }
    }
}

/// A trigger as the model gives it. Cron versus one-shot is explicit, never guessed from text.
#[derive(Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum TriggerArgs {
    Cron { expression: String, timezone: Option<String> },
    Once { at: String, timezone: Option<String> },
}

/// A validated trigger, ready to store.
pub struct Timing {
    pub kind: &'static str,
    pub spec: String,
    pub timezone: Tz,
    pub next_at: DateTime<Utc>,
}

/// Validates `trigger` at `now`. Cron expressions are read in their time
/// zone (`default` when absent); a one-shot time must be in the future and unambiguous.
pub fn timing(trigger: TriggerArgs, default: Tz, now: DateTime<Utc>) -> Result<Timing, String> {
    match trigger {
        TriggerArgs::Cron { expression, timezone } => {
            let timezone = zone(timezone.as_deref(), default)?;

            let cron = match parse_cron(&expression) {
                Ok(cron) => cron,
                Err(error) => return Err(format!("invalid cron expression `{expression}` (five fields, minute resolution): {error}")),
            };

            let Some(next_at) = next_cron(&cron, timezone, now) else {
                return Err(format!("`{expression}` never matches a time that exists in {timezone}"));
            };

            Ok(Timing { kind: "cron", spec: expression, timezone, next_at })
        }
        TriggerArgs::Once { at, timezone } => {
            let zone_given = match &timezone {
                Some(name) => Some(zone(Some(name), default)?),
                None => None,
            };

            let next_at = once_at(&at, zone_given)?;

            if next_at <= now {
                return Err(format!("`{at}` is in the past"));
            }

            Ok(Timing { kind: "once", spec: at, timezone: zone_given.unwrap_or(default), next_at })
        }
    }
}

fn zone(name: Option<&str>, default: Tz) -> Result<Tz, String> {
    let Some(name) = name else {
        return Ok(default);
    };

    match name.parse::<Tz>() {
        Ok(timezone) => Ok(timezone),
        Err(_) => Err(format!("unknown time zone `{name}`; use an IANA name such as Europe/Paris")),
    }
}

/// A one-shot instant: an RFC 3339 time with its offset, or a local time
/// read in an explicitly given zone. A local time that happens twice or
/// never in that zone is refused rather than guessed.
fn once_at(at: &str, timezone: Option<Tz>) -> Result<DateTime<Utc>, String> {
    if let Ok(instant) = DateTime::parse_from_rfc3339(at) {
        return Ok(instant.with_timezone(&Utc));
    }

    let Some(timezone) = timezone else {
        return Err(format!("`{at}` has no UTC offset; add one (like 2026-11-06T09:00:00+01:00) or give `timezone`"));
    };

    let Some(local) = ["%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M"].iter().find_map(|format| NaiveDateTime::parse_from_str(at, format).ok())
    else {
        return Err(format!("`{at}` is not a time like 2026-11-06T09:00"));
    };

    match timezone.from_local_datetime(&local) {
        LocalResult::Single(instant) => Ok(instant.with_timezone(&Utc)),
        LocalResult::Ambiguous(first, second) => Err(format!(
            "`{at}` happens twice in {timezone} (clocks go back); give the offset: {} or {}",
            first.format("%:z"),
            second.format("%:z")
        )),
        LocalResult::None => Err(format!("`{at}` does not exist in {timezone} (clocks go forward); choose another time")),
    }
}

/// What a job is, in one paragraph, for receipts and listings.
pub fn describe(id: &str, definition: &Definition) -> String {
    let timezone = definition.timezone.parse::<Tz>().unwrap_or(chrono_tz::UTC);

    let trigger = match definition.kind.as_str() {
        "cron" => format!("cron `{}` in {}", definition.spec, definition.timezone),
        _ => format!("once, `{}`", definition.spec),
    };

    let next = match (definition.enabled, definition.next_at.and_then(|at| DateTime::from_timestamp(at, 0))) {
        (false, Some(_)) => "paused".to_owned(),
        (_, Some(next)) => local(next, timezone),
        (_, None) => "none (done)".to_owned(),
    };

    let caps = if definition.caps.is_empty() {
        "none".to_owned()
    } else {
        definition.caps.join(", ")
    };

    let file = if definition.file {
        format!("\n  file: jobs/{id}.toml")
    } else {
        String::new()
    };

    format!(
        "\"{}\" ({id}){file}\n  trigger: {trigger}\n  next run: {next}\n  caps: {caps}\n  channels: {}\n  prompt: {}",
        definition.title,
        definition.channels.join(", "),
        definition.prompt
    )
}

/// When a job resumed at `now` runs next: a cron job from its next
/// occurrence (time spent paused is not caught up), a one-shot job at its
/// instant if it has not run yet.
pub fn resumed_next(definition: &Definition, now: DateTime<Utc>) -> Result<i64, String> {
    let trigger = Trigger::parse(&definition.kind, &definition.spec, &definition.timezone)?;

    let next = match trigger {
        Trigger::Once => definition.next_at,
        Trigger::Cron { .. } => trigger.next_after(now).map(|next| next.timestamp()),
    };

    match next {
        Some(next) => Ok(next),
        None => Err("the job has no occurrence left to run; give it a new trigger".to_owned()),
    }
}

/// Every job, for the owner's CLI and the model's `schedule_list`.
pub fn list(jobs: &[Job], waiting: &[Waiting]) -> String {
    let mut text = String::new();

    if jobs.is_empty() {
        text.push_str("No scheduled jobs.\n\n");
    }

    for job in jobs {
        let last = match &job.last_run {
            Some((status, at)) => format!("{status} (due {})", when(Some(*at), &job.definition.timezone)),
            None => "none".to_owned(),
        };

        text.push_str(&format!("{}\n  last run: {last}\n\n", describe(&job.id, &job.definition)));
    }

    if !waiting.is_empty() {
        text.push_str("Waiting for the owner's approval (`/approve <code>` or `shade schedule approve <code>`):\n");
    }

    for change in waiting {
        let summary = change.receipt.lines().next().unwrap_or_default();
        text.push_str(&format!("  {}  {summary}\n", change.code));
    }

    text.truncate(text.trim_end().len());

    text
}

pub fn when(at: Option<i64>, timezone: &str) -> String {
    let Some(instant) = at.and_then(|at| DateTime::from_timestamp(at, 0)) else {
        return "none".to_owned();
    };

    local(instant, timezone.parse().unwrap_or(chrono_tz::UTC))
}

/// Five fields, minute resolution: no seconds and no year.
fn parse_cron(expression: &str) -> Result<Cron, croner::errors::CronError> {
    CronParser::builder().seconds(Seconds::Disallowed).year(Year::Disallowed).build().parse(expression)
}

fn next_cron(cron: &Cron, timezone: Tz, after: DateTime<Utc>) -> Option<DateTime<Utc>> {
    let mut candidate = after.with_timezone(&timezone);

    for _ in 0..MAX_CANDIDATES {
        candidate = cron.find_next_occurrence(&candidate, false).ok()?;

        if kept(cron, &candidate) {
            return Some(candidate.with_timezone(&Utc));
        }
    }

    None
}

/// Whether Shade runs `candidate`: its wall time still matches the pattern
/// (it was not moved out of a gap), and it is the first pass of its local time.
fn kept(cron: &Cron, candidate: &DateTime<Tz>) -> bool {
    if !matches!(cron.is_time_matching(candidate), Ok(true)) {
        return false;
    }

    match candidate.timezone().from_local_datetime(&candidate.naive_local()) {
        LocalResult::Ambiguous(first, _) => first == *candidate,
        _ => true,
    }
}

/// The input of a scheduled run: when it was due and when it started, in
/// the job's time zone, then the job's prompt. A late start means the
/// daemon was down or busy; missed occurrences were folded into this run.
pub fn input(title: &str, job: &str, timezone: &str, scheduled_for: DateTime<Utc>, started: DateTime<Utc>, prompt: &str) -> String {
    let timezone = timezone.parse::<Tz>().unwrap_or(chrono_tz::UTC);

    format!(
        "[Scheduled job \"{title}\" ({job}). Due {}; started {}.]\n\n{prompt}",
        local(scheduled_for, timezone),
        local(started, timezone)
    )
}

pub fn local(instant: DateTime<Utc>, timezone: Tz) -> String {
    instant.with_timezone(&timezone).format("%a %Y-%m-%d %H:%M %Z").to_string()
}

#[cfg(test)]
mod tests {
    use chrono::{DateTime, Utc};

    use super::{Trigger, TriggerArgs, timing};

    fn utc(text: &str) -> DateTime<Utc> {
        text.parse().unwrap_or_default()
    }

    fn paris(expression: &str) -> Trigger {
        match Trigger::parse("cron", expression, "Europe/Paris") {
            Ok(trigger) => trigger,
            Err(error) => panic!("{error}"),
        }
    }

    /// Paris springs forward on 2026-03-29 (02:00 CET -> 03:00 CEST) and
    /// falls back on 2026-10-25 (03:00 CEST -> 02:00 CET).
    #[test]
    fn daylight_saving_follows_shade_policy() {
        let cases = [
            // 02:30 does not exist on the 29th: skipped, not moved to 03:00 or 03:30.
            ("30 2 * * *", "2026-03-28T12:00:00Z", "2026-03-30T00:30:00Z"),
            // An hourly job just loses the missing hour.
            ("0 * * * *", "2026-03-29T00:30:00Z", "2026-03-29T01:00:00Z"),
            // 02:30 happens twice on the 25th: the first pass (CEST) runs...
            ("30 2 * * *", "2026-10-24T12:00:00Z", "2026-10-25T00:30:00Z"),
            // ...and the second pass (CET) does not.
            ("30 2 * * *", "2026-10-25T00:30:00Z", "2026-10-26T01:30:00Z"),
            // A wildcard job does not run the repeated hour again.
            ("0,30 * * * *", "2026-10-25T00:30:00Z", "2026-10-25T02:00:00Z"),
        ];

        for (expression, after, expected) in cases {
            assert_eq!(paris(expression).next_after(utc(after)), Some(utc(expected)), "`{expression}` after {after}");
        }
    }

    #[test]
    fn impossible_patterns_never_run() {
        // February 30th never comes; the search gives up instead of looping.
        assert_eq!(paris("0 0 30 2 *").next_after(utc("2026-01-01T00:00:00Z")), None);
    }

    /// One-shot times must be unambiguous instants in the future. Paris
    /// falls back on 2027-10-31 (02:30 happens twice) and springs forward
    /// on 2027-03-28 (02:30 never happens).
    #[test]
    fn one_shot_times_are_never_guessed() {
        let now: DateTime<Utc> = "2026-11-03T12:00:00Z".parse().unwrap_or_default();
        let paris = Some("Europe/Paris");

        let cases = [
            ("2027-10-31T02:30", paris, Err("happens twice")),
            ("2027-03-28T02:30", paris, Err("does not exist")),
            ("2026-11-06T09:00", None, Err("no UTC offset")),
            ("2026-11-02T09:00:00+01:00", None, Err("in the past")),
            ("2026-11-06T09:00", paris, Ok("2026-11-06T08:00:00Z")),
            // An offset wins over the zone, which is kept for display.
            ("2027-10-31T02:30:00+01:00", paris, Ok("2027-10-31T01:30:00Z")),
        ];

        for (at, timezone, expected) in cases {
            let trigger = TriggerArgs::Once { at: at.to_owned(), timezone: timezone.map(str::to_owned) };

            match (timing(trigger, chrono_tz::UTC, now), expected) {
                (Ok(timing), Ok(instant)) => assert_eq!(timing.next_at.to_rfc3339(), instant.replace('Z', "+00:00"), "{at}"),
                (Err(error), Err(reason)) => assert!(error.contains(reason), "{at}: {error}"),
                (Ok(timing), Err(reason)) => panic!("{at} was accepted as {} instead of `{reason}`", timing.next_at),
                (Err(error), Ok(_)) => panic!("{at} was refused: {error}"),
            }
        }
    }
}
