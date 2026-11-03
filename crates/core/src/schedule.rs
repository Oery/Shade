//! Schedule timing and the text a scheduled run starts with.
//!
//! Instants are stored in UTC; a cron job keeps its IANA time zone for the
//! next occurrence. `croner` moves a time inside a spring-forward gap to
//! after the gap and runs wildcard patterns in both passes of a fall-back
//! hour. Shade's policy differs, so its candidates are filtered: a time
//! that no longer matches the pattern is skipped, and so is the second pass
//! of a repeated local time.

use chrono::{DateTime, LocalResult, TimeZone, Utc};
use chrono_tz::Tz;
use croner::Cron;
use croner::parser::{CronParser, Seconds, Year};

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

    use super::Trigger;

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
}
