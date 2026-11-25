//! What runs of the fires missed while the orchestrator was down or a flow
//! or its engine was paused, by the installation's settings.

use std::collections::VecDeque;

use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};

use crate::OnInterrupt;

/// A fire seen later than this was missed (the scheduler ticks far more
/// often).
pub const MISSED_AFTER_SECS: i64 = 120;

/// What to do with the fires missed.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MissedRuns {
    /// Run once, the last one missed.
    #[default]
    Latest,
    /// Run none: go on with the next one.
    Skip,
    /// Run each one, in order, up to the maximum.
    All,
}

impl MissedRuns {
    /// The stored name.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Latest => "latest",
            Self::Skip => "skip",
            Self::All => "all",
        }
    }

    /// Reads a stored name.
    #[must_use]
    pub fn parse(raw: &str) -> Option<Self> {
        [Self::Latest, Self::Skip, Self::All]
            .into_iter()
            .find(|m| m.as_str() == raw)
    }
}

/// The installation's choices for every engine.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Settings {
    pub missed_runs: MissedRuns,
    /// Missed longer ago than this, none runs.
    pub missed_max_hours: u32,
    /// With [`MissedRuns::All`], at most this many.
    pub missed_max_runs: u32,
    /// What an interrupted run does, for the flows without their own rule.
    pub on_interrupt: OnInterrupt,
    /// A queued run no runner claims for this long is cancelled; `None`, it
    /// waits.
    pub queued_wait_minutes: Option<u32>,
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            missed_runs: MissedRuns::Latest,
            missed_max_hours: 24,
            missed_max_runs: 10,
            on_interrupt: OnInterrupt::None,
            queued_wait_minutes: None,
        }
    }
}

impl Settings {
    /// Whether the numbers are in range.
    pub fn validate(&self) -> Result<(), String> {
        if !(1..=720).contains(&self.missed_max_hours) {
            return Err("the maximum age of a missed fire is 1 to 720 hours".into());
        }
        if !(1..=100).contains(&self.missed_max_runs) {
            return Err("the maximum of missed runs is 1 to 100".into());
        }
        if self
            .queued_wait_minutes
            .is_some_and(|m| !(1..=10_080).contains(&m))
        {
            return Err("the wait of a queued run is 1 minute to 7 days".into());
        }
        Ok(())
    }
}

/// The fires to run for a schedule whose next fire was `due`, seen at `now`:
/// on time, that one; later than [`MISSED_AFTER_SECS`], what `settings` says
/// of the fires missed between `due` and `now` (none older than its maximum
/// age), in order. `next_after` is the schedule's next fire strictly after
/// an instant (the `schedule` crate's), `None` when it fires no more.
pub fn fires_to_run(
    next_after: impl Fn(DateTime<Utc>) -> Option<DateTime<Utc>>,
    due: DateTime<Utc>,
    now: DateTime<Utc>,
    settings: &Settings,
) -> Vec<DateTime<Utc>> {
    if now - due <= Duration::seconds(MISSED_AFTER_SECS) {
        return vec![due];
    }
    let oldest = now - Duration::hours(i64::from(settings.missed_max_hours));
    // Only the last ones matter: at most as many as can run.
    let keep = match settings.missed_runs {
        MissedRuns::Skip => return Vec::new(),
        MissedRuns::Latest => 1,
        MissedRuns::All => usize::try_from(settings.missed_max_runs).unwrap_or(1),
    };
    let mut missed = VecDeque::new();
    let mut fire = due;
    // A bound on the walk: a cron every minute for a month is 43,200.
    for _ in 0..100_000 {
        if fire > now {
            break;
        }
        if fire >= oldest {
            missed.push_back(fire);
            if missed.len() > keep {
                missed.pop_front();
            }
        }
        match next_after(fire) {
            Some(next) if next > fire => fire = next,
            _ => break,
        }
    }
    missed.into()
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{DurationRound, TimeDelta};

    fn at(s: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(s).unwrap().with_timezone(&Utc)
    }

    /// Every hour on the hour.
    fn hourly(after: DateTime<Utc>) -> Option<DateTime<Utc>> {
        let hour = TimeDelta::hours(1);
        Some(after.duration_trunc(hour).ok()? + hour)
    }

    fn settings(missed_runs: MissedRuns) -> Settings {
        Settings {
            missed_runs,
            missed_max_runs: 3,
            ..Default::default()
        }
    }

    #[test]
    fn on_time_it_runs_and_late_the_policy_decides() {
        let run = |due: &str, now: &str, s: &Settings| fires_to_run(hourly, at(due), at(now), s);
        let latest = settings(MissedRuns::Latest);
        assert_eq!(
            run("2026-10-25T10:00:00Z", "2026-10-25T10:01:00Z", &latest),
            [at("2026-10-25T10:00:00Z")],
            "on time"
        );
        // Down from 10:00 to 15:30: 10, 11, 12, 13, 14 and 15 were missed.
        let (due, now) = ("2026-10-25T10:00:00Z", "2026-10-25T15:30:00Z");
        assert_eq!(run(due, now, &latest), [at("2026-10-25T15:00:00Z")]);
        assert!(run(due, now, &settings(MissedRuns::Skip)).is_empty());
        assert_eq!(
            run(due, now, &settings(MissedRuns::All)),
            [
                at("2026-10-25T13:00:00Z"),
                at("2026-10-25T14:00:00Z"),
                at("2026-10-25T15:00:00Z")
            ],
            "the last 3, in order"
        );
        // Too old: none of those older than an hour.
        let short = Settings {
            missed_max_hours: 1,
            ..settings(MissedRuns::All)
        };
        assert_eq!(
            run("2026-10-24T10:00:00Z", "2026-10-25T15:30:00Z", &short),
            [at("2026-10-25T15:00:00Z")]
        );
    }

    #[test]
    fn a_schedule_that_ends_stops_the_walk() {
        let once = |_| None;
        let got = fires_to_run(
            once,
            at("2026-10-25T10:00:00Z"),
            at("2026-10-25T15:00:00Z"),
            &settings(MissedRuns::All),
        );
        assert_eq!(got, [at("2026-10-25T10:00:00Z")]);
    }

    #[test]
    fn settings_out_of_range_are_refused() {
        assert!(Settings::default().validate().is_ok());
        for bad in [
            Settings {
                missed_max_hours: 0,
                ..Default::default()
            },
            Settings {
                missed_max_runs: 101,
                ..Default::default()
            },
            Settings {
                queued_wait_minutes: Some(0),
                ..Default::default()
            },
        ] {
            assert!(bad.validate().is_err(), "{bad:?}");
        }
        for m in [MissedRuns::Latest, MissedRuns::Skip, MissedRuns::All] {
            assert_eq!(MissedRuns::parse(m.as_str()), Some(m));
        }
    }
}
