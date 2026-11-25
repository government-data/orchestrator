//! Interrupted runs: why a run was cut without failing by itself, what its
//! flow says to do then, and when it runs again.
//!
//! A run that is cut stays `failed` (or `lost`) with its [`Interruption`].
//! Its flow's [`OnInterrupt`] says whether it is repeated; a repetition is a
//! new run that points at the one it repeats and carries its attempt, at
//! most [`MAX_ATTEMPTS`] counting the first, after a growing delay.

use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};

/// Why a run was cut without failing by itself.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Interruption {
    /// The operating system ended the engine (out of memory, shutdown).
    System,
    /// The runner was stopped during the run and stopped the engine.
    RunnerStopped,
    /// The runner restarted: the engine died with the previous process.
    RunnerRestarted,
    /// The runner stopped renewing the lease: whether the engine still runs
    /// is not known.
    RunnerLost,
    /// The runner lost the orchestrator and stopped the engine, so that the
    /// run does not run twice if it is repeated elsewhere.
    RunnerIsolated,
    /// The runner's disk went under its minimum and it stopped the run, to
    /// be repeated on another runner.
    LowDisk,
}

impl Interruption {
    /// Every interruption, in the order above.
    pub const ALL: [Self; 6] = [
        Self::System,
        Self::RunnerStopped,
        Self::RunnerRestarted,
        Self::RunnerLost,
        Self::RunnerIsolated,
        Self::LowDisk,
    ];

    /// The stored name.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::System => "system",
            Self::RunnerStopped => "runner_stopped",
            Self::RunnerRestarted => "runner_restarted",
            Self::RunnerLost => "runner_lost",
            Self::RunnerIsolated => "runner_isolated",
            Self::LowDisk => "low_disk",
        }
    }

    /// Reads a stored name.
    #[must_use]
    pub fn parse(raw: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|i| i.as_str() == raw)
    }

    /// Whether the engine is known to have stopped: only then can the run be
    /// repeated without running twice. A lost runner may still be running
    /// it, unless it stops a run it can no longer renew (`runner_fences`).
    #[must_use]
    pub fn engine_stopped(self, runner_fences: bool) -> bool {
        !matches!(self, Self::RunnerLost) || runner_fences
    }
}

/// What to do with an interrupted run, by its flow.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OnInterrupt {
    /// Tell, without repeating it.
    #[default]
    None,
    /// The flow is idempotent: repeat it from the start.
    Repeat,
    /// Repeat it telling the engine the run it repeats and where that one
    /// was, so that the flow skips what it already did.
    Continue,
}

impl OnInterrupt {
    /// The stored name.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Repeat => "repeat",
            Self::Continue => "continue",
        }
    }

    /// Reads a stored name.
    #[must_use]
    pub fn parse(raw: &str) -> Option<Self> {
        [Self::None, Self::Repeat, Self::Continue]
            .into_iter()
            .find(|o| o.as_str() == raw)
    }
}

/// How many times an interrupted run may run, counting the first: a flow
/// the system always ends is not repeated without end.
pub const MAX_ATTEMPTS: u32 = 3;

/// An interrupted run older than this is not repeated.
pub const RETRY_MAX_AGE_HOURS: i64 = 24;

/// The delay before the first repetition; each next one doubles it.
pub const RETRY_FIRST_DELAY_SECS: i64 = 30;

/// The longest delay before a repetition.
pub const RETRY_MAX_DELAY_SECS: i64 = 600;

/// The delay before attempt `attempt + 1` of a run cut at attempt `attempt`:
/// 30 s, then 60 s, 120 s… up to 10 minutes.
#[must_use]
pub fn retry_delay(attempt: u32) -> Duration {
    let doublings = attempt.saturating_sub(1).min(16);
    let secs = RETRY_FIRST_DELAY_SECS.saturating_mul(1 << doublings);
    Duration::seconds(secs.min(RETRY_MAX_DELAY_SECS))
}

/// An interrupted run, as the retry rule reads it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Interrupted {
    pub interruption: Interruption,
    /// The attempt that was cut (the first is 1).
    pub attempt: u32,
    pub finished_at: DateTime<Utc>,
    /// The flow's own rule, or the installation's when it has none.
    pub on_interrupt: OnInterrupt,
    /// Whether the flow is enabled now.
    pub flow_enabled: bool,
    /// Whether its runner stops a run it can no longer renew before the
    /// lease ends.
    pub runner_fences: bool,
}

/// A repetition to queue.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Retry {
    /// Its attempt.
    pub attempt: u32,
    /// Not handed out before this.
    pub not_before: DateTime<Utc>,
    /// Whether the engine is told the run it repeats ([`OnInterrupt::Continue`]).
    pub resume: bool,
}

/// Whether `run` is repeated, seen at `now`, and how. A run cut for a low
/// disk is repeated on another runner whatever its flow says; any other is
/// repeated when its flow asks for it, the engine is known to have stopped,
/// attempts are left and it is not too old.
#[must_use]
pub fn retry(run: &Interrupted, now: DateTime<Utc>) -> Option<Retry> {
    let wanted = match run.interruption {
        Interruption::LowDisk => true,
        _ => run.on_interrupt != OnInterrupt::None,
    };
    let eligible = wanted
        && run.flow_enabled
        && run.interruption.engine_stopped(run.runner_fences)
        && run.attempt < MAX_ATTEMPTS
        && now - run.finished_at < Duration::hours(RETRY_MAX_AGE_HOURS);
    eligible.then(|| Retry {
        attempt: run.attempt + 1,
        not_before: run.finished_at + retry_delay(run.attempt),
        resume: run.on_interrupt == OnInterrupt::Continue,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(s: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(s).unwrap().with_timezone(&Utc)
    }

    fn cut(interruption: Interruption, attempt: u32, on: OnInterrupt) -> Interrupted {
        Interrupted {
            interruption,
            attempt,
            finished_at: at("2026-11-25T10:00:00Z"),
            on_interrupt: on,
            flow_enabled: true,
            runner_fences: false,
        }
    }

    #[test]
    fn names_round_trip() {
        for i in Interruption::ALL {
            assert_eq!(Interruption::parse(i.as_str()), Some(i));
        }
        for o in [
            OnInterrupt::None,
            OnInterrupt::Repeat,
            OnInterrupt::Continue,
        ] {
            assert_eq!(OnInterrupt::parse(o.as_str()), Some(o));
        }
        assert_eq!(Interruption::parse("agent_lost"), None);
    }

    #[test]
    fn a_lost_runner_counts_as_stopped_only_if_it_fences() {
        assert!(!Interruption::RunnerLost.engine_stopped(false));
        assert!(Interruption::RunnerLost.engine_stopped(true));
        assert!(Interruption::System.engine_stopped(false));
    }

    #[test]
    fn the_delay_doubles_up_to_ten_minutes() {
        assert_eq!(retry_delay(1), Duration::seconds(30));
        assert_eq!(retry_delay(2), Duration::seconds(60));
        assert_eq!(retry_delay(3), Duration::seconds(120));
        assert_eq!(retry_delay(10), Duration::seconds(600));
        assert_eq!(retry_delay(u32::MAX), Duration::seconds(600));
    }

    #[test]
    fn a_flow_that_asks_is_repeated_until_its_attempts_run_out() {
        let now = at("2026-11-25T10:05:00Z");
        let first = cut(Interruption::System, 1, OnInterrupt::Repeat);
        assert_eq!(
            retry(&first, now),
            Some(Retry {
                attempt: 2,
                not_before: at("2026-11-25T10:00:30Z"),
                resume: false,
            })
        );
        let second = cut(Interruption::RunnerRestarted, 2, OnInterrupt::Continue);
        let again = retry(&second, now).unwrap();
        assert_eq!(again.attempt, MAX_ATTEMPTS);
        assert!(again.resume);
        let last = cut(Interruption::System, MAX_ATTEMPTS, OnInterrupt::Repeat);
        assert_eq!(retry(&last, now), None, "no attempts left");
    }

    #[test]
    fn what_is_not_repeated() {
        let now = at("2026-11-25T10:05:00Z");
        assert_eq!(
            retry(&cut(Interruption::System, 1, OnInterrupt::None), now),
            None
        );
        let lost = cut(Interruption::RunnerLost, 1, OnInterrupt::Repeat);
        assert_eq!(retry(&lost, now), None, "it may still run");
        let fenced = Interrupted {
            runner_fences: true,
            ..lost
        };
        assert!(retry(&fenced, now).is_some());
        let disabled = Interrupted {
            flow_enabled: false,
            ..cut(Interruption::System, 1, OnInterrupt::Repeat)
        };
        assert_eq!(retry(&disabled, now), None);
        let old = cut(Interruption::System, 1, OnInterrupt::Repeat);
        assert_eq!(retry(&old, at("2026-11-26T10:00:00Z")), None, "too old");
    }

    #[test]
    fn a_low_disk_is_repeated_whatever_the_flow_says() {
        let now = at("2026-11-25T10:05:00Z");
        let low = cut(Interruption::LowDisk, 1, OnInterrupt::None);
        assert_eq!(retry(&low, now).map(|r| r.attempt), Some(2));
        let spent = cut(Interruption::LowDisk, MAX_ATTEMPTS, OnInterrupt::None);
        assert_eq!(retry(&spent, now), None);
    }
}
