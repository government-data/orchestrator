//! The next fire of a schedule: its cron, or its trigger with the random
//! delay added.
//!
//! The cron is standard 5-field (`min hour day month weekday`), evaluated in
//! the schedule's time zone: `0 6 * * 1-5` is "06:00 in Bogotá, Monday to
//! Friday" whatever zone the server runs in.

use std::fmt;
use std::str::FromStr;

use chrono::{DateTime, Timelike, Utc};
use chrono_tz::Tz;
use croner::Cron;
use orchestration_model::CronCalendar;
use serde::{Deserialize, Serialize};

use crate::{next_trigger, random_delay, validate_trigger, ScheduleTrigger};

/// Why a schedule is not valid.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScheduleError {
    /// The cron expression does not parse.
    Cron(String),
    /// The time zone is not a known IANA name.
    Timezone(String),
    /// It never fires again.
    NeverFires,
    /// The trigger is not valid (and why).
    Trigger(String),
}

impl fmt::Display for ScheduleError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Cron(e) => write!(f, "invalid cron expression: {e}"),
            Self::Timezone(tz) => write!(f, "unknown time zone {tz:?}"),
            Self::NeverFires => f.write_str("the schedule never fires again (is it past?)"),
            Self::Trigger(e) => f.write_str(e),
        }
    }
}

impl std::error::Error for ScheduleError {}

fn timezone(raw: &str) -> Result<Tz, ScheduleError> {
    Tz::from_str(raw.trim()).map_err(|_| ScheduleError::Timezone(raw.to_string()))
}

fn parse(expr: &str, zone: &str) -> Result<(Cron, Tz), ScheduleError> {
    let expr = expr.trim();
    // Exactly 5 fields: `croner` also takes 6 (with seconds), but a flow is
    // not scheduled to the second and an extra field is almost always a copy
    // mistake that would shift everything one position.
    if expr.split_whitespace().count() != 5 {
        return Err(ScheduleError::Cron(
            "5 fields expected: minute hour day month weekday".into(),
        ));
    }
    let cron = Cron::from_str(expr).map_err(|e| ScheduleError::Cron(e.to_string()))?;
    Ok((cron, timezone(zone)?))
}

/// The first instant `expr` names in `zone` after `from` (at `from` too
/// when `inclusive`), in UTC; `None` if it never fires again.
fn occurrence(
    expr: &str,
    zone: &str,
    from: DateTime<Utc>,
    inclusive: bool,
) -> Result<Option<DateTime<Utc>>, ScheduleError> {
    let (cron, tz) = parse(expr, zone)?;
    // No fractions of a second: `croner` carries them over from the starting
    // instant, and a fire must land on the exact second (06:00:00, not
    // 06:00:00.908).
    let from = from.with_nanosecond(0).unwrap_or(from);
    Ok(cron
        .find_next_occurrence(&from.with_timezone(&tz), inclusive)
        .ok()
        .map(|local| local.with_timezone(&Utc)))
}

/// The cron's first fire strictly after `after`, in UTC.
pub fn next_fire(
    expr: &str,
    zone: &str,
    after: DateTime<Utc>,
) -> Result<DateTime<Utc>, ScheduleError> {
    occurrence(expr, zone, after, false)?.ok_or(ScheduleError::NeverFires)
}

/// When a schedule fires: its trigger if it has one (the cron is then
/// empty), otherwise its cron, in its time zone.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScheduleSpec {
    #[serde(default)]
    pub cron: String,
    pub timezone: String,
    #[serde(default)]
    pub trigger: Option<ScheduleTrigger>,
}

impl ScheduleSpec {
    /// The first fire strictly after `after`, with the random delay of a
    /// trigger already added; `None`: it does not fire again (once, or
    /// expired). `seed` (the schedule's id) makes the random delay stable.
    pub fn next_after(
        &self,
        seed: &str,
        after: DateTime<Utc>,
    ) -> Result<Option<DateTime<Utc>>, ScheduleError> {
        let Some(t) = &self.trigger else {
            return next_fire(&self.cron, &self.timezone, after).map(Some);
        };
        let tz = timezone(&self.timezone)?;
        let after = after.with_nanosecond(0).unwrap_or(after);
        Ok(next_trigger(t, tz, after)
            .map(|next| next + random_delay(seed, next, t.random_delay_secs)))
    }

    /// Checks it before it is saved; returns its first fire after `now`.
    pub fn validate(&self, seed: &str, now: DateTime<Utc>) -> Result<DateTime<Utc>, ScheduleError> {
        if let Some(t) = &self.trigger {
            validate_trigger(t).map_err(ScheduleError::Trigger)?;
        }
        self.next_after(seed, now)?.ok_or(ScheduleError::NeverFires)
    }
}

/// The cron arithmetic of `orchestration_model`'s maintenance windows.
#[derive(Debug, Clone, Copy, Default)]
pub struct Calendar;

impl CronCalendar for Calendar {
    fn at_or_after(
        &self,
        expr: &str,
        zone: &str,
        from: DateTime<Utc>,
    ) -> Result<Option<DateTime<Utc>>, String> {
        occurrence(expr, zone, from, true).map_err(|e| e.to_string())
    }
}

#[cfg(test)]
#[path = "cron_tests.rs"]
mod tests;
