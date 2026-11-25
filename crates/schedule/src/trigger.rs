//! Triggers in the style of the Windows Task Scheduler: once, daily every N
//! days, weekly every N weeks on some days, or monthly in some months and
//! days; with repetition every X minutes for a while, a random delay and
//! expiry. What a cron cannot say ("every 2 weeks from 9 July", "the last
//! Friday", "just once").
//!
//! Everything is computed in the local time of the schedule's zone and
//! comes back in UTC. An instant that does not exist in that zone (the jump
//! of a clock change) is skipped; one that exists twice takes the first.

use chrono::{DateTime, Datelike, Duration, NaiveDate, NaiveDateTime, TimeZone, Utc};
use chrono_tz::Tz;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// How often, counted from the start day.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Recurrence {
    /// Only the start day and time.
    Once,
    /// Every `every` days.
    Daily { every: u32 },
    /// Every `every` weeks, on the given weekdays (1 = Monday … 7 = Sunday).
    Weekly { every: u32, days: Vec<u8> },
    /// The given months (1–12), and in them the days of the month (1–31; 32 =
    /// the last) or, with `nth` (1–4; 5 = the last) and `weekdays`, "the first
    /// Monday", "the last Friday"…
    Monthly {
        months: Vec<u8>,
        #[serde(default)]
        days: Vec<u8>,
        #[serde(default)]
        nth: Vec<u8>,
        #[serde(default)]
        weekdays: Vec<u8>,
    },
}

/// Repeat every `every_minutes` after each fire, for `for_minutes`
/// (without it: indefinitely).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Repeat {
    pub every_minutes: u32,
    #[serde(default)]
    pub for_minutes: Option<u32>,
}

/// A trigger.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScheduleTrigger {
    /// The first day, and the time of each fire, in the schedule's zone.
    pub start: NaiveDateTime,
    pub recurrence: Recurrence,
    #[serde(default)]
    pub repeat: Option<Repeat>,
    /// Up to how much each fire is shifted at random (not to launch many at once).
    #[serde(default)]
    pub random_delay_secs: u32,
    /// After this it does not fire again.
    #[serde(default)]
    pub expires: Option<NaiveDateTime>,
}

/// Why a trigger is not valid.
pub fn validate_trigger(t: &ScheduleTrigger) -> Result<(), String> {
    let in_range = |v: &[u8], lo: u8, hi: u8| v.iter().all(|x| (lo..=hi).contains(x));
    match &t.recurrence {
        Recurrence::Once => {}
        Recurrence::Daily { every } => {
            if !(1..=365).contains(every) {
                return Err("every how many days: 1 to 365".into());
            }
        }
        Recurrence::Weekly { every, days } => {
            if !(1..=52).contains(every) {
                return Err("every how many weeks: 1 to 52".into());
            }
            if days.is_empty() || !in_range(days, 1, 7) {
                return Err("choose at least one weekday".into());
            }
        }
        Recurrence::Monthly {
            months,
            days,
            nth,
            weekdays,
        } => {
            if months.is_empty() || !in_range(months, 1, 12) {
                return Err("choose at least one month".into());
            }
            let by_day = !days.is_empty();
            let by_week = !nth.is_empty() && !weekdays.is_empty();
            if !by_day && !by_week {
                return Err(
                    "choose days of the month, or a week and a weekday (e.g. the first Monday)"
                        .into(),
                );
            }
            if !in_range(days, 1, 32) || !in_range(nth, 1, 5) || !in_range(weekdays, 1, 7) {
                return Err("a day, week or weekday is out of range".into());
            }
        }
    }
    if let Some(r) = &t.repeat {
        if !(1..=24 * 60).contains(&r.every_minutes) {
            return Err("repeat every: 1 minute to 1 day".into());
        }
        if r.for_minutes.is_some_and(|d| d < r.every_minutes) {
            return Err("the repetition must last longer than its interval".into());
        }
    }
    if t.random_delay_secs > 24 * 3600 {
        return Err("the random delay is at most 1 day".into());
    }
    if t.expires.is_some_and(|e| e <= t.start) {
        return Err("the expiry must be after the start".into());
    }
    Ok(())
}

fn weekday_number(d: NaiveDate) -> u8 {
    d.weekday().number_from_monday() as u8
}

fn monday_of(d: NaiveDate) -> NaiveDate {
    d - Duration::days(d.weekday().num_days_from_monday() as i64)
}

fn last_day_of_month(d: NaiveDate) -> bool {
    (d + Duration::days(1)).month() != d.month()
}

/// Whether day `d` is due by the recurrence (counting from `first`).
fn day_matches(recurrence: &Recurrence, first: NaiveDate, d: NaiveDate) -> bool {
    if d < first {
        return false;
    }
    match recurrence {
        Recurrence::Once => d == first,
        Recurrence::Daily { every } => (d - first).num_days() % (*every as i64) == 0,
        Recurrence::Weekly { every, days } => {
            days.contains(&weekday_number(d))
                && ((monday_of(d) - monday_of(first)).num_weeks() % (*every as i64)) == 0
        }
        Recurrence::Monthly {
            months,
            days,
            nth,
            weekdays,
        } => {
            if !months.contains(&(d.month() as u8)) {
                return false;
            }
            let by_day =
                days.contains(&(d.day() as u8)) || (days.contains(&32) && last_day_of_month(d));
            let week_of_month = ((d.day() - 1) / 7 + 1) as u8;
            let is_last_week = (d + Duration::days(7)).month() != d.month();
            let by_week = weekdays.contains(&weekday_number(d))
                && (nth.contains(&week_of_month) || (nth.contains(&5) && is_last_week));
            by_day || by_week
        }
    }
}

/// The local time in the zone, or nothing if that instant does not exist (clock change).
fn localize(tz: Tz, local: NaiveDateTime) -> Option<DateTime<Utc>> {
    tz.from_local_datetime(&local)
        .earliest()
        .map(|d| d.with_timezone(&Utc))
}

/// How many days ahead are searched before assuming it does not fire again
/// (e.g. "the 31st of February").
const HORIZON_DAYS: i64 = 366 * 5;

/// The first fire strictly after `after` (without the random delay), or
/// `None` if it does not fire again.
pub fn next_trigger(t: &ScheduleTrigger, tz: Tz, after: DateTime<Utc>) -> Option<DateTime<Utc>> {
    let after_local = after.with_timezone(&tz).naive_local();
    let expired = |local: NaiveDateTime| t.expires.is_some_and(|e| local > e);
    let accept = |local: NaiveDateTime| -> Option<Option<DateTime<Utc>>> {
        if expired(local) {
            return Some(None);
        }
        if local < t.start {
            return None;
        }
        localize(tz, local).filter(|u| *u > after).map(Some)
    };

    // Indefinite repetition: from the start, every X minutes forever.
    if let Some(Repeat {
        every_minutes,
        for_minutes: None,
    }) = &t.repeat
    {
        let every = Duration::minutes(*every_minutes as i64);
        let k = if after_local <= t.start {
            0
        } else {
            (after_local - t.start).num_minutes() / *every_minutes as i64
        };
        // A few extra steps in case an instant falls in a clock jump.
        for step in k..k + 4 {
            if let Some(found) = accept(t.start + every * step as i32) {
                return found;
            }
        }
        return None;
    }

    let (every, window) = match &t.repeat {
        Some(Repeat {
            every_minutes,
            for_minutes: Some(d),
        }) => (
            Duration::minutes(*every_minutes as i64),
            Duration::minutes(*d as i64),
        ),
        _ => (Duration::zero(), Duration::zero()),
    };
    let first_day = t.start.date();
    let time = t.start.time();
    // A fire from yesterday may still be repeating today.
    let mut day = ((after_local - window).date()).max(first_day);
    let last_day = after_local.date() + Duration::days(HORIZON_DAYS);
    while day <= last_day {
        if day_matches(&t.recurrence, first_day, day) {
            let base = day.and_time(time);
            if expired(base) {
                return None;
            }
            let mut offset = Duration::zero();
            loop {
                if let Some(found) = accept(base + offset) {
                    return found;
                }
                offset += every;
                if every.is_zero() || offset > window {
                    break;
                }
            }
        }
        if matches!(t.recurrence, Recurrence::Once) && day >= first_day {
            return None;
        }
        day += Duration::days(1);
    }
    None
}

/// The random delay of a fire: stable for the same schedule and the same
/// instant (every replica computes the same).
pub fn random_delay(seed: &str, at: DateTime<Utc>, max_secs: u32) -> Duration {
    if max_secs == 0 {
        return Duration::zero();
    }
    let hash = Sha256::digest(format!("{seed}@{}", at.timestamp()).as_bytes());
    let n = u64::from_be_bytes(hash[..8].try_into().unwrap_or_default());
    Duration::seconds((n % (max_secs as u64 + 1)) as i64)
}

#[cfg(test)]
#[path = "trigger_tests.rs"]
mod tests;
