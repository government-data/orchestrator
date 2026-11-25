//! Maintenance windows, per cluster: once, or every time a cron fires, for a
//! while. While a cluster's window is open its runners are handed nothing
//! new; what is scheduled meanwhile is queued or skipped, as the window says.

use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};

/// The cron arithmetic a recurrent window needs, as the `schedule` crate
/// computes it.
pub trait CronCalendar {
    /// The first instant at or after `from` that the 5-field cron `expr`
    /// names in `timezone`; `Ok(None)` if it never fires again, `Err` with
    /// why if `expr` or `timezone` is not valid.
    fn at_or_after(
        &self,
        expr: &str,
        timezone: &str,
        from: DateTime<Utc>,
    ) -> Result<Option<DateTime<Utc>>, String>;
}

/// When a window is open.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum WindowTime {
    Once {
        starts_at: DateTime<Utc>,
        ends_at: DateTime<Utc>,
    },
    Recurrent {
        cron: String,
        timezone: String,
        duration_minutes: u32,
    },
}

impl WindowTime {
    /// Whether it is valid at `now`: a start before its end; a cron that
    /// fires, a known time zone and a duration up to a week.
    pub fn validate(&self, calendar: &impl CronCalendar, now: DateTime<Utc>) -> Result<(), String> {
        match self {
            Self::Once { starts_at, ends_at } if ends_at <= starts_at => {
                Err("a window has to end after it starts".into())
            }
            Self::Once { .. } => Ok(()),
            Self::Recurrent {
                cron,
                timezone,
                duration_minutes,
            } => {
                if !(1..=10_080).contains(duration_minutes) {
                    return Err("a window lasts 1 minute to 7 days".into());
                }
                match calendar.at_or_after(cron, timezone, now)? {
                    Some(_) => Ok(()),
                    None => Err("the window's cron never fires again".into()),
                }
            }
        }
    }

    /// Until when it is open at `now`, if it is.
    pub fn open_until(
        &self,
        calendar: &impl CronCalendar,
        now: DateTime<Utc>,
    ) -> Option<DateTime<Utc>> {
        match self {
            Self::Once { starts_at, ends_at } => {
                (*starts_at <= now && now < *ends_at).then_some(*ends_at)
            }
            Self::Recurrent {
                cron,
                timezone,
                duration_minutes,
            } => {
                let duration = Duration::minutes(i64::from(*duration_minutes));
                let start = calendar
                    .at_or_after(cron, timezone, now - duration)
                    .ok()??;
                (start <= now && now < start + duration).then_some(start + duration)
            }
        }
    }

    /// When it opens next after `now` (when it opened, if it is open).
    pub fn next_open(
        &self,
        calendar: &impl CronCalendar,
        now: DateTime<Utc>,
    ) -> Option<DateTime<Utc>> {
        match self {
            Self::Once { starts_at, ends_at } => (now < *ends_at).then_some(*starts_at),
            Self::Recurrent {
                cron,
                timezone,
                duration_minutes,
            } => {
                let duration = Duration::minutes(i64::from(*duration_minutes));
                calendar.at_or_after(cron, timezone, now - duration).ok()?
            }
        }
    }
}

/// What is scheduled while a window is open.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OnSchedule {
    /// Queued, to run when the window closes.
    Queue,
    /// Not run.
    Skip,
}

/// A maintenance window for some clusters.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MaintenanceWindow {
    pub id: String,
    pub name: String,
    pub time: WindowTime,
    pub on_schedule: OnSchedule,
    pub enabled: bool,
    /// The clusters it covers (their machine ids).
    pub clusters: Vec<String>,
}

impl MaintenanceWindow {
    /// Whether it can be saved at `now`.
    pub fn validate(&self, calendar: &impl CronCalendar, now: DateTime<Utc>) -> Result<(), String> {
        if self.name.trim().is_empty() || self.name.chars().count() > 120 {
            return Err("a window's name is required, up to 120 characters".into());
        }
        if self.clusters.is_empty() {
            return Err("a window covers at least one cluster".into());
        }
        self.time.validate(calendar, now)
    }
}

/// A cluster's open window, as the claims read it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OpenWindow {
    pub cluster: String,
    pub window_id: String,
    pub until: DateTime<Utc>,
    /// What is scheduled meanwhile is skipped.
    pub skips: bool,
}

/// The windows open at `now`, one per cluster: the one that ends last, and
/// skipping if any of them skips.
pub fn open_windows(
    windows: &[MaintenanceWindow],
    calendar: &impl CronCalendar,
    now: DateTime<Utc>,
) -> Vec<OpenWindow> {
    let mut open: Vec<OpenWindow> = Vec::new();
    for w in windows.iter().filter(|w| w.enabled) {
        let Some(until) = w.time.open_until(calendar, now) else {
            continue;
        };
        let skips = w.on_schedule == OnSchedule::Skip;
        for cluster in &w.clusters {
            match open.iter_mut().find(|o| &o.cluster == cluster) {
                Some(o) => {
                    o.skips |= skips;
                    if until > o.until {
                        o.until = until;
                        o.window_id.clone_from(&w.id);
                    }
                }
                None => open.push(OpenWindow {
                    cluster: cluster.clone(),
                    window_id: w.id.clone(),
                    until,
                    skips,
                }),
            }
        }
    }
    open
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{Datelike, Timelike, Weekday};

    fn at(s: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(s).unwrap().with_timezone(&Utc)
    }

    /// A fake calendar that knows one cron: `weekly`, Mondays at 03:00 UTC.
    struct Mondays;

    impl CronCalendar for Mondays {
        fn at_or_after(
            &self,
            expr: &str,
            _: &str,
            from: DateTime<Utc>,
        ) -> Result<Option<DateTime<Utc>>, String> {
            if expr != "weekly" {
                return Err(format!("unknown cron {expr:?}"));
            }
            let mut day = from.date_naive();
            loop {
                let start = day.and_hms_opt(3, 0, 0).unwrap().and_utc();
                if day.weekday() == Weekday::Mon && start >= from {
                    return Ok(Some(start));
                }
                day = day.succ_opt().unwrap();
            }
        }
    }

    #[test]
    fn a_window_is_open_once_or_every_time_its_cron_fires() {
        let once = WindowTime::Once {
            starts_at: at("2026-10-25T02:00:00Z"),
            ends_at: at("2026-10-25T04:00:00Z"),
        };
        assert_eq!(once.open_until(&Mondays, at("2026-10-25T01:59:00Z")), None);
        assert_eq!(
            once.open_until(&Mondays, at("2026-10-25T03:00:00Z")),
            Some(at("2026-10-25T04:00:00Z"))
        );
        assert_eq!(once.open_until(&Mondays, at("2026-10-25T04:00:00Z")), None);

        // Mondays at 03:00 UTC (26 October 2026 is a Monday), for 2 hours.
        let weekly = WindowTime::Recurrent {
            cron: "weekly".into(),
            timezone: "UTC".into(),
            duration_minutes: 120,
        };
        let now = at("2026-10-25T00:00:00Z");
        assert!(weekly.validate(&Mondays, now).is_ok());
        assert_eq!(
            weekly.open_until(&Mondays, at("2026-10-26T04:00:00Z")),
            Some(at("2026-10-26T05:00:00Z"))
        );
        assert_eq!(
            weekly.open_until(&Mondays, at("2026-10-26T05:00:00Z")),
            None
        );
        assert_eq!(
            weekly.open_until(&Mondays, at("2026-10-25T04:00:00Z")),
            None
        );
        let next = weekly
            .next_open(&Mondays, at("2026-10-25T04:00:00Z"))
            .unwrap();
        assert_eq!((next.day(), next.hour()), (26, 3));
    }

    #[test]
    fn invalid_windows_are_refused() {
        let now = at("2026-10-25T00:00:00Z");
        let backwards = WindowTime::Once {
            starts_at: at("2026-10-25T04:00:00Z"),
            ends_at: at("2026-10-25T02:00:00Z"),
        };
        assert!(backwards.validate(&Mondays, now).is_err());
        let never = WindowTime::Recurrent {
            cron: "never".into(),
            timezone: "UTC".into(),
            duration_minutes: 60,
        };
        assert!(never.validate(&Mondays, now).is_err());
        let too_long = WindowTime::Recurrent {
            cron: "weekly".into(),
            timezone: "UTC".into(),
            duration_minutes: 10_081,
        };
        assert!(too_long.validate(&Mondays, now).is_err());
        let nameless = MaintenanceWindow {
            id: "w".into(),
            name: " ".into(),
            time: WindowTime::Once {
                starts_at: now,
                ends_at: now + Duration::hours(1),
            },
            on_schedule: OnSchedule::Queue,
            enabled: true,
            clusters: vec!["c1".into()],
        };
        assert!(nameless.validate(&Mondays, now).is_err());
        let empty = MaintenanceWindow {
            name: "upgrade".into(),
            clusters: vec![],
            ..nameless
        };
        assert!(empty.validate(&Mondays, now).is_err());
    }

    #[test]
    fn a_cluster_in_two_windows_stays_until_the_last_ends() {
        let window = |id: &str, ends: &str, skip: bool| MaintenanceWindow {
            id: id.into(),
            name: id.into(),
            time: WindowTime::Once {
                starts_at: at("2026-10-25T00:00:00Z"),
                ends_at: at(ends),
            },
            on_schedule: if skip {
                OnSchedule::Skip
            } else {
                OnSchedule::Queue
            },
            enabled: true,
            clusters: vec!["c1".into()],
        };
        let open = open_windows(
            &[
                window("a", "2026-10-25T02:00:00Z", true),
                window("b", "2026-10-25T03:00:00Z", false),
            ],
            &Mondays,
            at("2026-10-25T01:00:00Z"),
        );
        assert_eq!(
            open,
            [OpenWindow {
                cluster: "c1".into(),
                window_id: "b".into(),
                until: at("2026-10-25T03:00:00Z"),
                skips: true,
            }]
        );
        let closed = open_windows(
            &[window("a", "2026-10-25T02:00:00Z", true)],
            &Mondays,
            at("2026-10-25T02:00:00Z"),
        );
        assert!(closed.is_empty());
        let mut off = window("a", "2026-10-25T02:00:00Z", true);
        off.enabled = false;
        assert!(open_windows(&[off], &Mondays, at("2026-10-25T01:00:00Z")).is_empty());
    }
}
