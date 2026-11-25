use super::*;
use chrono::{Duration, NaiveDate, TimeZone};
use orchestration_model::{fires_to_run, MissedRuns, Settings, WindowTime};

use crate::Recurrence;

fn utc(y: i32, mo: u32, d: u32, h: u32, mi: u32) -> DateTime<Utc> {
    Utc.with_ymd_and_hms(y, mo, d, h, mi, 0).unwrap()
}

fn at(s: &str) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(s).unwrap().with_timezone(&Utc)
}

fn cron(expr: &str, zone: &str) -> ScheduleSpec {
    ScheduleSpec {
        cron: expr.into(),
        timezone: zone.into(),
        trigger: None,
    }
}

#[test]
fn evaluates_in_the_schedule_timezone() {
    // 06:00 in Bogotá (UTC-5) is 11:00 UTC.
    let next = next_fire("0 6 * * *", "America/Bogota", utc(2026, 10, 16, 12, 0)).unwrap();
    assert_eq!(next, utc(2026, 10, 17, 11, 0));
}

#[test]
fn fires_on_the_whole_second() {
    let from = utc(2026, 10, 16, 12, 0) + Duration::milliseconds(908);
    let next = next_fire("* * * * *", "UTC", from).unwrap();
    assert_eq!(next, utc(2026, 10, 16, 12, 1));
    assert_eq!(next.nanosecond(), 0);
}

#[test]
fn next_is_strictly_after() {
    let next = next_fire("0 6 * * *", "America/Bogota", utc(2026, 10, 16, 11, 0)).unwrap();
    assert_eq!(next, utc(2026, 10, 17, 11, 0));
}

#[test]
fn weekdays_skip_the_weekend() {
    // Friday 16 October 2026 at noon → Monday the 19th at 06:00.
    let next = next_fire("0 6 * * 1-5", "America/Bogota", utc(2026, 10, 16, 17, 0)).unwrap();
    assert_eq!(next, utc(2026, 10, 19, 11, 0));
}

#[test]
fn rejects_bad_input() {
    let now = utc(2026, 10, 16, 0, 0);
    let check = |expr: &str, zone: &str| cron(expr, zone).validate("s", now);
    assert!(matches!(
        check("0 0 6 * * *", "UTC"),
        Err(ScheduleError::Cron(_))
    ));
    assert!(matches!(
        check("61 * * * *", "UTC"),
        Err(ScheduleError::Cron(_))
    ));
    assert!(matches!(
        check("0 6 * * *", "Mars/Olympus"),
        Err(ScheduleError::Timezone(_))
    ));
    assert_eq!(check("0 6 * * *", "UTC"), Ok(utc(2026, 10, 16, 6, 0)));
}

#[test]
fn a_trigger_wins_over_the_cron_with_its_random_delay() {
    let start = NaiveDate::from_ymd_opt(2026, 7, 9)
        .unwrap()
        .and_hms_opt(8, 0, 0)
        .unwrap();
    let spec = ScheduleSpec {
        cron: String::new(),
        timezone: "America/Bogota".into(),
        trigger: Some(ScheduleTrigger {
            start,
            recurrence: Recurrence::Daily { every: 1 },
            repeat: None,
            random_delay_secs: 300,
            expires: None,
        }),
    };
    let after = utc(2026, 7, 9, 0, 0);
    let first = spec.validate("schedule-1", after).unwrap();
    let base = utc(2026, 7, 9, 13, 0);
    assert!(first >= base && first <= base + Duration::seconds(300));
    assert_eq!(
        spec.next_after("schedule-1", after),
        Ok(Some(first)),
        "stable"
    );
    let mut bad = spec.clone();
    bad.trigger.as_mut().unwrap().recurrence = Recurrence::Daily { every: 0 };
    assert!(matches!(
        bad.validate("s", after),
        Err(ScheduleError::Trigger(_))
    ));
}

#[test]
fn missed_fires_walk_the_real_cron() {
    // Every hour on the hour in Bogotá; down from 10:00 to 15:30 UTC.
    let spec = cron("0 * * * *", "America/Bogota");
    let next = |t| spec.next_after("s", t).ok().flatten();
    let all = Settings {
        missed_runs: MissedRuns::All,
        missed_max_runs: 3,
        ..Default::default()
    };
    let got = fires_to_run(
        next,
        at("2026-10-25T10:00:00Z"),
        at("2026-10-25T15:30:00Z"),
        &all,
    );
    assert_eq!(
        got,
        [
            at("2026-10-25T13:00:00Z"),
            at("2026-10-25T14:00:00Z"),
            at("2026-10-25T15:00:00Z")
        ]
    );
}

#[test]
fn a_recurrent_window_opens_with_its_cron() {
    // Sundays at 22:00 Bogotá (03:00 UTC on Monday), for 2 hours.
    let weekly = WindowTime::Recurrent {
        cron: "0 22 * * 0".into(),
        timezone: "America/Bogota".into(),
        duration_minutes: 120,
    };
    assert!(weekly
        .validate(&Calendar, at("2026-10-25T00:00:00Z"))
        .is_ok());
    assert_eq!(
        weekly.open_until(&Calendar, at("2026-10-26T04:00:00Z")),
        Some(at("2026-10-26T05:00:00Z"))
    );
    assert_eq!(
        weekly.open_until(&Calendar, at("2026-10-26T05:00:00Z")),
        None
    );
    assert_eq!(
        weekly.open_until(&Calendar, at("2026-10-25T04:00:00Z")),
        None
    );
    assert_eq!(
        weekly.next_open(&Calendar, at("2026-10-25T04:00:00Z")),
        Some(at("2026-10-26T03:00:00Z"))
    );
    let never = WindowTime::Recurrent {
        cron: "never".into(),
        timezone: "America/Bogota".into(),
        duration_minutes: 60,
    };
    assert!(never
        .validate(&Calendar, at("2026-10-25T00:00:00Z"))
        .is_err());
}
