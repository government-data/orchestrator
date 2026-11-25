use super::*;
use chrono::NaiveTime;

const BOGOTA: Tz = chrono_tz::America::Bogota;

fn local(y: i32, m: u32, d: u32, h: u32, mi: u32) -> NaiveDateTime {
    NaiveDate::from_ymd_opt(y, m, d)
        .unwrap()
        .and_time(NaiveTime::from_hms_opt(h, mi, 0).unwrap())
}

/// Bogotá time (UTC-5, no clock changes) in UTC.
fn bog(y: i32, m: u32, d: u32, h: u32, mi: u32) -> DateTime<Utc> {
    localize(BOGOTA, local(y, m, d, h, mi)).unwrap()
}

fn trigger(start: NaiveDateTime, recurrence: Recurrence) -> ScheduleTrigger {
    ScheduleTrigger {
        start,
        recurrence,
        repeat: None,
        random_delay_secs: 0,
        expires: None,
    }
}

/// The next `n` fires from `from`.
fn fires(t: &ScheduleTrigger, from: DateTime<Utc>, n: usize) -> Vec<DateTime<Utc>> {
    let mut out = Vec::new();
    let mut at = from;
    while out.len() < n {
        match next_trigger(t, BOGOTA, at) {
            Some(next) => {
                out.push(next);
                at = next;
            }
            None => break,
        }
    }
    out
}

#[test]
fn once_fires_a_single_time() {
    let t = trigger(local(2026, 7, 9, 9, 27), Recurrence::Once);
    assert_eq!(
        fires(&t, bog(2026, 7, 1, 0, 0), 3),
        [bog(2026, 7, 9, 9, 27)]
    );
    assert_eq!(next_trigger(&t, BOGOTA, bog(2026, 7, 9, 9, 27)), None);
}

#[test]
fn every_two_days_counts_from_the_start_across_months() {
    let t = trigger(local(2026, 1, 30, 6, 0), Recurrence::Daily { every: 2 });
    assert_eq!(
        fires(&t, bog(2026, 1, 1, 0, 0), 3),
        [
            bog(2026, 1, 30, 6, 0),
            bog(2026, 2, 1, 6, 0),
            bog(2026, 2, 3, 6, 0)
        ]
    );
}

#[test]
fn every_two_weeks_on_monday_and_thursday() {
    // Thursday 9 July 2026: the week of Monday the 6th.
    let t = trigger(
        local(2026, 7, 9, 8, 0),
        Recurrence::Weekly {
            every: 2,
            days: vec![1, 4],
        },
    );
    assert_eq!(
        fires(&t, bog(2026, 7, 1, 0, 0), 4),
        [
            bog(2026, 7, 9, 8, 0),
            bog(2026, 7, 20, 8, 0),
            bog(2026, 7, 23, 8, 0),
            bog(2026, 8, 3, 8, 0)
        ]
    );
}

#[test]
fn monthly_on_the_last_day_and_on_the_first_monday() {
    let last_day = trigger(
        local(2026, 1, 1, 23, 0),
        Recurrence::Monthly {
            months: (1..=12).collect(),
            days: vec![32],
            nth: vec![],
            weekdays: vec![],
        },
    );
    assert_eq!(
        fires(&last_day, bog(2026, 1, 1, 0, 0), 3),
        [
            bog(2026, 1, 31, 23, 0),
            bog(2026, 2, 28, 23, 0),
            bog(2026, 3, 31, 23, 0)
        ]
    );
    let first_monday = trigger(
        local(2026, 1, 1, 7, 0),
        Recurrence::Monthly {
            months: vec![3, 6],
            days: vec![],
            nth: vec![1],
            weekdays: vec![1],
        },
    );
    assert_eq!(
        fires(&first_monday, bog(2026, 1, 1, 0, 0), 2),
        [bog(2026, 3, 2, 7, 0), bog(2026, 6, 1, 7, 0)]
    );
    let last_friday = trigger(
        local(2026, 1, 1, 7, 0),
        Recurrence::Monthly {
            months: vec![10],
            days: vec![],
            nth: vec![5],
            weekdays: vec![5],
        },
    );
    assert_eq!(
        next_trigger(&last_friday, BOGOTA, bog(2026, 1, 1, 0, 0)),
        Some(bog(2026, 10, 30, 7, 0))
    );
}

#[test]
fn repeats_every_fifteen_minutes_for_an_hour_each_day() {
    let mut t = trigger(local(2026, 7, 9, 8, 0), Recurrence::Daily { every: 1 });
    t.repeat = Some(Repeat {
        every_minutes: 15,
        for_minutes: Some(60),
    });
    let got = fires(&t, bog(2026, 7, 9, 8, 50), 4);
    assert_eq!(
        got,
        [
            bog(2026, 7, 9, 9, 0),
            bog(2026, 7, 10, 8, 0),
            bog(2026, 7, 10, 8, 15),
            bog(2026, 7, 10, 8, 30)
        ]
    );
}

#[test]
fn repeats_indefinitely_and_stops_at_expiration() {
    let mut t = trigger(local(2026, 7, 9, 8, 0), Recurrence::Once);
    t.repeat = Some(Repeat {
        every_minutes: 30,
        for_minutes: None,
    });
    t.expires = Some(local(2026, 7, 9, 9, 0));
    assert_eq!(
        fires(&t, bog(2026, 7, 9, 7, 0), 5),
        [
            bog(2026, 7, 9, 8, 0),
            bog(2026, 7, 9, 8, 30),
            bog(2026, 7, 9, 9, 0)
        ]
    );
}

#[test]
fn a_date_that_never_comes_never_fires() {
    let t = trigger(
        local(2026, 1, 1, 7, 0),
        Recurrence::Monthly {
            months: vec![2],
            days: vec![30],
            nth: vec![],
            weekdays: vec![],
        },
    );
    assert_eq!(next_trigger(&t, BOGOTA, bog(2026, 1, 1, 0, 0)), None);
}

#[test]
fn a_skipped_hour_is_skipped_and_the_next_day_fires() {
    // New York moves the clock forward on 8 March 2026 at 02:00.
    let ny: Tz = chrono_tz::America::New_York;
    let t = trigger(local(2026, 3, 7, 2, 30), Recurrence::Daily { every: 1 });
    let after = Utc.with_ymd_and_hms(2026, 3, 7, 12, 0, 0).unwrap();
    let next = next_trigger(&t, ny, after).unwrap();
    assert_eq!(
        next.with_timezone(&ny).naive_local(),
        local(2026, 3, 9, 2, 30)
    );
}

#[test]
fn the_random_delay_is_stable_and_bounded() {
    let at = bog(2026, 7, 9, 8, 0);
    let a = random_delay("schedule-1", at, 300);
    assert_eq!(a, random_delay("schedule-1", at, 300));
    assert!(a <= Duration::seconds(300));
    assert_eq!(random_delay("x", at, 0), Duration::zero());
}

#[test]
fn validation_explains_what_is_wrong() {
    let mut t = trigger(
        local(2026, 7, 9, 8, 0),
        Recurrence::Weekly {
            every: 1,
            days: vec![],
        },
    );
    assert!(validate_trigger(&t).unwrap_err().contains("weekday"));
    t.recurrence = Recurrence::Daily { every: 1 };
    t.expires = Some(local(2026, 7, 1, 0, 0));
    assert!(validate_trigger(&t).unwrap_err().contains("expiry"));
    t.expires = None;
    t.repeat = Some(Repeat {
        every_minutes: 30,
        for_minutes: Some(10),
    });
    assert!(validate_trigger(&t).is_err());
}
