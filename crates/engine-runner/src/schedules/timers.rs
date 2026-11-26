//! systemd's timers: each timer `systemctl list-timers --all` lists, the
//! service it activates and that service's command. A timer's calendar
//! (`OnCalendar`, as `systemctl show` normalises it) becomes a 5-field
//! cron when it has one; a timer relative to boot or activation has none.
//! A timer is disabled by `systemctl disable --now`.

use std::collections::BTreeMap;
use std::io;
use std::process::Command;

use engine_port::{FoundCalendar, FoundSchedule, ScheduleSource};

use super::{split_words, StartReader};

/// What the scanner asks of systemd.
pub trait Systemctl: Send + Sync {
    /// Every timer loaded, with the unit it activates.
    fn timers(&self) -> io::Result<Vec<(String, String)>>;
    /// The properties `names` of `unit`, as `systemctl show` writes them.
    fn show(&self, unit: &str, names: &[&str]) -> io::Result<BTreeMap<String, String>>;
    /// `systemctl disable --now <unit>`.
    fn disable_now(&self, unit: &str) -> io::Result<()>;
}

/// The cluster's own `systemctl`.
#[derive(Debug, Clone, Copy, Default)]
pub struct SystemSystemctl;

fn systemctl(args: &[&str]) -> io::Result<String> {
    let out = Command::new("systemctl").args(args).output()?;
    if !out.status.success() {
        return Err(io::Error::other(format!(
            "systemctl {} exited with {}",
            args.first().unwrap_or(&""),
            out.status
        )));
    }
    String::from_utf8(out.stdout).map_err(io::Error::other)
}

impl Systemctl for SystemSystemctl {
    fn timers(&self) -> io::Result<Vec<(String, String)>> {
        let text = systemctl(&[
            "list-timers",
            "--all",
            "--plain",
            "--no-legend",
            "--no-pager",
        ])?;
        Ok(text
            .lines()
            .filter_map(|l| {
                let mut words = l.split_whitespace().rev();
                let activates = words.next()?;
                let unit = words.next()?;
                unit.ends_with(".timer")
                    .then(|| (unit.to_string(), activates.to_string()))
            })
            .collect())
    }

    fn show(&self, unit: &str, names: &[&str]) -> io::Result<BTreeMap<String, String>> {
        let mut args = vec!["show", "--no-pager"];
        for n in names {
            args.extend(["-p", n]);
        }
        args.push(unit);
        Ok(systemctl(&args)?
            .lines()
            .filter_map(|l| l.split_once('='))
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect())
    }

    fn disable_now(&self, unit: &str) -> io::Result<()> {
        systemctl(&["disable", "--now", "--no-pager", unit]).map(drop)
    }
}

/// The values of `key=` inside a `{ … ; … }` property, one per entry.
fn entries<'a>(property: &'a str, key: &str) -> Vec<&'a str> {
    let tag = format!("{key}=");
    let mut out = Vec::new();
    for part in property.split(['{', '}', ';']) {
        if let Some(value) = part.trim().strip_prefix(&tag) {
            out.push(value.trim());
        }
    }
    out
}

const DAYS: [&str; 7] = ["sun", "mon", "tue", "wed", "thu", "fri", "sat"];

fn number(raw: &str, days: bool) -> Option<u32> {
    if days {
        let lower = raw.to_ascii_lowercase();
        let i = DAYS.iter().position(|d| lower.starts_with(d))?;
        return u32::try_from(i).ok();
    }
    raw.parse().ok()
}

/// One field of a systemd calendar as cron writes it (`a..b` → `a-b`).
fn field(raw: &str, days: bool) -> Option<String> {
    let mut out = Vec::new();
    for item in raw.split(',') {
        let (base, step) = match item.split_once('/') {
            Some((b, s)) => (b, Some(s.parse::<u32>().ok()?)),
            None => (item, None),
        };
        let base = if base == "*" {
            "*".to_string()
        } else if let Some((a, b)) = base.split_once("..") {
            format!("{}-{}", number(a, days)?, number(b, days)?)
        } else {
            number(base, days)?.to_string()
        };
        out.push(match step {
            Some(s) => format!("{base}/{s}"),
            None => base,
        });
    }
    Some(out.join(","))
}

/// A normalised `OnCalendar` (`[days] Y-M-D H:M:S [zone]`) as a 5-field
/// cron and its zone; `None` when cron has no equivalent (a year, seconds,
/// the last days of a month).
#[must_use]
pub fn calendar_to_cron(calendar: &str) -> Option<(String, Option<String>)> {
    let words: Vec<&str> = calendar.split_whitespace().collect();
    let (days, rest) = match words.first() {
        Some(w) if w.starts_with(|c: char| c.is_ascii_alphabetic()) => (Some(*w), &words[1..]),
        _ => (None, &words[..]),
    };
    let (date, time, zone) = match rest {
        [d, t] => (*d, *t, None),
        [d, t, z] => (*d, *t, Some((*z).to_string())),
        _ => return None,
    };
    let date: Vec<&str> = date.split('-').collect();
    let time: Vec<&str> = time.split(':').collect();
    let ([year, month, day], [hour, minute, second]) = (date.as_slice(), time.as_slice()) else {
        return None;
    };
    if *year != "*" || !matches!(*second, "00" | "0") || day.contains('~') {
        return None;
    }
    let weekday = days.map_or(Some("*".to_string()), |d| field(d, true))?;
    let cron = [
        field(minute, false)?,
        field(hour, false)?,
        field(day, false)?,
        field(month, false)?,
        weekday,
    ]
    .join(" ");
    Some((cron, zone))
}

/// The timers whose service starts the engine.
#[must_use]
pub fn scan_timers(
    systemd: &dyn Systemctl,
    reader: &dyn StartReader,
    timezone: Option<&str>,
) -> Vec<FoundSchedule> {
    let Ok(timers) = systemd.timers() else {
        return Vec::new();
    };
    let mut found = Vec::new();
    for (timer, service) in timers {
        let Ok(svc) = systemd.show(&service, &["ExecStart", "User"]) else {
            continue;
        };
        let exec = svc.get("ExecStart").map_or("", String::as_str);
        let Some(start) = entries(exec, "argv[]")
            .first()
            .and_then(|argv| split_words(argv))
            .and_then(|argv| reader.read(&argv))
        else {
            continue;
        };
        let Ok(t) = systemd.show(&timer, &["TimersCalendar", "TimersMonotonic", "Persistent"])
        else {
            continue;
        };
        let get = |k: &str| t.get(k).map_or("", String::as_str);
        let mut calendars: Vec<(FoundCalendar, Option<String>)> =
            entries(get("TimersCalendar"), "OnCalendar")
                .into_iter()
                .map(|c| match calendar_to_cron(c) {
                    Some((cron, zone)) => (FoundCalendar::Cron(cron), zone),
                    None => (FoundCalendar::Unsupported("calendar".into()), None),
                })
                .collect();
        if !get("TimersMonotonic").trim().is_empty() {
            calendars.push((FoundCalendar::Unsupported("boot_relative".into()), None));
        }
        let user = svc
            .get("User")
            .map(String::as_str)
            .filter(|u| !u.is_empty());
        for (calendar, zone) in calendars {
            found.push(FoundSchedule {
                source: ScheduleSource::Timer {
                    unit: timer.clone(),
                },
                user: user.unwrap_or("root").to_string(),
                calendar,
                timezone: zone.or_else(|| timezone.map(str::to_string)),
                artefact: start.artefact.clone(),
                parameters: start.parameters.clone(),
                persistent: get("Persistent") == "yes",
            });
        }
    }
    found
}

/// Stops and disables `unit`, a timer.
pub fn disable_timer(systemd: &dyn Systemctl, unit: &str) -> io::Result<()> {
    if !unit.ends_with(".timer") {
        return Err(io::Error::other("not a timer"));
    }
    systemd.disable_now(unit)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn calendars_with_a_cron_equivalent_convert() {
        let cron = |c: &str| calendar_to_cron(c).map(|(c, _)| c);
        assert_eq!(cron("*-*-* 06:00:00").as_deref(), Some("0 6 * * *"));
        assert_eq!(
            cron("Mon..Fri *-*-* 06:30:00").as_deref(),
            Some("30 6 * * 1-5")
        );
        assert_eq!(
            cron("Mon,Wed *-*-01 00:00:00").as_deref(),
            Some("0 0 1 * 1,3")
        );
        assert_eq!(cron("*-*-* *:00/15:00").as_deref(), Some("0/15 * * * *"));
        let zoned = calendar_to_cron("*-*-* 06:00:00 Europe/Madrid").unwrap();
        assert_eq!(zoned.1.as_deref(), Some("Europe/Madrid"));
        for none in [
            "2026-*-* 06:00:00",
            "*-*-* 06:00:30",
            "*-*~01 00:00:00",
            "garbage",
        ] {
            assert_eq!(calendar_to_cron(none), None, "{none}");
        }
    }

    #[test]
    fn entries_are_read_from_a_show_property() {
        let exec = "{ path=/opt/e/run ; argv[]=/opt/e/run -a b ; ignore_errors=no }";
        assert_eq!(entries(exec, "argv[]"), ["/opt/e/run -a b"]);
        let two = "{ OnCalendar=*-*-* 06:00:00 ; next_elapse=x } { OnCalendar=Mon *-*-* 07:00:00 ; next_elapse=y }";
        assert_eq!(
            entries(two, "OnCalendar"),
            ["*-*-* 06:00:00", "Mon *-*-* 07:00:00"]
        );
    }
}
