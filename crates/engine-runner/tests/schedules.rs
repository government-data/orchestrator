//! The Linux schedules scanner on a cluster's files laid out in a folder
//! and a fake systemd: what it reads from cron and timers, and how it
//! disables them.

use std::collections::BTreeMap;
use std::fs;
use std::io;
use std::path::PathBuf;
use std::sync::Mutex;

use engine_port::{FoundCalendar, ScheduleSource};
use engine_runner::schedules::*;
use serde_json::{json, Map};

/// A fake engine: `/opt/engine/run -artefact=<path> [-var=k=v]…`.
struct Reader;

impl StartReader for Reader {
    fn read(&self, argv: &[String]) -> Option<Start> {
        if argv.first()? != "/opt/engine/run" {
            return None;
        }
        let mut start = Start {
            artefact: String::new(),
            parameters: Map::new(),
        };
        for arg in &argv[1..] {
            if let Some(path) = arg.strip_prefix("-artefact=") {
                start.artefact = path.to_string();
            } else if let Some((k, v)) = arg.strip_prefix("-var=").and_then(|kv| kv.split_once('='))
            {
                start.parameters.insert(k.into(), json!(v));
            }
        }
        (!start.artefact.is_empty()).then_some(start)
    }
}

fn cluster(name: &str) -> Root {
    let dir = std::env::temp_dir().join(format!("qo-test-sched-{name}-{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    for sub in ["etc/cron.d", "var/spool/cron/crontabs"] {
        fs::create_dir_all(dir.join(sub)).unwrap();
    }
    fs::write(dir.join("etc/timezone"), "America/Bogota\n").unwrap();
    Root(dir)
}

fn write(root: &Root, path: &str, text: &str) -> PathBuf {
    let file = root.join(path);
    fs::write(&file, text).unwrap();
    file
}

#[test]
fn cron_lines_that_start_the_engine_are_read() {
    let root = cluster("read");
    write(
        &root,
        "/etc/crontab",
        "SHELL=/bin/sh\n17 * * * * root cd / && run-parts /etc/cron.hourly\n",
    );
    write(
        &root,
        "/etc/cron.d/flows",
        "# flows\n0 6 * * 1-5 svc /opt/engine/run -artefact=/srv/a -var=region=north\n@reboot svc /opt/engine/run -artefact=/srv/b\n",
    );
    write(
        &root,
        "/etc/cron.d/flows.disabled",
        "0 1 * * * svc /opt/engine/run -artefact=/srv/x\n",
    );
    write(
        &root,
        "/var/spool/cron/crontabs/ana",
        "30 2 * * * /opt/engine/run '-artefact=/srv/c d'\n",
    );
    let found = scan_cron(&root, &Reader);
    let got: Vec<_> = found
        .iter()
        .map(|f| {
            (
                f.source.clone(),
                f.user.as_str(),
                f.calendar.clone(),
                f.artefact.as_str(),
            )
        })
        .collect();
    let cron = |file: &str, line| ScheduleSource::Cron {
        file: file.into(),
        line,
    };
    assert_eq!(
        got,
        vec![
            (
                cron("/etc/cron.d/flows", 2),
                "svc",
                FoundCalendar::Cron("0 6 * * 1-5".into()),
                "/srv/a"
            ),
            (
                cron("/etc/cron.d/flows", 3),
                "svc",
                FoundCalendar::Unsupported("reboot".into()),
                "/srv/b"
            ),
            (
                cron("/var/spool/cron/crontabs/ana", 1),
                "ana",
                FoundCalendar::Cron("30 2 * * *".into()),
                "/srv/c d"
            ),
        ]
    );
    assert_eq!(found[0].parameters["region"], "north");
    assert_eq!(found[0].timezone.as_deref(), Some("America/Bogota"));
    assert!(found.iter().all(|f| f.problem().is_none()));
}

#[test]
fn a_cron_line_is_disabled_once_with_its_marker() {
    let root = cluster("disable");
    let file = write(
        &root,
        "/etc/cron.d/flows",
        "0 6 * * * svc /opt/engine/run -artefact=/srv/a\n0 7 * * * svc /opt/engine/run -artefact=/srv/b\n",
    );
    disable_cron(&root, "/etc/cron.d/flows", 1, "/srv/a", &Reader).unwrap();
    disable_cron(&root, "/etc/cron.d/flows", 1, "/srv/a", &Reader).unwrap();
    let text = fs::read_to_string(&file).unwrap();
    assert_eq!(
        text,
        format!("{DISABLED_MARKER}0 6 * * * svc /opt/engine/run -artefact=/srv/a\n0 7 * * * svc /opt/engine/run -artefact=/srv/b\n")
    );
    assert_eq!(
        scan_cron(&root, &Reader).len(),
        1,
        "the disabled line is not read"
    );
    // A line that now starts something else, or a file cron does not read.
    assert!(disable_cron(&root, "/etc/cron.d/flows", 2, "/srv/a", &Reader).is_err());
    assert!(disable_cron(&root, "/etc/cron.d/flows", 9, "/srv/b", &Reader).is_err());
    assert!(disable_cron(&root, "/etc/passwd", 1, "/srv/b", &Reader).is_err());
    let left: Vec<_> = fs::read_dir(root.join("/etc/cron.d"))
        .unwrap()
        .flatten()
        .collect();
    assert_eq!(left.len(), 1, "no temporary file left");
}

/// A fake systemd with one timer per entry.
#[derive(Default)]
struct Systemd {
    units: BTreeMap<String, BTreeMap<String, String>>,
    disabled: Mutex<Vec<String>>,
}

impl Systemd {
    fn timer(mut self, name: &str, calendar: &str, monotonic: &str, exec: &str) -> Self {
        let props = |pairs: &[(&str, &str)]| {
            pairs
                .iter()
                .map(|(k, v)| ((*k).into(), (*v).into()))
                .collect()
        };
        self.units.insert(
            format!("{name}.timer"),
            props(&[
                ("TimersCalendar", calendar),
                ("TimersMonotonic", monotonic),
                ("Persistent", "yes"),
            ]),
        );
        let argv = format!("{{ path=/opt/engine/run ; argv[]={exec} ; ignore_errors=no }}");
        self.units.insert(
            format!("{name}.service"),
            props(&[("ExecStart", &argv), ("User", "svc")]),
        );
        self
    }
}

impl Systemctl for Systemd {
    fn timers(&self) -> io::Result<Vec<(String, String)>> {
        Ok(self
            .units
            .keys()
            .filter_map(|u| u.strip_suffix(".timer"))
            .map(|n| (format!("{n}.timer"), format!("{n}.service")))
            .collect())
    }

    fn show(&self, unit: &str, names: &[&str]) -> io::Result<BTreeMap<String, String>> {
        let props = self
            .units
            .get(unit)
            .ok_or_else(|| io::Error::other("no unit"))?;
        Ok(props
            .iter()
            .filter(|(k, _)| names.contains(&k.as_str()))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect())
    }

    fn disable_now(&self, unit: &str) -> io::Result<()> {
        self.disabled.lock().unwrap().push(unit.into());
        Ok(())
    }
}

#[test]
fn timers_that_start_the_engine_are_read_and_disabled() {
    let systemd = Systemd::default()
        .timer(
            "close",
            "{ OnCalendar=Mon..Fri *-*-* 06:00:00 ; next_elapse=x }",
            "",
            "/opt/engine/run -artefact=/srv/a",
        )
        .timer(
            "boot",
            "",
            "{ OnBootUSec=5min ; next_elapse=0 }",
            "/opt/engine/run -artefact=/srv/b",
        )
        .timer(
            "other",
            "{ OnCalendar=*-*-* 01:00:00 ; next_elapse=x }",
            "",
            "/usr/bin/logrotate",
        );
    let found = scan_timers(&systemd, &Reader, Some("UTC"));
    let got: Vec<_> = found
        .iter()
        .map(|f| (f.source.clone(), f.calendar.clone(), f.artefact.as_str()))
        .collect();
    let timer = |unit: &str| ScheduleSource::Timer { unit: unit.into() };
    assert_eq!(
        got,
        vec![
            (
                timer("boot.timer"),
                FoundCalendar::Unsupported("boot_relative".into()),
                "/srv/b"
            ),
            (
                timer("close.timer"),
                FoundCalendar::Cron("0 6 * * 1-5".into()),
                "/srv/a"
            ),
        ]
    );
    assert!(found.iter().all(|f| f.persistent && f.user == "svc"));
    disable_timer(&systemd, "close.timer").unwrap();
    assert!(disable_timer(&systemd, "close.service").is_err());
    assert_eq!(*systemd.disabled.lock().unwrap(), ["close.timer"]);
}
