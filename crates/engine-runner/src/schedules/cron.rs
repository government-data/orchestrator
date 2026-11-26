//! Cron's schedules: `/etc/crontab` and `/etc/cron.d/*` (with a user
//! field) and users' crontabs (`/var/spool/cron/crontabs/<user>` on Debian
//! and Ubuntu, `/var/spool/cron/<user>` elsewhere). A line is disabled by
//! commenting it with a marker, the file replaced whole with its owner and
//! mode kept, so cron reads it again by its new modification time.

use std::fs;
use std::io;
use std::os::unix::fs::{chown, MetadataExt, PermissionsExt};
use std::path::Path;

use engine_port::{FoundCalendar, FoundSchedule, ScheduleSource};

use super::{split_words, Root, Start, StartReader};

/// What a disabled line starts with: the line follows it as it was.
pub const DISABLED_MARKER: &str = "# qd-orchestrator disabled: ";

/// A cron line read: its calendar, its user and its command.
#[derive(Debug, PartialEq)]
struct Line {
    calendar: FoundCalendar,
    user: String,
    command: String,
}

fn is_assignment(line: &str) -> bool {
    let name = line.split('=').next().unwrap_or("");
    line.contains('=')
        && !name.is_empty()
        && name
            .trim_end()
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_')
        && !name.starts_with(|c: char| c.is_ascii_digit())
}

fn keyword(word: &str) -> Option<FoundCalendar> {
    let cron = match word {
        "@reboot" => return Some(FoundCalendar::Unsupported("reboot".into())),
        "@yearly" | "@annually" => "0 0 1 1 *",
        "@monthly" => "0 0 1 * *",
        "@weekly" => "0 0 * * 0",
        "@daily" | "@midnight" => "0 0 * * *",
        "@hourly" => "0 * * * *",
        _ => return None,
    };
    Some(FoundCalendar::Cron(cron.into()))
}

/// Takes `n` words off the front of `rest`.
fn take_words(rest: &str, n: usize) -> Option<(Vec<&str>, &str)> {
    let mut words = Vec::with_capacity(n);
    let mut rest = rest.trim_start();
    for _ in 0..n {
        let end = rest.find([' ', '\t'])?;
        words.push(&rest[..end]);
        rest = rest[end..].trim_start();
    }
    Some((words, rest))
}

/// The command as cron runs it: up to its first `%` not escaped.
fn command_of(raw: &str) -> String {
    let mut out = String::new();
    let mut chars = raw.chars().peekable();
    while let Some(c) = chars.next() {
        match c {
            '\\' if chars.peek() == Some(&'%') => out.push(chars.next().unwrap_or('%')),
            '%' => break,
            other => out.push(other),
        }
    }
    out.trim().to_string()
}

fn parse_line(raw: &str, user: Option<&str>) -> Option<Line> {
    let line = raw.trim();
    if line.is_empty() || line.starts_with('#') || is_assignment(line) {
        return None;
    }
    let (calendar, rest) = if line.starts_with('@') {
        let (words, rest) = take_words(line, 1)?;
        (keyword(words[0])?, rest)
    } else {
        let (words, rest) = take_words(line, 5)?;
        (FoundCalendar::Cron(words.join(" ")), rest)
    };
    let (user, rest) = match user {
        Some(u) => (u.to_string(), rest),
        None => {
            let (words, rest) = take_words(rest, 1)?;
            (words[0].to_string(), rest)
        }
    };
    let command = command_of(rest);
    (!command.is_empty()).then_some(Line {
        calendar,
        user,
        command,
    })
}

fn start_of(line: &Line, reader: &dyn StartReader) -> Option<Start> {
    reader.read(&split_words(&line.command)?)
}

/// cron reads only files of these names in `/etc/cron.d` (run-parts's).
fn is_cron_d_name(name: &str) -> bool {
    !name.is_empty()
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
}

/// Every crontab on the cluster, with the user it runs as when the file
/// names none.
fn crontabs(root: &Root) -> Vec<(String, Option<String>)> {
    let mut files = vec![("/etc/crontab".to_string(), None)];
    let listed = |dir: &str| -> Vec<String> {
        let mut names: Vec<String> = fs::read_dir(root.join(dir))
            .into_iter()
            .flatten()
            .flatten()
            .filter(|e| e.file_type().is_ok_and(|t| t.is_file()))
            .filter_map(|e| e.file_name().into_string().ok())
            .collect();
        names.sort();
        names
    };
    for name in listed("/etc/cron.d") {
        if is_cron_d_name(&name) {
            files.push((format!("/etc/cron.d/{name}"), None));
        }
    }
    for dir in ["/var/spool/cron/crontabs", "/var/spool/cron"] {
        for name in listed(dir) {
            if !name.starts_with('.') && is_cron_d_name(&name) {
                files.push((format!("{dir}/{name}"), Some(name)));
            }
        }
    }
    files
}

/// The cron lines that start the engine.
#[must_use]
pub fn scan_cron(root: &Root, reader: &dyn StartReader) -> Vec<FoundSchedule> {
    let timezone = root.timezone();
    let mut found = Vec::new();
    for (file, user) in crontabs(root) {
        let Ok(text) = fs::read_to_string(root.join(&file)) else {
            continue;
        };
        for (i, raw) in text.lines().enumerate() {
            let Some(line) = parse_line(raw, user.as_deref()) else {
                continue;
            };
            let Some(start) = start_of(&line, reader) else {
                continue;
            };
            found.push(FoundSchedule {
                source: ScheduleSource::Cron {
                    file: file.clone(),
                    line: u32::try_from(i + 1).unwrap_or(u32::MAX),
                },
                user: line.user,
                calendar: line.calendar,
                timezone: timezone.clone(),
                artefact: start.artefact,
                parameters: start.parameters,
                persistent: false,
            });
        }
    }
    found
}

/// Comments line `line` of `file` with [`DISABLED_MARKER`], once it still
/// starts `artefact`; a line already disabled is left as it is.
pub fn disable_cron(
    root: &Root,
    file: &str,
    line: u32,
    artefact: &str,
    reader: &dyn StartReader,
) -> io::Result<()> {
    let user = crontabs(root)
        .into_iter()
        .find(|(f, _)| f == file)
        .ok_or_else(|| io::Error::other("not a crontab cron reads"))?
        .1;
    let path = root.join(file);
    let text = fs::read_to_string(&path)?;
    let mut lines: Vec<String> = text.lines().map(str::to_string).collect();
    let index = usize::try_from(line).unwrap_or(0).wrapping_sub(1);
    let current = lines
        .get(index)
        .ok_or_else(|| io::Error::other("the line is not there any more"))?;
    if current.starts_with(DISABLED_MARKER) {
        return Ok(());
    }
    let starts = parse_line(current, user.as_deref())
        .and_then(|l| start_of(&l, reader))
        .is_some_and(|s| s.artefact == artefact);
    if !starts {
        return Err(io::Error::other(
            "the line changed: it does not start the artefact",
        ));
    }
    lines[index] = format!("{DISABLED_MARKER}{current}");
    let mut out = lines.join("\n");
    out.push('\n');
    replace(&path, &out)
}

/// Writes `text` over `path` by a rename, keeping its owner and mode.
fn replace(path: &Path, text: &str) -> io::Result<()> {
    let meta = fs::metadata(path)?;
    let name = path
        .file_name()
        .and_then(|n| n.to_str())
        .ok_or_else(|| io::Error::other("no file name"))?;
    // A name cron skips (a dot), in the same folder so the rename is atomic.
    let tmp = path.with_file_name(format!(".{name}.qd-tmp"));
    fs::write(&tmp, text)?;
    fs::set_permissions(&tmp, fs::Permissions::from_mode(meta.mode() & 0o7777))?;
    let written = fs::metadata(&tmp)?;
    if (written.uid(), written.gid()) != (meta.uid(), meta.gid()) {
        if let Err(e) = chown(&tmp, Some(meta.uid()), Some(meta.gid())) {
            let _ = fs::remove_file(&tmp);
            return Err(e);
        }
    }
    fs::rename(&tmp, path)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_line_reads_its_calendar_user_and_command() {
        let line = parse_line("15 6 * * 1-5 svc /opt/e/run -a b % stdin", None).unwrap();
        assert_eq!(line.calendar, FoundCalendar::Cron("15 6 * * 1-5".into()));
        assert_eq!(
            (line.user.as_str(), line.command.as_str()),
            ("svc", "/opt/e/run -a b")
        );
        let user = parse_line("@daily /opt/e/run 50\\%", Some("ana")).unwrap();
        assert_eq!(user.calendar, FoundCalendar::Cron("0 0 * * *".into()));
        assert_eq!(
            (user.user.as_str(), user.command.as_str()),
            ("ana", "/opt/e/run 50%")
        );
        let boot = parse_line("@reboot root /opt/e/run", None).unwrap();
        assert_eq!(boot.calendar, FoundCalendar::Unsupported("reboot".into()));
        for skipped in [
            "",
            "# 0 6 * * * root x",
            "PATH=/usr/bin",
            "SHELL = /bin/sh",
            "0 6 * *",
        ] {
            assert_eq!(parse_line(skipped, None), None, "{skipped:?}");
        }
    }

    #[test]
    fn cron_d_reads_only_run_parts_names() {
        assert!(is_cron_d_name("flows-1_a"));
        for name in ["flows.disabled", "x~", ""] {
            assert!(!is_cron_d_name(name), "{name}");
        }
    }
}
