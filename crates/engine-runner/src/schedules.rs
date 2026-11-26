//! The external schedules on a Linux cluster that start an engine's
//! artefacts (decision 0011, import): cron (`/etc/crontab`, `/etc/cron.d`,
//! users' crontabs) and systemd timers. What a command runs, and with which
//! parameters, is the engine's to read ([`StartReader`]); this part finds
//! the commands, converts their calendars and disables them once the
//! orchestrator's schedules take over.

mod cron;
mod timers;

use std::path::{Path, PathBuf};

use serde_json::{Map, Value};

pub use cron::{disable_cron, scan_cron, DISABLED_MARKER};
pub use timers::{calendar_to_cron, disable_timer, scan_timers, SystemSystemctl, Systemctl};

/// What a command starts, as the engine reads it.
#[derive(Debug, Clone, PartialEq)]
pub struct Start {
    /// The artefact's absolute path.
    pub artefact: String,
    /// The parameters the command passes.
    pub parameters: Map<String, Value>,
}

/// The engine's reading of a command line.
pub trait StartReader: Send + Sync {
    /// What `argv` starts, if it starts the engine.
    fn read(&self, argv: &[String]) -> Option<Start>;
}

/// Where the scanner reads: `/` on a cluster, a folder in tests.
#[derive(Debug, Clone)]
pub struct Root(pub PathBuf);

impl Root {
    /// The cluster's own file system.
    #[must_use]
    pub fn system() -> Self {
        Self(PathBuf::from("/"))
    }

    /// `path` (absolute, as the cluster names it) under this root.
    #[must_use]
    pub fn join(&self, path: &str) -> PathBuf {
        self.0.join(path.trim_start_matches('/'))
    }

    /// The cluster's time zone: `/etc/timezone`, else the zone
    /// `/etc/localtime` links to.
    #[must_use]
    pub fn timezone(&self) -> Option<String> {
        if let Ok(raw) = std::fs::read_to_string(self.join("/etc/timezone")) {
            let zone = raw.trim();
            if !zone.is_empty() {
                return Some(zone.to_string());
            }
        }
        let link = std::fs::read_link(self.join("/etc/localtime")).ok()?;
        zone_of(&link)
    }
}

fn zone_of(link: &Path) -> Option<String> {
    let text = link.to_str()?;
    let (_, zone) = text.split_once("zoneinfo/")?;
    (!zone.is_empty()).then(|| zone.to_string())
}

/// A command line split as `sh` would split plain words: single and double
/// quotes and backslashes; `None` for what needs a shell to read
/// (variables, substitutions, pipes and lists are left in their words, so
/// the engine sees them and refuses what it does not know).
#[must_use]
pub fn split_words(line: &str) -> Option<Vec<String>> {
    let mut words = Vec::new();
    let mut word = String::new();
    let mut in_word = false;
    let mut chars = line.chars();
    while let Some(c) = chars.next() {
        match c {
            ' ' | '\t' => {
                if in_word {
                    words.push(std::mem::take(&mut word));
                    in_word = false;
                }
            }
            '\'' => {
                in_word = true;
                loop {
                    match chars.next()? {
                        '\'' => break,
                        other => word.push(other),
                    }
                }
            }
            '"' => {
                in_word = true;
                loop {
                    match chars.next()? {
                        '"' => break,
                        '\\' => word.push(chars.next()?),
                        other => word.push(other),
                    }
                }
            }
            '\\' => {
                in_word = true;
                word.push(chars.next()?);
            }
            other => {
                in_word = true;
                word.push(other);
            }
        }
    }
    if in_word {
        words.push(word);
    }
    Some(words)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn words_split_as_the_shell_splits_plain_ones() {
        let got = split_words(r#"/opt/e/run -dir="/srv/a b" -x='c d' e\ f"#).unwrap();
        assert_eq!(got, ["/opt/e/run", "-dir=/srv/a b", "-x=c d", "e f"]);
        assert_eq!(split_words("a 'open"), None);
        assert_eq!(split_words("  "), Some(vec![]));
    }

    #[test]
    fn the_zone_is_read_from_the_localtime_link() {
        let zone = zone_of(Path::new("/usr/share/zoneinfo/America/Bogota"));
        assert_eq!(zone.as_deref(), Some("America/Bogota"));
        assert_eq!(zone_of(Path::new("/etc/other")), None);
    }
}
