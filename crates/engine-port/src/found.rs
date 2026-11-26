//! What a runner finds on its cluster that no deploy put there (decision
//! 0011, import): each artefact's path, SHA-256, size and what the engine
//! flags for a person to review, and the external schedules that start
//! them. Never an artefact's content or a run's values.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::Refused;

/// The most artefacts, and the most schedules, one report carries.
pub const MAX_FOUND: usize = 5000;

/// The most flags one artefact carries.
pub const MAX_FLAGS: usize = 500;

/// The longest path on a cluster, in bytes.
pub const MAX_PATH_BYTES: usize = 1000;

/// What the engine flags for review before an artefact is imported; an
/// import names every kind its artefacts carry as confirmed.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum FlagKind {
    /// It runs code of its own (a script node).
    Code,
    /// It reads or writes a path out of its folder, or one the runner does
    /// not see.
    Path,
    /// It needs an extension the engine does not have.
    Missing,
}

/// One thing flagged: its kind and what the engine names (a node, a path,
/// an extension), at most 500 bytes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Flag {
    pub kind: FlagKind,
    pub subject: String,
}

/// An artefact on the cluster that no deploy placed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct FoundArtefact {
    /// Its absolute path on the cluster.
    pub path: String,
    /// Its path under the engine's root, by folder: the first proposes the
    /// folder, the rest the name.
    pub segments: Vec<String>,
    /// The SHA-256 of its content (lowercase hex), as the engine digests it.
    pub sha256: String,
    pub size_bytes: u64,
    #[serde(default)]
    pub flags: Vec<Flag>,
}

/// Where an external schedule is, so that the runner can disable it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case", tag = "kind", deny_unknown_fields)]
pub enum ScheduleSource {
    /// A cron line: its file and its line number (from 1).
    Cron { file: String, line: u32 },
    /// A systemd timer, by its unit name.
    Timer { unit: String },
}

/// When an external schedule starts its artefact.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case", tag = "calendar", content = "value")]
pub enum FoundCalendar {
    /// A 5-field cron the orchestrator's schedules take as it is.
    Cron(String),
    /// No equivalent (`@reboot`, `OnBootSec`, a calendar the scanner does
    /// not convert): counted for a person, never created.
    Unsupported(String),
}

/// An external schedule that starts a found artefact.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct FoundSchedule {
    pub source: ScheduleSource,
    /// The account it runs as.
    pub user: String,
    pub calendar: FoundCalendar,
    /// The cluster's time zone (IANA), if known.
    pub timezone: Option<String>,
    /// The found artefact's path it starts.
    pub artefact: String,
    /// The parameters its command passes, as the engine reads them.
    #[serde(default)]
    pub parameters: Map<String, Value>,
    /// Whether it also keeps a missed start for later (`Persistent=true`).
    #[serde(default)]
    pub persistent: bool,
}

/// `PUT /internal/engines/{engine}/{line}/found`: everything found now; it
/// replaces the runner's previous report.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Found {
    pub artefacts: Vec<FoundArtefact>,
    #[serde(default)]
    pub schedules: Vec<FoundSchedule>,
}

/// The answer to a report of what was found.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct FoundAnswer {
    /// Artefacts kept to be imported.
    pub kept: u32,
    /// Purged flows whose artefact appeared again, imported over their
    /// same record.
    pub relinked: u32,
}

/// A SHA-256 as lowercase hex.
#[must_use]
pub fn is_sha256(raw: &str) -> bool {
    raw.len() == 64 && raw.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'))
}

pub(crate) fn is_text(raw: &str, max: usize) -> bool {
    !raw.trim().is_empty() && raw.len() <= max && !raw.chars().any(char::is_control)
}

pub(crate) fn is_segment(raw: &str) -> bool {
    is_text(raw, 255) && !raw.contains(['/', '\\']) && raw != "." && raw != ".."
}

impl FoundArtefact {
    /// Why it cannot be kept, if it cannot.
    #[must_use]
    pub fn problem(&self) -> Option<&'static str> {
        Some(
            if !is_text(&self.path, MAX_PATH_BYTES) || !self.path.starts_with('/') {
                "path: absolute, at most 1000 bytes"
            } else if self.segments.is_empty()
                || self.segments.len() > 16
                || !self.segments.iter().all(|s| is_segment(s))
            {
                "segments: 1 to 16 folder names"
            } else if !is_sha256(&self.sha256) {
                "sha256: 64 lowercase hex digits"
            } else if self.flags.len() > MAX_FLAGS
                || !self.flags.iter().all(|f| is_text(&f.subject, 500))
            {
                "flags: at most 500, each naming at most 500 bytes"
            } else {
                return None;
            },
        )
    }

    /// The name proposed for it: its folders after the first, joined.
    #[must_use]
    pub fn proposed_name(&self) -> String {
        match self.segments.as_slice() {
            [] => "flow".into(),
            [one] => one.clone(),
            [_, rest @ ..] => rest.join(" - "),
        }
    }
}

impl FoundSchedule {
    /// Why it cannot be kept, if it cannot.
    #[must_use]
    pub fn problem(&self) -> Option<&'static str> {
        let source = match &self.source {
            ScheduleSource::Cron { file, line } => {
                is_text(file, MAX_PATH_BYTES) && file.starts_with('/') && *line >= 1
            }
            ScheduleSource::Timer { unit } => is_text(unit, 255) && unit.ends_with(".timer"),
        };
        let calendar = match &self.calendar {
            FoundCalendar::Cron(c) => c.split_whitespace().count() == 5 && c.len() <= 200,
            FoundCalendar::Unsupported(why) => is_text(why, 200),
        };
        Some(if !source {
            "source: an absolute cron file and line, or a .timer unit"
        } else if !is_text(&self.user, 64) {
            "user: at most 64 bytes"
        } else if !calendar {
            "calendar: a 5-field cron, or what has no equivalent"
        } else if !is_text(&self.artefact, MAX_PATH_BYTES) {
            "artefact: the path of a found artefact"
        } else if self.parameters.len() > 100
            || self.timezone.as_ref().is_some_and(|t| !is_text(t, 64))
        {
            "parameters: at most 100; timezone at most 64 bytes"
        } else {
            return None;
        })
    }
}

impl Found {
    /// At most [`MAX_FOUND`] of each; each item is then checked on its own
    /// and a bad one dropped, so that one odd folder does not hide the rest.
    pub fn validate(&self) -> Result<(), Refused> {
        if self.artefacts.len() > MAX_FOUND || self.schedules.len() > MAX_FOUND {
            return Err(Refused::Invalid(format!(
                "at most {MAX_FOUND} artefacts and {MAX_FOUND} schedules a report"
            )));
        }
        Ok(())
    }
}
