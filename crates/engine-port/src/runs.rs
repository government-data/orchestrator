//! The messages of a claimed run: the claim, the renewal of its lease, its
//! log lines and its end; and the engine's drain hold. A message carries
//! states, counts and classes, never the data a run reads or writes.

use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use orchestration_model::{FinalState, Interruption};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::Refused;

/// The most log lines one call appends.
pub const MAX_LOG_LINES: usize = 500;

/// The longest log line, in bytes; a longer one is cut by the runner.
pub const MAX_LOG_LINE_BYTES: usize = 8192;

/// The most counts one finish carries.
pub const MAX_COUNTS: usize = 64;

/// A runner asks for a run of its engine and line.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Claim {
    /// The runner, unique on its machine (several may run on one).
    pub runner: String,
}

/// The run that tells the engine where the run it repeats was.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Resume {
    pub previous_run: String,
    /// The engine's reference of the previous run, if it reported one.
    pub engine_ref: Option<String>,
    /// The previous run's last progress detail, if it reported one.
    pub last_detail: Option<String>,
}

/// A run handed to a runner, under a lease.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Claimed {
    pub run: String,
    pub flow: String,
    pub kind: String,
    /// The run's parameters (the flow's with the fire's over them).
    pub parameters: Value,
    /// Its attempt (the first is 1).
    pub attempt: u32,
    pub timeout_secs: u32,
    /// The lease the runner names in every call about this run: a call with
    /// another lease is refused, so an expired holder changes nothing.
    pub lease: String,
    pub lease_until: DateTime<Utc>,
    /// With `continue`, where the run it repeats was.
    pub resume: Option<Resume>,
    /// The flow's variables, secret ones opened: for the engine only,
    /// never logged or reported back.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub variables: BTreeMap<String, Value>,
}

/// How far a run is.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Progress {
    /// 0 to 1, if the engine knows.
    pub fraction: Option<f64>,
    /// A short step name (no data), at most 200 bytes.
    pub detail: Option<String>,
}

/// A runner renews its lease while it runs.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Renew {
    pub lease: String,
    pub progress: Option<Progress>,
    /// The engine's own reference of the run (a query id), stored before
    /// the engine starts it, so that a retry can stop it first.
    pub engine_ref: Option<String>,
}

/// What a runner does after a renewal.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum RenewAction {
    /// Go on.
    Continue,
    /// Someone cancelled it: stop the engine and finish it `cancelled`.
    Cancel,
    /// The engine is held: go on with this run, claim nothing new.
    Held,
}

/// The answer to a renewal.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Renewed {
    pub action: RenewAction,
    pub lease_until: DateTime<Utc>,
    /// Someone asked for a scan: report what is on the cluster now, without
    /// waiting for the next timed report. Absent means false, and it is
    /// sent only when true, so an answer without it reads as before; it is
    /// sent again at each renewal until the runner's next found report.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub scan: bool,
}

/// Where a log line came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum LogStream {
    Stdout,
    Stderr,
    /// The runner's own messages (start, timeout, cancel).
    System,
}

/// A numbered log line: numbers grow from 1 within a run, and a line sent
/// twice is kept once.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct LogLine {
    pub seq: u64,
    pub stream: LogStream,
    pub line: String,
}

/// A runner appends log lines.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Logs {
    pub lease: String,
    pub lines: Vec<LogLine>,
}

impl Logs {
    /// At most [`MAX_LOG_LINES`] lines of at most [`MAX_LOG_LINE_BYTES`],
    /// numbered upwards from 1.
    pub fn validate(&self) -> Result<(), Refused> {
        if self.lines.len() > MAX_LOG_LINES {
            return Err(Refused::Invalid(format!(
                "at most {MAX_LOG_LINES} lines a call"
            )));
        }
        let mut last = 0;
        for l in &self.lines {
            if l.seq <= last {
                return Err(Refused::Invalid(
                    "log lines are numbered upwards from 1".into(),
                ));
            }
            if l.line.len() > MAX_LOG_LINE_BYTES {
                return Err(Refused::Invalid(format!(
                    "a log line is at most {MAX_LOG_LINE_BYTES} bytes"
                )));
            }
            last = l.seq;
        }
        Ok(())
    }
}

/// A runner reports a run's end.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Finish {
    pub lease: String,
    #[schemars(schema_with = "crate::schema::final_state")]
    pub state: FinalState,
    /// Named counts (rows, statements…), never values.
    #[serde(default)]
    pub counts: BTreeMap<String, u64>,
    /// The engine's class of the error (`[a-z0-9_]`, at most 64), never its
    /// message.
    pub error_class: Option<String>,
    /// Why it was cut, if it did not fail by itself.
    #[serde(default)]
    #[schemars(schema_with = "crate::schema::interruption")]
    pub interruption: Option<Interruption>,
}

fn is_class(raw: &str) -> bool {
    !raw.is_empty()
        && raw.len() <= 64
        && raw
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
}

impl Finish {
    /// Counts and the error class named as classes, an error class only on
    /// a failure.
    pub fn validate(&self) -> Result<(), Refused> {
        if self.counts.len() > MAX_COUNTS || !self.counts.keys().all(|k| is_class(k)) {
            return Err(Refused::Invalid(format!(
                "counts: at most {MAX_COUNTS}, each named [a-z0-9_]"
            )));
        }
        match (&self.error_class, self.state) {
            (Some(c), _) if !is_class(c) => Err(Refused::Invalid(
                "error_class: [a-z0-9_], at most 64".into(),
            )),
            (Some(_), FinalState::Succeeded) => Err(Refused::Invalid(
                "a succeeded run has no error class".into(),
            )),
            _ => Ok(()),
        }
    }
}

/// The engine's drain hold: while it is in force the orchestrator answers
/// no claim of that engine and counts what still runs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Hold {
    /// The hold's id: asking again with it extends the hold.
    pub hold: String,
    pub until: DateTime<Utc>,
}

/// A hold's state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum HoldState {
    /// Nothing of the engine runs: it may restart.
    Free,
    /// Runs of the engine are still running.
    Draining,
}

/// The answer to a hold.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct HoldAnswer {
    pub state: HoldState,
    pub running: u32,
}

/// The end of a hold.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Release {
    pub hold: String,
}
