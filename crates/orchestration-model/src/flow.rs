//! Flows, the folders that group them, the engine and line each is pinned
//! to, and what keeps a flow from firing (its own pause, its engine's, or a
//! line that is not installed).

use std::fmt;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::OnInterrupt;

/// A name the orchestrator does not take: why.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InvalidName(pub String);

impl fmt::Display for InvalidName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for InvalidName {}

/// An engine's name: free, checked against the engines registered now,
/// never against a list. `[a-z][a-z0-9-]`, at most 40 bytes.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct EngineName(String);

impl EngineName {
    pub fn new(raw: &str) -> Result<Self, InvalidName> {
        let ok = raw.len() <= 40
            && raw.starts_with(|c: char| c.is_ascii_lowercase())
            && raw
                .chars()
                .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-');
        if ok {
            Ok(Self(raw.to_string()))
        } else {
            Err(InvalidName(format!(
                "engine name {raw:?}: lowercase letters, digits and '-', starting with a letter, at most 40"
            )))
        }
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// An engine's major line: an incompatible major version (`3`), or `-` for
/// an engine with one line. `[a-z0-9.]`, at most 16 bytes.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct Line(String);

impl Line {
    /// The line of an engine that has only one.
    pub const SINGLE: &'static str = "-";

    pub fn new(raw: &str) -> Result<Self, InvalidName> {
        let ok = raw == Self::SINGLE
            || (!raw.is_empty()
                && raw.len() <= 16
                && raw
                    .chars()
                    .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '.'));
        if ok {
            Ok(Self(raw.to_string()))
        } else {
            Err(InvalidName(format!(
                "line {raw:?}: '-', or lowercase letters, digits and '.', at most 16"
            )))
        }
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

macro_rules! name_conversions {
    ($($name:ident),*) => {$(
        impl TryFrom<String> for $name {
            type Error = InvalidName;
            fn try_from(raw: String) -> Result<Self, InvalidName> {
                Self::new(&raw)
            }
        }

        impl From<$name> for String {
            fn from(name: $name) -> String {
                name.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(&self.0)
            }
        }
    )*};
}

name_conversions!(EngineName, Line);

/// The engine and line a flow is pinned to; its runs copy it, and a runner
/// claims only the runs of its own.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct EngineLine {
    pub engine: EngineName,
    pub line: Line,
}

/// A folder of flows, for listing.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Folder {
    pub id: String,
    pub name: String,
}

/// A flow: what runs, on which engine and line, with which parameters.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Flow {
    pub id: String,
    pub folder_id: String,
    pub name: String,
    pub description: String,
    #[serde(flatten)]
    pub engine: EngineLine,
    /// One of the run kinds its engine's descriptor declares.
    pub kind: String,
    /// The cluster its artefact is on; `None`: any runner of its engine and
    /// line.
    pub cluster: Option<String>,
    /// Its default parameters (`{"name": value}`).
    pub parameters: Value,
    pub timeout_secs: u32,
    /// Whether a run may start while another run of it is still open.
    pub allow_overlap: bool,
    /// Its own rule; `None`: the installation's.
    pub on_interrupt: Option<OnInterrupt>,
    /// When it was paused (its schedules do not fire), if it is.
    pub paused_at: Option<DateTime<Utc>>,
    pub enabled: bool,
}

impl Flow {
    /// What an interrupted run of it does: its own rule, or the
    /// installation's.
    #[must_use]
    pub fn on_interrupt_or(&self, installation: OnInterrupt) -> OnInterrupt {
        self.on_interrupt.unwrap_or(installation)
    }
}

/// An engine on pause: nothing new of it is handed out.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EnginePause {
    pub engine: EngineName,
    pub reason: String,
    /// Who paused it (the subject of their sign-in).
    pub paused_by: Option<String>,
    pub paused_at: DateTime<Utc>,
}

/// Why a flow's schedule does not fire now.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Blocked {
    FlowDisabled,
    FlowPaused,
    EnginePaused,
    /// No module of its engine and line is installed: its data stays, and
    /// adding the line back resumes it.
    EngineNotInstalled,
}

/// Whether `flow` fires: `None` if it does, otherwise why not.
#[must_use]
pub fn blocked(flow: &Flow, line_installed: bool, engine_paused: bool) -> Option<Blocked> {
    if !flow.enabled {
        Some(Blocked::FlowDisabled)
    } else if !line_installed {
        Some(Blocked::EngineNotInstalled)
    } else if engine_paused {
        Some(Blocked::EnginePaused)
    } else if flow.paused_at.is_some() {
        Some(Blocked::FlowPaused)
    } else {
        None
    }
}

/// Merges parameters: those of `over` replace those of `base` with the same
/// name. If either is not a JSON object, `over` wins (or `base` when `over`
/// is null), so that no merge nobody asked for is made up.
#[must_use]
pub fn merge_parameters(base: &Value, over: &Value) -> Value {
    match (base, over) {
        (Value::Object(b), Value::Object(o)) => {
            let mut merged = b.clone();
            merged.extend(o.iter().map(|(k, v)| (k.clone(), v.clone())));
            Value::Object(merged)
        }
        (_, Value::Null) => base.clone(),
        _ => over.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn flow() -> Flow {
        Flow {
            id: "f-1".into(),
            folder_id: "d-1".into(),
            name: "daily load".into(),
            description: String::new(),
            engine: EngineLine {
                engine: EngineName::new("engine-a").unwrap(),
                line: Line::new("3").unwrap(),
            },
            kind: "load".into(),
            cluster: None,
            parameters: json!({}),
            timeout_secs: 3600,
            allow_overlap: false,
            on_interrupt: None,
            paused_at: None,
            enabled: true,
        }
    }

    #[test]
    fn engine_names_and_lines_are_free_but_well_formed() {
        assert!(EngineName::new("any-engine2").is_ok());
        for bad in ["", "2x", "Upper", "a_b", &"a".repeat(41)] {
            assert!(EngineName::new(bad).is_err(), "{bad:?}");
        }
        for good in ["-", "3", "5.1", "lts"] {
            assert!(Line::new(good).is_ok(), "{good:?}");
        }
        for bad in ["", "--", "3-x", "V3"] {
            assert!(Line::new(bad).is_err(), "{bad:?}");
        }
        let e: EngineName = serde_json::from_value(json!("x")).unwrap();
        assert_eq!(e.as_str(), "x");
        assert!(serde_json::from_value::<Line>(json!("--")).is_err());
    }

    #[test]
    fn a_flow_serialises_its_engine_and_line_flat() {
        let v = serde_json::to_value(flow()).unwrap();
        assert_eq!(v["engine"], "engine-a");
        assert_eq!(v["line"], "3");
        let back: Flow = serde_json::from_value(v).unwrap();
        assert_eq!(back, flow());
    }

    #[test]
    fn its_own_rule_wins_over_the_installation() {
        let mut f = flow();
        assert_eq!(f.on_interrupt_or(OnInterrupt::Repeat), OnInterrupt::Repeat);
        f.on_interrupt = Some(OnInterrupt::None);
        assert_eq!(f.on_interrupt_or(OnInterrupt::Repeat), OnInterrupt::None);
    }

    #[test]
    fn what_keeps_a_flow_from_firing() {
        let mut f = flow();
        assert_eq!(blocked(&f, true, false), None);
        assert_eq!(blocked(&f, false, false), Some(Blocked::EngineNotInstalled));
        assert_eq!(blocked(&f, true, true), Some(Blocked::EnginePaused));
        f.paused_at = Some(Utc::now());
        assert_eq!(blocked(&f, true, false), Some(Blocked::FlowPaused));
        f.enabled = false;
        assert_eq!(blocked(&f, true, false), Some(Blocked::FlowDisabled));
    }

    #[test]
    fn a_fire_s_parameters_override_the_flow_s() {
        let merged = merge_parameters(
            &json!({"date": "today", "region": "north"}),
            &json!({"date": "2026-10-16"}),
        );
        assert_eq!(merged, json!({"date": "2026-10-16", "region": "north"}));
        assert_eq!(
            merge_parameters(&json!({"a": 1}), &Value::Null),
            json!({"a": 1})
        );
        assert_eq!(merge_parameters(&json!({"a": 1}), &json!([1])), json!([1]));
    }
}
