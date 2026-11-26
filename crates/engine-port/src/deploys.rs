//! The messages of a deploy (decision 0011, import): a runner claims a
//! deploy of its engine and line, places the artefact in the managed
//! layout, and reports its end. A deploy here imports an artefact already
//! on the runner's cluster: it carries no upload.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::found::{is_sha256, is_text, MAX_PATH_BYTES};
use crate::{FolderRecord, Refused, ScheduleSource};

/// The longest version tag, in bytes.
pub const MAX_VERSION_BYTES: usize = 200;

/// A deploy handed to a runner of its engine and line, on the cluster its
/// artefact is on. Claiming again before its finish hands the same deploy
/// to the same runner, so a runner that restarts takes it up again.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct DeployClaimed {
    pub deploy: String,
    pub flow: String,
    /// The artefact's absolute path on the cluster now.
    pub source_path: String,
    /// Its SHA-256 as the runner found it: a different artefact there now
    /// is not deployed.
    pub source_sha256: String,
    /// The external schedules that start it, disabled by the runner once
    /// the flow is ready.
    #[serde(default)]
    pub schedules: Vec<ScheduleSource>,
    /// The flow's folder and name, the managed layout's.
    pub folder: FolderRecord,
    pub name: String,
    /// The rest of the flow's settings, for `orchestrator.json`.
    #[serde(default)]
    pub description: String,
    pub kind: String,
    #[serde(default)]
    pub parameters: Map<String, Value>,
    pub timeout_secs: u32,
    #[serde(default)]
    pub allow_overlap: bool,
    #[serde(default)]
    pub on_interrupt: Option<String>,
}

/// How a deploy ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum DeployState {
    /// The artefact is in the managed layout, tagged and recorded.
    Succeeded,
    /// It was not placed: the artefact stays where it was.
    Failed,
}

impl DeployState {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
        }
    }
}

/// `POST /internal/deploys/{id}/finish`: a deploy's end. Sending the same
/// finish again changes nothing; another one after it is refused.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct DeployFinish {
    pub state: DeployState,
    /// Where the artefact is now (absolute), on success.
    pub path: Option<String>,
    /// The version tag the deploy gave it, on success.
    pub version: Option<String>,
    /// The SHA-256 of what was placed (lowercase hex), on success.
    pub sha256: Option<String>,
    /// Every external schedule that starts it was disabled: if not, the
    /// flow's schedules are created paused.
    pub external_disabled: bool,
}

impl DeployFinish {
    /// A success names its absolute path, version and SHA-256; a failure
    /// none of them.
    pub fn validate(&self) -> Result<(), Refused> {
        let placed = [&self.path, &self.version, &self.sha256];
        match self.state {
            DeployState::Failed if placed.iter().any(|p| p.is_some()) => Err(Refused::Invalid(
                "a failed deploy names no path, version or sha256".into(),
            )),
            DeployState::Failed => Ok(()),
            DeployState::Succeeded => {
                let path = self.path.as_deref().unwrap_or_default();
                if !is_text(path, MAX_PATH_BYTES) || !path.starts_with('/') {
                    return Err(Refused::Invalid(
                        "path: absolute, at most 1000 bytes".into(),
                    ));
                }
                if !is_text(
                    self.version.as_deref().unwrap_or_default(),
                    MAX_VERSION_BYTES,
                ) {
                    return Err(Refused::Invalid("version: at most 200 bytes".into()));
                }
                if !is_sha256(self.sha256.as_deref().unwrap_or_default()) {
                    return Err(Refused::Invalid("sha256: 64 lowercase hex digits".into()));
                }
                Ok(())
            }
        }
    }
}
