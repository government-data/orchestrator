//! What deploys leave on a cluster (decision 0011, re-attach): an
//! `orchestrator.json` beside each deployed artefact, with what registers
//! its flow again with the same id, and the runner's report of them at
//! start. No secret and no value of a run.

use chrono::{DateTime, Utc};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::found::{is_sha256, is_text, MAX_FOUND, MAX_PATH_BYTES};
use crate::{is_kind_name, Refused};

/// The file a deploy writes beside its artefact.
pub const RECORD_FILE: &str = "orchestrator.json";

/// The format of [`ArtefactRecord`] this port writes and reads.
pub const RECORD_FORMAT: u32 = 1;

/// A folder, as a record names it: found again by id, else by name, else
/// created with that id.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct FolderRecord {
    pub id: String,
    pub name: String,
}

/// `orchestrator.json`: one deployed version of a flow.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ArtefactRecord {
    pub format: u32,
    /// The flow: the same id in every version.
    pub flow: String,
    pub deploy: String,
    /// The version tag the deploy gave it.
    pub version: String,
    pub sha256: String,
    pub folder: FolderRecord,
    pub name: String,
    #[serde(default)]
    pub description: String,
    pub kind: String,
    /// The flow's parameters (an object).
    #[serde(default)]
    pub parameters: Map<String, Value>,
    pub timeout_secs: u32,
    #[serde(default)]
    pub allow_overlap: bool,
    /// `none`, `repeat` or `continue`; absent: the installation's rule.
    #[serde(default)]
    pub on_interrupt: Option<String>,
    pub deployed_at: DateTime<Utc>,
}

/// One record the runner found, with the folder it is in.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct InventoryItem {
    /// The artefact's absolute path on the cluster.
    pub path: String,
    pub record: ArtefactRecord,
}

/// `PUT /internal/engines/{engine}/{line}/inventory`, at start: every
/// record on the cluster.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Inventory {
    pub items: Vec<InventoryItem>,
}

/// Why a record was not taken.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum SkipReason {
    /// The record is malformed or of a newer format.
    Invalid,
    /// A newer version of the same flow is on the cluster.
    OlderVersion,
    /// The flow is placed on a cluster that is live.
    OnLiveCluster,
    /// Another flow is registered at that path.
    PathRegistered,
}

/// A record not taken, and why.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Skipped {
    pub path: String,
    pub reason: SkipReason,
}

/// What the orchestrator did with an inventory.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct InventoryAnswer {
    /// The cluster had adopted its inventory before: nothing changed.
    pub already_adopted: bool,
    /// Known flows moved to this cluster, their runs, schedules,
    /// variables and folder kept (flow ids).
    #[serde(default)]
    pub reattached: Vec<String>,
    /// Unknown flows registered again with their same id.
    #[serde(default)]
    pub registered: Vec<String>,
    /// Folders created again because neither their id nor name existed.
    #[serde(default)]
    pub folders_created: u32,
    #[serde(default)]
    pub skipped: Vec<Skipped>,
}

fn is_uuid(raw: &str) -> bool {
    raw.len() == 36
        && raw.char_indices().all(|(i, c)| {
            if matches!(i, 8 | 13 | 18 | 23) {
                c == '-'
            } else {
                c.is_ascii_hexdigit()
            }
        })
}

impl ArtefactRecord {
    /// Whether it can register its flow again.
    #[must_use]
    pub fn is_valid(&self) -> bool {
        self.format >= 1
            && self.format <= RECORD_FORMAT
            && is_uuid(&self.flow)
            && is_uuid(&self.deploy)
            && is_uuid(&self.folder.id)
            && is_text(&self.folder.name, 120)
            && is_text(&self.name, 200)
            && self.description.len() <= 4000
            && is_text(&self.version, 200)
            && is_sha256(&self.sha256)
            && is_kind_name(&self.kind)
            && self.timeout_secs >= 1
            && self
                .on_interrupt
                .as_deref()
                .is_none_or(|o| matches!(o, "none" | "repeat" | "continue"))
    }
}

impl InventoryItem {
    /// Whether it can be adopted: an absolute path and a valid record.
    #[must_use]
    pub fn is_valid(&self) -> bool {
        is_text(&self.path, MAX_PATH_BYTES) && self.path.starts_with('/') && self.record.is_valid()
    }
}

impl Inventory {
    /// At most [`MAX_FOUND`] items.
    pub fn validate(&self) -> Result<(), Refused> {
        if self.items.len() > MAX_FOUND {
            return Err(Refused::Invalid(format!(
                "at most {MAX_FOUND} items an inventory"
            )));
        }
        Ok(())
    }

    /// The items to adopt, the latest version of each flow, and those
    /// skipped (invalid, or an older version of a flow).
    #[must_use]
    pub fn latest(self) -> (Vec<InventoryItem>, Vec<Skipped>) {
        let mut latest: Vec<InventoryItem> = Vec::new();
        let mut skipped = Vec::new();
        let skip = |item: &InventoryItem, reason| Skipped {
            path: item.path.clone(),
            reason,
        };
        for item in self.items {
            if !item.is_valid() {
                skipped.push(skip(&item, SkipReason::Invalid));
                continue;
            }
            match latest
                .iter_mut()
                .find(|l| l.record.flow == item.record.flow)
            {
                Some(current) if current.record.deployed_at >= item.record.deployed_at => {
                    skipped.push(skip(&item, SkipReason::OlderVersion));
                }
                Some(current) => {
                    let older = std::mem::replace(current, item);
                    skipped.push(skip(&older, SkipReason::OlderVersion));
                }
                None => latest.push(item),
            }
        }
        (latest, skipped)
    }
}
