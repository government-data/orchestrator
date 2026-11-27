//! The import (decision 0011): a person holding `orchestrator:ImportFlows`
//! picks, from what a runner found on its cluster, which artefacts to
//! load, the folder and name of each, and confirms every flag the engine
//! raised; each becomes a flow of that cluster and a deploy with no upload
//! that the runner runs. At most [`MAX_IMPORT`] at once. Each item is
//! imported or skipped with its reason; an import repeated skips what it
//! already made.

use std::collections::{BTreeSet, HashSet};

use engine_port::{Flag, FlagKind, FoundArtefact};
use orchestration_model::EngineLine;
use orchestrator_postgres::{ImportRefused, NewImport, StoreError};
use serde::{Deserialize, Serialize};

use crate::{runner_of, Clock, EnginePort, Machine};

/// The most artefacts one import takes.
pub const MAX_IMPORT: usize = 500;

/// One artefact to import, as a person chose it.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ImportItem {
    /// The found artefact's path.
    pub path: String,
    /// The folder's name: found, or created.
    pub folder: String,
    pub name: String,
    /// One of the engine's run kinds.
    pub kind: String,
    pub timeout_secs: u32,
    /// Every flag of the artefact, confirmed.
    #[serde(default)]
    pub confirmed_flags: Vec<Flag>,
}

/// Why an item was not imported.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Skipped {
    /// The runner's last report has no artefact at that path.
    NotFound,
    /// A flag of the artefact was not confirmed, or one confirmed is not
    /// its.
    FlagsNotConfirmed,
    /// The engine has no run kind of that name.
    UnknownKind,
    /// A folder, name or timeout out of range.
    Invalid,
    /// The same path twice in the request: the first is taken.
    Duplicate,
    AlreadyImported,
    NameTaken,
}

/// What an import did, item by item.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct ImportAnswer {
    pub imported: Vec<ImportedItem>,
    pub skipped: Vec<SkippedItem>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ImportedItem {
    pub path: String,
    pub flow: String,
    pub deploy: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SkippedItem {
    pub path: String,
    pub reason: Skipped,
}

/// Why an import was not done at all.
#[derive(Debug)]
pub enum ImportError {
    /// None, or more than [`MAX_IMPORT`] items.
    Count,
    /// No engine and line of that name is registered.
    UnknownEngine,
    Store(StoreError),
}

impl From<StoreError> for ImportError {
    fn from(e: StoreError) -> Self {
        Self::Store(e)
    }
}

/// Whether `confirmed` is exactly the artefact's flags, in any order.
fn all_confirmed(artefact: &FoundArtefact, confirmed: &[Flag]) -> bool {
    fn key(f: &Flag) -> (FlagKind, &str) {
        (f.kind, f.subject.as_str())
    }
    let raised: BTreeSet<_> = artefact.flags.iter().map(key).collect();
    let given: BTreeSet<_> = confirmed.iter().map(key).collect();
    raised == given
}

fn in_range(item: &ImportItem) -> bool {
    let len = |s: &str, max: usize| (1..=max).contains(&s.chars().count());
    len(&item.folder, 120)
        && len(&item.name, 200)
        && item.timeout_secs > 0
        && i32::try_from(item.timeout_secs).is_ok()
}

/// The found artefact `item` imports, or why it is skipped.
fn check<'a>(
    item: &'a ImportItem,
    found: &'a [FoundArtefact],
    kinds: &HashSet<&str>,
    seen: &mut HashSet<&'a str>,
) -> Result<&'a FoundArtefact, Skipped> {
    if !seen.insert(item.path.as_str()) {
        return Err(Skipped::Duplicate);
    }
    let artefact = found
        .iter()
        .find(|a| a.path == item.path)
        .ok_or(Skipped::NotFound)?;
    if !all_confirmed(artefact, &item.confirmed_flags) {
        return Err(Skipped::FlagsNotConfirmed);
    }
    if !kinds.contains(item.kind.as_str()) {
        return Err(Skipped::UnknownKind);
    }
    if !in_range(item) {
        return Err(Skipped::Invalid);
    }
    Ok(artefact)
}

impl<C: Clock> EnginePort<C> {
    /// Imports `items` from what the runner of `machine`, `engine` and
    /// line last reported.
    pub async fn import(
        &self,
        machine: &Machine,
        engine: &EngineLine,
        items: &[ImportItem],
    ) -> Result<ImportAnswer, ImportError> {
        if items.is_empty() || items.len() > MAX_IMPORT {
            return Err(ImportError::Count);
        }
        let registered = self.store.engines().await?;
        let Some(e) = registered.iter().find(|e| &e.engine == engine) else {
            return Err(ImportError::UnknownEngine);
        };
        let kinds: HashSet<&str> = e.kinds.iter().map(|k| k.name.as_str()).collect();
        let runner = runner_of(machine, engine);
        let found = self.store.found_of(&runner).await?;
        let mut seen = HashSet::new();
        let mut answer = ImportAnswer::default();
        for item in items {
            let skip = |reason| SkippedItem {
                path: item.path.clone(),
                reason,
            };
            let artefact = match check(item, &found, &kinds, &mut seen) {
                Ok(artefact) => artefact,
                Err(reason) => {
                    answer.skipped.push(skip(reason));
                    continue;
                }
            };
            let schedules = self.store.found_schedules_of(&runner, &item.path).await?;
            let new = NewImport {
                folder: item.folder.clone(),
                name: item.name.clone(),
                engine: engine.clone(),
                kind: item.kind.clone(),
                timeout_secs: item.timeout_secs,
                cluster: machine.0.clone(),
                source_path: artefact.path.clone(),
                source_sha256: artefact.sha256.clone(),
                schedules: schedules.into_iter().map(|s| s.source).collect(),
                queued_at: self.clock.now(),
            };
            match self.store.import_flow(&new).await? {
                Ok(done) => answer.imported.push(ImportedItem {
                    path: item.path.clone(),
                    flow: done.flow_id,
                    deploy: done.deploy_id,
                }),
                Err(ImportRefused::AlreadyImported) => {
                    answer.skipped.push(skip(Skipped::AlreadyImported));
                }
                Err(ImportRefused::NameTaken) => answer.skipped.push(skip(Skipped::NameTaken)),
            }
        }
        tracing::info!(
            runner,
            imported = answer.imported.len(),
            skipped = answer.skipped.len(),
            "import"
        );
        Ok(answer)
    }
}
