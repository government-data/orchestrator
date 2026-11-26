//! What a runner reports of its cluster for import and re-attach
//! (decision 0011): its inventory once at start, and what it found at
//! start and every [`FOUND_EVERY`]. An engine without artefacts reports
//! neither.

use std::time::Duration;

use engine_port::{FoundAnswer, InventoryAnswer};

use super::Runner;
use crate::{CallError, Engine, Guard, Orchestrator};

/// How often a runner reports what it found.
pub const FOUND_EVERY: Duration = Duration::from_secs(30 * 60);

impl<E: Engine, O: Orchestrator, G: Guard> Runner<E, O, G> {
    /// Reports the records deploys left on the cluster; call at start,
    /// after [`Runner::register`]. `None`: the engine keeps none.
    pub async fn report_inventory(&self) -> Result<Option<InventoryAnswer>, CallError> {
        let Some(inventory) = self.engine.inventory().await else {
            return Ok(None);
        };
        let answer = self
            .orchestrator
            .inventory(&self.descriptor, &inventory)
            .await?;
        tracing::info!(
            runner = %self.name,
            items = inventory.items.len(),
            already_adopted = answer.already_adopted,
            reattached = answer.reattached.len(),
            registered = answer.registered.len(),
            skipped = answer.skipped.len(),
            "inventory reported"
        );
        Ok(Some(answer))
    }

    /// Reports what is on the cluster that no deploy placed; call at start
    /// and every [`FOUND_EVERY`]. `None`: the engine has no artefacts.
    pub async fn report_found(&self) -> Result<Option<FoundAnswer>, CallError> {
        let Some(found) = self.engine.found().await else {
            return Ok(None);
        };
        let answer = self.orchestrator.found(&self.descriptor, &found).await?;
        tracing::info!(runner = %self.name, kept = answer.kept, "found reported");
        Ok(Some(answer))
    }
}
