//! What a runner reports of its cluster for import and re-attach
//! (decision 0011): its inventory once at start, and what it found at
//! start, every [`FOUND_EVERY`] and when a renewal asks for a scan. An
//! engine without artefacts reports neither.

use std::time::Duration;

use engine_port::{FoundAnswer, InventoryAnswer};

use super::Runner;
use crate::{CallError, Engine, Guard, Orchestrator};

/// How often a runner reports what it found.
pub const FOUND_EVERY: Duration = Duration::from_secs(30 * 60);

/// Why a found report is due.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FoundDue {
    /// Its period passed.
    Timer,
    /// A renewal asked for a scan.
    Requested,
}

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

    /// Waits until the next found report is due: `every` from now, or
    /// sooner when a renewal asks for a scan. Report at start, then
    /// `loop { runner.found_due(FOUND_EVERY).await; runner.report_found().await }`.
    pub async fn found_due(&self, every: Duration) -> FoundDue {
        tokio::select! {
            () = self.scan.notified() => FoundDue::Requested,
            () = tokio::time::sleep(every) => FoundDue::Timer,
        }
    }

    /// Reports what is on the cluster that no deploy placed; call at start
    /// and whenever [`Runner::found_due`] says. `None`: the engine has no
    /// artefacts.
    pub async fn report_found(&self) -> Result<Option<FoundAnswer>, CallError> {
        let Some(found) = self.engine.found().await else {
            return Ok(None);
        };
        let answer = self.orchestrator.found(&self.descriptor, &found).await?;
        tracing::info!(runner = %self.name, kept = answer.kept, "found reported");
        Ok(Some(answer))
    }
}
