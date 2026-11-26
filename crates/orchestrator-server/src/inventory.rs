//! A runner's inventory at start (decision 0011, re-attach): a cluster seen
//! for the first time adopts it once, against the machines the core lists
//! as live; later reports answer `already_adopted` and change nothing.

use std::collections::HashSet;

use engine_port::{Inventory, InventoryAnswer};
use orchestration_model::EngineLine;
use orchestrator_postgres::{Adoption, StoreError};

use crate::{runner_of, Clock, EnginePort, Machine, PortError};

fn already_adopted() -> InventoryAnswer {
    InventoryAnswer {
        already_adopted: true,
        ..InventoryAnswer::default()
    }
}

impl<C: Clock> EnginePort<C> {
    /// Adopts the inventory of the runner of `machine`, `engine` and line,
    /// once. The runner registers first; while the core's live machines
    /// cannot be read, nothing is adopted and the call is unavailable.
    pub async fn inventory(
        &self,
        machine: &Machine,
        engine: &EngineLine,
        inventory: Inventory,
    ) -> Result<InventoryAnswer, PortError> {
        inventory.validate()?;
        let runner = runner_of(machine, engine);
        match self.store.adopts_inventory(&runner).await {
            Err(StoreError::Invalid(_)) => return Err(PortError::UnknownEngine),
            Ok(false) => return Ok(already_adopted()),
            other => other?,
        };
        let live: HashSet<String> = match &self.machines {
            Some(machines) => machines.live().await,
            None => None,
        }
        .ok_or(PortError::Unavailable)?
        .into_iter()
        .collect();
        let (latest, mut skipped) = inventory.latest();
        let adoption = Adoption {
            runner: &runner,
            cluster: &machine.0,
            engine,
            live: &live,
            now: self.clock.now(),
        };
        let Some(mut answer) = self.store.adopt_inventory(&adoption, &latest).await? else {
            return Ok(already_adopted());
        };
        tracing::info!(
            runner,
            reattached = answer.reattached.len(),
            registered = answer.registered.len(),
            skipped = answer.skipped.len() + skipped.len(),
            "inventory adopted"
        );
        skipped.append(&mut answer.skipped);
        answer.skipped = skipped;
        Ok(answer)
    }
}
