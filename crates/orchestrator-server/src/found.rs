//! A runner's report of what it found on its cluster (decision 0011,
//! import): checked item by item, a bad one dropped and counted, the rest
//! kept until its next report for an administrator to import.

use std::collections::BTreeSet;

use engine_port::{Found, FoundAnswer};
use orchestration_model::EngineLine;
use orchestrator_postgres::StoreError;

use crate::{runner_of, Clock, EnginePort, Machine, PortError};

impl<C: Clock> EnginePort<C> {
    /// Replaces what the runner of `machine`, `engine` and line found.
    /// The runner registers first; a schedule is kept only with the
    /// artefact it starts.
    pub async fn found(
        &self,
        machine: &Machine,
        engine: &EngineLine,
        found: Found,
    ) -> Result<FoundAnswer, PortError> {
        found.validate()?;
        let runner = runner_of(machine, engine);
        let total = found.artefacts.len() + found.schedules.len();
        let artefacts: Vec<_> = found
            .artefacts
            .into_iter()
            .filter(|a| a.problem().is_none())
            .collect();
        let paths: BTreeSet<&str> = artefacts.iter().map(|a| a.path.as_str()).collect();
        let schedules: Vec<_> = found
            .schedules
            .into_iter()
            .filter(|s| s.problem().is_none() && paths.contains(s.artefact.as_str()))
            .collect();
        let dropped = total - artefacts.len() - schedules.len();
        if dropped > 0 {
            tracing::info!(
                runner,
                dropped,
                "found items dropped: malformed or of no artefact"
            );
        }
        match self
            .store
            .replace_found(&runner, &artefacts, &schedules)
            .await
        {
            Err(StoreError::Invalid(_)) => return Err(PortError::UnknownEngine),
            other => other?,
        }
        Ok(FoundAnswer {
            kept: u32::try_from(artefacts.len()).unwrap_or(u32::MAX),
            relinked: 0,
        })
    }
}
