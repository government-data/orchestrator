//! A scan on request (decision 0011, import): a person asks a runner, by
//! its machine, engine and line, to report what is on its cluster now. Its
//! renewals answer `scan` until its next found report, which clears it: a
//! lost answer, or a runner that crashed before reporting, is told again,
//! and a scan run twice only replaces a report with the same one. The
//! people's route waits for its API; this is the use case it calls.

use orchestration_model::EngineLine;
pub use orchestrator_postgres::ScanAsked;

use crate::{runner_of, Clock, EnginePort, Machine, PortError};

impl<C: Clock> EnginePort<C> {
    /// Asks the runner of `machine`, `engine` and line for a scan; asking
    /// again before its report changes nothing.
    pub async fn request_scan(
        &self,
        machine: &Machine,
        engine: &EngineLine,
    ) -> Result<ScanAsked, PortError> {
        let runner = runner_of(machine, engine);
        let now = self.clock.now();
        let asked = self.store.request_scan(&runner, now).await?;
        if asked == ScanAsked::Asked {
            tracing::info!(runner, "scan asked");
        }
        Ok(asked)
    }
}
