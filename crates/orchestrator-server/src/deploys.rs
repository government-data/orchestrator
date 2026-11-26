//! Deploys over the engine port (decision 0011, import): a runner of the
//! engine and line claims a deploy on its own cluster and reports its end:
//! where the artefact is now, its version and SHA-256, and whether the
//! external schedules were disabled (if not, the flow's schedules are
//! created paused).

use engine_port::{Claim, DeployClaimed, DeployFinish};
use orchestration_model::EngineLine;
use orchestrator_postgres::{DeployEnd, StoreError};

use crate::{runner_of, Clock, EnginePort, Machine, PortError};

impl<C: Clock> EnginePort<C> {
    /// Hands the runner a deploy of its engine and line on its machine, or
    /// none; the one it claimed and did not finish first.
    pub async fn claim_deploy(
        &self,
        machine: &Machine,
        engine: &EngineLine,
        claim: &Claim,
    ) -> Result<Option<DeployClaimed>, PortError> {
        let now = self.clock.now();
        let registered = self.store.engines().await?;
        if !registered.iter().any(|e| &e.engine == engine) {
            return Err(PortError::UnknownEngine);
        }
        let runner = runner_of(machine, engine);
        self.store.seen(&runner, None, now).await?;
        let instance = format!("{runner}/{}", claim.runner);
        let claimed = self
            .store
            .claim_deploy(engine, &machine.0, &instance, now)
            .await?;
        if let Some(d) = &claimed {
            tracing::info!(deploy = %d.deploy, flow = %d.flow, "deploy claimed");
        }
        Ok(claimed)
    }

    /// Records a deploy's end, reported from its machine; the same end
    /// again changes nothing.
    pub async fn finish_deploy(
        &self,
        machine: &Machine,
        deploy: &str,
        f: &DeployFinish,
    ) -> Result<(), PortError> {
        f.validate()?;
        let now = self.clock.now();
        match self.store.finish_deploy(deploy, &machine.0, f, now).await {
            Ok(DeployEnd::Recorded) => {
                tracing::info!(
                    deploy,
                    state = f.state.as_str(),
                    external_disabled = f.external_disabled,
                    "deploy finished"
                );
                Ok(())
            }
            Ok(DeployEnd::Unchanged) => Ok(()),
            Err(StoreError::Conflict(_) | StoreError::Invalid(_)) => Err(PortError::NotTheirs),
            Err(e) => Err(e.into()),
        }
    }
}
