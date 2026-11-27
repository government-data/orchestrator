//! The engine port's use cases (decision 0011): a runner registers its
//! engine, claims runs under a lease, renews it, appends logs and finishes;
//! an engine's agent holds and releases the engine.

use std::fmt;
use std::sync::Arc;

use chrono::Duration;
use engine_port::{
    Claim, Claimed, Descriptor, Finish, Hold, HoldAnswer, HoldState, Lock, LockAnswer, Logs,
    Refused, Registered, Release, Renew, RenewAction, Renewed, LEASE_SECS,
};
use orchestration_model::{open_windows, EngineLine, EngineName};
use orchestrator_postgres::{ClaimFor, PgStore, StoreError};
use schedule::Calendar;

use std::collections::BTreeMap;

use orchestrator_postgres::VariableValue;
use serde_json::Value;

use crate::{Clock, MachineList, Metrics, Sealer};

/// Who calls the engine port: a machine of the installation, as its
/// certificate names it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Machine(pub String);

/// Why an engine port call was not done.
#[derive(Debug)]
pub enum PortError {
    /// The message is refused (a port not served, a malformed body).
    Refused(Refused),
    /// The lease named is not the run's any more.
    Lease,
    /// No engine or line of that name is registered.
    UnknownEngine,
    /// The deploy is not one this machine claimed, or it ended otherwise.
    NotTheirs,
    /// What the call needs from the core cannot be read now.
    Unavailable,
    /// The store failed.
    Store(StoreError),
}

impl fmt::Display for PortError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Refused(r) => r.fmt(f),
            Self::Lease => f.write_str("the lease is not this run's any more"),
            Self::UnknownEngine => f.write_str("no such engine and line is registered"),
            Self::NotTheirs => {
                f.write_str("the deploy is not this machine's to finish, or it ended otherwise")
            }
            Self::Unavailable => f.write_str("the core's live machines cannot be read now"),
            Self::Store(e) => e.fmt(f),
        }
    }
}

impl std::error::Error for PortError {}

impl From<StoreError> for PortError {
    fn from(e: StoreError) -> Self {
        match e {
            StoreError::Lease => Self::Lease,
            other => Self::Store(other),
        }
    }
}

impl From<Refused> for PortError {
    fn from(r: Refused) -> Self {
        Self::Refused(r)
    }
}

/// A runner's identity: its machine, engine and line.
#[must_use]
pub fn runner_of(machine: &Machine, engine: &EngineLine) -> String {
    format!("{}/{}/{}", machine.0, engine.engine, engine.line)
}

/// The engine port, on a store and a clock.
#[derive(Clone)]
pub struct EnginePort<C> {
    pub(crate) store: PgStore,
    pub(crate) clock: C,
    metrics: Option<Arc<Metrics>>,
    sealer: Option<Arc<Sealer>>,
    pub(crate) machines: Option<Arc<dyn MachineList>>,
}

impl<C> fmt::Debug for EnginePort<C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EnginePort").finish_non_exhaustive()
    }
}

impl<C: Clock> EnginePort<C> {
    pub fn new(store: PgStore, clock: C) -> Self {
        Self {
            store,
            clock,
            metrics: None,
            sealer: None,
            machines: None,
        }
    }

    /// Reads which machines are live from the core (decision 0011,
    /// re-attach); without it, no inventory is adopted.
    #[must_use]
    pub fn with_machines(mut self, machines: Arc<dyn MachineList>) -> Self {
        self.machines = Some(machines);
        self
    }

    /// Opens sealed variables and secrets with `sealer` when it hands a run
    /// out.
    #[must_use]
    pub fn with_sealer(mut self, sealer: Arc<Sealer>) -> Self {
        self.sealer = Some(sealer);
        self
    }

    /// Counts the runs that end in `metrics`.
    #[must_use]
    pub fn with_metrics(mut self, metrics: Arc<Metrics>) -> Self {
        self.metrics = Some(metrics);
        self
    }

    /// Registers a runner's descriptor: the path's engine and line must be
    /// the descriptor's. Idempotent.
    pub async fn register(
        &self,
        machine: &Machine,
        module: Option<&str>,
        engine: &EngineLine,
        d: &Descriptor,
    ) -> Result<Registered, PortError> {
        if d.engine != engine.engine || d.line != engine.line {
            return Err(Refused::Invalid("the path names another engine or line".into()).into());
        }
        d.validate()?;
        let now = self.clock.now();
        let runner = runner_of(machine, engine);
        self.store.register(d, &runner, now).await?;
        if let Some(module) = module {
            self.store.record_module(engine, &runner, module).await?;
        }
        Ok(Registered::for_port(d.port))
    }

    /// Hands out a run of this engine and line to the runner, or none: none
    /// either while the runner's cluster is in an open maintenance window.
    pub async fn claim(
        &self,
        machine: &Machine,
        engine: &EngineLine,
        claim: &Claim,
    ) -> Result<Option<Claimed>, PortError> {
        let now = self.clock.now();
        let registered = self.store.engines().await?;
        if !registered.iter().any(|e| &e.engine == engine) {
            return Err(PortError::UnknownEngine);
        }
        let runner = runner_of(machine, engine);
        self.store.seen(&runner, None, now).await?;
        let windows = self.store.maintenance_windows().await?;
        if open_windows(&windows, &Calendar, now)
            .iter()
            .any(|w| w.cluster == machine.0)
        {
            return Ok(None);
        }
        let lease = uuid::Uuid::new_v4().to_string();
        let instance = format!("{runner}/{}", claim.runner);
        let c = ClaimFor {
            engine,
            runner: &instance,
            cluster: &machine.0,
            lease: &lease,
            lease_until: now + Duration::seconds(i64::from(LEASE_SECS)),
            now,
        };
        let Some(mut claimed) = self.store.claim(&c).await? else {
            return Ok(None);
        };
        match self.variables(&claimed.flow).await? {
            Some(variables) => {
                claimed.variables = variables;
                Ok(Some(claimed))
            }
            None => {
                // A secret it needs does not open: the run fails at once,
                // and no runner ever sees part of its variables.
                tracing::warn!(run = %claimed.run, "a flow variable did not open");
                let failed = Finish {
                    lease: claimed.lease.clone(),
                    state: orchestration_model::FinalState::Failed,
                    counts: Default::default(),
                    error_class: Some("secret_unavailable".into()),
                    interruption: None,
                };
                self.store.finish(&claimed.run, &failed, now).await?;
                Ok(None)
            }
        }
    }

    /// A flow's variables, sealed ones and secrets opened; `None` when one
    /// does not open (no key, another key, a secret deleted).
    async fn variables(&self, flow: &str) -> Result<Option<BTreeMap<String, Value>>, PortError> {
        let mut out = BTreeMap::new();
        for v in self.store.variables(flow).await? {
            let sealed = match v.value {
                VariableValue::Plain(value) => {
                    out.insert(v.name, value);
                    continue;
                }
                VariableValue::Sealed(bytes) => Some(bytes),
                VariableValue::Secret(id) => self.store.sealed_secret(&id).await?,
            };
            let opened = sealed.and_then(|bytes| {
                let plain = self.sealer.as_ref()?.open(&bytes).ok()?;
                serde_json::from_slice::<Value>(&plain)
                    .ok()
                    .or_else(|| String::from_utf8(plain).ok().map(Value::String))
            });
            let Some(value) = opened else { return Ok(None) };
            out.insert(v.name, value);
        }
        Ok(Some(out))
    }

    /// Extends a run's lease: `cancel` when someone cancelled it, `held`
    /// while its engine is held, else `continue`; with `scan` while a scan
    /// asked of its runner waits for its found report.
    pub async fn renew(&self, run: &str, r: &Renew) -> Result<Renewed, PortError> {
        let now = self.clock.now();
        let lease_until = now + Duration::seconds(i64::from(LEASE_SECS));
        let renewal = self
            .store
            .renew(
                run,
                &r.lease,
                r.progress.as_ref(),
                r.engine_ref.as_deref(),
                lease_until,
            )
            .await?;
        let action = if renewal.cancel_requested {
            RenewAction::Cancel
        } else if self.store.held(&renewal.engine, now).await? {
            RenewAction::Held
        } else {
            RenewAction::Continue
        };
        Ok(Renewed {
            action,
            lease_until,
            scan: renewal.scan_requested,
        })
    }

    pub async fn logs(&self, run: &str, logs: &Logs) -> Result<(), PortError> {
        logs.validate()?;
        let now = self.clock.now();
        Ok(self
            .store
            .append_logs(run, &logs.lease, &logs.lines, now)
            .await?)
    }

    pub async fn finish(&self, run: &str, f: &Finish) -> Result<(), PortError> {
        f.validate()?;
        let now = self.clock.now();
        self.store.finish(run, f, now).await?;
        if let Some(m) = &self.metrics {
            if let Err(e) = m.count_ended(&self.store, &[run.to_string()]).await {
                tracing::warn!(error = %e, "metrics: a run's end was not counted");
            }
        }
        Ok(())
    }

    /// Takes a lock for a running run (core decision 0090, point 6.2.4):
    /// granted unless another running run holds it.
    pub async fn take_lock(&self, run: &str, l: &Lock) -> Result<LockAnswer, PortError> {
        l.validate()?;
        let now = self.clock.now();
        let granted = self.store.take_lock(run, &l.lease, &l.name, now).await?;
        Ok(LockAnswer { granted })
    }

    /// Gives a run's lock back; one it does not hold changes nothing.
    pub async fn release_lock(&self, run: &str, l: &Lock) -> Result<(), PortError> {
        l.validate()?;
        Ok(self.store.release_lock(run, &l.lease, &l.name).await?)
    }

    /// The engine's drain hold: from now no claim of the engine is
    /// answered; `free` once nothing of it runs.
    pub async fn hold(&self, engine: &EngineName, h: &Hold) -> Result<HoldAnswer, PortError> {
        let now = self.clock.now();
        if h.until <= now {
            return Err(Refused::Invalid("a hold ends after now".into()).into());
        }
        let running = self.store.hold(engine, &h.hold, h.until, now).await?;
        let state = if running == 0 {
            HoldState::Free
        } else {
            HoldState::Draining
        };
        Ok(HoldAnswer { state, running })
    }

    /// Whether `module` may be removed (decision 0015).
    pub async fn removal(&self, module: &str) -> Result<crate::RemovalAnswer, PortError> {
        Ok(crate::removal(&self.store, crate::ORCHESTRATOR_MODULE, module).await?)
    }

    /// Ends a hold; releasing one that is not in force changes nothing.
    pub async fn release(&self, engine: &EngineName, r: &Release) -> Result<(), PortError> {
        self.store.release(engine, &r.hold).await?;
        Ok(())
    }
}
