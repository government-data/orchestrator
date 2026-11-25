//! The engine port's use cases (decision 0011): a runner registers its
//! engine, claims runs under a lease, renews it, appends logs and finishes;
//! an engine's agent holds and releases the engine.

use std::fmt;

use chrono::Duration;
use engine_port::{
    Claim, Claimed, Descriptor, Finish, Hold, HoldAnswer, HoldState, Logs, Refused, Registered,
    Release, Renew, RenewAction, Renewed, LEASE_SECS,
};
use orchestration_model::{open_windows, EngineLine, EngineName};
use orchestrator_postgres::{ClaimFor, PgStore, StoreError};
use schedule::Calendar;

use crate::Clock;

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
    /// The store failed.
    Store(StoreError),
}

impl fmt::Display for PortError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Refused(r) => r.fmt(f),
            Self::Lease => f.write_str("the lease is not this run's any more"),
            Self::UnknownEngine => f.write_str("no such engine and line is registered"),
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
#[derive(Debug, Clone)]
pub struct EnginePort<C> {
    store: PgStore,
    clock: C,
}

impl<C: Clock> EnginePort<C> {
    pub fn new(store: PgStore, clock: C) -> Self {
        Self { store, clock }
    }

    /// Registers a runner's descriptor: the path's engine and line must be
    /// the descriptor's. Idempotent.
    pub async fn register(
        &self,
        machine: &Machine,
        engine: &EngineLine,
        d: &Descriptor,
    ) -> Result<Registered, PortError> {
        if d.engine != engine.engine || d.line != engine.line {
            return Err(Refused::Invalid("the path names another engine or line".into()).into());
        }
        d.validate()?;
        let now = self.clock.now();
        self.store
            .register(d, &runner_of(machine, engine), now)
            .await?;
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
        Ok(self.store.claim(&c).await?)
    }

    /// Extends a run's lease: `cancel` when someone cancelled it, `held`
    /// while its engine is held, else `continue`.
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
        Ok(self.store.finish(run, f, now).await?)
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

    /// Ends a hold; releasing one that is not in force changes nothing.
    pub async fn release(&self, engine: &EngineName, r: &Release) -> Result<(), PortError> {
        self.store.release(engine, &r.hold).await?;
        Ok(())
    }
}
