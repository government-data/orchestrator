//! The claim loop: register, claim while a slot is free and the runner's
//! own guard allows it, and drive each run: renew its lease, pass a cancel
//! to the engine, stop it when the lease is lost (so it never runs twice),
//! stop it past its timeout or on a low disk, and report its end; a scan
//! asked at a renewal wakes the found report.

use std::sync::Arc;
use std::time::Duration;

use engine_port::{
    Claim, Claimed, Descriptor, Finish, Logs, Registered, Renew, RenewAction, Renewed,
};
use orchestration_model::{FinalState, Interruption};
use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore};
use tokio::task::JoinHandle;
use tokio::time::Instant;

use crate::{CallError, Engine, Orchestrator, Outcome, RunContext, RunRef, RunnerMetrics};

mod report;

pub use report::{FoundDue, FOUND_EVERY};

/// Why a runner claims nothing now (its own guard, decision 0011).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Pause {
    LowDisk,
    LowMemory,
}

/// The runner's view of its machine: whether it may claim, and whether a
/// running run must stop for the disk.
pub trait Guard: Send + Sync + 'static {
    /// `Some` while disk or memory is under its minimum.
    fn claim_paused(&self) -> Option<Pause>;
    /// Whether the disk is under its minimum now.
    fn disk_low(&self) -> bool;
}

/// A guard that never pauses.
#[derive(Debug, Clone, Copy, Default)]
pub struct NoGuard;

impl Guard for NoGuard {
    fn claim_paused(&self) -> Option<Pause> {
        None
    }

    fn disk_low(&self) -> bool {
        false
    }
}

/// How a driven run ended, for the runner's own record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Ended {
    /// Reported to the orchestrator.
    Reported(FinalState),
    /// The lease was lost: the engine was stopped and nothing reported.
    Fenced,
}

/// A runner of one engine.
pub struct Runner<E, O, G> {
    engine: Arc<E>,
    orchestrator: Arc<O>,
    guard: Arc<G>,
    name: String,
    descriptor: Descriptor,
    slots: Arc<Semaphore>,
    renew_every: Duration,
    metrics: Option<Arc<RunnerMetrics>>,
    /// A scan asked at a renewal and not reported yet: asked twice before
    /// the report, it is one scan.
    scan: Arc<Notify>,
}

impl<E, O, G> std::fmt::Debug for Runner<E, O, G> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Runner")
            .field("name", &self.name)
            .finish_non_exhaustive()
    }
}

impl<E: Engine, O: Orchestrator, G: Guard> Runner<E, O, G> {
    /// A runner named `name` (unique on its machine).
    pub fn new(engine: E, orchestrator: O, guard: G, name: &str) -> Self {
        let descriptor = engine.descriptor();
        let slots = Arc::new(Semaphore::new(descriptor.concurrency as usize));
        Self {
            engine: Arc::new(engine),
            orchestrator: Arc::new(orchestrator),
            guard: Arc::new(guard),
            name: name.to_string(),
            descriptor,
            slots,
            renew_every: Duration::from_secs(u64::from(engine_port::RENEW_EVERY_SECS)),
            metrics: None,
            scan: Arc::new(Notify::new()),
        }
    }

    /// Renews more often (tests).
    #[must_use]
    pub fn renewing_every(mut self, every: Duration) -> Self {
        self.renew_every = every;
        self
    }

    /// Counts in `metrics` (its metrics file).
    #[must_use]
    pub fn with_metrics(mut self, metrics: Arc<RunnerMetrics>) -> Self {
        self.metrics = Some(metrics);
        self
    }

    fn busy(&self) -> usize {
        (self.descriptor.concurrency as usize).saturating_sub(self.slots.available_permits())
    }

    /// The orchestrator it calls.
    pub(crate) fn orchestrator(&self) -> &O {
        &self.orchestrator
    }

    /// Registers the engine's descriptor; call at start.
    pub async fn register(&self) -> Result<Registered, CallError> {
        self.orchestrator.register(&self.descriptor).await
    }

    /// Sends the engine's slot pools (core decision 0090, point 6.3), from
    /// its own configuration; call after [`Runner::register`].
    pub async fn send_pools(&self, pools: &engine_port::Pools) -> Result<(), CallError> {
        self.orchestrator.set_pools(&self.descriptor, pools).await
    }

    /// Claims one run if a slot is free and the guard allows it, and drives
    /// it in a task; `None` when there was nothing to take.
    pub async fn claim_once(&self) -> Result<Option<JoinHandle<Ended>>, CallError> {
        let paused = self.guard.claim_paused();
        if let Some(m) = &self.metrics {
            m.paused(paused);
            m.busy(self.busy());
        }
        if paused.is_some() {
            return Ok(None);
        }
        let Ok(permit) = Arc::clone(&self.slots).try_acquire_owned() else {
            return Ok(None);
        };
        let claim = Claim {
            runner: self.name.clone(),
        };
        let Some(run) = self.orchestrator.claim(&self.descriptor, &claim).await? else {
            return Ok(None);
        };
        let drive = Drive {
            engine: Arc::clone(&self.engine),
            orchestrator: Arc::clone(&self.orchestrator),
            guard: Arc::clone(&self.guard),
            renew_every: self.renew_every,
            scan: Arc::clone(&self.scan),
            metrics: self.metrics.clone(),
        };
        let metrics = self.metrics.clone();
        let slots = Arc::clone(&self.slots);
        let concurrency = self.descriptor.concurrency as usize;
        let kind = run.kind.clone();
        if let Some(m) = &metrics {
            m.busy(self.busy());
        }
        Ok(Some(tokio::spawn(async move {
            let ended = drive.run(run, permit).await;
            if let Some(m) = metrics {
                m.ended(&kind, &ended);
                m.busy(concurrency.saturating_sub(slots.available_permits()));
            }
            ended
        })))
    }
}

struct Drive<E, O, G> {
    engine: Arc<E>,
    orchestrator: Arc<O>,
    guard: Arc<G>,
    renew_every: Duration,
    scan: Arc<Notify>,
    metrics: Option<Arc<RunnerMetrics>>,
}

/// Why the runner stopped the engine itself.
#[derive(Clone, Copy)]
enum Stop {
    Cancel,
    Timeout,
    LowDisk,
}

impl<E: Engine, O: Orchestrator, G: Guard> Drive<E, O, G> {
    async fn run(self, run: Claimed, _permit: OwnedSemaphorePermit) -> Ended {
        if let Err(refused) = self.engine.validate(&run).await {
            return self
                .finish(
                    &run,
                    &RunContext::default(),
                    Outcome::failed(&refused.class),
                )
                .await;
        }
        let ctx = RunContext {
            timing: self.metrics.clone().map(|m| (m, run.kind.clone())),
            locks: Some(Arc::new(crate::client::LeasedLocks {
                orchestrator: Arc::clone(&self.orchestrator),
                run: run.run.clone(),
                lease: run.lease.clone(),
            })),
            ..RunContext::default()
        };
        let deadline = Instant::now() + Duration::from_secs(u64::from(run.timeout_secs));
        let engine = Arc::clone(&self.engine);
        let mut work = tokio::spawn(engine_run(engine, run.clone(), ctx.clone()));
        let mut stopped: Option<Stop> = None;
        // The lease runs on the orchestrator's clock: the runner counts it
        // from its last renewal on its own, and stops the run before it ends.
        let lease = Duration::from_secs(u64::from(engine_port::LEASE_SECS));
        let mut renewed_at = Instant::now();
        loop {
            tokio::select! {
                done = &mut work => {
                    // A reference or progress the engine set just before
                    // it ended still reaches the orchestrator: a repeated
                    // attempt resumes from it.
                    if ctx.with_state(|s| s.ref_changed) {
                        let _ = self.renew(&run, &ctx).await;
                    }
                    let outcome = done.unwrap_or_else(|_| Outcome::failed("runner_panic"));
                    let outcome = adjust(outcome, stopped);
                    return self.finish(&run, &ctx, outcome).await;
                }
                () = tokio::time::sleep(self.renew_every) => {}
                () = ctx.wake.notified() => {}
            }
            if stopped.is_none() && Instant::now() >= deadline {
                stopped = Some(Stop::Timeout);
                self.stop(&run, &ctx).await;
            }
            if stopped.is_none() && self.guard.disk_low() {
                stopped = Some(Stop::LowDisk);
                self.stop(&run, &ctx).await;
            }
            let renewal = self.renew(&run, &ctx).await;
            if let Ok(r) = &renewal {
                renewed_at = Instant::now();
                if r.scan {
                    self.scan.notify_one();
                }
            }
            match renewal.map(|r| r.action) {
                Ok(RenewAction::Cancel) if stopped.is_none() => {
                    stopped = Some(Stop::Cancel);
                    self.stop(&run, &ctx).await;
                }
                Ok(_) => {}
                Err(CallError::Lease) => return self.fence(&run, &ctx, work).await,
                Err(e) => tracing::warn!(run = %run.run, error = %e, "renewal failed"),
            }
            if renewed_at.elapsed() + self.renew_every >= lease {
                return self.fence(&run, &ctx, work).await;
            }
        }
    }

    async fn renew(&self, run: &Claimed, ctx: &RunContext) -> Result<Renewed, CallError> {
        self.flush_logs(run, ctx).await?;
        let (progress, engine_ref) = ctx.with_state(|s| {
            s.ref_changed = false;
            (s.progress.clone(), s.engine_ref.clone())
        });
        let r = Renew {
            lease: run.lease.clone(),
            progress,
            engine_ref,
        };
        self.orchestrator.renew(&run.run, &r).await
    }

    async fn flush_logs(&self, run: &Claimed, ctx: &RunContext) -> Result<(), CallError> {
        loop {
            let lines = ctx.with_state(|s| {
                let n = s.lines.len().min(engine_port::MAX_LOG_LINES);
                s.lines.drain(..n).collect::<Vec<_>>()
            });
            if lines.is_empty() {
                return Ok(());
            }
            let logs = Logs {
                lease: run.lease.clone(),
                lines,
            };
            self.orchestrator.logs(&run.run, &logs).await?;
        }
    }

    async fn stop(&self, run: &Claimed, ctx: &RunContext) {
        let r = RunRef {
            run: run.run.clone(),
            engine_ref: ctx.with_state(|s| s.engine_ref.clone()),
        };
        if let Err(e) = self.engine.cancel(&r).await {
            tracing::warn!(run = %run.run, error = %e, "the engine did not stop the run");
        }
    }

    /// The lease is gone: stop the engine, so that the run never runs twice
    /// when it is repeated elsewhere, and report nothing.
    async fn fence(&self, run: &Claimed, ctx: &RunContext, work: JoinHandle<Outcome>) -> Ended {
        self.stop(run, ctx).await;
        let _ = work.await;
        tracing::warn!(run = %run.run, "lease lost: the run was stopped");
        Ended::Fenced
    }

    async fn finish(&self, run: &Claimed, ctx: &RunContext, outcome: Outcome) -> Ended {
        if let Err(e) = self.flush_logs(run, ctx).await {
            tracing::warn!(run = %run.run, error = %e, "last log lines not sent");
        }
        let f = Finish {
            lease: run.lease.clone(),
            state: outcome.state,
            counts: outcome.counts,
            error_class: outcome.error_class,
            interruption: outcome.interruption,
        };
        match self.orchestrator.finish(&run.run, &f).await {
            Ok(()) => Ended::Reported(f.state),
            Err(CallError::Lease) => Ended::Fenced,
            Err(e) => {
                tracing::warn!(run = %run.run, error = %e, "the end was not reported");
                Ended::Fenced
            }
        }
    }
}

async fn engine_run<E: Engine>(engine: Arc<E>, run: Claimed, ctx: RunContext) -> Outcome {
    engine.run(run, ctx).await
}

/// What the runner reports when it stopped the engine itself.
fn adjust(outcome: Outcome, stopped: Option<Stop>) -> Outcome {
    match stopped {
        Some(Stop::Timeout) => Outcome {
            state: FinalState::TimedOut,
            ..outcome
        },
        Some(Stop::LowDisk) => Outcome {
            state: FinalState::Failed,
            error_class: Some("low_disk".into()),
            interruption: Some(Interruption::LowDisk),
            ..outcome
        },
        Some(Stop::Cancel) => Outcome {
            state: FinalState::Cancelled,
            error_class: None,
            ..outcome
        },
        None => outcome,
    }
}
