//! What an engine module implements: its descriptor, and how it validates,
//! runs and cancels a run. Logs, progress and the engine's own reference
//! of a run go through the [`RunContext`].

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use engine_port::{
    Claimed, Descriptor, Found, Inventory, LogLine, LogStream, Progress, MAX_LOG_LINE_BYTES,
};
use orchestration_model::{FinalState, Interruption};
use tokio::sync::Notify;

use crate::client::RunLocks;
use crate::{CallError, RunnerMetrics};

/// A run the engine refuses before it starts (its parameters do not fit
/// its kind): it ends `failed` with this class.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Refused {
    /// `[a-z0-9_]`, at most 64: a class, never a message.
    pub class: String,
}

/// The engine could not do what it was asked.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EngineError(pub String);

impl std::fmt::Display for EngineError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for EngineError {}

/// How a run ended, as the engine saw it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Outcome {
    pub state: FinalState,
    /// Named counts, never values.
    pub counts: BTreeMap<String, u64>,
    pub error_class: Option<String>,
    pub interruption: Option<Interruption>,
}

impl Outcome {
    #[must_use]
    pub fn succeeded() -> Self {
        Self {
            state: FinalState::Succeeded,
            counts: BTreeMap::new(),
            error_class: None,
            interruption: None,
        }
    }

    #[must_use]
    pub fn failed(class: &str) -> Self {
        Self {
            state: FinalState::Failed,
            error_class: Some(class.to_string()),
            ..Self::succeeded()
        }
    }

    #[must_use]
    pub fn cancelled() -> Self {
        Self {
            state: FinalState::Cancelled,
            ..Self::succeeded()
        }
    }
}

/// A run the engine is asked to cancel: its id and the engine's reference.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunRef {
    pub run: String,
    pub engine_ref: Option<String>,
}

/// An engine, as its module implements it.
#[async_trait]
pub trait Engine: Send + Sync + 'static {
    /// Engine, line, version and kinds.
    fn descriptor(&self) -> Descriptor;
    /// Checks a claimed run before it starts.
    async fn validate(&self, run: &Claimed) -> Result<(), Refused>;
    /// Runs it to its end; logs and progress through `ctx`. A cancelled run
    /// returns [`Outcome::cancelled`].
    async fn run(&self, run: Claimed, ctx: RunContext) -> Outcome;
    /// Stops a run: `run` then returns soon.
    async fn cancel(&self, run: &RunRef) -> Result<(), EngineError>;
    /// What is on the cluster that no deploy placed, and the external
    /// schedules that start it, for import; `None` (the default) for an
    /// engine without artefacts, which then reports nothing.
    async fn found(&self) -> Option<Found> {
        None
    }
    /// The records its deploys left beside their artefacts
    /// (`orchestrator.json`), for re-attach; `None` (the default) for an
    /// engine without artefacts.
    async fn inventory(&self) -> Option<Inventory> {
        None
    }
}

#[derive(Debug, Default)]
pub(crate) struct Reported {
    pub(crate) lines: Vec<LogLine>,
    pub(crate) next_seq: u64,
    pub(crate) progress: Option<Progress>,
    pub(crate) engine_ref: Option<String>,
    /// The engine reference changed since the last renewal.
    pub(crate) ref_changed: bool,
}

/// What a running engine reports through: log lines, progress and its own
/// reference of the run, sent at the next renewal (the reference at once).
#[derive(Debug, Clone, Default)]
pub struct RunContext {
    pub(crate) state: Arc<Mutex<Reported>>,
    pub(crate) wake: Arc<Notify>,
    /// The runner's metrics and the run's kind, for its stages' times.
    pub(crate) timing: Option<(Arc<RunnerMetrics>, String)>,
    /// The run's locks at the orchestrator, under its lease.
    pub(crate) locks: Option<Arc<dyn RunLocks>>,
}

impl RunContext {
    pub(crate) fn with_state<T>(&self, f: impl FnOnce(&mut Reported) -> T) -> T {
        let mut guard = self.state.lock().unwrap_or_else(|e| e.into_inner());
        f(&mut guard)
    }

    /// A log line; a longer one than the port takes is cut.
    pub fn log(&self, stream: LogStream, line: &str) {
        let mut end = line.len().min(MAX_LOG_LINE_BYTES);
        while !line.is_char_boundary(end) {
            end -= 1;
        }
        self.with_state(|s| {
            s.next_seq += 1;
            let seq = s.next_seq;
            s.lines.push(LogLine {
                seq,
                stream,
                line: line[..end].to_string(),
            });
        });
    }

    pub fn progress(&self, fraction: Option<f64>, detail: Option<&str>) {
        self.with_state(|s| {
            s.progress = Some(Progress {
                fraction,
                detail: detail.map(str::to_string),
            });
        });
    }

    /// Takes the lock `name` for this run (core decision 0090, point
    /// 6.2.4: one writer per table): `false` while another running run
    /// holds it, so ask again later. It is the run's until
    /// [`RunContext::unlock`] or the run's end.
    pub async fn lock(&self, name: &str) -> Result<bool, CallError> {
        match &self.locks {
            Some(l) => l.take(name).await,
            None => Err(CallError::Refused(404, "no run to lock for".into())),
        }
    }

    /// Gives the lock `name` back.
    pub async fn unlock(&self, name: &str) -> Result<(), CallError> {
        match &self.locks {
            Some(l) => l.give_back(name).await,
            None => Err(CallError::Refused(404, "no run to lock for".into())),
        }
    }

    /// A stage of the run (a name the engine fixes) took `took`: counted in
    /// the runner's metrics, if it has them.
    pub fn timed(&self, stage: &'static str, took: std::time::Duration) {
        if let Some((metrics, kind)) = &self.timing {
            metrics.stage(kind, stage, took);
        }
    }

    /// The engine's own reference of the run (a query id): sent before the
    /// engine goes on, so that a retry can stop it first.
    pub fn engine_ref(&self, reference: &str) {
        self.with_state(|s| {
            s.engine_ref = Some(reference.to_string());
            s.ref_changed = true;
        });
        self.wake.notify_one();
    }
}
