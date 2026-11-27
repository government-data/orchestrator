//! The runner's own metrics (decision 0011 section 4), in its metrics file
//! for Alloy (the core's `logging::metrics`): whether its guard pauses its
//! claims and why, its busy slots, the runs it ended by result, and how
//! long each stage of a run took (core decision 0090, ruling 70). Labels
//! are `engine`, `line`, `kind`, `result`, `reason` and `stage` only; a
//! stage is a name the engine fixes in its code, never an id or a value.

use std::sync::Arc;
use std::time::Duration;

use engine_port::Descriptor;
use orchestration_model::FinalState;
use prometheus_client::encoding::{EncodeLabelSet, EncodeLabelValue};
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::family::Family;
use prometheus_client::metrics::gauge::Gauge;
use prometheus_client::metrics::histogram::{exponential_buckets, Histogram};
use prometheus_client::registry::Registry;

use crate::{Ended, Pause};

/// Why a runner claims nothing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, EncodeLabelValue)]
#[allow(non_camel_case_types)]
pub enum PauseReason {
    low_disk,
    low_memory,
}

/// How a run the runner drove ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, EncodeLabelValue)]
#[allow(non_camel_case_types)]
pub enum RunnerResult {
    succeeded,
    failed,
    timed_out,
    cancelled,
    /// Its lease was lost and the runner stopped it.
    fenced,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, EncodeLabelSet)]
pub struct LineLabels {
    pub engine: String,
    pub line: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, EncodeLabelSet)]
pub struct PausedLabels {
    pub engine: String,
    pub line: String,
    pub reason: PauseReason,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, EncodeLabelSet)]
pub struct EndedLabels {
    pub engine: String,
    pub line: String,
    pub kind: String,
    pub result: RunnerResult,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, EncodeLabelSet)]
pub struct StageLabels {
    pub engine: String,
    pub line: String,
    pub kind: String,
    pub stage: String,
}

type Buckets = fn() -> Histogram;

/// A stage's buckets: 50 ms to about an hour, by four.
fn stage_buckets() -> Histogram {
    Histogram::new(exponential_buckets(0.05, 4.0, 9))
}

/// Whether `s` can be a stage's label: `[a-z_]`, 1 to 32.
fn is_stage(s: &str) -> bool {
    !s.is_empty() && s.len() <= 32 && s.bytes().all(|b| b.is_ascii_lowercase() || b == b'_')
}

/// The runner's metrics.
#[derive(Debug)]
pub struct RunnerMetrics {
    engine: String,
    line: String,
    pub claim_paused: Family<PausedLabels, Gauge>,
    pub slots_busy: Family<LineLabels, Gauge>,
    pub runs: Family<EndedLabels, Counter>,
    pub stages: Family<StageLabels, Histogram, Buckets>,
}

impl RunnerMetrics {
    /// The metrics of a runner of `d`, registered under `orchestrator_runner_`.
    pub fn register(d: &Descriptor, registry: &mut Registry) -> Arc<Self> {
        let m = Self {
            engine: d.engine.to_string(),
            line: d.line.to_string(),
            claim_paused: Family::default(),
            slots_busy: Family::default(),
            runs: Family::default(),
            stages: Family::new_with_constructor(stage_buckets),
        };
        let r = registry.sub_registry_with_prefix("orchestrator_runner");
        r.register(
            "claim_paused",
            "1 while the guard pauses claims",
            m.claim_paused.clone(),
        );
        r.register(
            "slots_busy",
            "Runs this runner drives now",
            m.slots_busy.clone(),
        );
        r.register("runs", "Runs this runner ended", m.runs.clone());
        r.register(
            "stage_seconds",
            "How long a stage of a run took",
            m.stages.clone(),
        );
        Arc::new(m)
    }

    fn line(&self) -> LineLabels {
        LineLabels {
            engine: self.engine.clone(),
            line: self.line.clone(),
        }
    }

    /// What the guard says now: one reason at 1, the others at 0.
    pub fn paused(&self, pause: Option<Pause>) {
        for (reason, on) in [
            (PauseReason::low_disk, pause == Some(Pause::LowDisk)),
            (PauseReason::low_memory, pause == Some(Pause::LowMemory)),
        ] {
            let labels = PausedLabels {
                engine: self.engine.clone(),
                line: self.line.clone(),
                reason,
            };
            self.claim_paused.get_or_create(&labels).set(i64::from(on));
        }
    }

    /// How many slots are busy.
    pub fn busy(&self, n: usize) {
        let n = i64::try_from(n).unwrap_or(i64::MAX);
        self.slots_busy.get_or_create(&self.line()).set(n);
    }

    /// A run of `kind` ended so.
    pub fn ended(&self, kind: &str, ended: &Ended) {
        let result = match ended {
            Ended::Reported(FinalState::Succeeded) => RunnerResult::succeeded,
            Ended::Reported(FinalState::Failed) => RunnerResult::failed,
            Ended::Reported(FinalState::TimedOut) => RunnerResult::timed_out,
            Ended::Reported(FinalState::Cancelled) => RunnerResult::cancelled,
            Ended::Fenced => RunnerResult::fenced,
        };
        let labels = EndedLabels {
            engine: self.engine.clone(),
            line: self.line.clone(),
            kind: kind.to_string(),
            result,
        };
        self.runs.get_or_create(&labels).inc();
    }

    /// A stage of a run of `kind` took `took`; a stage that is not a
    /// fixed name (`[a-z_]`, 1 to 32) is not counted.
    pub fn stage(&self, kind: &str, stage: &'static str, took: Duration) {
        if !is_stage(stage) {
            return;
        }
        let labels = StageLabels {
            engine: self.engine.clone(),
            line: self.line.clone(),
            kind: kind.to_string(),
            stage: stage.to_string(),
        };
        self.stages
            .get_or_create(&labels)
            .observe(took.as_secs_f64());
    }
}
