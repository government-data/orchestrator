//! The engine runner (decision 0011): what every engine module's runner
//! shares. It registers its engine, claims runs from the orchestrator over
//! the engine port while it has a free slot and its own guard allows it,
//! renews each run's lease, passes a cancel on to the engine, stops a run
//! whose lease it lost so it never runs twice, and reports its end. An
//! engine implements [`Engine`]; the [`contract`] suite checks a runner and
//! an orchestrator against each other. It names no engine.

mod client;
pub mod contract;
mod engine;
mod metrics;
mod runner;

pub use client::{CallError, HttpOrchestrator, Orchestrator};
pub use engine::{Engine, EngineError, Outcome, Refused, RunContext, RunRef};
pub use metrics::{PauseReason, RunnerMetrics, RunnerResult};
pub use runner::{Ended, Guard, NoGuard, Pause, Runner, FOUND_EVERY};

/// The runner's metrics file in `QD_METRICS_DIR`, with `registry`, named
/// after its engine module (`<module>.prom`); `None` when off.
#[must_use]
pub fn metrics_file(
    module: &str,
    registry: prometheus_client::registry::Registry,
) -> Option<logging::metrics::MetricsFile> {
    logging::metrics::MetricsFile::from_env(module, registry)
}
