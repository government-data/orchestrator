//! The orchestrator's server (decision 0011): the scheduler that fires
//! schedules and looks after runs, and the engine port that engine modules'
//! runners pull their runs from. It never names an engine.

mod clock;
mod http;
mod identity;
mod metrics;
mod port;
mod scheduler;
mod token;

pub use clock::{Clock, FixedClock, SystemClock};
pub use http::{counted, router, ApiError, Authenticate, Caller, PortState};
pub use identity::{named_machine, NamedMachine, TraefikMachine, CLIENT_CERT_INFO};
pub use metrics::{Metrics, RunResult};
pub use port::{runner_of, EnginePort, Machine, PortError};
pub use scheduler::{Scheduler, Tick, RUNNERS_FENCE};
pub use token::{ed25519_keys, module_of, Expected, InternalCaller, JwksKeys, KeyList, Keys};

/// Installs the JSON technical logs on stdout (the core's `logging`): run
/// ids, never parameters, values or SQL.
pub fn init_logs() {
    logging::init("orchestrator");
}

/// The metrics file in `QD_METRICS_DIR`, with `registry`; `None` when off.
#[must_use]
pub fn metrics_file(
    registry: prometheus_client::registry::Registry,
) -> Option<logging::metrics::MetricsFile> {
    logging::metrics::MetricsFile::from_env("orchestrator", registry)
}
