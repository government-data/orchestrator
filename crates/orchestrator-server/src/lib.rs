//! The orchestrator's server (decision 0011): the scheduler that fires
//! schedules and looks after runs, and the engine port that engine modules'
//! runners pull their runs from. It never names an engine.

mod clock;
mod core_api;
mod core_calls;
mod core_schedules;
mod deploys;
mod events;
mod found;
mod http;
mod identity;
mod import;
mod inventory;
mod machines;
mod metrics;
mod modules;
mod outbox;
mod people;
mod people_http;
mod port;
mod removal;
mod scan;
mod scheduler;
mod sealer;
mod settings;
mod token;

pub use clock::{Clock, FixedClock, SystemClock};
pub use core_api::{
    CoreToken, HttpEffectiveActions, HttpMachineList, HttpModuleList, MachineClient, CORE_AUDIENCE,
};
pub use core_calls::{core_router, DrainAnswer};
pub use core_schedules::{is_core_id, CoreFire, CoreSchedule, CoreSchedules};
pub use events::{
    iam_changes_once, watch_iam_changes, CoreEvents, Event, HttpEvents, Published, Received,
    IAM_CHANGED,
};
pub use http::{counted, router, ApiError, Authenticate, Caller, PortState};
pub use identity::{named_machine, NamedMachine, TraefikMachine, CLIENT_CERT_INFO};
pub use import::{
    ImportAnswer, ImportError, ImportItem, ImportedItem, Skipped, SkippedItem, MAX_IMPORT,
};
pub use machines::MachineList;
pub use metrics::{Metrics, RunResult};
pub use modules::{installed_lines, ModuleList};
pub use outbox::{send_outbox, OutboxSent};
pub use people::{
    person_of, CachedActions, EffectiveActions, OidcPeople, People, Person, PersonTokens, Refused,
    IMPORT_FLOWS,
};
pub use people_http::{people_router, PeopleState};
pub use port::{runner_of, EnginePort, Machine, PortError};
pub use removal::{removal, RemovalAnswer, ORCHESTRATOR_MODULE};
pub use scan::ScanAsked;
pub use scheduler::{Scheduler, Tick, RUNNERS_FENCE};
pub use sealer::{NotOpened, Sealer};
pub use settings::{machine_dir, machine_http, Settings};
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
