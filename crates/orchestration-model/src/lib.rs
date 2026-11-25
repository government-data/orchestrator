//! The orchestrator's model, for every engine without naming one (decision
//! 0011): flows and the engine and line each is pinned to, runs and their state
//! machine, what an interrupted run does, what keeps a flow from firing, the fires
//! missed and the maintenance windows.
//!
//! Pure: no I/O, no clock. The server's adapters store it and its use cases
//! drive it.

mod attempts;
mod flow;
mod missed;
mod run;
mod window;

pub use attempts::{
    retry, retry_delay, Interrupted, Interruption, OnInterrupt, Retry, MAX_ATTEMPTS,
    RETRY_FIRST_DELAY_SECS, RETRY_MAX_AGE_HOURS, RETRY_MAX_DELAY_SECS,
};
pub use flow::{
    blocked, merge_parameters, Blocked, EngineLine, EngineName, EnginePause, Flow, Folder,
    InvalidName, Line,
};
pub use missed::{fires_to_run, MissedRuns, Settings, MISSED_AFTER_SECS};
pub use run::{FinalState, RunState, Transition, Trigger, UnknownState};
pub use window::{
    open_windows, CronCalendar, MaintenanceWindow, OnSchedule, OpenWindow, WindowTime,
};
