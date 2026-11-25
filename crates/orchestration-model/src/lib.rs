//! The orchestrator's model, for every engine without naming one (decision
//! 0011): runs and their state machine, and what an interrupted run does.
//!
//! Pure: no I/O, no clock. The server's adapters store it and its use cases
//! drive it.

mod attempts;
mod run;

pub use attempts::{
    retry, retry_delay, Interrupted, Interruption, OnInterrupt, Retry, MAX_ATTEMPTS,
    RETRY_FIRST_DELAY_SECS, RETRY_MAX_AGE_HOURS, RETRY_MAX_DELAY_SECS,
};
pub use run::{FinalState, RunState, Transition, Trigger, UnknownState};
