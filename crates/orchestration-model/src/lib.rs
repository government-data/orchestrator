//! The orchestrator's model, for every engine without naming one (decision
//! 0011): runs and their state machine.
//!
//! Pure: no I/O, no clock. The server's adapters store it and its use cases
//! drive it.

mod run;

pub use run::{FinalState, RunState, Transition, Trigger, UnknownState};
