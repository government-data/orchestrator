//! The engine runner (decision 0011): what every engine module's runner
//! shares. It registers its engine, claims runs from the orchestrator over
//! the engine port while it has a free slot and its own guard allows it,
//! renews each run's lease, passes a cancel on to the engine, stops a run
//! whose lease it lost so it never runs twice, and reports its end. An
//! engine implements [`Engine`]. It names no engine.

mod client;
mod engine;
mod runner;

pub use client::{CallError, HttpOrchestrator, Orchestrator};
pub use engine::{Engine, EngineError, Outcome, Refused, RunContext, RunRef};
pub use runner::{Ended, Guard, NoGuard, Pause, Runner};
