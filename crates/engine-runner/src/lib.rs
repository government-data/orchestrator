//! The engine runner (decision 0011): what every engine module's runner
//! shares. An engine implements [`Engine`]; the runner calls the
//! orchestrator over the engine port ([`HttpOrchestrator`]). It names no
//! engine.

mod client;
mod engine;

pub use client::{CallError, HttpOrchestrator, Orchestrator};
pub use engine::{Engine, EngineError, Outcome, Refused, RunContext, RunRef};
