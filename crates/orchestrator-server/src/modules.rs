//! Which modules the core lists as installed (decision 0015, point 4): a
//! line is installed only while the module that registered it is listed,
//! so a line removed by hand or by an old agent stops firing. The core's
//! answer is cached by the scheduler; when the core cannot be read, the
//! last list is kept, and before any list every registered line counts.
//! Its HTTP adapter is [`crate::HttpModuleList`].

use std::collections::HashSet;

use async_trait::async_trait;
use orchestration_model::EngineLine;
use orchestrator_postgres::RegisteredEngine;

/// The core's list of installed modules.
#[async_trait]
pub trait ModuleList: Send + Sync + 'static {
    /// The installed modules' names; `None` when the core did not answer.
    async fn installed(&self) -> Option<Vec<String>>;
}

/// The lines installed: registered, and by a module the core lists (a line
/// whose module is not known counts while it is registered).
#[must_use]
pub fn installed_lines(
    engines: &[RegisteredEngine],
    listed: Option<&HashSet<String>>,
) -> HashSet<EngineLine> {
    engines
        .iter()
        .filter(|e| match (&e.module, listed) {
            (Some(module), Some(listed)) => listed.contains(module),
            _ => true,
        })
        .map(|e| e.engine.clone())
        .collect()
}
