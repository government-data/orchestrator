//! The orchestrator's answer to the check before a module's removal
//! (decision 0015): an engine module is not removed while a flow is pinned
//! to a line it registered or a run of one is queued or running; the
//! orchestrator itself is not removed while an engine module registered a
//! line. Counts only, never a flow's name.

use std::collections::BTreeMap;

use orchestrator_postgres::{PgStore, StoreError};
use serde::Serialize;

/// The orchestrator's own module name, as its manifest says.
pub const ORCHESTRATOR_MODULE: &str = "orchestrator";

/// The answer the core relays to the agent.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RemovalAnswer {
    pub refuse: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    pub counts: BTreeMap<String, u32>,
}

impl RemovalAnswer {
    fn allow() -> Self {
        Self {
            refuse: false,
            reason: None,
            counts: BTreeMap::new(),
        }
    }
}

/// Whether `module` may be removed; `own` is the orchestrator's own module
/// name.
pub async fn removal(
    store: &PgStore,
    own: &str,
    module: &str,
) -> Result<RemovalAnswer, StoreError> {
    if module == own {
        let engines = store.engine_modules().await?;
        if engines == 0 {
            return Ok(RemovalAnswer::allow());
        }
        return Ok(RemovalAnswer {
            refuse: true,
            reason: Some(format!(
                "{engines} engine module(s) still run on the orchestrator: remove them first"
            )),
            counts: BTreeMap::from([("engine_modules".to_string(), engines)]),
        });
    }
    let on = store.on_lines_of(module).await?;
    if on.flows == 0 && on.runs == 0 {
        return Ok(RemovalAnswer::allow());
    }
    Ok(RemovalAnswer {
        refuse: true,
        reason: Some(format!(
            "{} flow(s) and {} queued or running run(s) are on its engine line(s): \
             move them first (a test run of each flow on the new line, move it, \
             then remove the line)",
            on.flows, on.runs
        )),
        counts: BTreeMap::from([
            ("flows".to_string(), on.flows),
            ("lines".to_string(), on.lines),
            ("runs".to_string(), on.runs),
        ]),
    })
}
