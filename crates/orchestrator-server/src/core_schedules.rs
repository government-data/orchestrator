//! The schedules the core keeps and the orchestrator fires (core decision
//! 0090, point 2). The core lists its schedules; the scheduler keeps each
//! as a schedule of one flow, fires it by the orchestrator's rules, and at
//! each fire asks the core what to run: a chain of steps (a cut's prepare,
//! extract, derive, evaluate), or nothing (a cut that is blocked). The
//! core compiles a step's statements only when its job runner first asks
//! for the run's bundle, not at the fire.
//!
//! Its HTTP adapter is [`crate::HttpCoreSchedules`].

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use orchestration_model::ChainStep;
use schedule::ScheduleSpec;

/// One schedule as the core keeps it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CoreSchedule {
    /// The core's id, `[A-Za-z0-9_-]`, at most 200 bytes.
    pub id: String,
    pub spec: ScheduleSpec,
    pub enabled: bool,
}

/// What the core says to run for one fire.
#[derive(Debug, Clone, PartialEq)]
pub struct CoreFire {
    /// The chain's steps in order; empty: nothing runs for this fire.
    pub steps: Vec<ChainStep>,
}

/// The core's schedules and fires.
#[async_trait]
pub trait CoreSchedules: Send + Sync + 'static {
    /// Every schedule the core keeps; `None` when the core did not answer.
    async fn schedules(&self) -> Option<Vec<CoreSchedule>>;

    /// The chain for the fire at `fire_at` of the core's schedule `id`;
    /// `None` when the core did not answer (the fire is asked again). The
    /// same fire asked twice gets the same answer.
    async fn fire(&self, id: &str, fire_at: DateTime<Utc>) -> Option<CoreFire>;
}

/// Whether `id` is a core schedule's id the orchestrator keeps.
#[must_use]
pub fn is_core_id(id: &str) -> bool {
    (1..=200).contains(&id.len())
        && id
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
}
