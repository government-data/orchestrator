//! Chains: ordered steps of one flow, each a run of its own kind, queued
//! only when the step before it succeeded (core decision 0090, point 2: a
//! cut fires as a chain). A step may be a setup that runs only when its
//! key's fingerprint changed since it last succeeded (ruling 66: what
//! prepares a dataset's tables runs before its first load and when its
//! columns change). The orchestrator names no kind: the steps come from
//! whoever fires the chain.

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::RunState;

/// The most steps one chain has.
pub const MAX_STEPS: usize = 16;

/// A setup step's key and the fingerprint of what it sets up.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Setup {
    /// What it sets up, `[a-z0-9_.:-]`, 1 to 200 bytes.
    pub key: String,
    /// What it was set up from (a digest), `[A-Za-z0-9_.:-]`, 1 to 128 bytes.
    pub fingerprint: String,
}

/// One step: a run of `kind` with `parameters` over the flow's.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChainStep {
    /// `[a-z][a-z0-9_]`, at most 40 bytes.
    pub kind: String,
    #[serde(default)]
    pub parameters: Value,
    /// A setup step is skipped while its fingerprint is the one that last
    /// succeeded for its key.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub setup: Option<Setup>,
}

fn is_kind(s: &str) -> bool {
    s.len() <= 40
        && s.starts_with(|c: char| c.is_ascii_lowercase())
        && s.chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
}

fn is_token(s: &str, max: usize, upper: bool) -> bool {
    (1..=max).contains(&s.len())
        && s.chars().all(|c| {
            c.is_ascii_lowercase()
                || c.is_ascii_digit()
                || (upper && c.is_ascii_uppercase())
                || matches!(c, '_' | '.' | ':' | '-')
        })
}

/// Checks a chain's steps: 1 to [`MAX_STEPS`], each kind well formed, its
/// parameters an object (or absent), a setup's key and fingerprint well
/// formed.
pub fn validate_steps(steps: &[ChainStep]) -> Result<(), String> {
    if steps.is_empty() || steps.len() > MAX_STEPS {
        return Err(format!("steps: 1 to {MAX_STEPS}"));
    }
    for (i, s) in steps.iter().enumerate() {
        if !is_kind(&s.kind) {
            return Err(format!("step {i}: kind [a-z][a-z0-9_], at most 40"));
        }
        if !(s.parameters.is_object() || s.parameters.is_null()) {
            return Err(format!("step {i}: parameters is an object"));
        }
        if let Some(setup) = &s.setup {
            if !is_token(&setup.key, 200, false) || !is_token(&setup.fingerprint, 128, true) {
                return Err(format!(
                    "step {i}: setup key or fingerprint not well formed"
                ));
            }
        }
    }
    Ok(())
}

/// What a chain does next.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChainMove {
    /// Its step's run is queued or running.
    Wait,
    /// Queue a run of this step.
    Queue(usize),
    /// Every step succeeded or was skipped.
    Succeeded,
    /// Its step's last run ended without success and is not repeated.
    Failed,
}

/// The chain at step `at`, whose newest run of that step is in `newest`
/// (`None`: none queued yet). `done` says whether a setup's fingerprint is
/// the one that last succeeded for its key; a step that just succeeded is
/// never skipped again. Returns the step it is at now, and what to do.
pub fn next_move(
    steps: &[ChainStep],
    at: usize,
    newest: Option<RunState>,
    done: impl Fn(&Setup) -> bool,
) -> (usize, ChainMove) {
    let mut at = at;
    match newest {
        Some(RunState::Queued | RunState::Running) => return (at, ChainMove::Wait),
        Some(RunState::Succeeded) => at += 1,
        Some(_) => return (at, ChainMove::Failed),
        None => {}
    }
    while steps
        .get(at)
        .and_then(|s| s.setup.as_ref())
        .is_some_and(&done)
    {
        at += 1;
    }
    if at >= steps.len() {
        (steps.len(), ChainMove::Succeeded)
    } else {
        (at, ChainMove::Queue(at))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn step(kind: &str, setup: Option<(&str, &str)>) -> ChainStep {
        ChainStep {
            kind: kind.into(),
            parameters: json!({"dataset": "sales"}),
            setup: setup.map(|(key, fingerprint)| Setup {
                key: key.into(),
                fingerprint: fingerprint.into(),
            }),
        }
    }

    fn cut() -> Vec<ChainStep> {
        vec![
            step("prepare", Some(("sales", "f1"))),
            step("extract", None),
            step("evaluate", None),
        ]
    }

    #[test]
    fn a_new_chain_queues_its_setup_when_its_fingerprint_is_new() {
        assert_eq!(
            next_move(&cut(), 0, None, |_| false),
            (0, ChainMove::Queue(0))
        );
    }

    #[test]
    fn a_setup_already_done_with_that_fingerprint_is_skipped() {
        let done = |s: &Setup| s.key == "sales" && s.fingerprint == "f1";
        assert_eq!(next_move(&cut(), 0, None, done), (1, ChainMove::Queue(1)));
    }

    #[test]
    fn a_running_step_waits_and_a_succeeded_one_queues_the_next() {
        for state in [RunState::Queued, RunState::Running] {
            assert_eq!(
                next_move(&cut(), 1, Some(state), |_| false),
                (1, ChainMove::Wait)
            );
        }
        assert_eq!(
            next_move(&cut(), 1, Some(RunState::Succeeded), |_| false),
            (2, ChainMove::Queue(2))
        );
        assert_eq!(
            next_move(&cut(), 2, Some(RunState::Succeeded), |_| false),
            (3, ChainMove::Succeeded)
        );
    }

    #[test]
    fn a_step_that_ended_otherwise_fails_the_chain_where_it_is() {
        for state in [
            RunState::Failed,
            RunState::TimedOut,
            RunState::Cancelled,
            RunState::Lost,
        ] {
            assert_eq!(
                next_move(&cut(), 1, Some(state), |_| true),
                (1, ChainMove::Failed)
            );
        }
    }

    #[test]
    fn steps_are_checked() {
        assert!(validate_steps(&cut()).is_ok());
        assert!(validate_steps(&[]).is_err());
        assert!(validate_steps(&vec![step("extract", None); MAX_STEPS + 1]).is_err());
        assert!(validate_steps(&[step("Extract", None)]).is_err());
        assert!(validate_steps(&[step("prepare", Some(("sales", "")))]).is_err());
        assert!(validate_steps(&[step("prepare", Some(("Sales", "f1")))]).is_err());
        let mut odd = step("extract", None);
        odd.parameters = json!(["a"]);
        assert!(validate_steps(&[odd]).is_err());
        let wire: ChainStep = serde_json::from_value(json!({"kind": "extract"})).unwrap();
        assert!(wire.parameters.is_null() && wire.setup.is_none());
        assert!(serde_json::from_value::<ChainStep>(json!({"kind": "x", "sql": "no"})).is_err());
    }
}
