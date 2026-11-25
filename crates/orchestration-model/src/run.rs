//! The state machine of a run.
//!
//! ```text
//! queued ──claim──▶ running ──▶ succeeded | failed | timed_out | cancelled
//!   │                  └──(lease expired)──▶ lost
//!   └──cancel──▶ cancelled
//! ```
//!
//! Final states never change. A runner reports only the final states it can
//! observe ([`FinalState`]); `lost` is the orchestrator's, when a runner
//! stops renewing its lease.

use std::fmt;

use serde::{Deserialize, Serialize};

/// Where a run is.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunState {
    /// Queued, waiting for a runner of its engine and line to claim it.
    Queued,
    /// Claimed by a runner, under its lease.
    Running,
    /// The engine finished it without error.
    Succeeded,
    /// The engine finished it with an error, or could not start it.
    Failed,
    /// It went past its maximum time and the runner stopped it.
    TimedOut,
    /// Someone cancelled it.
    Cancelled,
    /// Its runner stopped renewing the lease: how it ended is not known.
    Lost,
}

impl RunState {
    /// Every state, in the order above.
    pub const ALL: [Self; 7] = [
        Self::Queued,
        Self::Running,
        Self::Succeeded,
        Self::Failed,
        Self::TimedOut,
        Self::Cancelled,
        Self::Lost,
    ];

    /// The stored name.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Running => "running",
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
            Self::TimedOut => "timed_out",
            Self::Cancelled => "cancelled",
            Self::Lost => "lost",
        }
    }

    /// Reads a stored name.
    pub fn parse(raw: &str) -> Result<Self, UnknownState> {
        Self::ALL
            .into_iter()
            .find(|s| s.as_str() == raw)
            .ok_or_else(|| UnknownState(raw.to_string()))
    }

    /// Whether it can no longer change.
    #[must_use]
    pub fn is_final(self) -> bool {
        !matches!(self, Self::Queued | Self::Running)
    }

    /// The state after `to`, if the machine allows it: a queued run is
    /// claimed or cancelled; a running one ends in a final state; a final
    /// state never changes.
    pub fn transition(self, to: Self) -> Result<Self, Transition> {
        let allowed = match self {
            Self::Queued => matches!(to, Self::Running | Self::Cancelled),
            Self::Running => to.is_final(),
            _ => false,
        };
        if allowed {
            Ok(to)
        } else {
            Err(Transition { from: self, to })
        }
    }
}

impl fmt::Display for RunState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A state name that is not one of [`RunState`]'s.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnknownState(pub String);

impl fmt::Display for UnknownState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "unknown run state {:?}", self.0)
    }
}

impl std::error::Error for UnknownState {}

/// A change of state the machine does not allow.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Transition {
    pub from: RunState,
    pub to: RunState,
}

impl fmt::Display for Transition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "a {} run cannot become {}", self.from, self.to)
    }
}

impl std::error::Error for Transition {}

/// The final states a runner may report.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FinalState {
    Succeeded,
    Failed,
    TimedOut,
    /// The runner stopped it because it was asked to cancel.
    Cancelled,
}

impl FinalState {
    /// Reads what a runner sends: never an open state, never `lost`.
    pub fn parse(raw: &str) -> Result<Self, UnknownState> {
        match RunState::parse(raw)? {
            RunState::Succeeded => Ok(Self::Succeeded),
            RunState::Failed => Ok(Self::Failed),
            RunState::TimedOut => Ok(Self::TimedOut),
            RunState::Cancelled => Ok(Self::Cancelled),
            _ => Err(UnknownState(raw.to_string())),
        }
    }

    /// The run state it stands for.
    #[must_use]
    pub fn state(self) -> RunState {
        match self {
            Self::Succeeded => RunState::Succeeded,
            Self::Failed => RunState::Failed,
            Self::TimedOut => RunState::TimedOut,
            Self::Cancelled => RunState::Cancelled,
        }
    }
}

/// What started a run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "id", rename_all = "snake_case")]
pub enum Trigger {
    /// A person, from the screens or the API (the subject of their sign-in).
    Manual(String),
    /// A schedule (its id).
    Schedule(String),
}

impl Trigger {
    /// `manual` or `schedule`, as stored.
    #[must_use]
    pub fn kind(&self) -> &'static str {
        match self {
            Self::Manual(_) => "manual",
            Self::Schedule(_) => "schedule",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn states_round_trip_and_only_queued_and_running_are_open() {
        for s in RunState::ALL {
            assert_eq!(RunState::parse(s.as_str()), Ok(s));
            assert_eq!(
                s.is_final(),
                !matches!(s, RunState::Queued | RunState::Running)
            );
        }
        assert!(RunState::parse("finished").is_err());
    }

    #[test]
    fn the_machine_allows_only_its_arrows() {
        use RunState::*;
        assert_eq!(Queued.transition(Running), Ok(Running));
        assert_eq!(Queued.transition(Cancelled), Ok(Cancelled));
        assert!(Queued.transition(Succeeded).is_err());
        assert!(Queued.transition(Lost).is_err());
        for end in [Succeeded, Failed, TimedOut, Cancelled, Lost] {
            assert_eq!(Running.transition(end), Ok(end));
            for to in RunState::ALL {
                assert!(end.transition(to).is_err(), "{end} is final");
            }
        }
        assert!(Running.transition(Queued).is_err());
        assert_eq!(
            Succeeded.transition(Running).unwrap_err().to_string(),
            "a succeeded run cannot become running"
        );
    }

    #[test]
    fn a_runner_cannot_report_an_open_state_or_lost() {
        assert!(FinalState::parse("running").is_err());
        assert!(FinalState::parse("queued").is_err());
        assert!(FinalState::parse("lost").is_err());
        assert_eq!(FinalState::parse("timed_out"), Ok(FinalState::TimedOut));
        assert_eq!(FinalState::Cancelled.state(), RunState::Cancelled);
    }

    #[test]
    fn a_trigger_says_what_started_it() {
        let t = Trigger::Schedule("s-1".into());
        assert_eq!(t.kind(), "schedule");
        assert_eq!(
            serde_json::to_value(&t).unwrap(),
            serde_json::json!({"kind": "schedule", "id": "s-1"})
        );
        assert_eq!(Trigger::Manual("p".into()).kind(), "manual");
    }
}
