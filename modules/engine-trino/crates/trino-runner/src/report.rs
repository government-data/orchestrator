//! A run's report to the core (core decision 0090, ruling 75): after a job
//! that ran to its end, `{digest, state, statements, snapshots,
//! error_class?}`, with `rows` only for a statement whose `expects` is a
//! `Rows` read. Counts and names the bundle reads back, never a source's
//! values; the core checks the digest and each statement's rows against
//! the pinned bundle and keeps a report once, so sending it again is safe.

use std::time::Duration;

use engine_port::LogStream;
use engine_runner::{Outcome, RunContext};
use orchestration_model::FinalState;
use serde::Serialize;
use serde_json::Value;

use crate::bundle::{Bundle, Expects, Statement};
use crate::check::{as_text, Answer};
use crate::engine::TrinoEngine;
use crate::ports::{Core, Delivery, Trino};
use crate::steps::Known;

/// How many times a report is sent before the run gives up on it.
const TRIES: u32 = 5;

/// How the job ended, as the core takes it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ReportState {
    Done,
    Failed,
}

/// How a statement ended in this job.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum StatementState {
    Done,
    Skipped,
    Failed,
}

/// One statement's line of the report.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct StatementReport {
    pub id: String,
    pub state: StatementState,
    /// The rows of a `Rows` read this attempt ran; never for a write.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rows: Option<Vec<Vec<Value>>>,
}

/// A placeholder as the job filled it: a snapshot id or a bigint.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SnapshotFilled {
    pub placeholder: String,
    pub value: String,
}

/// What the runner reports of a run.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct RunReport {
    pub digest: String,
    pub state: ReportState,
    pub statements: Vec<StatementReport>,
    pub snapshots: Vec<SnapshotFilled>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_class: Option<String>,
}

/// The statements as they ended, kept while the job runs.
#[derive(Debug, Default)]
pub(crate) struct Trail {
    pub(crate) statements: Vec<StatementReport>,
    pub(crate) snapshots: Vec<SnapshotFilled>,
}

impl Trail {
    pub(crate) fn push(&mut self, id: &str, state: StatementState) {
        self.statements.push(StatementReport {
            id: id.to_string(),
            state,
            rows: None,
        });
    }

    /// A statement done, with its rows if it is a `Rows` read.
    pub(crate) fn done(&mut self, id: &str, expects: &Expects, rows: &[Vec<Value>]) {
        self.statements.push(StatementReport {
            id: id.to_string(),
            state: StatementState::Done,
            rows: matches!(expects, Expects::Rows { .. }).then(|| rows.to_vec()),
        });
    }

    /// The placeholders the job filled, in the bundle's order.
    pub(crate) fn filled(&mut self, bundle: &Bundle, known: &Known) {
        self.snapshots = bundle
            .placeholders
            .iter()
            .filter_map(|p| {
                known.0.get(&p.name).map(|v| SnapshotFilled {
                    placeholder: p.name.clone(),
                    value: v.clone(),
                })
            })
            .collect();
    }

    /// The report of a job that ended so, or `None` if it did not end
    /// (cut by the system, to be repeated, or cancelled).
    pub(crate) fn report(self, digest: &str, outcome: &Outcome) -> Option<RunReport> {
        if outcome.interruption.is_some() {
            return None;
        }
        let state = match outcome.state {
            FinalState::Succeeded => ReportState::Done,
            FinalState::Failed | FinalState::TimedOut => ReportState::Failed,
            FinalState::Cancelled => return None,
        };
        Some(RunReport {
            digest: digest.to_string(),
            state,
            statements: self.statements,
            snapshots: self.snapshots,
            error_class: outcome.error_class.clone(),
        })
    }
}

impl<C: Core, T: Trino> TrinoEngine<C, T> {
    /// Sends `report` until the core keeps it (or already had it), a few
    /// times while it does not answer; the run fails if it never does, or
    /// if the core refuses it, keeping a failed job's own class.
    pub(crate) async fn deliver(
        &self,
        job: &str,
        report: &RunReport,
        outcome: Outcome,
        ctx: &RunContext,
    ) -> Outcome {
        let mut wait = self.report_wait;
        for attempt in 1..=TRIES {
            match self.core.report(job, report).await {
                Ok(Delivery::Kept) => return outcome,
                Ok(Delivery::Refused(why)) => {
                    ctx.log(LogStream::System, &format!("report refused: {why}"));
                    return Self::undelivered(outcome, "report_refused");
                }
                Err(_) if attempt < TRIES => {
                    tokio::time::sleep(wait).await;
                    wait = wait.saturating_mul(2);
                }
                Err(_) => {}
            }
        }
        ctx.log(
            LogStream::System,
            "report not delivered: the core did not answer",
        );
        Self::undelivered(outcome, "report_not_delivered")
    }

    /// Sends a values read's rows until the core keeps them, a few times
    /// while it does not answer; then the run is cut by the system and a
    /// repeated attempt reads them again. A refusal fails it for good. No
    /// row reaches a log: only their statement's id and their count.
    async fn post_rows(
        &self,
        job: &str,
        statement: &str,
        columns: Option<&[String]>,
        rows: &[Vec<Value>],
        ctx: &RunContext,
    ) -> Result<(), Outcome> {
        let mut wait = self.report_wait;
        for attempt in 1..=TRIES {
            match self.core.rows(job, statement, columns, rows).await {
                Ok(Delivery::Kept) => {
                    let n = rows.len();
                    ctx.log(
                        LogStream::System,
                        &format!("statement {statement}: {n} rows sent"),
                    );
                    return Ok(());
                }
                Ok(Delivery::Refused(why)) => {
                    ctx.log(LogStream::System, &format!("rows refused: {why}"));
                    return Err(Outcome::failed("rows_refused"));
                }
                Err(_) if attempt < TRIES => {
                    tokio::time::sleep(wait).await;
                    wait = wait.saturating_mul(2);
                }
                Err(_) => {}
            }
        }
        Err(crate::engine::transient("core_unavailable"))
    }

    /// A values read's rows to the core: a `values` read's as Trino
    /// answered them, a `text` read's with its answer's column names and
    /// every value as text. Nothing for any other statement.
    pub(crate) async fn values(
        &self,
        job: &str,
        st: &Statement,
        answer: &Answer,
        ctx: &RunContext,
    ) -> Result<(), Outcome> {
        match st.expects {
            Expects::Values { .. } => self.post_rows(job, &st.id, None, &answer.rows, ctx).await,
            Expects::Text { .. } => {
                let names: Vec<String> = answer.columns.iter().map(|c| c.0.clone()).collect();
                let rows = as_text(&answer.rows);
                self.post_rows(job, &st.id, Some(&names), &rows, ctx).await
            }
            _ => Ok(()),
        }
    }

    fn undelivered(outcome: Outcome, class: &str) -> Outcome {
        if outcome.error_class.is_some() {
            outcome
        } else {
            Outcome {
                counts: outcome.counts,
                ..Outcome::failed(class)
            }
        }
    }
}

/// How long the first retry of a report waits (then twice as long each).
pub(crate) const REPORT_WAIT: Duration = Duration::from_secs(2);
