//! A run's statements in order (core decision 0090, points 6.2 and 7.3;
//! amendment A1's skipped deletes and placeholders).

use std::collections::BTreeMap;
use std::time::Instant;

use engine_port::LogStream;
use engine_runner::{Outcome, RunContext};
use serde_json::Value;

use crate::bundle::{Bundle, Expects, PlaceholderType, RunAs, Statement};
use crate::check::{class, fits, Answer};
use crate::engine::{transient, TrinoEngine};
use crate::ports::{Core, Trino, TrinoUser};
use crate::report::{StatementState, Trail};
use crate::steps::{checked, column, fill, Known};

impl<C: Core, T: Trino> TrinoEngine<C, T> {
    pub(crate) async fn statements(
        &self,
        job: &str,
        bundle: &Bundle,
        (from, mut known): (usize, Known),
        token: &str,
        ctx: &RunContext,
        trail: &mut Trail,
    ) -> Outcome {
        let total = bundle.statements.len();
        let (mut written, mut skipped) = (0u64, 0u64);
        let mut report_token = None;
        // Done by an earlier attempt: their rows are not read again.
        for st in bundle.statements.iter().take(from) {
            trail.push(&st.id, StatementState::Done);
        }
        for (i, st) in bundle.statements.iter().enumerate().skip(from) {
            if let Some(o) = &st.only_if_rows {
                match known.0.get(&format!("c.{}", o.after)).map(String::as_str) {
                    Some("0") => {
                        ctx.log(LogStream::System, &format!("statement {} skipped", st.id));
                        trail.push(&st.id, StatementState::Skipped);
                        skipped += 1;
                        ctx.progress(Some(fraction(i + 1, total)), Some(&known.detail(i + 1)));
                        continue;
                    }
                    Some(_) => {}
                    None => return failed(trail, st, Outcome::failed("step_value_missing")),
                }
            }
            let token = match st.run_as {
                RunAs::Job => token.to_string(),
                RunAs::Report => match &report_token {
                    Some(t) => Clone::clone(t),
                    None => match self.core.token(job, TrinoUser::Report).await {
                        Ok(t) => report_token.insert(t).clone(),
                        Err(_) => return transient("core_unavailable"),
                    },
                },
            };
            let clock = Instant::now();
            let answer = match self.step(job, st, &known, &token, ctx).await {
                Ok(a) => a,
                Err(o) => return failed(trail, st, o),
            };
            ctx.timed(stage(&st.expects), clock.elapsed());
            let Some(n) = fits(&answer, &st.expects) else {
                ctx.log(
                    LogStream::System,
                    &format!("statement {} answered another shape", st.id),
                );
                return failed(trail, st, Outcome::failed("unexpected_answer"));
            };
            written += n;
            if let Err(o) = learn(bundle, st, &answer, &mut known) {
                return failed(trail, st, o);
            }
            if matches!(st.expects, Expects::Values { .. }) {
                // Before its progress: a repeated attempt reads it again.
                if let Err(o) = self.post_rows(job, &st.id, &answer.rows, ctx).await {
                    return failed(trail, st, o);
                }
            }
            trail.done(&st.id, &st.expects, &answer.rows);
            ctx.progress(Some(fraction(i + 1, total)), Some(&known.detail(i + 1)));
            // Sends "statement i done" with the renewal now, not in 30 s.
            if let Some(id) = &answer.query_id {
                ctx.engine_ref(id);
            }
        }
        trail.filled(bundle, &known);
        let mut counts = BTreeMap::from([
            ("statements".into(), (total - from) as u64 - skipped),
            ("statements_skipped".into(), skipped),
            ("rows_written".into(), written),
        ]);
        // The snapshots the job pinned, for the core's manifest.
        for p in bundle
            .placeholders
            .iter()
            .filter(|p| p.kind == PlaceholderType::SnapshotId)
        {
            if let Some(v) = known.0.get(&p.name).and_then(|v| v.parse().ok()) {
                counts.insert(format!("snapshot_{}", p.name), v);
            }
        }
        Outcome {
            counts,
            ..Outcome::succeeded()
        }
    }

    /// One statement filled and run, its failure classed.
    async fn step(
        &self,
        job: &str,
        st: &Statement,
        known: &Known,
        token: &str,
        ctx: &RunContext,
    ) -> Result<Answer, Outcome> {
        let Some(sql) = fill(&st.sql, known) else {
            return Err(Outcome::failed("step_value_missing"));
        };
        ctx.log(LogStream::System, &format!("statement {} started", st.id));
        let started = |id: &str| ctx.engine_ref(id);
        self.run_one(job, st, &sql, token, &started)
            .await
            .map_err(|kind| {
                let c = class(&kind);
                ctx.log(
                    LogStream::System,
                    &format!("statement {} failed: {}", st.id, c.class),
                );
                if c.transient {
                    transient(c.class)
                } else {
                    Outcome::failed(c.class)
                }
            })
    }

    /// One statement; a source's statement is tried once more with the
    /// previous secrets if it failed for good with the current ones.
    async fn run_one(
        &self,
        job: &str,
        st: &Statement,
        sql: &str,
        token: &str,
        started: &(dyn for<'q> Fn(&'q str) + Send + Sync),
    ) -> Result<Answer, String> {
        if !st.reads_source {
            return self
                .trino
                .run(token, sql, &[], started)
                .await
                .map_err(|f| f.kind);
        }
        let creds = self
            .core
            .credentials(job)
            .await
            .map_err(|_| "unavailable".to_string())?;
        let current: Vec<(String, String)> = creds
            .iter()
            .map(|c| (c.name.clone(), c.current.clone()))
            .collect();
        match self.trino.run(token, sql, &current, started).await {
            Ok(a) => Ok(a),
            Err(f) if class(&f.kind).transient || creds.iter().all(|c| c.previous.is_none()) => {
                Err(f.kind)
            }
            Err(_) => {
                let previous: Vec<(String, String)> = creds
                    .iter()
                    .map(|c| {
                        (
                            c.name.clone(),
                            c.previous.clone().unwrap_or_else(|| c.current.clone()),
                        )
                    })
                    .collect();
                self.trino
                    .run(token, sql, &previous, started)
                    .await
                    .map_err(|f| f.kind)
            }
        }
    }
}

/// A statement's stage in the runner's metrics, by what it answers: a
/// fixed name, never its id.
fn stage(expects: &Expects) -> &'static str {
    match expects {
        Expects::Count { .. } => "write",
        Expects::Rows { .. } => "read",
        Expects::Values { .. } => "values",
        Expects::Done => "layout",
    }
}

/// The job's end at `st`: a failure for good marks it failed in the
/// report; one cut by the system is not reported.
fn failed(trail: &mut Trail, st: &Statement, o: Outcome) -> Outcome {
    if o.interruption.is_none() {
        trail.push(&st.id, StatementState::Failed);
    }
    o
}

#[allow(clippy::cast_precision_loss)]
fn fraction(done: usize, total: usize) -> f64 {
    done as f64 / total as f64
}

/// What a later step needs from `st`'s answer: a count an `only_if_rows`
/// waits on, and the placeholders it is the source of, checked by type.
fn learn(
    bundle: &Bundle,
    st: &Statement,
    answer: &Answer,
    known: &mut Known,
) -> Result<(), Outcome> {
    let counted = bundle
        .statements
        .iter()
        .any(|s| s.only_if_rows.as_ref().is_some_and(|o| o.after == st.id));
    if counted {
        let n = answer
            .rows
            .first()
            .and_then(|r| r.first())
            .and_then(Value::as_u64);
        let n = n.ok_or_else(|| Outcome::failed("step_value_invalid"))?;
        known.0.insert(format!("c.{}", st.id), n.to_string());
    }
    for p in bundle
        .placeholders
        .iter()
        .filter(|p| p.from.statement == st.id)
    {
        let value = column(answer, &p.from.column)
            .and_then(|v| checked(v, p.kind))
            .ok_or_else(|| Outcome::failed("step_value_invalid"))?;
        known.0.insert(p.name.clone(), value);
    }
    Ok(())
}
