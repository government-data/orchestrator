//! A run's statements in order (core decision 0090, points 6.2 and 7.3;
//! amendment A1's skipped deletes and placeholders).

use std::collections::BTreeMap;

use engine_port::LogStream;
use engine_runner::{Outcome, RunContext};
use serde_json::Value;

use crate::bundle::{Bundle, PlaceholderType, RunAs};
use crate::check::{class, fits};
use crate::engine::{transient, TrinoEngine};
use crate::ports::{Core, Trino, TrinoUser};
use crate::steps::{checked, column, fill, Known};

impl<C: Core, T: Trino> TrinoEngine<C, T> {
    pub(crate) async fn statements(
        &self,
        job: &str,
        bundle: &Bundle,
        (from, mut known): (usize, Known),
        token: &str,
        ctx: &RunContext,
    ) -> Outcome {
        let total = bundle.statements.len();
        let (mut written, mut skipped) = (0u64, 0u64);
        let mut report_token = None;
        for (i, st) in bundle.statements.iter().enumerate().skip(from) {
            if let Some(o) = &st.only_if_rows {
                match known.0.get(&format!("c.{}", o.after)).map(String::as_str) {
                    Some("0") => {
                        ctx.log(LogStream::System, &format!("statement {} skipped", st.id));
                        skipped += 1;
                        ctx.progress(Some(fraction(i + 1, total)), Some(&known.detail(i + 1)));
                        continue;
                    }
                    Some(_) => {}
                    None => return Outcome::failed("step_value_missing"),
                }
            }
            let Some(sql) = fill(&st.sql, &known) else {
                return Outcome::failed("step_value_missing");
            };
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
            ctx.log(LogStream::System, &format!("statement {} started", st.id));
            let started = |id: &str| ctx.engine_ref(id);
            let answer = match self.run_one(job, st, &sql, &token, &started).await {
                Ok(a) => a,
                Err(kind) => {
                    let c = class(&kind);
                    ctx.log(
                        LogStream::System,
                        &format!("statement {} failed: {}", st.id, c.class),
                    );
                    return if c.transient {
                        transient(c.class)
                    } else {
                        Outcome::failed(c.class)
                    };
                }
            };
            let Some(n) = fits(&answer, &st.expects) else {
                ctx.log(
                    LogStream::System,
                    &format!("statement {} answered another shape", st.id),
                );
                return Outcome::failed("unexpected_answer");
            };
            written += n;
            if let Err(o) = learn(bundle, st, &answer, &mut known) {
                return o;
            }
            ctx.progress(Some(fraction(i + 1, total)), Some(&known.detail(i + 1)));
            // Sends "statement i done" with the renewal now, not in 30 s.
            if let Some(id) = &answer.query_id {
                ctx.engine_ref(id);
            }
        }
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

    /// One statement; a source's statement is tried once more with the
    /// previous secrets if it failed for good with the current ones.
    async fn run_one(
        &self,
        job: &str,
        st: &crate::bundle::Statement,
        sql: &str,
        token: &str,
        started: &(dyn for<'q> Fn(&'q str) + Send + Sync),
    ) -> Result<crate::check::Answer, String> {
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

#[allow(clippy::cast_precision_loss)]
fn fraction(done: usize, total: usize) -> f64 {
    done as f64 / total as f64
}

/// What a later step needs from `st`'s answer: a count an `only_if_rows`
/// waits on, and the placeholders it is the source of, checked by type.
fn learn(
    bundle: &Bundle,
    st: &crate::bundle::Statement,
    answer: &crate::check::Answer,
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
