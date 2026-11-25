//! The engine `trino` as engine-runner drives it (decision 0011): a run is
//! a job of the core; its parameters are the job's id and its bundle's
//! digest. The runner verifies the bundle, then runs its statements in
//! order, each with the job's token, checking each answer before the next
//! (core decision 0090, points 2, 6.2 and 7.3).

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use engine_port::{Claimed, Descriptor, LogStream, RunKind, PORT};
use engine_runner::{Engine, EngineError, Outcome, Refused, RunContext, RunRef};
use orchestration_model::{EngineName, Interruption, Line};
use serde_json::{json, Value};
use tokio::sync::Notify;

use crate::bundle::{Bundle, BundleKey, RunAs};
use crate::check::{class, fits};
use crate::ports::{Core, Trino};

/// The job kinds of core decision 0090 (point 2).
pub const KINDS: [&str; 12] = [
    "catalog",
    "tables",
    "map",
    "records",
    "extract",
    "derive",
    "evaluate",
    "test_rule",
    "retract",
    "slas",
    "maintain",
    "reindex",
];

/// How long a retry waits for the previous attempt's query to end.
const KILL_WAIT: Duration = Duration::from_secs(60);

/// The engine of engine-trino's job runner.
pub struct TrinoEngine<C, T> {
    core: C,
    trino: T,
    key: BundleKey,
    version: String,
    concurrency: u32,
    kill_poll: Duration,
    /// Per running run: the job's token and its cancel signal.
    running: Mutex<HashMap<String, (String, Arc<Notify>)>>,
}

impl<C, T> std::fmt::Debug for TrinoEngine<C, T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TrinoEngine")
            .field("version", &self.version)
            .finish_non_exhaustive()
    }
}

/// The progress detail after `n` statements are done: what a repeated
/// attempt resumes from.
fn done(n: usize) -> String {
    format!("done:{n}")
}

fn parse_done(detail: Option<&str>) -> usize {
    detail
        .and_then(|d| d.strip_prefix("done:"))
        .and_then(|n| n.parse().ok())
        .unwrap_or(0)
}

fn transient(c: &str) -> Outcome {
    Outcome {
        interruption: Some(Interruption::System),
        ..Outcome::failed(c)
    }
}

impl<C: Core, T: Trino> TrinoEngine<C, T> {
    /// The engine for Trino `version` (the coordinator's release).
    pub fn new(core: C, trino: T, key: BundleKey, version: &str, concurrency: u32) -> Self {
        Self {
            core,
            trino,
            key,
            version: version.to_string(),
            concurrency,
            kill_poll: Duration::from_secs(1),
            running: Mutex::new(HashMap::new()),
        }
    }

    /// Polls a killed query faster (tests).
    #[must_use]
    pub fn polling_every(mut self, every: Duration) -> Self {
        self.kill_poll = every;
        self
    }

    fn params(run: &Claimed) -> Option<(String, String)> {
        let job = run.parameters.get("job_id")?.as_str()?;
        let digest = run.parameters.get("bundle_digest")?.as_str()?;
        let job_ok = job.len() == 36 && job.bytes().all(|b| b.is_ascii_hexdigit() || b == b'-');
        let digest_ok = digest.len() == 64 && digest.bytes().all(|b| b.is_ascii_hexdigit());
        (job_ok && digest_ok).then(|| (job.to_string(), digest.to_string()))
    }

    /// The previous attempt's query, ended before anything runs again, so
    /// that two attempts never write one cut (point 6.2.3).
    async fn end_previous(&self, run: &Claimed, token: &str) -> Result<(), Outcome> {
        let Some(id) = run.resume.as_ref().and_then(|r| r.engine_ref.as_deref()) else {
            return Ok(());
        };
        let start = tokio::time::Instant::now();
        let _ = self.trino.kill(token, id).await;
        while start.elapsed() < KILL_WAIT {
            if let Ok(true) = self.trino.ended(token, id).await {
                return Ok(());
            }
            tokio::time::sleep(self.kill_poll).await;
        }
        Err(transient("previous_query_running"))
    }

    async fn job(&self, run: &Claimed, ctx: &RunContext, cancel: &Notify) -> Outcome {
        let Some((job, digest)) = Self::params(run) else {
            return Outcome::failed("invalid_parameters");
        };
        let signed = match self.core.bundle(&job).await {
            Ok(b) => b,
            Err(_) => return transient("core_unavailable"),
        };
        let bundle = match signed.verify(&self.key, &digest, &job) {
            Ok(b) if b.kind == run.kind => b,
            Ok(_) => return Outcome::failed("bundle_invalid"),
            Err(refused) => return Outcome::failed(refused.class()),
        };
        let Ok(token) = self.core.token(&job, RunAs::Job).await else {
            return transient("core_unavailable");
        };
        self.running
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .entry(run.run.clone())
            .and_modify(|e| e.0.clone_from(&token));
        if let Err(o) = self.end_previous(run, &token).await {
            return o;
        }
        let from = parse_done(run.resume.as_ref().and_then(|r| r.last_detail.as_deref()));
        tokio::select! {
            o = self.statements(&job, &bundle, from, &token, ctx) => o,
            () = cancel.notified() => Outcome::cancelled(),
        }
    }

    async fn statements(
        &self,
        job: &str,
        bundle: &Bundle,
        from: usize,
        token: &str,
        ctx: &RunContext,
    ) -> Outcome {
        let total = bundle.statements.len();
        let mut written = 0u64;
        let mut report_token = None;
        for (i, st) in bundle.statements.iter().enumerate().skip(from) {
            let token = match st.run_as {
                RunAs::Job => token.to_string(),
                RunAs::Report => match &report_token {
                    Some(t) => Clone::clone(t),
                    None => match self.core.token(job, RunAs::Report).await {
                        Ok(t) => report_token.insert(t).clone(),
                        Err(_) => return transient("core_unavailable"),
                    },
                },
            };
            ctx.log(LogStream::System, &format!("statement {} started", st.id));
            let started = |id: &str| ctx.engine_ref(id);
            let answer = match self.run_one(job, st, &token, &started).await {
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
            #[allow(clippy::cast_precision_loss)]
            let fraction = (i + 1) as f64 / total as f64;
            ctx.progress(Some(fraction), Some(&done(i + 1)));
            // Sends "statement i done" with the renewal now, not in 30 s.
            if let Some(id) = &answer.query_id {
                ctx.engine_ref(id);
            }
        }
        Outcome {
            counts: BTreeMap::from([
                ("statements".into(), (total - from) as u64),
                ("rows_written".into(), written),
            ]),
            ..Outcome::succeeded()
        }
    }

    /// One statement; a source's statement is tried once more with the
    /// previous secrets if it failed for good with the current ones.
    async fn run_one(
        &self,
        job: &str,
        st: &crate::bundle::Statement,
        token: &str,
        started: &(dyn for<'q> Fn(&'q str) + Send + Sync),
    ) -> Result<crate::check::Answer, String> {
        if !st.reads_source {
            return self
                .trino
                .run(token, &st.sql, &[], started)
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
        match self.trino.run(token, &st.sql, &current, started).await {
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
                    .run(token, &st.sql, &previous, started)
                    .await
                    .map_err(|f| f.kind)
            }
        }
    }
}

#[async_trait]
impl<C: Core, T: Trino> Engine for TrinoEngine<C, T> {
    fn descriptor(&self) -> Descriptor {
        let parameters = json!({
            "type": "object",
            "required": ["job_id", "bundle_digest"],
            "additionalProperties": false,
            "properties": {
                "job_id": {"type": "string", "pattern": "^[0-9a-f-]{36}$"},
                "bundle_digest": {"type": "string", "pattern": "^[0-9a-f]{64}$"}
            }
        });
        Descriptor {
            engine: EngineName::new("trino").unwrap_or_else(|_| unreachable!()),
            line: Line::new(Line::SINGLE).unwrap_or_else(|_| unreachable!()),
            engine_version: self.version.clone(),
            port: PORT,
            kinds: KINDS
                .iter()
                .map(|k| RunKind {
                    name: (*k).to_string(),
                    parameters: parameters.clone(),
                })
                .collect(),
            concurrency: self.concurrency,
        }
    }

    async fn validate(&self, run: &Claimed) -> Result<(), Refused> {
        match Self::params(run) {
            Some(_) if KINDS.contains(&run.kind.as_str()) => Ok(()),
            _ => Err(Refused {
                class: "invalid_parameters".into(),
            }),
        }
    }

    async fn run(&self, run: Claimed, ctx: RunContext) -> Outcome {
        let cancel = Arc::new(Notify::new());
        self.running
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(run.run.clone(), (String::new(), Arc::clone(&cancel)));
        let outcome = self.job(&run, &ctx, &cancel).await;
        self.running
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&run.run);
        outcome
    }

    async fn cancel(&self, run: &RunRef) -> Result<(), EngineError> {
        let entry = self
            .running
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&run.run)
            .cloned();
        let Some((token, cancel)) = entry else {
            return Ok(());
        };
        cancel.notify_one();
        match (&run.engine_ref, token.is_empty()) {
            (Some(id), false) => self
                .trino
                .kill(&token, id)
                .await
                .map_err(|f| EngineError(format!("kill: {}", f.kind))),
            _ => Ok(()),
        }
    }
}

/// The parameters of a run of job `job` whose bundle has `digest`.
#[must_use]
pub fn parameters(job: &str, digest: &str) -> Value {
    json!({"job_id": job, "bundle_digest": digest})
}
