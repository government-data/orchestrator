//! The engine `trino` as engine-runner drives it (decision 0011): a run is
//! a job of the core; its parameters are the job's id and its bundle's
//! digest. The runner verifies the bundle, then runs its statements in
//! order, each with the job's token, checking each answer before the next
//! (core decision 0090, points 2, 6.2 and 7.3).

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use engine_port::{Claimed, Descriptor, RunKind, PORT};
use engine_runner::{Engine, EngineError, Outcome, Refused, RunContext, RunRef};
use orchestration_model::{EngineName, Interruption, Line};
use serde_json::{json, Value};
use tokio::sync::Notify;

use crate::bundle::{bundle_kid, Bundle, BundleRefused, Keys, SignedBundle};
use crate::locks::{give_back, Locks};
use crate::ports::{Core, JobRequest, Trino, TrinoUser};
use crate::report::Trail;
use crate::steps::Known;

/// The job kinds of core decision 0090 (point 2), with `prepare` (ruling
/// 66), `rule_test` (ruling 54), `backfill` (ruling 77), `query`, a
/// person's console statement (rulings 80 and 84), and `entity_key`, a
/// person's lookup by a typed document (rulings 81, 85 and 102).
pub const KINDS: [&str; 17] = [
    "catalog",
    "tables",
    "map",
    "records",
    "prepare",
    "extract",
    "derive",
    "evaluate",
    "backfill",
    "test_rule",
    "rule_test",
    "retract",
    "slas",
    "maintain",
    "reindex",
    "query",
    "entity_key",
];

/// A name the core gives a dataset or a source: `[a-z0-9_]`, 1 to 63.
fn is_name(s: &str) -> bool {
    !s.is_empty()
        && s.len() <= 63
        && s.bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
}

/// How long a retry waits for the previous attempt's query to end.
const KILL_WAIT: Duration = Duration::from_secs(60);

/// The engine of engine-trino's job runner.
pub struct TrinoEngine<C, T> {
    pub(crate) core: C,
    pub(crate) trino: T,
    /// The provider's keys, fetched again for a `kid` they lack.
    keys: Mutex<Keys>,
    version: String,
    concurrency: u32,
    kill_poll: Duration,
    /// How long a report's first retry waits.
    pub(crate) report_wait: Duration,
    /// How long a lock not granted waits before it is asked again.
    lock_wait: Duration,
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

pub(crate) fn transient(c: &str) -> Outcome {
    Outcome {
        interruption: Some(Interruption::System),
        ..Outcome::failed(c)
    }
}

impl<C: Core, T: Trino> TrinoEngine<C, T> {
    /// The engine for Trino `version` (the coordinator's release).
    pub fn new(core: C, trino: T, version: &str, concurrency: u32) -> Self {
        Self {
            core,
            trino,
            keys: Mutex::new(Keys::default()),
            version: version.to_string(),
            concurrency,
            kill_poll: Duration::from_secs(1),
            report_wait: crate::report::REPORT_WAIT,
            lock_wait: crate::locks::LOCK_WAIT,
            running: Mutex::new(HashMap::new()),
        }
    }

    /// Polls a killed query, retries a report and asks again for a lock
    /// faster (tests).
    #[must_use]
    pub fn polling_every(mut self, every: Duration) -> Self {
        self.kill_poll = every;
        self.report_wait = every;
        self.lock_wait = every;
        self
    }

    /// The job the operator handed out: the run's kind and attempt, and
    /// its dataset, cut and source from the parameters.
    fn request(run: &Claimed) -> Option<JobRequest> {
        let text = |k: &str| match run.parameters.get(k) {
            None | Some(Value::Null) => Some(None),
            Some(Value::String(s)) => Some(Some(s.clone())),
            Some(_) => None,
        };
        let dataset = text("dataset")?;
        let cut = text("cut")?;
        let source = text("source")?;
        let names_ok =
            dataset.as_deref().is_none_or(is_name) && source.as_deref().is_none_or(is_name);
        let cut_ok = cut.as_deref().is_none_or(|c| {
            c.len() == 10
                && c.bytes().enumerate().all(|(i, b)| {
                    if i == 4 || i == 7 {
                        b == b'-'
                    } else {
                        b.is_ascii_digit()
                    }
                })
        });
        let known = run.parameters.as_object().is_some_and(|o| {
            o.keys()
                .all(|k| ["dataset", "cut", "source"].contains(&k.as_str()))
        });
        (names_ok && cut_ok && known).then(|| JobRequest {
            kind: run.kind.clone(),
            dataset,
            cut,
            source,
            attempt: run.attempt,
        })
    }

    /// The bundle verified with the provider's keys, fetched again once
    /// when its `kid` is not among those held (a rotated key).
    async fn verified(&self, signed: &SignedBundle, job: &str) -> Result<Bundle, Outcome> {
        let kid = bundle_kid(&signed.jws).map_err(|r| Outcome::failed(r.class()))?;
        let held = self
            .keys
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .has(&kid);
        if !held {
            match self.core.keys().await {
                Ok(k) => *self.keys.lock().unwrap_or_else(|e| e.into_inner()) = k,
                Err(_) => return Err(transient("core_unavailable")),
            }
        }
        let keys = self.keys.lock().unwrap_or_else(|e| e.into_inner()).clone();
        signed
            .verify(&keys, job)
            .map_err(|r: BundleRefused| Outcome::failed(r.class()))
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
        let Some(request) = Self::request(run) else {
            return Outcome::failed("invalid_parameters");
        };
        let Some(user) = TrinoUser::of_kind(&run.kind) else {
            return Outcome::failed("kind_not_on_trino");
        };
        // The core's job is the operator's run (core decision 0090, step 18).
        let job = run.run.clone();
        let mut clock = Instant::now();
        let mut lap = |stage: &'static str| {
            ctx.timed(stage, clock.elapsed());
            clock = Instant::now();
        };
        let signed = match self.core.bundle(&job, &request).await {
            Ok(b) => b,
            Err(_) => return transient("core_unavailable"),
        };
        let bundle = match self.verified(&signed, &job).await {
            Ok(b) if b.kind == run.kind => b,
            Ok(_) => return Outcome::failed("bundle_invalid"),
            Err(o) => return o,
        };
        lap("bundle");
        let Ok(token) = self.core.token(&job, user).await else {
            return transient("core_unavailable");
        };
        lap("token");
        self.running
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .entry(run.run.clone())
            .and_modify(|e| e.0.clone_from(&token));
        if run.resume.as_ref().is_some_and(|r| r.engine_ref.is_some()) {
            if let Err(o) = self.end_previous(run, &token).await {
                return o;
            }
            lap("previous_query");
        }
        let texts = match self.texts(&job, &bundle).await {
            Ok(t) => t,
            Err(o) => return o,
        };
        let (from, known) =
            Known::parse(run.resume.as_ref().and_then(|r| r.last_detail.as_deref()));
        let mut trail = Trail::default();
        let held = Arc::default();
        let mut locks = Locks::new(&bundle, ctx, self.lock_wait, Arc::clone(&held));
        let outcome = tokio::select! {
            o = self.statements(&job, &bundle, (from, known, texts), &token, &mut locks, &mut trail) => o,
            () = cancel.notified() => {
                give_back(ctx, &held).await;
                return Outcome::cancelled();
            }
        };
        give_back(ctx, &held).await;
        let Some(report) = trail.report(&signed.digest, &outcome) else {
            return outcome;
        };
        let clock = Instant::now();
        let outcome = self.deliver(&job, &report, outcome, ctx).await;
        ctx.timed("report", clock.elapsed());
        outcome
    }
}

#[async_trait]
impl<C: Core, T: Trino> Engine for TrinoEngine<C, T> {
    fn descriptor(&self) -> Descriptor {
        let parameters = json!({
            "type": "object",
            "additionalProperties": false,
            "properties": {
                "dataset": {"type": "string", "pattern": "^[a-z0-9_]{1,63}$"},
                "cut": {"type": "string", "pattern": "^[0-9]{4}-[0-9]{2}-[0-9]{2}$"},
                "source": {"type": "string", "pattern": "^[a-z0-9_]{1,63}$"}
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
        match Self::request(run) {
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

/// The parameters of a run of a dataset's cut.
#[must_use]
pub fn parameters(dataset: &str, cut: &str) -> Value {
    json!({"dataset": dataset, "cut": cut})
}
