//! A fake orchestrator in memory, for an engine module's CI: it serves the
//! engine port's calls as [`Orchestrator`] and is its own [`Seed`], so an
//! engine module runs the contract suite, and its own engine's cases,
//! without a server or a database.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use chrono::Utc;
use engine_port::{
    Claim, Claimed, Descriptor, Finish, LogLine, Logs, Registered, Renew, RenewAction, Renewed,
    Resume, LEASE_SECS, RENEW_EVERY_SECS,
};
use orchestration_model::RunState;
use serde_json::Value;

use super::Seed;
use crate::{CallError, Orchestrator};

/// A run as the fake keeps it.
#[derive(Debug, Clone, PartialEq)]
pub struct FakeRun {
    pub kind: String,
    pub parameters: Value,
    pub attempt: u32,
    pub resume: Option<Resume>,
    pub state: RunState,
    /// The lease of its current claim.
    pub lease: Option<String>,
    pub cancel_asked: bool,
    pub engine_ref: Option<String>,
    pub progress_detail: Option<String>,
    pub logs: Vec<LogLine>,
    pub counts: BTreeMap<String, u64>,
    pub error_class: Option<String>,
}

#[derive(Debug, Default)]
struct State {
    registered: Vec<Descriptor>,
    runs: BTreeMap<String, FakeRun>,
    next: u64,
    held: bool,
}

/// The fake orchestrator; clones share one state.
#[derive(Debug, Clone, Default)]
pub struct FakeOrchestrator {
    state: Arc<Mutex<State>>,
}

/// The kind [`Seed::queue`] gives a run: the contract suite's fake engine's.
const SEED_KIND: &str = "job";

impl FakeOrchestrator {
    fn with<T>(&self, f: impl FnOnce(&mut State) -> T) -> T {
        let mut guard = self.state.lock().unwrap_or_else(|e| e.into_inner());
        f(&mut guard)
    }

    /// Queues a run of `kind`, its first attempt; its id.
    #[must_use]
    pub fn queue_run(&self, kind: &str, parameters: Value) -> String {
        self.queue_attempt(kind, parameters, 1, None)
    }

    /// Queues an attempt of a run that repeats another (`resume`).
    #[must_use]
    pub fn queue_attempt(
        &self,
        kind: &str,
        parameters: Value,
        attempt: u32,
        resume: Option<Resume>,
    ) -> String {
        self.with(|s| {
            s.next += 1;
            let id = format!("run-{}", s.next);
            s.runs.insert(
                id.clone(),
                FakeRun {
                    kind: kind.to_string(),
                    parameters,
                    attempt,
                    resume,
                    state: RunState::Queued,
                    lease: None,
                    cancel_asked: false,
                    engine_ref: None,
                    progress_detail: None,
                    logs: Vec::new(),
                    counts: BTreeMap::new(),
                    error_class: None,
                },
            );
            id
        })
    }

    /// A run as kept now.
    #[must_use]
    pub fn run(&self, id: &str) -> Option<FakeRun> {
        self.with(|s| s.runs.get(id).cloned())
    }

    /// The descriptors registered, in order.
    #[must_use]
    pub fn registered(&self) -> Vec<Descriptor> {
        self.with(|s| s.registered.clone())
    }

    /// The engine's drain hold on or off: while on, claims get nothing and
    /// renewals answer `held`.
    pub fn hold(&self, on: bool) {
        self.with(|s| s.held = on);
    }

    fn leased<'a>(s: &'a mut State, run: &str, lease: &str) -> Result<&'a mut FakeRun, CallError> {
        match s.runs.get_mut(run) {
            Some(r) if r.state == RunState::Running && r.lease.as_deref() == Some(lease) => Ok(r),
            Some(_) => Err(CallError::Lease),
            None => Err(CallError::Refused(404, "no such run".into())),
        }
    }
}

fn until() -> chrono::DateTime<Utc> {
    Utc::now() + chrono::Duration::seconds(i64::from(LEASE_SECS))
}

#[async_trait]
impl Orchestrator for FakeOrchestrator {
    async fn register(&self, d: &Descriptor) -> Result<Registered, CallError> {
        d.validate()
            .map_err(|e| CallError::Refused(400, e.to_string()))?;
        self.with(|s| s.registered.push(d.clone()));
        Ok(Registered {
            port: d.port,
            lease_secs: LEASE_SECS,
            renew_every_secs: RENEW_EVERY_SECS,
        })
    }

    async fn claim(&self, d: &Descriptor, _: &Claim) -> Result<Option<Claimed>, CallError> {
        Ok(self.with(|s| {
            if s.held {
                return None;
            }
            s.next += 1;
            let lease = format!("lease-{}", s.next);
            let (id, run) = s.runs.iter_mut().find(|(_, r)| {
                r.state == RunState::Queued && !r.cancel_asked && d.kind(&r.kind).is_some()
            })?;
            run.state = RunState::Running;
            run.lease = Some(lease.clone());
            Some(Claimed {
                run: id.clone(),
                flow: "flow-1".into(),
                kind: run.kind.clone(),
                parameters: run.parameters.clone(),
                attempt: run.attempt,
                timeout_secs: 60,
                lease,
                lease_until: until(),
                resume: run.resume.clone(),
                variables: Default::default(),
            })
        }))
    }

    async fn renew(&self, run: &str, r: &Renew) -> Result<Renewed, CallError> {
        self.with(|s| {
            let held = s.held;
            let entry = Self::leased(s, run, &r.lease)?;
            if r.engine_ref.is_some() {
                entry.engine_ref.clone_from(&r.engine_ref);
            }
            if let Some(p) = &r.progress {
                entry.progress_detail.clone_from(&p.detail);
            }
            let action = match (entry.cancel_asked, held) {
                (true, _) => RenewAction::Cancel,
                (false, true) => RenewAction::Held,
                (false, false) => RenewAction::Continue,
            };
            Ok(Renewed {
                action,
                lease_until: until(),
            })
        })
    }

    async fn logs(&self, run: &str, l: &Logs) -> Result<(), CallError> {
        l.validate()
            .map_err(|e| CallError::Refused(400, e.to_string()))?;
        self.with(|s| {
            let entry = Self::leased(s, run, &l.lease)?;
            let last = entry.logs.last().map_or(0, |x| x.seq);
            entry
                .logs
                .extend(l.lines.iter().filter(|x| x.seq > last).cloned());
            Ok(())
        })
    }

    async fn finish(&self, run: &str, f: &Finish) -> Result<(), CallError> {
        f.validate()
            .map_err(|e| CallError::Refused(400, e.to_string()))?;
        self.with(|s| {
            let entry = Self::leased(s, run, &f.lease)?;
            entry.state = f.state.state();
            entry.lease = None;
            entry.counts.clone_from(&f.counts);
            entry.error_class.clone_from(&f.error_class);
            Ok(())
        })
    }
}

#[async_trait]
impl Seed for FakeOrchestrator {
    async fn queue(&self, parameters: Value) -> String {
        self.queue_run(SEED_KIND, parameters)
    }

    async fn cancel(&self, run: &str) {
        self.with(|s| {
            if let Some(r) = s.runs.get_mut(run) {
                r.cancel_asked = true;
                if r.state == RunState::Queued {
                    r.state = RunState::Cancelled;
                }
            }
        });
    }

    async fn state(&self, run: &str) -> String {
        self.with(|s| {
            s.runs
                .get(run)
                .map_or("unknown", |r| r.state.as_str())
                .to_string()
        })
    }
}
