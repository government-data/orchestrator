//! The scheduler's tick (decision 0011): keep the core's schedules; fire
//! the schedules due, by the missed-fire policy (a core's schedule starts
//! the chain the core says, core decision 0090 point 2); mark `lost` the
//! runs whose lease expired; queue the repetitions their flows ask for;
//! move the chains on; cancel what waited too long.

use std::collections::HashSet;
use std::sync::Arc;

use tokio::sync::Mutex;

use chrono::{DateTime, Duration, Utc};
use orchestration_model::{
    blocked, fires_to_run, merge_parameters, open_windows, retry, EngineLine, Trigger,
};
use orchestrator_postgres::{NewChain, NewRun, PgStore, Schedule, StoreError};
use schedule::Calendar;

use crate::{installed_lines, is_core_id, Clock, CoreSchedules, Metrics, ModuleList};

/// Runners on engine port 1 stop a run they can no longer renew before its
/// lease ends, so a lost run's engine is known to have stopped.
pub const RUNNERS_FENCE: bool = true;

/// What one tick did, in counts (no ids).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Tick {
    pub queued: u32,
    /// Fires missed and not run, by the policy or a window that skips.
    pub skipped: u32,
    /// The longest delay of a schedule seen late, in seconds.
    pub lag_secs: i64,
    pub lost: u32,
    pub retried: u32,
    pub cancelled: u32,
    /// Runs queued by chains moving on.
    pub chained: u32,
}

/// The scheduler, on a store and a clock.
#[derive(Clone)]
pub struct Scheduler<C> {
    store: PgStore,
    clock: C,
    metrics: Option<Arc<Metrics>>,
    modules: Option<Arc<dyn ModuleList>>,
    last_listed: Arc<Mutex<Option<HashSet<String>>>>,
    /// The core's schedules, and the flow their chains run on.
    core: Option<(Arc<dyn CoreSchedules>, String)>,
}

impl<C: Clock> Scheduler<C> {
    /// Reads which modules are installed from the core (decision 0015).
    #[must_use]
    pub fn with_modules(mut self, modules: Arc<dyn ModuleList>) -> Self {
        self.modules = Some(modules);
        self
    }

    /// The core's last list of installed modules: read now, or kept from
    /// the last answer when the core does not answer.
    async fn listed(&self) -> Option<HashSet<String>> {
        let modules = self.modules.as_ref()?;
        let fresh = modules.installed().await.map(|m| m.into_iter().collect());
        let mut last = self.last_listed.lock().await;
        if fresh.is_some() {
            *last = fresh;
        } else {
            tracing::warn!("the core's installed modules were not read: the last list is kept");
        }
        last.clone()
    }

    pub fn new(store: PgStore, clock: C) -> Self {
        Self {
            store,
            clock,
            metrics: None,
            modules: None,
            last_listed: Arc::default(),
            core: None,
        }
    }

    /// Keeps and fires the core's schedules (core decision 0090, point 2),
    /// their chains on the flow `flow_id`.
    #[must_use]
    pub fn with_core_schedules(mut self, core: Arc<dyn CoreSchedules>, flow_id: String) -> Self {
        self.core = Some((core, flow_id));
        self
    }

    /// Counts what each tick does in `metrics` and refreshes its gauges.
    #[must_use]
    pub fn with_metrics(mut self, metrics: Arc<Metrics>) -> Self {
        self.metrics = Some(metrics);
        self
    }

    /// One pass; each part can run twice and on two servers at once.
    pub async fn tick(&self) -> Result<Tick, StoreError> {
        let now = self.clock.now();
        let mut tick = Tick::default();
        self.keep_core_schedules(now).await?;
        self.fire(now, &mut tick).await?;
        let lost = self.store.reap(now).await?;
        tick.lost = count(lost.len());
        for (run, cut) in self.store.interrupted(now, RUNNERS_FENCE).await? {
            if let Some(r) = retry(&cut, now) {
                if self.store.queue_retry(&run, &r, now).await?.is_some() {
                    tick.retried += 1;
                }
            }
        }
        tick.chained = self.store.move_chains(now).await?.queued;
        if let Some(wait) = self.store.settings().await?.queued_wait_minutes {
            let before = now - Duration::minutes(i64::from(wait));
            tick.cancelled = count(self.store.cancel_stale(before, now).await?.len());
        }
        if let Some(m) = &self.metrics {
            m.count_ended(&self.store, &lost).await?;
            m.missed_fires_total.inc_by(u64::from(tick.skipped));
            #[allow(clippy::cast_precision_loss)]
            m.schedule_lag_seconds.set(tick.lag_secs.max(0) as f64);
            m.refresh(&self.store, now).await?;
        }
        Ok(tick)
    }

    async fn fire(&self, now: DateTime<Utc>, tick: &mut Tick) -> Result<(), StoreError> {
        let settings = self.store.settings().await?;
        let engines = self.store.engines().await?;
        let listed = self.listed().await;
        let installed: HashSet<EngineLine> = installed_lines(&engines, listed.as_ref());
        let paused: HashSet<_> = self.store.paused_engines().await?.into_iter().collect();
        let windows = self.store.maintenance_windows().await?;
        let open = open_windows(&windows, &Calendar, now);
        for s in self.store.due_schedules(now).await? {
            let Some(due) = s.next_fire_at else { continue };
            let Some(flow) = self.store.flow(&s.flow_id).await? else {
                continue;
            };
            // A blocked flow keeps its fire due: when it is unblocked, the
            // missed-fire policy decides what runs.
            let line_installed = installed.contains(&flow.engine);
            if blocked(&flow, line_installed, paused.contains(&flow.engine.engine)).is_some() {
                continue;
            }
            tick.lag_secs = tick.lag_secs.max((now - due).num_seconds());
            let next_after = |t| s.spec.next_after(&s.id, t).ok().flatten();
            let fires = fires_to_run(next_after, due, now, &settings);
            let skips = flow
                .cluster
                .as_ref()
                .is_some_and(|c| open.iter().any(|w| &w.cluster == c && w.skips));
            let mut last = None;
            let mut unanswered = false;
            for fire in &fires {
                last = Some(*fire);
                if skips {
                    tick.skipped += 1;
                    continue;
                }
                let queued = match &s.core_id {
                    Some(core_id) => match self.fire_core(&s, core_id, *fire, now).await? {
                        Some(queued) => queued,
                        None => {
                            unanswered = true;
                            break;
                        }
                    },
                    None => self
                        .store
                        .enqueue(&run_for(&flow, &s, *fire, now))
                        .await?
                        .is_some(),
                };
                if queued {
                    tick.queued += 1;
                }
            }
            // The core did not answer: the fire stays due, and the fires
            // already started are not started twice (one chain per fire).
            if unanswered {
                continue;
            }
            if fires.is_empty() {
                tick.skipped += 1;
            }
            let next = s.spec.next_after(&s.id, now).ok().flatten();
            self.store.advance_schedule(&s.id, due, last, next).await?;
        }
        Ok(())
    }
}

impl<C: Clock> Scheduler<C> {
    /// Keeps the core's schedules as schedules of the chains' flow: new
    /// ones created, changed ones replaced, those it no longer lists
    /// disabled. Nothing changes while the core does not answer.
    async fn keep_core_schedules(&self, now: DateTime<Utc>) -> Result<(), StoreError> {
        let Some((core, flow)) = &self.core else {
            return Ok(());
        };
        let Some(listed) = core.schedules().await else {
            tracing::warn!("the core's schedules were not read: they are kept as they were");
            return Ok(());
        };
        let mut kept = Vec::new();
        for c in listed.iter().filter(|c| is_core_id(&c.id)) {
            let next = if c.enabled {
                c.spec.next_after(&c.id, now).ok().flatten()
            } else {
                None
            };
            self.store
                .keep_core_schedule(flow, &c.id, &c.spec, next)
                .await?;
            kept.push(c.id.as_str());
        }
        self.store.drop_core_schedules(&kept).await?;
        Ok(())
    }

    /// Asks the core what one fire of its schedule runs and starts that
    /// chain: `Some(true)` if a chain started, `Some(false)` if nothing
    /// runs (or the fire's chain exists), `None` if the core did not answer.
    async fn fire_core(
        &self,
        s: &Schedule,
        core_id: &str,
        fire: DateTime<Utc>,
        now: DateTime<Utc>,
    ) -> Result<Option<bool>, StoreError> {
        let Some((core, flow)) = &self.core else {
            return Ok(None);
        };
        let Some(answer) = core.fire(core_id, fire).await else {
            tracing::warn!(schedule = %s.id, "the core did not say what a fire runs");
            return Ok(None);
        };
        if answer.steps.is_empty() {
            return Ok(Some(false));
        }
        if let Err(why) = orchestration_model::validate_steps(&answer.steps) {
            tracing::warn!(schedule = %s.id, %why, "the core's chain is not valid: nothing runs");
            return Ok(Some(false));
        }
        let chain = NewChain {
            flow_id: flow.clone(),
            origin: format!("core:{core_id}:{}", fire.timestamp()),
            trigger: Trigger::Schedule(s.id.clone()),
            steps: answer.steps,
            now,
        };
        Ok(Some(self.store.start_chain(&chain).await?.is_some()))
    }
}

fn run_for(
    flow: &orchestration_model::Flow,
    s: &Schedule,
    fire: DateTime<Utc>,
    now: DateTime<Utc>,
) -> NewRun {
    NewRun {
        flow_id: flow.id.clone(),
        engine: flow.engine.clone(),
        kind: flow.kind.clone(),
        cluster: flow.cluster.clone(),
        trigger: Trigger::Schedule(s.id.clone()),
        fire_at: Some(fire),
        parameters: merge_parameters(&flow.parameters, &s.parameters),
        timeout_secs: s.timeout_secs.unwrap_or(flow.timeout_secs),
        lock_name: None,
        slot_pool: None,
        queued_at: now,
    }
}

fn count(n: usize) -> u32 {
    u32::try_from(n).unwrap_or(u32::MAX)
}

impl<C> std::fmt::Debug for Scheduler<C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Scheduler").finish_non_exhaustive()
    }
}
