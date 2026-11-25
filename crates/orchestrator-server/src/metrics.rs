//! The orchestrator's metrics (decision 0011): counters and gauges kept in a
//! `prometheus-client` registry and written to the metrics file Alloy reads
//! (the core's `logging::metrics`). Labels are `engine`, `line`, `kind`,
//! `result` and, for HTTP, the route's template and the status: never a
//! flow, a folder, a person or an id.

use std::sync::atomic::AtomicU64;

use chrono::{DateTime, Utc};

use orchestration_model::{EngineLine, EngineName, FinalState};
use orchestrator_postgres::{PgStore, StoreError};
use prometheus_client::encoding::{EncodeLabelSet, EncodeLabelValue};
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::family::Family;
use prometheus_client::metrics::gauge::Gauge;
use prometheus_client::metrics::histogram::Histogram;
use prometheus_client::registry::Registry;

/// A run's result, as counted.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, EncodeLabelValue)]
#[allow(non_camel_case_types)]
pub enum RunResult {
    succeeded,
    failed,
    timed_out,
    cancelled,
    lost,
}

impl From<FinalState> for RunResult {
    fn from(s: FinalState) -> Self {
        match s {
            FinalState::Succeeded => Self::succeeded,
            FinalState::Failed => Self::failed,
            FinalState::TimedOut => Self::timed_out,
            FinalState::Cancelled => Self::cancelled,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, EncodeLabelSet)]
pub struct EngineLabels {
    pub engine: String,
    pub line: String,
}

impl From<&EngineLine> for EngineLabels {
    fn from(e: &EngineLine) -> Self {
        Self {
            engine: e.engine.to_string(),
            line: e.line.to_string(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, EncodeLabelSet)]
pub struct KindLabels {
    pub engine: String,
    pub line: String,
    pub kind: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, EncodeLabelSet)]
pub struct ResultLabels {
    pub engine: String,
    pub line: String,
    pub kind: String,
    pub result: RunResult,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, EncodeLabelSet)]
pub struct HoldLabels {
    pub engine: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, EncodeLabelSet)]
pub struct HttpLabels {
    /// The route's template (`/internal/runs/{id}/renew`), never its path.
    pub route: String,
    pub status: u16,
}

type FloatGauge = Gauge<f64, AtomicU64>;

/// Every metric of the orchestrator.
#[derive(Debug)]
pub struct Metrics {
    pub queue_ready_runs: Family<EngineLabels, Gauge>,
    pub queue_oldest_ready_seconds: Family<EngineLabels, FloatGauge>,
    pub runs_total: Family<ResultLabels, Counter>,
    pub run_duration_seconds: Family<KindLabels, Histogram, fn() -> Histogram>,
    pub runners_live: Family<EngineLabels, Gauge>,
    pub schedule_lag_seconds: FloatGauge,
    pub missed_fires_total: Counter,
    pub hold_state: Family<HoldLabels, Gauge>,
    pub lost_runs_total: Family<EngineLabels, Counter>,
    pub http_requests_total: Family<HttpLabels, Counter>,
}

fn duration_buckets() -> Histogram {
    // 1 s to about 9 hours.
    Histogram::new(prometheus_client::metrics::histogram::exponential_buckets(
        1.0, 3.0, 11,
    ))
}

impl Metrics {
    /// The metrics, registered in `registry` under `orchestrator_`.
    pub fn register(registry: &mut Registry) -> std::sync::Arc<Self> {
        let m = Self {
            queue_ready_runs: Family::default(),
            queue_oldest_ready_seconds: Family::default(),
            runs_total: Family::default(),
            run_duration_seconds: Family::new_with_constructor(duration_buckets),
            runners_live: Family::default(),
            schedule_lag_seconds: FloatGauge::default(),
            missed_fires_total: Counter::default(),
            hold_state: Family::default(),
            lost_runs_total: Family::default(),
            http_requests_total: Family::default(),
        };
        let r = registry.sub_registry_with_prefix("orchestrator");
        r.register(
            "queue_ready_runs",
            "Runs ready to be claimed",
            m.queue_ready_runs.clone(),
        );
        r.register(
            "queue_oldest_ready_seconds",
            "How long the oldest ready run has waited",
            m.queue_oldest_ready_seconds.clone(),
        );
        r.register("runs", "Runs ended, by result", m.runs_total.clone());
        r.register(
            "run_duration_seconds",
            "How long runs ran",
            m.run_duration_seconds.clone(),
        );
        r.register(
            "runners_live",
            "Runners seen lately",
            m.runners_live.clone(),
        );
        r.register(
            "schedule_lag_seconds",
            "The latest fire's delay",
            m.schedule_lag_seconds.clone(),
        );
        r.register(
            "missed_fires",
            "Fires missed and not run",
            m.missed_fires_total.clone(),
        );
        r.register(
            "hold_state",
            "1 while an engine is held",
            m.hold_state.clone(),
        );
        r.register(
            "lost_runs",
            "Runs whose lease expired",
            m.lost_runs_total.clone(),
        );
        r.register(
            "http_requests",
            "Requests, by route template",
            m.http_requests_total.clone(),
        );
        std::sync::Arc::new(m)
    }

    /// A run ended as its runner reported it, after `seconds`.
    pub fn run_ended(&self, engine: &EngineLine, kind: &str, state: FinalState, seconds: f64) {
        let e = EngineLabels::from(engine);
        self.runs_total
            .get_or_create(&ResultLabels {
                engine: e.engine.clone(),
                line: e.line.clone(),
                kind: kind.to_string(),
                result: state.into(),
            })
            .inc();
        self.run_duration_seconds
            .get_or_create(&KindLabels {
                engine: e.engine,
                line: e.line,
                kind: kind.to_string(),
            })
            .observe(seconds);
    }

    /// A run of `engine` and `kind` was reaped after `seconds`.
    pub fn run_lost(&self, engine: &EngineLine, kind: &str, seconds: f64) {
        self.lost_runs_total
            .get_or_create(&EngineLabels::from(engine))
            .inc();
        let e = EngineLabels::from(engine);
        self.runs_total
            .get_or_create(&ResultLabels {
                engine: e.engine.clone(),
                line: e.line.clone(),
                kind: kind.to_string(),
                result: RunResult::lost,
            })
            .inc();
        self.run_duration_seconds
            .get_or_create(&KindLabels {
                engine: e.engine,
                line: e.line,
                kind: kind.to_string(),
            })
            .observe(seconds);
    }

    /// Sets the gauges read from the store: the queue, the runners seen
    /// within a lease's length, the holds.
    pub async fn refresh(&self, store: &PgStore, now: DateTime<Utc>) -> Result<(), StoreError> {
        let queue = store.queue_stats(now).await?;
        self.queue_ready_runs.clear();
        self.queue_oldest_ready_seconds.clear();
        for q in &queue {
            let labels = EngineLabels::from(&q.engine);
            self.queue_ready_runs.get_or_create(&labels).set(q.ready);
            self.queue_oldest_ready_seconds
                .get_or_create(&labels)
                .set(q.oldest_secs);
        }
        let since = now - chrono::Duration::seconds(i64::from(engine_port::LEASE_SECS));
        for l in store.live_runners(since).await? {
            let labels = EngineLabels::from(&l.engine);
            self.runners_live
                .get_or_create(&labels)
                .set(i64::from(l.live));
        }
        for (engine, held) in store.holds(now).await? {
            self.held(&engine, held);
        }
        Ok(())
    }

    /// Counts the runs in `ids` that ended (finished or reaped).
    pub async fn count_ended(&self, store: &PgStore, ids: &[String]) -> Result<(), StoreError> {
        for r in store.ended_runs(ids).await? {
            match FinalState::parse(r.state.as_str()) {
                Ok(state) => self.run_ended(&r.engine, &r.kind, state, r.seconds),
                Err(_) => self.run_lost(&r.engine, &r.kind, r.seconds),
            }
        }
        Ok(())
    }

    /// Whether `engine` is held now.
    pub fn held(&self, engine: &EngineName, held: bool) {
        let labels = HoldLabels {
            engine: engine.to_string(),
        };
        self.hold_state.get_or_create(&labels).set(i64::from(held));
    }
}
