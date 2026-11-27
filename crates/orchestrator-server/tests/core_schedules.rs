//! The core's schedules, with a fake core: kept as schedules of one flow,
//! fired by the orchestrator's rules, each fire starting the chain the core
//! says; a fire the core does not answer stays due, an empty chain runs
//! nothing, a schedule the core no longer lists stops.

mod common;

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use common::{at, harness};
use orchestration_model::{ChainStep, Setup};
use orchestrator_server::{CoreFire, CoreSchedule, CoreSchedules, Scheduler};
use schedule::ScheduleSpec;
use serde_json::json;

#[derive(Default)]
struct FakeCore {
    schedules: Mutex<Option<Vec<CoreSchedule>>>,
    /// `None`: the core does not answer fires.
    steps: Mutex<Option<Vec<ChainStep>>>,
    fires: Mutex<Vec<(String, DateTime<Utc>)>>,
}

#[async_trait]
impl CoreSchedules for FakeCore {
    async fn schedules(&self) -> Option<Vec<CoreSchedule>> {
        self.schedules.lock().unwrap().clone()
    }

    async fn fire(&self, id: &str, fire_at: DateTime<Utc>) -> Option<CoreFire> {
        self.fires.lock().unwrap().push((id.to_string(), fire_at));
        let steps = self.steps.lock().unwrap().clone()?;
        Some(CoreFire { steps })
    }
}

fn hourly(enabled: bool) -> Vec<CoreSchedule> {
    vec![CoreSchedule {
        id: "sched-1".into(),
        spec: ScheduleSpec {
            cron: "0 * * * *".into(),
            timezone: "UTC".into(),
            trigger: None,
        },
        enabled,
    }]
}

fn cut_chain() -> Vec<ChainStep> {
    let step = |kind: &str| ChainStep {
        kind: kind.into(),
        parameters: json!({"dataset": "sales", "cut": "2026-11-25"}),
        setup: None,
    };
    let mut prepare = step("prepare");
    prepare.setup = Some(Setup {
        key: "dataset:sales".into(),
        fingerprint: "c0ffee".into(),
    });
    vec![prepare, step("extract"), step("evaluate")]
}

#[tokio::test]
async fn a_core_schedule_fires_the_chain_the_core_says() {
    let h = harness().await;
    h.register().await;
    let flow = h.flow("quality").await;
    let core = Arc::new(FakeCore::default());
    *core.schedules.lock().unwrap() = Some(hourly(true));
    let scheduler = Scheduler::new(h.store.clone(), h.clock.clone())
        .with_core_schedules(core.clone(), flow.id.clone());

    let tick = scheduler.tick().await.unwrap();
    assert_eq!((tick.queued, tick.chained), (0, 0), "kept, due at 11:00");
    let kept = h.store.schedules(&flow.id).await.unwrap();
    assert_eq!(kept.len(), 1);
    assert_eq!(kept[0].core_id.as_deref(), Some("sched-1"));
    assert_eq!(kept[0].next_fire_at, Some(at("2026-11-25T11:00:00Z")));

    // The core does not answer: the fire stays due and is asked again.
    h.clock.advance(Duration::hours(1) + Duration::seconds(5));
    assert_eq!(scheduler.tick().await.unwrap().queued, 0);
    let still = &h.store.schedules(&flow.id).await.unwrap()[0];
    assert_eq!(still.next_fire_at, Some(at("2026-11-25T11:00:00Z")));

    *core.steps.lock().unwrap() = Some(cut_chain());
    let tick = scheduler.tick().await.unwrap();
    assert_eq!(
        (tick.queued, tick.chained),
        (1, 1),
        "a chain, its first step queued"
    );
    assert_eq!(scheduler.tick().await.unwrap().queued, 0, "moved on");
    let fires = core.fires.lock().unwrap().clone();
    assert_eq!(fires.len(), 2);
    assert!(fires
        .iter()
        .all(|f| f == &("sched-1".into(), at("2026-11-25T11:00:00Z"))));
    let (_, claimed) = h.claim().await;
    assert_eq!(claimed["kind"], "prepare");
    assert_eq!(claimed["parameters"]["dataset"], "sales");
}

#[tokio::test]
async fn an_empty_chain_runs_nothing_and_a_dropped_schedule_stops() {
    let h = harness().await;
    h.register().await;
    let flow = h.flow("quality").await;
    let core = Arc::new(FakeCore::default());
    *core.schedules.lock().unwrap() = Some(hourly(true));
    *core.steps.lock().unwrap() = Some(Vec::new());
    let scheduler = Scheduler::new(h.store.clone(), h.clock.clone())
        .with_core_schedules(core.clone(), flow.id.clone());
    scheduler.tick().await.unwrap();
    h.clock.advance(Duration::hours(1) + Duration::seconds(5));
    assert_eq!(scheduler.tick().await.unwrap().queued, 0, "a blocked cut");
    let kept = &h.store.schedules(&flow.id).await.unwrap()[0];
    assert_eq!(
        kept.next_fire_at,
        Some(at("2026-11-25T12:00:00Z")),
        "moved on"
    );

    // Not answered: kept as it was. Disabled, then no longer listed.
    *core.schedules.lock().unwrap() = None;
    scheduler.tick().await.unwrap();
    assert!(h.store.schedules(&flow.id).await.unwrap()[0].enabled);
    *core.schedules.lock().unwrap() = Some(hourly(false));
    scheduler.tick().await.unwrap();
    assert!(!h.store.schedules(&flow.id).await.unwrap()[0].enabled);
    *core.schedules.lock().unwrap() = Some(hourly(true));
    scheduler.tick().await.unwrap();
    assert!(
        h.store.schedules(&flow.id).await.unwrap()[0].enabled,
        "enabled again"
    );
    *core.schedules.lock().unwrap() = Some(Vec::new());
    scheduler.tick().await.unwrap();
    let gone = &h.store.schedules(&flow.id).await.unwrap()[0];
    assert!(!gone.enabled && gone.next_fire_at.is_none());
}
