//! The scheduler's tick: a due schedule queues its fire once, a blocked flow
//! keeps its fire due, and a window that skips runs nothing.

mod common;

use chrono::Duration;
use common::{at, harness};
use orchestration_model::{MaintenanceWindow, OnSchedule, WindowTime};
use orchestrator_postgres::NewSchedule;
use orchestrator_server::Scheduler;
use schedule::ScheduleSpec;
use serde_json::json;

fn hourly(flow_id: &str) -> NewSchedule {
    NewSchedule {
        flow_id: flow_id.into(),
        spec: ScheduleSpec {
            cron: "0 * * * *".into(),
            timezone: "UTC".into(),
            trigger: None,
        },
        parameters: json!({"n": 2}),
        timeout_secs: Some(60),
        next_fire_at: Some(at("2026-11-25T10:00:00Z")),
        created_by: None,
    }
}

async fn queued(h: &common::Harness) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM runs WHERE state = 'queued'")
        .fetch_one(h.store.pool())
        .await
        .unwrap()
}

#[tokio::test]
async fn a_due_schedule_queues_its_fire_once() {
    let h = harness().await;
    h.register().await;
    let flow = h.flow("daily").await;
    let s = h.store.create_schedule(&hourly(&flow.id)).await.unwrap();
    let scheduler = Scheduler::new(h.store.clone(), h.clock.clone());
    h.clock.advance(Duration::seconds(5));
    assert_eq!(scheduler.tick().await.unwrap().queued, 1);
    assert_eq!(scheduler.tick().await.unwrap().queued, 0, "moved on");
    let after = &h.store.schedules(&flow.id).await.unwrap()[0];
    assert_eq!(after.next_fire_at, Some(at("2026-11-25T11:00:00Z")));
    let (_, claimed) = h.claim().await;
    assert_eq!(
        claimed["parameters"],
        json!({"n": 2}),
        "the schedule's over the flow's"
    );
    assert_eq!(claimed["timeout_secs"], 60);
    assert_eq!(s.id, after.id);
}

#[tokio::test]
async fn a_paused_flow_keeps_its_fire_until_it_resumes() {
    let h = harness().await;
    h.register().await;
    let flow = h.flow("daily").await;
    h.store.create_schedule(&hourly(&flow.id)).await.unwrap();
    use orchestrator_server::Clock;
    h.store
        .pause_flow(&flow.id, Some(h.clock.now()))
        .await
        .unwrap();
    let scheduler = Scheduler::new(h.store.clone(), h.clock.clone());
    h.clock.advance(Duration::hours(3));
    assert_eq!(scheduler.tick().await.unwrap().queued, 0);
    h.store.pause_flow(&flow.id, None).await.unwrap();
    // Missed 10:00 to 13:00: the installation's policy runs the latest.
    assert_eq!(scheduler.tick().await.unwrap().queued, 1);
    assert_eq!(queued(&h).await, 1);
}

#[tokio::test]
async fn a_flow_of_an_engine_not_registered_does_not_fire() {
    let h = harness().await;
    let flow = h.flow("daily").await;
    h.store.create_schedule(&hourly(&flow.id)).await.unwrap();
    let scheduler = Scheduler::new(h.store.clone(), h.clock.clone());
    h.clock.advance(Duration::seconds(5));
    assert_eq!(scheduler.tick().await.unwrap().queued, 0);
    h.register().await;
    assert_eq!(
        scheduler.tick().await.unwrap().queued,
        1,
        "the line back: it resumes"
    );
}

#[tokio::test]
async fn a_window_that_skips_runs_nothing_on_its_cluster() {
    let h = harness().await;
    h.register().await;
    let mut flow = h.flow("daily").await;
    sqlx::query("UPDATE flows SET cluster = 'machine-1' WHERE id = $1::uuid")
        .bind(&flow.id)
        .execute(h.store.pool())
        .await
        .unwrap();
    flow.cluster = Some("machine-1".into());
    h.store.create_schedule(&hourly(&flow.id)).await.unwrap();
    let window = MaintenanceWindow {
        id: String::new(),
        name: "upgrade".into(),
        time: WindowTime::Once {
            starts_at: at("2026-11-25T09:00:00Z"),
            ends_at: at("2026-11-25T12:00:00Z"),
        },
        on_schedule: OnSchedule::Skip,
        enabled: true,
        clusters: vec!["machine-1".into()],
    };
    h.store.save_window(&window).await.unwrap();
    let scheduler = Scheduler::new(h.store.clone(), h.clock.clone());
    h.clock.advance(Duration::seconds(5));
    let tick = scheduler.tick().await.unwrap();
    assert_eq!((tick.queued, tick.skipped), (0, 1));
    assert_eq!(queued(&h).await, 0);
}

/// A fake core: the modules it lists, or no answer.
struct FakeCore(std::sync::Mutex<Option<Vec<String>>>);

#[async_trait::async_trait]
impl orchestrator_server::ModuleList for FakeCore {
    async fn installed(&self) -> Option<Vec<String>> {
        self.0.lock().unwrap().clone()
    }
}

#[tokio::test]
async fn a_line_the_core_stops_listing_does_not_fire() {
    let h = harness().await;
    h.register().await;
    let flow = h.flow("daily").await;
    h.store.create_schedule(&hourly(&flow.id)).await.unwrap();
    let core = std::sync::Arc::new(FakeCore(std::sync::Mutex::new(Some(vec!["other".into()]))));
    let scheduler = Scheduler::new(h.store.clone(), h.clock.clone()).with_modules(core.clone());
    h.clock.advance(Duration::seconds(5));
    assert_eq!(
        scheduler.tick().await.unwrap().queued,
        0,
        "engine-fake not listed"
    );
    *core.0.lock().unwrap() = None;
    assert_eq!(
        scheduler.tick().await.unwrap().queued,
        0,
        "no answer: the last list holds"
    );
    *core.0.lock().unwrap() = Some(vec!["engine-fake".into()]);
    assert_eq!(
        scheduler.tick().await.unwrap().queued,
        1,
        "listed again: it fires"
    );
}
