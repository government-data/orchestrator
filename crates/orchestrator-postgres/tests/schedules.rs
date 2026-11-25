//! Schedules and settings in the store.

mod common;

use chrono::Duration;
use common::{at, new_flow};
use orchestration_model::{MissedRuns, OnInterrupt, Settings};
use orchestrator_postgres::{NewSchedule, StoreError};
use schedule::ScheduleSpec;
use serde_json::json;

#[tokio::test]
async fn schedules_fire_once_per_scheduler() {
    let db = common::database().await;
    let store = db.store();
    let folder = store.create_folder("f").await.unwrap();
    let flow = store.create_flow(&new_flow(&folder.id, "x")).await.unwrap();
    let first = at("2026-11-25T11:00:00Z");
    let new = NewSchedule {
        flow_id: flow.id.clone(),
        spec: ScheduleSpec {
            cron: "0 * * * *".into(),
            timezone: "UTC".into(),
            trigger: None,
        },
        parameters: json!({"day": "yesterday"}),
        timeout_secs: None,
        next_fire_at: Some(first),
        created_by: Some("person-1".into()),
    };
    let s = store.create_schedule(&new).await.unwrap();
    assert!(s.enabled);
    assert_eq!(
        store.schedules(&flow.id).await.unwrap(),
        std::slice::from_ref(&s)
    );
    assert!(store
        .due_schedules(first - Duration::seconds(1))
        .await
        .unwrap()
        .is_empty());
    assert_eq!(store.due_schedules(first).await.unwrap().len(), 1);

    let next = first + Duration::hours(1);
    assert!(store
        .advance_schedule(&s.id, first, Some(first), Some(next))
        .await
        .unwrap());
    assert!(
        !store
            .advance_schedule(&s.id, first, Some(first), Some(next))
            .await
            .unwrap(),
        "a second scheduler does not move it again"
    );
    // A schedule that fires no more is disabled.
    assert!(store
        .advance_schedule(&s.id, next, Some(next), None)
        .await
        .unwrap());
    let done = &store.schedules(&flow.id).await.unwrap()[0];
    assert!(!done.enabled);
    assert_eq!(done.last_fire_at, Some(next));

    let paused = store
        .create_schedule(&NewSchedule {
            next_fire_at: None,
            ..new
        })
        .await
        .unwrap();
    assert!(!paused.enabled);
}

#[tokio::test]
async fn settings_are_one_row_and_checked() {
    let db = common::database().await;
    let store = db.store();
    assert_eq!(store.settings().await.unwrap(), Settings::default());
    let mine = Settings {
        missed_runs: MissedRuns::All,
        missed_max_runs: 5,
        on_interrupt: OnInterrupt::Continue,
        queued_wait_minutes: Some(60),
        ..Settings::default()
    };
    store.save_settings(&mine).await.unwrap();
    assert_eq!(store.settings().await.unwrap(), mine);
    let bad = Settings {
        missed_max_hours: 0,
        ..mine
    };
    assert!(matches!(
        store.save_settings(&bad).await,
        Err(StoreError::Invalid(_))
    ));
}
