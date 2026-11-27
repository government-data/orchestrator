//! The core's drain before an update (quality-sdk decision 0002): the runs
//! running now, and the queued runs and schedule fires due within the
//! horizon; a paused flow's schedule does not fire, so it is not counted.

mod common;

use axum::http::{Method, StatusCode};
use common::{at, harness};
use orchestrator_postgres::NewSchedule;
use orchestrator_server::Clock;
use schedule::ScheduleSpec;
use serde_json::{json, Value};

async fn drain(h: &common::Harness, horizon_secs: u32) -> Value {
    let path = format!("/internal/drain?horizon_secs={horizon_secs}");
    let (status, answer) = h.call(Method::GET, &path, Some("core"), Value::Null).await;
    assert_eq!(status, StatusCode::OK);
    answer
}

#[tokio::test]
async fn counts_what_runs_and_what_is_due_within_the_horizon() {
    let h = harness().await;
    assert_eq!(
        drain(&h, 3600).await,
        json!({"running": 0, "scheduled_soon": 0})
    );
    h.register().await;
    let flow = h.flow("daily").await;
    let schedule = NewSchedule {
        flow_id: flow.id.clone(),
        spec: ScheduleSpec {
            cron: "0 * * * *".into(),
            timezone: "UTC".into(),
            trigger: None,
        },
        parameters: json!({}),
        timeout_secs: None,
        next_fire_at: Some(at("2026-11-25T11:00:00Z")),
        created_by: None,
    };
    h.store.create_schedule(&schedule).await.unwrap();
    assert_eq!(
        drain(&h, 600).await["scheduled_soon"],
        0,
        "fires in an hour"
    );
    assert_eq!(drain(&h, 3600).await["scheduled_soon"], 1);
    h.queue(&flow).await;
    assert_eq!(
        drain(&h, 3600).await,
        json!({"running": 0, "scheduled_soon": 2})
    );
    let (status, _) = h.claim().await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        drain(&h, 3600).await,
        json!({"running": 1, "scheduled_soon": 1})
    );
    h.store
        .pause_flow(&flow.id, Some(h.clock.now()))
        .await
        .unwrap();
    assert_eq!(
        drain(&h, 3600).await,
        json!({"running": 1, "scheduled_soon": 0}),
        "a paused flow does not fire; its running run still counts"
    );
}

#[tokio::test]
async fn only_a_machine_of_the_installation_asks() {
    let h = harness().await;
    let path = "/internal/drain?horizon_secs=60";
    let (status, _) = h.call(Method::GET, path, None, Value::Null).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    let (status, _) = h
        .call(Method::GET, "/internal/drain", Some("core"), Value::Null)
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "no horizon");
}
