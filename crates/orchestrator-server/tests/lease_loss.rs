//! A fake engine loses its lease: the run is reaped, repeated once by its
//! flow's rule, and the old holder changes nothing.

mod common;

use axum::http::StatusCode;
use chrono::Duration;
use common::harness;
use orchestrator_server::Scheduler;
use serde_json::json;

#[tokio::test]
async fn a_lost_lease_is_retried_and_the_old_holder_changes_nothing() {
    let h = harness().await;
    h.register().await;
    let flow = h.flow("daily").await;
    let run = h.queue(&flow).await;
    let (_, first) = h.claim().await;
    let old = first["lease"].clone();

    // The runner goes silent past its two-minute lease.
    h.clock.advance(Duration::minutes(3));
    let scheduler = Scheduler::new(h.store.clone(), h.clock.clone());
    let tick = scheduler.tick().await.unwrap();
    assert_eq!((tick.lost, tick.retried), (1, 1));
    let renew = json!({"lease": old, "progress": null, "engine_ref": null});
    let (status, _) = h.post(&format!("/internal/runs/{run}/renew"), renew).await;
    assert_eq!(status, StatusCode::CONFLICT, "the old lease is gone");

    let (status, _) = h.claim().await;
    assert_eq!(status, StatusCode::NO_CONTENT, "not before its delay");
    h.clock.advance(Duration::seconds(31));
    let (status, again) = h.claim().await;
    assert_eq!(status, StatusCode::OK);
    assert_ne!(again["run"], run.as_str());
    assert_eq!(again["attempt"], 2);
    let tick = scheduler.tick().await.unwrap();
    assert_eq!(
        (tick.lost, tick.retried),
        (0, 0),
        "a second pass changes nothing"
    );
}
