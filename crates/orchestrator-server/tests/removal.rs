//! The check before a module's removal (decision 0015): an engine module is
//! refused while a flow (paused ones too) is on a line it registered or a
//! run of one is queued or running; the orchestrator while an engine module
//! registered a line.

mod common;

use axum::http::{Method, StatusCode};
use common::harness;
use serde_json::{json, Value};

async fn ask(h: &common::Harness, module: &str) -> Value {
    let path = format!("/internal/removal?module={module}");
    let (status, answer) = h.call(Method::GET, &path, Some("core"), Value::Null).await;
    assert_eq!(status, StatusCode::OK);
    answer
}

#[tokio::test]
async fn a_line_with_flows_or_runs_is_not_removed() {
    let h = harness().await;
    assert_eq!(ask(&h, "engine-fake").await, json!({"refuse": false}));
    h.register().await;
    assert_eq!(
        ask(&h, "engine-fake").await,
        json!({"refuse": false}),
        "no flow yet"
    );
    let flow = h.flow("daily").await;
    use orchestrator_server::Clock;
    h.store
        .pause_flow(&flow.id, Some(h.clock.now()))
        .await
        .unwrap();
    let refused = ask(&h, "engine-fake").await;
    assert_eq!(refused["refuse"], true, "a paused flow counts");
    assert_eq!(
        refused["counts"],
        json!({"flows": 1, "lines": 1, "runs": 0})
    );
    assert!(refused["reason"]
        .as_str()
        .unwrap()
        .contains("move them first"));
    assert!(!refused.to_string().contains("daily"), "no flow's name");
    assert_eq!(ask(&h, "another-module").await, json!({"refuse": false}));

    let orchestrator = ask(&h, "orchestrator").await;
    assert_eq!(orchestrator["refuse"], true);
    assert_eq!(orchestrator["counts"], json!({"engine_modules": 1}));

    let (status, _) = h
        .call(Method::GET, "/internal/removal?module=x", None, Value::Null)
        .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn a_queued_run_counts_and_a_deleted_flow_does_not() {
    let h = harness().await;
    h.register().await;
    let flow = h.flow("daily").await;
    h.queue(&flow).await;
    let refused = ask(&h, "engine-fake").await;
    assert_eq!(
        refused["counts"],
        json!({"flows": 1, "lines": 1, "runs": 1})
    );
    sqlx::query("DELETE FROM flows WHERE id = $1::uuid")
        .bind(&flow.id)
        .execute(h.store.pool())
        .await
        .unwrap();
    assert_eq!(ask(&h, "engine-fake").await, json!({"refuse": false}));
}
