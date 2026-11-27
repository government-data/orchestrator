//! A run's locks over the engine port: one running holder per name, a
//! malformed name refused, another lease refused, given back for the next.

mod common;

use axum::http::StatusCode;
use common::harness;
use serde_json::json;

#[tokio::test]
async fn a_lock_is_one_running_runs_at_a_time() {
    let h = harness().await;
    h.register().await;
    h.queue(&h.flow("a").await).await;
    h.queue(&h.flow("b").await).await;
    let (_, first) = h.claim().await;
    let (_, second) = h.claim().await;
    let (a, b) = (
        first["run"].as_str().unwrap(),
        second["run"].as_str().unwrap(),
    );
    let (la, lb) = (&first["lease"], &second["lease"]);
    let gold = "lake.quality_gold.rule_results";
    let take = |run: &str, lease: &serde_json::Value, name: &str| {
        (
            format!("/internal/runs/{run}/locks"),
            json!({"lease": lease, "name": name}),
        )
    };

    let (path, body) = take(a, la, gold);
    let (status, answer) = h.post(&path, body).await;
    assert_eq!((status, answer), (StatusCode::OK, json!({"granted": true})));
    let (path, body) = take(b, lb, gold);
    let (_, answer) = h.post(&path, body.clone()).await;
    assert_eq!(answer, json!({"granted": false}), "a's");
    let (status, _) = h
        .post(&path, json!({"lease": "another", "name": gold}))
        .await;
    assert_eq!(status, StatusCode::CONFLICT, "not b's lease");
    let (status, _) = h
        .post(&path, json!({"lease": lb, "name": "Gold Table"}))
        .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);

    let (status, _) = h
        .post(
            &format!("/internal/runs/{a}/locks/release"),
            json!({"lease": la, "name": gold}),
        )
        .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    let (_, answer) = h.post(&path, body).await;
    assert_eq!(answer, json!({"granted": true}), "b's now");
}
