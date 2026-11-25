//! The engine port with a fake engine: claim, cancel and the drain hold.

mod common;

use axum::http::{Method, StatusCode};
use chrono::Duration;
use common::harness;
use serde_json::{json, Value};

fn finish(lease: &Value, state: &str) -> Value {
    json!({"lease": lease, "state": state, "counts": {"rows": 3}, "error_class": null})
}

#[tokio::test]
async fn a_runner_registers_claims_logs_and_finishes() {
    let h = harness().await;
    h.register().await;
    let flow = h.flow("daily").await;
    let run = h.queue(&flow).await;

    let (status, claimed) = h.claim().await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(claimed["run"], run.as_str());
    assert_eq!(
        (claimed["kind"].as_str(), claimed["attempt"].as_u64()),
        (Some("job"), Some(1))
    );
    assert_eq!(claimed["parameters"], json!({"n": 1}));
    let lease = &claimed["lease"];
    let (status, _) = h.claim().await;
    assert_eq!(status, StatusCode::NO_CONTENT, "nothing else queued");

    let renew = json!({"lease": lease, "progress": {"fraction": 0.5, "detail": "half"}, "engine_ref": "q-1"});
    let (status, renewed) = h.post(&format!("/internal/runs/{run}/renew"), renew).await;
    assert_eq!(
        (status, renewed["action"].as_str()),
        (StatusCode::OK, Some("continue"))
    );
    let lines = json!({"lease": lease, "lines": [{"seq": 1, "stream": "stdout", "line": "hello"}]});
    let (status, _) = h.post(&format!("/internal/runs/{run}/logs"), lines).await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    let (status, _) = h
        .post(
            &format!("/internal/runs/{run}/finish"),
            finish(lease, "succeeded"),
        )
        .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    let (status, _) = h
        .post(
            &format!("/internal/runs/{run}/finish"),
            finish(lease, "failed"),
        )
        .await;
    assert_eq!(status, StatusCode::CONFLICT, "a run ends once");
}

#[tokio::test]
async fn a_cancel_reaches_the_runner_at_its_renewal() {
    let h = harness().await;
    h.register().await;
    let flow = h.flow("daily").await;
    let run = h.queue(&flow).await;
    let (_, claimed) = h.claim().await;
    let lease = &claimed["lease"];
    use orchestrator_server::Clock;
    h.store.cancel(&run, h.clock.now()).await.unwrap();
    let renew = json!({"lease": lease, "progress": null, "engine_ref": null});
    let (_, renewed) = h.post(&format!("/internal/runs/{run}/renew"), renew).await;
    assert_eq!(renewed["action"], "cancel");
    let (status, _) = h
        .post(
            &format!("/internal/runs/{run}/finish"),
            finish(lease, "cancelled"),
        )
        .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
}

#[tokio::test]
async fn the_drain_hold_answers_draining_then_free() {
    let h = harness().await;
    h.register().await;
    let first = h.flow("first").await;
    let second = h.flow("second").await;
    let run = h.queue(&first).await;
    h.queue(&second).await;
    let (_, claimed) = h.claim().await;
    let lease = &claimed["lease"];

    use orchestrator_server::Clock;
    let until = h.clock.now() + Duration::minutes(30);
    let hold = json!({"hold": "h-1", "until": until});
    let (status, answer) = h
        .post("/internal/engines/fake-engine/hold", hold.clone())
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(answer, json!({"state": "draining", "running": 1}));
    let (status, _) = h.claim().await;
    assert_eq!(
        status,
        StatusCode::NO_CONTENT,
        "no claim answered while held"
    );
    let renew = json!({"lease": lease, "progress": null, "engine_ref": null});
    let (_, renewed) = h.post(&format!("/internal/runs/{run}/renew"), renew).await;
    assert_eq!(renewed["action"], "held", "the running one goes on");
    h.post(
        &format!("/internal/runs/{run}/finish"),
        finish(lease, "succeeded"),
    )
    .await;
    let (_, answer) = h.post("/internal/engines/fake-engine/hold", hold).await;
    assert_eq!(answer, json!({"state": "free", "running": 0}));

    let (status, _) = h
        .post(
            "/internal/engines/fake-engine/release",
            json!({"hold": "h-1"}),
        )
        .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    let (status, _) = h.claim().await;
    assert_eq!(
        status,
        StatusCode::OK,
        "released: the second run is handed out"
    );
}

#[tokio::test]
async fn what_the_port_refuses() {
    let h = harness().await;
    let body = serde_json::to_value(common::descriptor()).unwrap();
    let path = "/internal/engines/fake-engine/-";
    let (status, _) = h.call(Method::PUT, path, None, body.clone()).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    let mut port_two = body.clone();
    port_two["port"] = json!(2);
    let (status, answer) = h.call(Method::PUT, path, Some("machine-1"), port_two).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    assert!(answer["error"].as_str().unwrap().contains("not served"));
    let other = "/internal/engines/other-engine/-";
    let (status, _) = h.call(Method::PUT, other, Some("machine-1"), body).await;
    assert_eq!(
        status,
        StatusCode::UNPROCESSABLE_ENTITY,
        "the path names another engine"
    );
    let (status, _) = h.claim().await;
    assert_eq!(status, StatusCode::NOT_FOUND, "not registered");
}
