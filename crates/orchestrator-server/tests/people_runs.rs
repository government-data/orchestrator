//! What people read of runs with a fake core: a page of runs, a run and
//! its log, behind `orchestrator:ViewFlows`; a call refused (401, 403,
//! 503) reads nothing, and a run is shown without its parameters or who
//! started it.

mod common;

use axum::http::{Method, StatusCode};
use common::harness;
use common::people::{call, people_app, refused_each_way};
use orchestrator_server::{IMPORT_FLOWS, VIEW_FLOWS};
use serde_json::{json, Value};

const NO_ID: &str = "00000000-0000-0000-0000-000000000000";

#[tokio::test]
async fn a_call_without_view_flows_is_refused() {
    let h = harness().await;
    let (app, core) = people_app(&h);
    let calls: Vec<(Method, String, Value)> = [
        "/api/v1/runs".to_string(),
        format!("/api/v1/runs/{NO_ID}"),
        format!("/api/v1/runs/{NO_ID}/logs"),
    ]
    .into_iter()
    .map(|uri| (Method::GET, uri, Value::Null))
    .collect();
    refused_each_way(&app, &core, VIEW_FLOWS, &calls).await;
    core.holds(&[IMPORT_FLOWS]);
    let (status, _) = call(&app, Method::GET, "/api/v1/runs", true, Value::Null).await;
    assert_eq!(
        status,
        StatusCode::FORBIDDEN,
        "another action is not enough"
    );
}

#[tokio::test]
async fn a_person_with_view_flows_reads_runs_and_their_logs() {
    let h = harness().await;
    h.register().await;
    let daily = h.flow("daily").await;
    let weekly = h.flow("weekly").await;
    let first = h.queue(&daily).await;
    let (_, claimed) = h.claim().await;
    assert_eq!(claimed["run"], first.as_str());
    let lease = &claimed["lease"];
    let lines = json!({"lease": lease, "lines": [
        {"seq": 1, "stream": "stdout", "line": "started"},
        {"seq": 2, "stream": "system", "line": "step 1 of 2"},
        {"seq": 3, "stream": "stderr", "line": "done"}]});
    let (status, _) = h.post(&format!("/internal/runs/{first}/logs"), lines).await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    h.clock.advance(chrono::Duration::minutes(1));
    let second = h.queue(&weekly).await;
    let (app, core) = people_app(&h);
    core.holds(&[VIEW_FLOWS]);
    let get = |uri: String| {
        let app = app.clone();
        async move { call(&app, Method::GET, &uri, true, Value::Null).await }
    };

    let (status, page) = get("/api/v1/runs".into()).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(page["total"], 2);
    let run = &page["runs"][1];
    assert_eq!(
        (run["id"].as_str(), run["state"].as_str()),
        (Some(first.as_str()), Some("running"))
    );
    assert_eq!(run["flow_name"], "daily");
    assert!(run.get("parameters").is_none() && run.get("triggered_by").is_none());
    let (_, page) = get("/api/v1/runs?state=queued".into()).await;
    assert_eq!(
        (page["total"].as_u64(), page["runs"][0]["id"].as_str()),
        (Some(1), Some(second.as_str()))
    );
    let (_, page) = get(format!("/api/v1/runs?flow={}&limit=1&offset=1", daily.id)).await;
    assert_eq!(
        (
            page["total"].as_u64(),
            page["runs"].as_array().unwrap().len()
        ),
        (Some(1), 0)
    );
    let (_, page) = get("/api/v1/runs?from=2026-11-26T00:00:00Z".into()).await;
    assert_eq!(page["total"], 0);
    for bad in ["state=done", "limit=0", "limit=201", "from=yesterday"] {
        let (status, _) = get(format!("/api/v1/runs?{bad}")).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{bad}");
    }
    let (status, run) = get(format!("/api/v1/runs/{second}")).await;
    assert_eq!(
        (status, run["flow_id"].as_str()),
        (StatusCode::OK, Some(weekly.id.as_str()))
    );
    let (status, _) = get(format!("/api/v1/runs/{NO_ID}")).await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    let (status, log) = get(format!("/api/v1/runs/{first}/logs")).await;
    assert_eq!(status, StatusCode::OK);
    let seqs: Vec<u64> = log["lines"]
        .as_array()
        .unwrap()
        .iter()
        .map(|l| l["seq"].as_u64().unwrap())
        .collect();
    assert_eq!(seqs, [1, 2, 3]);
    assert_eq!(log["lines"][1]["stream"], "system");
    let (_, log) = get(format!("/api/v1/runs/{first}/logs?after=1&limit=1")).await;
    assert_eq!(
        log["lines"],
        json!([{"seq": 2, "stream": "system", "line": "step 1 of 2",
                                     "logged_at": log["lines"][0]["logged_at"]}])
    );
    let (_, log) = get(format!("/api/v1/runs/{second}/logs")).await;
    assert_eq!(log["lines"], json!([]));
    let (status, _) = get(format!("/api/v1/runs/{NO_ID}/logs")).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}
