//! A person runs a flow now and cancels runs, with a fake core, behind
//! `orchestrator:RunWorkflow`; a call refused (401, 403, 503) queues and
//! cancels nothing.

mod common;

use axum::http::{Method, StatusCode};
use common::harness;
use common::people::{call, people_app, people_app_on, refused_each_way};
use orchestrator_postgres::RunFilter;
use orchestrator_server::{EnginePort, RUN_WORKFLOW, VIEW_FLOWS};
use serde_json::Value;

const NO_ID: &str = "00000000-0000-0000-0000-000000000000";

async fn runs(h: &common::Harness) -> Vec<(String, String)> {
    let all = RunFilter {
        limit: 100,
        ..RunFilter::default()
    };
    let page = h.store.runs_page(&all).await.unwrap();
    page.rows
        .into_iter()
        .map(|r| (r.id, r.state.as_str().to_string()))
        .collect()
}

#[tokio::test]
async fn a_call_without_run_workflow_is_refused_and_changes_nothing() {
    let h = harness().await;
    h.register().await;
    let flow = h.flow("daily").await;
    let run = h.queue(&flow).await;
    let (app, core) = people_app(&h);
    let calls = [
        (
            Method::POST,
            format!("/api/v1/flows/{}/runs", flow.id),
            Value::Null,
        ),
        (
            Method::POST,
            format!("/api/v1/runs/{run}/cancel"),
            Value::Null,
        ),
    ];
    refused_each_way(&app, &core, RUN_WORKFLOW, &calls).await;
    core.holds(&[VIEW_FLOWS]);
    for (method, uri, body) in &calls {
        let (status, _) = call(&app, method.clone(), uri, true, body.clone()).await;
        assert_eq!(
            status,
            StatusCode::FORBIDDEN,
            "{uri}: seeing is not running"
        );
    }
    assert_eq!(runs(&h).await, [(run, "queued".to_string())]);
}

#[tokio::test]
async fn a_person_runs_a_flow_now_and_cancels_runs() {
    let h = harness().await;
    h.register().await;
    let flow = h.flow("daily").await;
    let (app, core) = people_app(&h);
    core.holds(&[RUN_WORKFLOW]);
    let post = |uri: String| {
        let app = app.clone();
        async move { call(&app, Method::POST, &uri, true, Value::Null).await }
    };

    let (status, queued) = post(format!("/api/v1/flows/{}/runs", flow.id)).await;
    assert_eq!(status, StatusCode::ACCEPTED);
    let first = queued["run"].as_str().unwrap().to_string();
    let view = h.store.run_view(&first).await.unwrap().unwrap();
    assert_eq!(
        (view.trigger.as_str(), view.kind.as_str()),
        ("manual", "job")
    );
    for uri in [
        format!("/api/v1/flows/{NO_ID}/runs"),
        "/api/v1/flows/x/runs".into(),
    ] {
        assert_eq!(post(uri).await.0, StatusCode::NOT_FOUND);
    }

    let (status, answer) = post(format!("/api/v1/runs/{first}/cancel")).await;
    assert_eq!(
        (status, answer["state"].as_str()),
        (StatusCode::OK, Some("cancelled"))
    );
    let (status, _) = post(format!("/api/v1/runs/{first}/cancel")).await;
    assert_eq!(status, StatusCode::CONFLICT, "it has ended");
    for uri in [
        format!("/api/v1/runs/{NO_ID}/cancel"),
        "/api/v1/runs/x/cancel".into(),
    ] {
        assert_eq!(post(uri).await.0, StatusCode::NOT_FOUND);
    }

    let (_, queued) = post(format!("/api/v1/flows/{}/runs", flow.id)).await;
    let second = queued["run"].as_str().unwrap().to_string();
    let (_, claimed) = h.claim().await;
    assert_eq!(claimed["run"], second.as_str());
    let (status, answer) = post(format!("/api/v1/runs/{second}/cancel")).await;
    assert_eq!(status, StatusCode::ACCEPTED);
    assert_eq!(answer["cancel_requested"], true);
    let view = h.store.run_view(&second).await.unwrap().unwrap();
    assert!(
        view.cancel_requested,
        "its runner is told at its next renewal"
    );

    sqlx::query("UPDATE flows SET enabled = false WHERE id = $1::uuid")
        .bind(&flow.id)
        .execute(h.store.pool())
        .await
        .unwrap();
    let (status, _) = post(format!("/api/v1/flows/{}/runs", flow.id)).await;
    assert_eq!(status, StatusCode::CONFLICT, "a disabled flow does not run");
    assert_eq!(runs(&h).await.len(), 2);
}

#[tokio::test]
async fn the_cores_flow_is_not_run_by_a_person() {
    let h = harness().await;
    h.register().await;
    let flow = h.flow("core").await;
    let port = EnginePort::new(h.store.clone(), h.clock.clone()).with_core_flow(flow.id.clone());
    let (app, core) = people_app_on(port);
    core.holds(&[RUN_WORKFLOW]);
    let uri = format!("/api/v1/flows/{}/runs", flow.id);
    let (status, _) = call(&app, Method::POST, &uri, true, Value::Null).await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert!(runs(&h).await.is_empty());
}
