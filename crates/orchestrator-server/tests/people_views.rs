//! What people read of flows with a fake core: folders and flows, behind
//! `orchestrator:ViewFlows`; a call refused (401, 403, 503) reads nothing.

mod common;

use axum::http::{Method, StatusCode};
use common::harness;
use common::people::{call, people_app, refused_each_way};
use orchestrator_server::{IMPORT_FLOWS, VIEW_FLOWS};
use serde_json::Value;

const NO_ID: &str = "00000000-0000-0000-0000-000000000000";

#[tokio::test]
async fn a_call_without_view_flows_is_refused() {
    let h = harness().await;
    let (app, core) = people_app(&h);
    let calls: Vec<(Method, String, Value)> = [
        "/api/v1/folders".to_string(),
        "/api/v1/flows".into(),
        format!("/api/v1/flows/{NO_ID}"),
    ]
    .into_iter()
    .map(|uri| (Method::GET, uri, Value::Null))
    .collect();
    refused_each_way(&app, &core, VIEW_FLOWS, &calls).await;
    core.holds(&[IMPORT_FLOWS]);
    let (status, _) = call(&app, Method::GET, "/api/v1/flows", true, Value::Null).await;
    assert_eq!(
        status,
        StatusCode::FORBIDDEN,
        "another action is not enough"
    );
}

#[tokio::test]
async fn a_person_with_view_flows_reads_folders_and_flows() {
    let h = harness().await;
    h.register().await;
    let daily = h.flow("daily").await;
    let weekly = h.flow("weekly").await;
    let (app, core) = people_app(&h);
    core.holds(&[VIEW_FLOWS]);
    let get = |uri: String| {
        let app = app.clone();
        async move { call(&app, Method::GET, &uri, true, Value::Null).await }
    };

    let (status, folders) = get("/api/v1/folders".into()).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(folders["folders"].as_array().unwrap().len(), 2);
    let (_, flows) = get("/api/v1/flows".into()).await;
    assert_eq!(flows["flows"].as_array().unwrap().len(), 2);
    let (_, flows) = get(format!("/api/v1/flows?folder={}", daily.folder_id)).await;
    assert_eq!(flows["flows"][0]["id"], daily.id.as_str());
    assert_eq!(flows["flows"].as_array().unwrap().len(), 1);
    let (status, _) = get("/api/v1/flows?folder=not-an-id".into()).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let (status, flow) = get(format!("/api/v1/flows/{}", weekly.id)).await;
    assert_eq!(
        (status, flow["name"].as_str(), flow["engine"].as_str()),
        (StatusCode::OK, Some("weekly"), Some("fake-engine"))
    );
    let (status, _) = get(format!("/api/v1/flows/{NO_ID}")).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let (status, _) = get("/api/v1/flows/not-an-id".into()).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}
