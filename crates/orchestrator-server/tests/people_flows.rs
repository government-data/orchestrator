//! A person edits, pauses and resumes flows and engines with a fake core,
//! behind `orchestrator:ManageFlows`; a call refused (401, 403, 503)
//! changes nothing.

mod common;

use axum::http::{Method, StatusCode};
use common::harness;
use common::people::{call, people_app, refused_each_way};
use orchestrator_server::{MANAGE_FLOWS, VIEW_FLOWS};
use serde_json::{json, Value};

const NO_ID: &str = "00000000-0000-0000-0000-000000000000";

#[tokio::test]
async fn a_call_without_manage_flows_is_refused_and_changes_nothing() {
    let h = harness().await;
    h.register().await;
    let flow = h.flow("daily").await;
    let (app, core) = people_app(&h);
    let calls = [
        (
            Method::PATCH,
            format!("/api/v1/flows/{}", flow.id),
            json!({"name": "other", "enabled": false}),
        ),
        (
            Method::PUT,
            format!("/api/v1/flows/{}/pause", flow.id),
            Value::Null,
        ),
        (
            Method::DELETE,
            format!("/api/v1/flows/{}/pause", flow.id),
            Value::Null,
        ),
        (
            Method::PUT,
            "/api/v1/engines/fake-engine/pause".into(),
            json!({"reason": "upgrade"}),
        ),
        (
            Method::DELETE,
            "/api/v1/engines/fake-engine/pause".into(),
            Value::Null,
        ),
    ];
    refused_each_way(&app, &core, MANAGE_FLOWS, &calls).await;
    core.holds(&[VIEW_FLOWS]);
    for (method, uri, body) in &calls {
        let (status, _) = call(&app, method.clone(), uri, true, body.clone()).await;
        assert_eq!(
            status,
            StatusCode::FORBIDDEN,
            "{uri}: seeing is not managing"
        );
    }
    assert_eq!(h.store.flow(&flow.id).await.unwrap(), Some(flow));
    assert!(h.store.paused_engines().await.unwrap().is_empty());
}

#[tokio::test]
async fn a_person_edits_a_flow() {
    let h = harness().await;
    h.register().await;
    let flow = h.flow("daily").await;
    let other = h.flow("weekly").await;
    let (app, core) = people_app(&h);
    core.holds(&[MANAGE_FLOWS]);
    let uri = format!("/api/v1/flows/{}", flow.id);
    let patch = |body: Value| {
        let (app, uri) = (app.clone(), uri.clone());
        async move { call(&app, Method::PATCH, &uri, true, body).await }
    };

    let edit = json!({"name": "nightly", "description": "the close", "parameters": {"region": "north"},
                      "timeout_secs": 60, "allow_overlap": true, "on_interrupt": null, "enabled": false});
    let (status, edited) = patch(edit).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        (edited["name"].as_str(), edited["enabled"].as_bool()),
        (Some("nightly"), Some(false))
    );
    let kept = h.store.flow(&flow.id).await.unwrap().unwrap();
    assert_eq!(
        (kept.timeout_secs, kept.on_interrupt, kept.kind.as_str()),
        (60, None, "job")
    );
    let (_, edited) = patch(json!({"on_interrupt": "continue"})).await;
    assert_eq!(
        (edited["on_interrupt"].as_str(), edited["name"].as_str()),
        (Some("continue"), Some("nightly"))
    );

    let (status, _) = patch(json!({"folder_id": other.folder_id, "name": "weekly"})).await;
    assert_eq!(
        status,
        StatusCode::CONFLICT,
        "the name is taken in that folder"
    );
    let (status, _) = patch(json!({"folder_id": NO_ID})).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    for bad in [
        json!({"name": ""}),
        json!({"parameters": [1]}),
        json!({"timeout_secs": 0}),
        json!({"on_interrupt": "later"}),
    ] {
        assert_eq!(
            patch(bad.clone()).await.0,
            StatusCode::UNPROCESSABLE_ENTITY,
            "{bad}"
        );
    }
    assert_eq!(
        patch(json!({"engine": "other"})).await.0,
        StatusCode::BAD_REQUEST,
        "not editable"
    );
    let (status, _) = call(
        &app,
        Method::PATCH,
        &format!("/api/v1/flows/{NO_ID}"),
        true,
        json!({}),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(
        h.store.flow(&flow.id).await.unwrap().unwrap().name,
        "nightly"
    );
}

#[tokio::test]
async fn a_person_pauses_and_resumes_flows_and_engines() {
    let h = harness().await;
    h.register().await;
    let flow = h.flow("daily").await;
    let (app, core) = people_app(&h);
    core.holds(&[MANAGE_FLOWS]);
    let pause = format!("/api/v1/flows/{}/pause", flow.id);
    let (status, _) = call(&app, Method::PUT, &pause, true, Value::Null).await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    assert!(h
        .store
        .flow(&flow.id)
        .await
        .unwrap()
        .unwrap()
        .paused_at
        .is_some());
    let (status, _) = call(&app, Method::DELETE, &pause, true, Value::Null).await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    assert!(h
        .store
        .flow(&flow.id)
        .await
        .unwrap()
        .unwrap()
        .paused_at
        .is_none());
    for uri in [
        format!("/api/v1/flows/{NO_ID}/pause"),
        "/api/v1/flows/x/pause".into(),
    ] {
        assert_eq!(
            call(&app, Method::PUT, &uri, true, Value::Null).await.0,
            StatusCode::NOT_FOUND
        );
    }

    let engine = "/api/v1/engines/fake-engine/pause";
    let (status, _) = call(
        &app,
        Method::PUT,
        engine,
        true,
        json!({"reason": "upgrade"}),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    let paused = h.store.paused_engines().await.unwrap();
    assert_eq!(
        paused.iter().map(|e| e.as_str()).collect::<Vec<_>>(),
        ["fake-engine"]
    );
    let (status, _) = call(&app, Method::DELETE, engine, true, Value::Null).await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    assert!(h.store.paused_engines().await.unwrap().is_empty());
    let (status, _) = call(
        &app,
        Method::PUT,
        "/api/v1/engines/Bad_Name/pause",
        true,
        json!({}),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}
