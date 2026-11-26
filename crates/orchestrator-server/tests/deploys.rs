//! Deploys over the engine port: claimed by a registered runner on the
//! deploy's machine, finished from it once with its path, version, SHA-256
//! and whether the external schedules were disabled.

mod common;

use axum::http::{Method, StatusCode};
use common::harness;
use orchestrator_postgres::NewDeploy;
use orchestrator_server::Clock;
use serde_json::{json, Value};

const CLAIM: &str = "/internal/engines/fake-engine/-/deploys/claim";

fn finish(external_disabled: bool) -> Value {
    json!({
        "state": "succeeded", "path": "/srv/managed/sales/close", "version": "v-1",
        "sha256": "cd".repeat(32), "external_disabled": external_disabled
    })
}

async fn queue(h: &common::Harness, cluster: &str) -> String {
    let flow = h.flow("Sales").await;
    let new = NewDeploy {
        flow_id: flow.id,
        cluster: cluster.into(),
        source_path: "/srv/flows/sales/close".into(),
        source_sha256: "ab".repeat(32),
        schedules: Vec::new(),
        queued_at: h.clock.now(),
    };
    h.store.create_deploy(&new).await.unwrap()
}

#[tokio::test]
async fn a_runner_claims_a_deploy_on_its_machine_and_finishes_it_once() {
    let h = harness().await;
    let id = queue(&h, "machine-1").await;
    let runner = json!({"runner": "r1"});
    let (status, _) = h.call(Method::POST, CLAIM, None, runner.clone()).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    let (status, _) = h.post(CLAIM, runner.clone()).await;
    assert_eq!(status, StatusCode::NOT_FOUND, "a runner registers first");

    h.register().await;
    let (status, _) = h
        .call(Method::POST, CLAIM, Some("machine-2"), runner.clone())
        .await;
    assert_eq!(status, StatusCode::NO_CONTENT, "not on that machine");
    let (status, claimed) = h.post(CLAIM, runner.clone()).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(claimed["deploy"], id.as_str());
    assert_eq!(claimed["source_path"], "/srv/flows/sales/close");
    assert_eq!(claimed["folder"]["name"], "Sales");

    let path = format!("/internal/deploys/{id}/finish");
    let (status, _) = h
        .call(Method::POST, &path, Some("machine-2"), finish(false))
        .await;
    assert_eq!(status, StatusCode::CONFLICT, "only from its machine");
    let mut bad = finish(false);
    bad["path"] = json!("relative");
    let (status, _) = h.post(&path, bad).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);

    let (status, _) = h.post(&path, finish(false)).await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    let (status, _) = h.post(&path, finish(false)).await;
    assert_eq!(status, StatusCode::NO_CONTENT, "the same finish again");
    let (status, _) = h.post(&path, finish(true)).await;
    assert_eq!(status, StatusCode::CONFLICT, "another finish after it");

    let d = h.store.deploy(&id).await.unwrap().unwrap();
    assert_eq!(d.state, "succeeded");
    assert_eq!(d.version.as_deref(), Some("v-1"));
    assert_eq!(
        d.external_disabled,
        Some(false),
        "its schedules will be created paused"
    );
    let (status, _) = h.post(CLAIM, runner).await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    let (status, _) = h
        .post("/internal/deploys/not-a-deploy/finish", finish(false))
        .await;
    assert_eq!(status, StatusCode::CONFLICT);
}
