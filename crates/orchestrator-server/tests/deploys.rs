//! Deploys over the engine port: claimed by a registered runner on the
//! deploy's machine, finished from it once with its path, version, SHA-256
//! and whether the external schedules were disabled.

mod common;

use axum::http::{Method, StatusCode};
use chrono::Duration;
use common::harness;
use orchestration_model::{EngineName, MaintenanceWindow, OnSchedule, WindowTime};
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

#[tokio::test]
async fn a_deploy_is_not_handed_out_while_its_engine_is_held_or_paused() {
    let h = harness().await;
    h.register().await;
    let id = queue(&h, "machine-1").await;
    let runner = json!({"runner": "r1"});
    let (_, claimed) = h.post(CLAIM, runner.clone()).await;
    assert_eq!(claimed["deploy"], id.as_str());

    let until = h.clock.now() + Duration::minutes(30);
    let hold = json!({"hold": "h-1", "until": until});
    let (status, _) = h.post("/internal/engines/fake-engine/hold", hold).await;
    assert_eq!(status, StatusCode::OK);
    let (status, _) = h.post(CLAIM, runner.clone()).await;
    assert_eq!(
        status,
        StatusCode::NO_CONTENT,
        "held: not even the deploy it claimed"
    );
    h.post(
        "/internal/engines/fake-engine/release",
        json!({"hold": "h-1"}),
    )
    .await;
    let (_, again) = h.post(CLAIM, runner.clone()).await;
    assert_eq!(again["deploy"], id.as_str(), "released: taken up again");

    let engine = EngineName::new("fake-engine").unwrap();
    h.store
        .pause_engine(&engine, "upgrade", None, h.clock.now())
        .await
        .unwrap();
    let (status, _) = h.post(CLAIM, runner.clone()).await;
    assert_eq!(status, StatusCode::NO_CONTENT, "paused");
    h.store.resume_engine(&engine).await.unwrap();
    let (status, _) = h.post(CLAIM, runner).await;
    assert_eq!(status, StatusCode::OK, "resumed");
}

#[tokio::test]
async fn a_deploy_is_not_handed_out_in_a_maintenance_window_of_its_cluster() {
    let h = harness().await;
    h.register().await;
    let id = queue(&h, "machine-1").await;
    let now = h.clock.now();
    let window = MaintenanceWindow {
        id: String::new(),
        name: "upgrade".into(),
        time: WindowTime::Once {
            starts_at: now - Duration::minutes(5),
            ends_at: now + Duration::minutes(5),
        },
        on_schedule: OnSchedule::Skip,
        enabled: true,
        clusters: vec!["machine-1".into()],
    };
    h.store.save_window(&window).await.unwrap();
    let runner = json!({"runner": "r1"});
    let (status, _) = h.post(CLAIM, runner.clone()).await;
    assert_eq!(status, StatusCode::NO_CONTENT, "in its window");
    h.clock.advance(Duration::minutes(10));
    let (_, claimed) = h.post(CLAIM, runner).await;
    assert_eq!(claimed["deploy"], id.as_str(), "after it");
}
