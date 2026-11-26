//! A scan on request: asked of one runner, answered `scan` at each of its
//! renewals until its next found report, never to another runner's run;
//! refused for a runner that never reported what it found.

mod common;

use axum::http::{Method, StatusCode};
use chrono::Duration;
use common::{engine, harness, Harness};
use orchestrator_server::{EnginePort, Machine, ScanAsked};
use serde_json::{json, Value};

const RUNNER: &str = "machine-1/fake-engine/-";

fn port(h: &Harness) -> EnginePort<orchestrator_server::FixedClock> {
    EnginePort::new(h.store.clone(), h.clock.clone())
}

async fn ask(h: &Harness, machine: &str) -> ScanAsked {
    port(h)
        .request_scan(&Machine(machine.into()), &engine())
        .await
        .unwrap()
}

async fn report_found(h: &Harness, machine: &str) {
    let (status, _) = h
        .call(
            Method::PUT,
            "/internal/engines/fake-engine/-/found",
            Some(machine),
            json!({"artefacts": []}),
        )
        .await;
    assert_eq!(status, StatusCode::OK);
}

async fn register(h: &Harness, machine: &str) {
    let body = serde_json::to_value(common::descriptor()).unwrap();
    let (status, _) = h
        .call(
            Method::PUT,
            "/internal/engines/fake-engine/-",
            Some(machine),
            body,
        )
        .await;
    assert_eq!(status, StatusCode::OK);
}

/// Claims the next run from `machine`: its id and lease.
async fn claim(h: &Harness, machine: &str) -> (String, Value) {
    let (status, claimed) = h
        .call(
            Method::POST,
            "/internal/engines/fake-engine/-/claim",
            Some(machine),
            json!({"runner": "r1"}),
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    (
        claimed["run"].as_str().unwrap().into(),
        claimed["lease"].clone(),
    )
}

async fn renew(h: &Harness, machine: &str, run: &str, lease: &Value) -> Value {
    let body = json!({"lease": lease, "progress": null, "engine_ref": null});
    let (status, renewed) = h
        .call(
            Method::POST,
            &format!("/internal/runs/{run}/renew"),
            Some(machine),
            body,
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    renewed
}

#[tokio::test]
async fn a_scan_is_refused_for_a_runner_that_never_reported() {
    let h = harness().await;
    assert_eq!(ask(&h, "machine-1").await, ScanAsked::UnknownRunner);
    register(&h, "machine-1").await;
    assert_eq!(ask(&h, "machine-1").await, ScanAsked::NeverReported);
    assert_eq!(h.store.scan_requested(RUNNER).await.unwrap(), None);
}

#[tokio::test]
async fn a_scan_is_told_at_each_renewal_until_the_next_found_report() {
    let h = harness().await;
    for m in ["machine-1", "machine-2"] {
        register(&h, m).await;
        report_found(&h, m).await;
    }
    let first = h.flow("first").await;
    let second = h.flow("second").await;
    h.queue(&first).await;
    h.queue(&second).await;
    let (run, lease) = claim(&h, "machine-1").await;
    let (other, other_lease) = claim(&h, "machine-2").await;
    assert_eq!(
        renew(&h, "machine-1", &run, &lease).await.get("scan"),
        None,
        "no scan asked: the answer has no field"
    );

    assert_eq!(ask(&h, "machine-1").await, ScanAsked::Asked);
    let asked_at = h.store.scan_requested(RUNNER).await.unwrap();
    h.clock.advance(Duration::minutes(1));
    assert_eq!(ask(&h, "machine-1").await, ScanAsked::Asked);
    assert_eq!(
        h.store.scan_requested(RUNNER).await.unwrap(),
        asked_at,
        "asking again changes nothing"
    );

    let renewed = renew(&h, "machine-1", &run, &lease).await;
    assert_eq!(
        (renewed["action"].as_str(), renewed["scan"].as_bool()),
        (Some("continue"), Some(true))
    );
    assert_eq!(
        renew(&h, "machine-1", &run, &lease).await["scan"],
        true,
        "told again until it reports: a lost answer is not a lost scan"
    );
    assert_eq!(
        renew(&h, "machine-2", &other, &other_lease)
            .await
            .get("scan"),
        None,
        "another machine's runner is not asked"
    );

    report_found(&h, "machine-1").await;
    assert_eq!(h.store.scan_requested(RUNNER).await.unwrap(), None);
    assert_eq!(
        renew(&h, "machine-1", &run, &lease).await.get("scan"),
        None,
        "its report answered the scan"
    );
}
