//! A runner's inventory over the engine port: a new cluster adopts it once
//! against the machines the core lists as live (a fake core); a second
//! report changes nothing; skipped items carry their reason; refused before
//! the runner registers, and unavailable while the core does not answer.

mod common;

use std::sync::{Arc, Mutex};

use axum::http::{Method, StatusCode};
use common::harness;
use orchestration_model::Trigger;
use orchestrator_postgres::NewRun;
use orchestrator_server::Clock;
use serde_json::{json, Value};

const PATH: &str = "/internal/engines/fake-engine/-/inventory";
const UNKNOWN: &str = "00000000-0000-4000-8000-000000000001";

/// A fake core: the live machines it lists, or no answer.
struct FakeCore(Mutex<Option<Vec<String>>>);

#[async_trait::async_trait]
impl orchestrator_server::MachineList for FakeCore {
    async fn live(&self) -> Option<Vec<String>> {
        self.0.lock().unwrap().clone()
    }
}

fn item(path: &str, flow: &str, name: &str, deployed_at: &str) -> Value {
    json!({"path": path, "record": {
        "format": 1, "flow": flow, "deploy": "00000000-0000-4000-8000-000000000099",
        "version": "v2", "sha256": "ab".repeat(32),
        "folder": {"id": "00000000-0000-4000-8000-000000000050", "name": "Sales"},
        "name": name, "kind": "job", "timeout_secs": 600, "deployed_at": deployed_at
    }})
}

#[tokio::test]
async fn a_first_inventory_reattaches_and_registers_and_a_second_changes_nothing() {
    let mut h = harness().await;
    let core = Arc::new(FakeCore(Mutex::new(None)));
    h.with_machines(core.clone());
    let flow = h.flow("close").await;
    sqlx::query("UPDATE flows SET cluster = 'machine-gone' WHERE id = $1::uuid")
        .bind(&flow.id)
        .execute(h.store.pool())
        .await
        .unwrap();
    let run = NewRun {
        flow_id: flow.id.clone(),
        engine: flow.engine.clone(),
        kind: flow.kind.clone(),
        cluster: Some("machine-gone".into()),
        trigger: Trigger::Manual("person-1".into()),
        fire_at: None,
        parameters: json!({}),
        timeout_secs: 600,
        lock_name: None,
        slot_pool: None,
        queued_at: h.clock.now(),
    };
    h.store.enqueue(&run).await.unwrap().unwrap();
    let report = json!({"items": [
        item("/srv/flows/close", &flow.id, "close", "2026-11-20T10:00:00Z"),
        item("/srv/flows/pay", UNKNOWN, "pay", "2026-11-20T10:00:00Z"),
        item("/srv/flows/pay-old", UNKNOWN, "pay", "2026-11-19T10:00:00Z"),
        item("relative", UNKNOWN, "pay", "2026-11-21T10:00:00Z"),
    ]});
    let (status, _) = h
        .call(Method::PUT, PATH, Some("machine-1"), report.clone())
        .await;
    assert_eq!(status, StatusCode::NOT_FOUND, "a runner registers first");

    h.register().await;
    let (status, _) = h
        .call(Method::PUT, PATH, Some("machine-1"), report.clone())
        .await;
    assert_eq!(
        status,
        StatusCode::SERVICE_UNAVAILABLE,
        "the core did not answer"
    );
    assert!(
        h.store.flow(UNKNOWN).await.unwrap().is_none(),
        "nothing adopted"
    );

    *core.0.lock().unwrap() = Some(vec!["machine-1".into()]);
    let (status, answer) = h
        .call(Method::PUT, PATH, Some("machine-1"), report.clone())
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        answer,
        json!({
            "already_adopted": false, "reattached": [flow.id], "registered": [UNKNOWN],
            "folders_created": 1,
            "skipped": [
                {"path": "/srv/flows/pay-old", "reason": "older_version"},
                {"path": "relative", "reason": "invalid"},
            ]
        })
    );
    let moved = h.store.flow(&flow.id).await.unwrap().unwrap();
    assert_eq!(moved.cluster.as_deref(), Some("machine-1"));
    let (status, claimed) = h.claim().await;
    assert_eq!(status, StatusCode::OK, "its queued run is claimed here");
    assert_eq!(claimed["flow"], flow.id.as_str());

    let (_, again) = h.call(Method::PUT, PATH, Some("machine-1"), report).await;
    assert_eq!(again["already_adopted"], true);
    assert_eq!(again["registered"], json!([]));
    let records: i64 = sqlx::query_scalar("SELECT count(*) FROM flows WHERE id = ANY($1::uuid[])")
        .bind(vec![flow.id.clone(), UNKNOWN.to_string()])
        .fetch_one(h.store.pool())
        .await
        .unwrap();
    assert_eq!(records, 2, "one record for each flow id");
}

#[tokio::test]
async fn a_flow_on_a_live_cluster_is_skipped() {
    let mut h = harness().await;
    h.with_machines(Arc::new(FakeCore(Mutex::new(Some(vec![
        "machine-1".into(),
        "machine-2".into(),
    ])))));
    let flow = h.flow("close").await;
    sqlx::query("UPDATE flows SET cluster = 'machine-2' WHERE id = $1::uuid")
        .bind(&flow.id)
        .execute(h.store.pool())
        .await
        .unwrap();
    h.register().await;
    let report = json!({"items": [item("/srv/close", &flow.id, "close", "2026-11-20T10:00:00Z")]});
    let (status, answer) = h.call(Method::PUT, PATH, Some("machine-1"), report).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        answer["skipped"],
        json!([{"path": "/srv/close", "reason": "on_live_cluster"}])
    );
    let kept = h.store.flow(&flow.id).await.unwrap().unwrap();
    assert_eq!(kept.cluster.as_deref(), Some("machine-2"));
}
