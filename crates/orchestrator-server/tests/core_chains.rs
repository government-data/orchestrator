//! The core starts a chain on demand: only the core's machine with the
//! core's own token may (rulings 100 and 103), the core's job id makes it
//! once, its steps (a setup step's key and
//! fingerprint included) run on the core's flow; no flow set is `503`.

mod common;

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use common::core_token::CoreKey;
use common::harness;
use orchestrator_server::{chains_router, EnginePort, PortState, Scheduler, CLIENT_CERT_INFO};
use serde_json::{json, Value};
use tower::ServiceExt;

/// The core's machine, `OU=app`, as Traefik passes it.
const CORE: &str = "Subject%3D%22O%3Dqd%2COU%3Dapp%2CCN%3Dmaster%22%3BSerialNumber%3D%221%22";
/// A cluster's machine.
const WORKER: &str =
    "Subject%3D%22O%3Dqd%2COU%3Dworker%2CCN%3Dcluster-1%22%3BSerialNumber%3D%222%22";

/// The route served to the core, with the key the core signs with.
struct Core {
    app: axum::Router,
    key: CoreKey,
    clock: orchestrator_server::FixedClock,
}

async fn ask(core: &Core, cert: &str, body: &Value) -> (StatusCode, Value) {
    let token = core.key.core_token(&core.clock);
    send(&core.app, Some(cert), Some(&token), body).await
}

async fn send(
    app: &axum::Router,
    cert: Option<&str>,
    token: Option<&str>,
    body: &Value,
) -> (StatusCode, Value) {
    let mut req = Request::post("/internal/chains").header("content-type", "application/json");
    if let Some(cert) = cert {
        req = req.header(CLIENT_CERT_INFO, cert);
    }
    if let Some(token) = token {
        req = req.header("authorization", format!("Bearer {token}"));
    }
    let req = req.body(Body::from(body.to_string())).unwrap();
    let res = app.clone().oneshot(req).await.unwrap();
    let status = res.status();
    let bytes = axum::body::to_bytes(res.into_body(), 1 << 20)
        .await
        .unwrap();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    )
}

async fn app(h: &common::Harness, flow: Option<&str>) -> Core {
    if let Some(flow) = flow {
        h.store.set_core_flow(flow).await.unwrap();
    }
    let port = EnginePort::new(h.store.clone(), h.clock.clone());
    let key = CoreKey::new();
    let app = chains_router(Arc::new(PortState {
        port,
        auth: Arc::new(key.caller(h.clock.clone())),
    }));
    Core {
        app,
        key,
        clock: h.clock.clone(),
    }
}

fn tables_job() -> Value {
    json!({
        "job_id": "6f1c2b9e-8d4a-4e3b-9a7f-2c5d8e1b0a93",
        "steps": [
            {"kind": "prepare", "parameters": {"dataset": "sales"},
             "setup": {"key": "dataset:sales", "fingerprint": "9f86d081884c7d65"}},
            {"kind": "tables", "parameters": {"source": "erp"}}
        ]
    })
}

#[tokio::test]
async fn the_core_starts_one_chain_per_job_and_its_first_step_runs() {
    let h = harness().await;
    h.register().await;
    let flow = h.flow("quality").await;
    let app = app(&h, Some(&flow.id)).await;

    let (status, started) = ask(&app, CORE, &tables_job()).await;
    assert_eq!(status, StatusCode::CREATED, "{started}");
    let chain_id = started["chain_id"].as_str().unwrap().to_string();
    let (status, again) = ask(&app, CORE, &tables_job()).await;
    assert_eq!(status, StatusCode::OK, "the same job: its chain");
    assert_eq!(again["chain_id"], chain_id.as_str());

    let scheduler = Scheduler::new(h.store.clone(), h.clock.clone());
    assert_eq!(scheduler.tick().await.unwrap().chained, 1, "one chain");
    let (_, claimed) = h.claim().await;
    assert_eq!(claimed["kind"], "prepare");
    assert_eq!(claimed["parameters"]["dataset"], "sales");
    let state = h.store.chain_state(&chain_id).await.unwrap();
    assert_eq!(state, Some(("running".into(), 0)));
}

#[tokio::test]
async fn only_the_cores_machine_with_its_token_starts_a_chain() {
    let h = harness().await;
    h.register().await;
    let flow = h.flow("quality").await;
    let core = app(&h, Some(&flow.id)).await;
    let (status, _) = ask(&core, WORKER, &tables_job()).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED, "another machine");
    let (status, _) = ask(&core, "garbage", &tables_job()).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    let (status, _) = send(&core.app, Some(CORE), None, &tables_job()).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED, "the certificate alone");
    let token = core.key.core_token(&core.clock);
    let (status, _) = send(&core.app, None, Some(&token), &tables_job()).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED, "the token alone");
    let module = core.key.token_of("an-engine-machine", &core.clock);
    let (status, _) = send(&core.app, Some(CORE), Some(&module), &tables_job()).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED, "a module's token");
    let forged = CoreKey::new().core_token(&core.clock);
    let (status, _) = send(&core.app, Some(CORE), Some(&forged), &tables_job()).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED, "another key");
    let origin = "core-job:6f1c2b9e-8d4a-4e3b-9a7f-2c5d8e1b0a93";
    assert_eq!(h.store.chain_of_origin(origin).await.unwrap(), None);
}

#[tokio::test]
async fn malformed_requests_are_refused_and_start_nothing() {
    let h = harness().await;
    h.register().await;
    let flow = h.flow("quality").await;
    let app = app(&h, Some(&flow.id)).await;
    let mut bad_id = tables_job();
    bad_id["job_id"] = json!("a/b");
    let mut no_steps = tables_job();
    no_steps["steps"] = json!([]);
    let mut bad_setup = tables_job();
    bad_setup["steps"][0]["setup"]["key"] = json!("Dataset Sales");
    let mut extra = tables_job();
    extra["by"] = json!("person-1");
    for body in [bad_id, no_steps, bad_setup, extra] {
        let (status, _) = ask(&app, CORE, &body).await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
    }
    let scheduler = Scheduler::new(h.store.clone(), h.clock.clone());
    assert_eq!(scheduler.tick().await.unwrap().chained, 0);
}

#[tokio::test]
async fn with_no_flow_for_the_cores_chains_none_starts() {
    let h = harness().await;
    let (status, _) = ask(&app(&h, None).await, CORE, &tables_job()).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
}
