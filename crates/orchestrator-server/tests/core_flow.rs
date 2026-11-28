//! The core's flow (ruling 97): the core makes it once and reads it back,
//! with its machine and its own token; its chains then start on it, and
//! its runs may overlap.

mod common;

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Method, Request, StatusCode};
use common::core_token::CoreKey;
use common::harness;
use orchestration_model::{EngineLine, EngineName, Line};
use orchestrator_postgres::NewFlow;
use orchestrator_server::{chains_router, EnginePort, PortState, CLIENT_CERT_INFO};
use serde_json::{json, Value};
use tower::ServiceExt;

const CORE: &str = "Subject%3D%22O%3Dqd%2COU%3Dapp%2CCN%3Dmaster%22%3BSerialNumber%3D%221%22";

struct Core {
    app: axum::Router,
    token: String,
}

fn core(h: &common::Harness) -> Core {
    let key = CoreKey::new();
    let token = key.core_token(&h.clock);
    let app = chains_router(Arc::new(PortState {
        port: EnginePort::new(h.store.clone(), h.clock.clone()),
        auth: Arc::new(key.caller(h.clock.clone())),
    }));
    Core { app, token }
}

async fn call(
    c: &Core,
    method: Method,
    path: &str,
    token: bool,
    body: Option<Value>,
) -> (StatusCode, Value) {
    let mut req = Request::builder()
        .method(method)
        .uri(path)
        .header(CLIENT_CERT_INFO, CORE)
        .header("content-type", "application/json");
    if token {
        req = req.header("authorization", format!("Bearer {}", c.token));
    }
    let body = body.map_or_else(Body::empty, |b| Body::from(b.to_string()));
    let res = c
        .app
        .clone()
        .oneshot(req.body(body).unwrap())
        .await
        .unwrap();
    let status = res.status();
    let bytes = axum::body::to_bytes(res.into_body(), 1 << 20)
        .await
        .unwrap();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    )
}

fn asked(engine: &str) -> Option<Value> {
    Some(json!({"engine": engine, "line": "-", "timeout_secs": 3600}))
}

#[tokio::test]
async fn the_core_makes_its_flow_once_and_its_chains_start_on_it() {
    let h = harness().await;
    let c = core(&h);
    let (status, _) = call(&c, Method::GET, "/internal/core-flow", true, None).await;
    assert_eq!(status, StatusCode::NOT_FOUND, "none yet");
    let job = json!({"job_id": "j-1", "steps": [{"kind": "tables"}]});
    let (status, _) = call(
        &c,
        Method::POST,
        "/internal/chains",
        true,
        Some(job.clone()),
    )
    .await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "no flow, no chain");

    let path = "/internal/core-flow";
    let (status, made) = call(&c, Method::POST, path, true, asked("fake-engine")).await;
    assert_eq!(status, StatusCode::CREATED, "{made}");
    assert_eq!(
        (&made["engine"], &made["line"], &made["timeout_secs"]),
        (&json!("fake-engine"), &json!("-"), &json!(3600))
    );
    let id = made["flow_id"].as_str().unwrap();
    let (status, again) = call(&c, Method::POST, path, true, asked("fake-engine")).await;
    assert_eq!((status, &again), (StatusCode::OK, &made), "the same flow");
    let (status, other) = call(&c, Method::POST, path, true, asked("other-engine")).await;
    assert_eq!(status, StatusCode::CONFLICT, "another engine");
    assert_eq!(other["flow_id"], id);
    let (status, read) = call(&c, Method::GET, path, true, None).await;
    assert_eq!((status, &read), (StatusCode::OK, &made));

    let flow = h.store.flow(id).await.unwrap().unwrap();
    assert!(flow.allow_overlap, "the engine's pools bound its runs");
    assert_eq!(h.store.core_flow().await.unwrap().as_deref(), Some(id));
    let (status, started) = call(&c, Method::POST, "/internal/chains", true, Some(job)).await;
    assert_eq!(status, StatusCode::CREATED, "{started}");
}

#[tokio::test]
async fn only_the_core_with_its_token_and_a_valid_request_makes_it() {
    let h = harness().await;
    let c = core(&h);
    let path = "/internal/core-flow";
    let (status, _) = call(&c, Method::POST, path, false, asked("fake-engine")).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED, "the certificate alone");
    let (status, _) = call(&c, Method::GET, path, false, None).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    for bad in [
        json!({"engine": "fake-engine", "line": "-", "timeout_secs": 0}),
        json!({"engine": "Fake Engine", "line": "-", "timeout_secs": 60}),
        json!({"engine": "fake-engine", "line": "-", "timeout_secs": 60, "by": "x"}),
    ] {
        let (status, _) = call(&c, Method::POST, path, true, Some(bad.clone())).await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{bad}");
    }
    assert_eq!(h.store.core_flow().await.unwrap(), None);
}

#[tokio::test]
async fn a_persons_flow_of_that_name_is_not_taken() {
    let h = harness().await;
    let folder = h.store.create_folder("core").await.unwrap();
    let mut new = NewFlow {
        folder_id: folder.id,
        name: "chains".into(),
        description: String::new(),
        engine: EngineLine {
            engine: EngineName::new("fake-engine").unwrap(),
            line: Line::new("-").unwrap(),
        },
        kind: "job".into(),
        cluster: None,
        parameters: json!({}),
        timeout_secs: 60,
        allow_overlap: false,
        on_interrupt: None,
    };
    new.description = "a person's".into();
    h.store.create_flow(&new).await.unwrap();
    let c = core(&h);
    let (status, _) = call(
        &c,
        Method::POST,
        "/internal/core-flow",
        true,
        asked("fake-engine"),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(h.store.core_flow().await.unwrap(), None);
}
