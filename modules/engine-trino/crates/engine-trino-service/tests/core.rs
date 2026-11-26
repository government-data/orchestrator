//! The core's routes as the adapter reads them (the shapes in the crate's
//! documentation), against a stand-in core over HTTP.

use axum::routing::{get, post};
use axum::{Json, Router};
use engine_trino_service::{HttpCore, MachineClient};
use serde_json::{json, Value};
use trino_runner::{Core, JobRequest, RunAs};

const JOB: &str = "00000000-0000-4000-8000-00000000000a";

async fn core() -> HttpCore {
    let app = Router::new()
        .route(
            "/api/internal/quality/v1/runs/{run}/bundle",
            post(|Json(b): Json<Value>| async move {
                assert_eq!(
                    b,
                    json!({"kind": "evaluate", "dataset": "loans", "cut": null, "attempt": 2})
                );
                Json(json!({"job_id": "run-1", "digest": "ab", "bundle": "h.p.s"}))
            }),
        )
        .route(
            "/oauth/token",
            post(|body: String| async move {
                let aud = if body.contains("audience=quality-grants") {
                    "g"
                } else {
                    "m"
                };
                Json(json!({"access_token": aud, "expires_in": 300}))
            }),
        )
        .route(
            "/oauth/jwks",
            get(|| async {
                Json(json!({"keys": [{"kty": "EC", "crv": "P-256", "kid": "k1",
                "x": "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
                "y": "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"}]}))
            }),
        )
        .route(
            "/api/internal/jobs/{job}/token",
            post(|Json(b): Json<Value>| async move {
                Json(json!({"token": format!("t-{}", b["as"].as_str().unwrap())}))
            }),
        )
        .route(
            "/api/internal/jobs/{job}/credentials",
            get(|| async {
                Json(
                    json!({"credentials": [{"name": "src_crm_password", "current": "c",
                    "previous": null}]}),
                )
            }),
        )
        .route(
            "/api/internal/quality/v1/job-grants",
            get(|| async {
                Json(
                    json!({"grants": [{"subject": "job_sample_loans", "namespace": "loans_silver",
                    "tables": ["*"], "privileges": ["SELECT"]}]}),
                )
            }),
        );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await });
    let client = MachineClient {
        id: "engine-trino-machine".into(),
        secret: "s".into(),
    };
    HttpCore::new(
        reqwest::Client::new(),
        &format!("http://{addr}/api/"),
        &format!("http://{addr}"),
        client,
    )
}

#[tokio::test]
async fn the_adapter_reads_each_route() {
    let c = core().await;
    let job = JobRequest {
        kind: "evaluate".into(),
        dataset: Some("loans".into()),
        cut: None,
        attempt: 2,
    };
    let b = c.bundle("run-1", &job).await.unwrap();
    assert_eq!((b.jws.as_str(), b.digest.as_str()), ("h.p.s", "ab"));
    assert!(c.keys().await.unwrap().has("k1"));
    assert_eq!(c.token(JOB, RunAs::Report).await.unwrap(), "t-report");
    assert_eq!(
        c.credentials(JOB).await.unwrap()[0].name,
        "src_crm_password"
    );
    assert_eq!(c.grants().await.unwrap()[0].subject, "job_sample_loans");
    assert!(c.bundle("../x", &job).await.is_err(), "not a run id");
}
