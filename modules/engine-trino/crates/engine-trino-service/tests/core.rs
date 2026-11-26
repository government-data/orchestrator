//! The core's routes as the adapter reads them (the shapes in the crate's
//! documentation), against a stand-in core over HTTP.

use axum::routing::{get, post};
use axum::{Json, Router};
use engine_trino_service::{HttpCore, MachineClient};
use serde_json::{json, Value};
use trino_runner::{Core, JobRequest, TrinoUser};

const JOB: &str = "00000000-0000-4000-8000-00000000000a";

async fn core() -> HttpCore {
    let app = Router::new()
        .route(
            "/api/internal/quality/v1/runs/{run}/bundle",
            post(|Json(b): Json<Value>| async move {
                assert_eq!(
                    b,
                    json!({"kind": "tables", "dataset": null, "cut": null, "attempt": 2,
                        "source": "crm"})
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
            "/api/internal/quality/v1/runs/{run}/trino-token",
            post(|Json(b): Json<Value>| async move {
                Json(
                    json!({"access_token": format!("t-{}", b["user"].as_str().unwrap()),
                    "expires_in": 900}),
                )
            }),
        )
        .route(
            "/api/internal/quality/v1/runs/{run}/source-secrets",
            post(|| async {
                Json(json!({"current": {"password": "c", "user": "u"},
                    "previous": {"password": "p"}}))
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
        kind: "tables".into(),
        dataset: None,
        cut: None,
        source: Some("crm".into()),
        attempt: 2,
    };
    let b = c.bundle("run-1", &job).await.unwrap();
    assert_eq!((b.jws.as_str(), b.digest.as_str()), ("h.p.s", "ab"));
    assert!(c.keys().await.unwrap().has("k1"));
    assert_eq!(c.token(JOB, TrinoUser::Report).await.unwrap(), "t-report");
    assert_eq!(c.token(JOB, TrinoUser::Maint).await.unwrap(), "t-maint");
    let creds = c.credentials(JOB).await.unwrap();
    let pairs: Vec<_> = creds
        .iter()
        .map(|c| (c.name.as_str(), c.current.as_str(), c.previous.as_deref()))
        .collect();
    assert_eq!(pairs, [("password", "c", Some("p")), ("user", "u", None)]);
    assert_eq!(c.grants().await.unwrap()[0].subject, "job_sample_loans");
    assert!(c.bundle("../x", &job).await.is_err(), "not a run id");
}
