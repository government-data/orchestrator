//! The rules service serves the merged document, and 503 while the core's
//! grants cannot be read.

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use engine_trino_service::rules::{router, Grants};
use serde_json::{json, Value};
use tower::ServiceExt;
use trino_rules::Grant;

#[derive(Clone)]
struct FakeCore(Arc<Mutex<Option<Vec<Grant>>>>);

#[async_trait]
impl Grants for FakeCore {
    async fn grants(&self) -> Result<Vec<Grant>, String> {
        self.0.lock().unwrap().clone().ok_or_else(|| "down".into())
    }
}

async fn get(app: axum::Router) -> (StatusCode, Value) {
    let res = app
        .oneshot(Request::get("/rules.json").body(Body::empty()).unwrap())
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

#[tokio::test]
async fn the_document_comes_from_the_cores_grants_and_503_without_them() {
    let grants: Vec<Grant> = serde_json::from_value(json!([{"subject": "job_rules_loans",
        "namespace": "quality_gold", "tables": ["*"], "privileges": ["SELECT", "INSERT", "DELETE"],
        "rows": {"column": "dataset", "in": ["loans"]}}]))
    .unwrap();
    let core = FakeCore(Arc::new(Mutex::new(Some(grants))));
    let (status, doc) = get(router(core.clone(), vec![])).await;
    assert_eq!(status, StatusCode::OK);
    assert!(doc["tables"]
        .as_array()
        .unwrap()
        .iter()
        .any(|r| r["user"] == "job_rules_loans" && r["filter"] == "dataset IN ('loans')"));
    *core.0.lock().unwrap() = None;
    let (status, _) = get(router(core, vec![])).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
}
