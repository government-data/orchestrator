//! An engine's slot pools over the port: only its own kinds, checked.

mod common;

use axum::http::{Method, StatusCode};
use common::harness;
use serde_json::json;

#[tokio::test]
async fn an_engine_sends_its_pools() {
    let h = harness().await;
    let path = "/internal/engines/fake-engine/-/pools";
    let pools =
        json!({"pools": [{"name": "rules", "size": 1, "kinds": ["job"], "fair_by": "dataset"}]});
    let (status, _) = h
        .call(Method::PUT, path, Some("machine-1"), pools.clone())
        .await;
    assert_eq!(status, StatusCode::NOT_FOUND, "not registered yet");
    h.register().await;
    let (status, _) = h.call(Method::PUT, path, Some("machine-1"), pools).await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    let size: i32 = sqlx::query_scalar("SELECT size FROM slot_pools WHERE name = 'rules'")
        .fetch_one(h.store.pool())
        .await
        .unwrap();
    assert_eq!(size, 1);
    let other = json!({"pools": [{"name": "rules", "size": 1, "kinds": ["not_its"]}]});
    let (status, _) = h.call(Method::PUT, path, Some("machine-1"), other).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    let zero = json!({"pools": [{"name": "rules", "size": 0, "kinds": ["job"]}]});
    let (status, _) = h.call(Method::PUT, path, Some("machine-1"), zero).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
}
