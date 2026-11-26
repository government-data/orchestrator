//! A claimed run carries its flow's variables, sealed ones and secrets
//! opened; a variable that does not open fails the run before any runner
//! sees it.

mod common;

use std::sync::Arc;

use axum::http::StatusCode;
use common::harness;
use orchestrator_postgres::{Variable, VariableValue};
use orchestrator_server::{Clock, Sealer};
use serde_json::json;

const KEY: &str = "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8";

#[tokio::test]
async fn a_claim_carries_the_flow_s_variables_opened() {
    let mut h = harness().await;
    let sealer = Arc::new(Sealer::from_file_content(KEY).unwrap());
    h.with_sealer(Arc::clone(&sealer));
    h.register().await;
    let flow = h.flow("daily").await;
    let now = h.clock.now();
    let secret = h
        .store
        .create_secret("warehouse", "", &sealer.seal(b"\"dsn-1\"").unwrap(), now)
        .await
        .unwrap();
    for v in [
        Variable {
            name: "region".into(),
            value: VariableValue::Plain(json!("north")),
        },
        Variable {
            name: "token".into(),
            value: VariableValue::Sealed(sealer.seal(b"\"t-1\"").unwrap()),
        },
        Variable {
            name: "warehouse".into(),
            value: VariableValue::Secret(secret),
        },
    ] {
        h.store.set_variable(&flow.id, &v, now).await.unwrap();
    }
    h.queue(&flow).await;
    let (status, claimed) = h.claim().await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        claimed["variables"],
        json!({"region": "north", "token": "t-1", "warehouse": "dsn-1"})
    );
}

#[tokio::test]
async fn a_variable_that_does_not_open_fails_the_run() {
    let h = harness().await;
    h.register().await;
    let flow = h.flow("daily").await;
    let other = Sealer::from_file_content("Hx4dHBsaGRgXFhUUExIREA8ODQwLCgkIBwYFBAMCAQA").unwrap();
    let v = Variable {
        name: "token".into(),
        value: VariableValue::Sealed(other.seal(b"\"t-1\"").unwrap()),
    };
    h.store
        .set_variable(&flow.id, &v, h.clock.now())
        .await
        .unwrap();
    let run = h.queue(&flow).await;
    let (status, _) = h.claim().await;
    assert_eq!(
        status,
        StatusCode::NO_CONTENT,
        "no key here: nothing handed out"
    );
    let (state, class): (String, Option<String>) =
        sqlx::query_as("SELECT state, error_class FROM runs WHERE id = $1::uuid")
            .bind(&run)
            .fetch_one(h.store.pool())
            .await
            .unwrap();
    assert_eq!(
        (state.as_str(), class.as_deref()),
        ("failed", Some("secret_unavailable"))
    );
}
