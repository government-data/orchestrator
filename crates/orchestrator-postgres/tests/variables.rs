//! Flow variables and secrets in the store.

mod common;

use common::{at, new_flow};
use orchestrator_postgres::{StoreError, Variable, VariableValue};
use serde_json::json;

#[tokio::test]
async fn variables_and_secrets_stay_sealed() {
    let db = common::database().await;
    let store = db.store();
    let now = at("2026-11-25T10:00:00Z");
    let folder = store.create_folder("f").await.unwrap();
    let flow = store.create_flow(&new_flow(&folder.id, "x")).await.unwrap();
    let secret = store
        .create_secret("warehouse", "the warehouse", b"sealed-1", now)
        .await
        .unwrap();
    assert!(matches!(
        store.create_secret("warehouse", "", b"sealed-2", now).await,
        Err(StoreError::Conflict(_))
    ));
    let listed = store.secrets().await.unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].name, "warehouse");
    assert_eq!(
        store.sealed_secret(&secret).await.unwrap().unwrap(),
        b"sealed-1"
    );

    let vars = [
        Variable {
            name: "region".into(),
            value: VariableValue::Plain(json!("north")),
        },
        Variable {
            name: "token".into(),
            value: VariableValue::Sealed(b"sealed-3".to_vec()),
        },
        Variable {
            name: "warehouse".into(),
            value: VariableValue::Secret(secret.clone()),
        },
    ];
    for v in &vars {
        store.set_variable(&flow.id, v, now).await.unwrap();
    }
    assert_eq!(store.variables(&flow.id).await.unwrap(), vars);
    let changed = Variable {
        name: "region".into(),
        value: VariableValue::Plain(json!("south")),
    };
    store.set_variable(&flow.id, &changed, now).await.unwrap();
    assert_eq!(store.variables(&flow.id).await.unwrap()[0], changed);
    let bad = Variable {
        name: "1bad".into(),
        value: VariableValue::Plain(json!(1)),
    };
    assert!(matches!(
        store.set_variable(&flow.id, &bad, now).await,
        Err(StoreError::Invalid(_))
    ));
    assert!(store.delete_variable(&flow.id, "region").await.unwrap());
    assert!(!store.delete_variable(&flow.id, "region").await.unwrap());
}
