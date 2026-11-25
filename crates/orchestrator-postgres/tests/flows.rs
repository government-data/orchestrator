//! Folders and flows in the store.

mod common;

use common::{at, new_flow};
use orchestration_model::OnInterrupt;
use orchestrator_postgres::StoreError;

#[tokio::test]
async fn folders_and_flows() {
    let db = common::database().await;
    let store = db.store();
    let folder = store.create_folder("finance").await.unwrap();
    assert!(matches!(
        store.create_folder("finance").await,
        Err(StoreError::Conflict(_))
    ));
    let created = store
        .create_flow(&new_flow(&folder.id, "daily load"))
        .await
        .unwrap();
    assert_eq!(created.engine.engine.as_str(), "an-engine");
    assert_eq!(created.on_interrupt, Some(OnInterrupt::Repeat));
    assert!(created.enabled && created.paused_at.is_none());
    assert!(matches!(
        store.create_flow(&new_flow(&folder.id, "daily load")).await,
        Err(StoreError::Conflict(_))
    ));
    let other = store.create_folder("risk").await.unwrap();
    store
        .create_flow(&new_flow(&other.id, "daily load"))
        .await
        .unwrap();
    assert_eq!(store.flows(None).await.unwrap().len(), 2);
    assert_eq!(
        store.flows(Some(&folder.id)).await.unwrap(),
        std::slice::from_ref(&created)
    );
    assert_eq!(store.folders().await.unwrap().len(), 2);

    let now = at("2026-11-25T10:00:00Z");
    assert!(store.pause_flow(&created.id, Some(now)).await.unwrap());
    assert_eq!(
        store.flow(&created.id).await.unwrap().unwrap().paused_at,
        Some(now)
    );
    assert!(store.pause_flow(&created.id, None).await.unwrap());
    let unknown = "00000000-0000-0000-0000-000000000000";
    assert!(!store.pause_flow(unknown, None).await.unwrap());
    assert!(store.flow(unknown).await.unwrap().is_none());
}
