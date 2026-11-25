//! Engines and runners registered, engine pauses and drain holds.

mod common;

use chrono::Duration;
use common::{at, descriptor};
use orchestration_model::EngineName;

#[tokio::test]
async fn registering_twice_keeps_one_engine_and_one_runner() {
    let db = common::database().await;
    let store = db.store();
    let now = at("2026-11-25T10:00:00Z");
    let mut d = descriptor("an-engine", "-");
    store.register(&d, "machine-1/r1", now).await.unwrap();
    d.engine_version = "1.2.4".into();
    store
        .register(&d, "machine-1/r1", now + Duration::seconds(5))
        .await
        .unwrap();
    store
        .register(&descriptor("other", "3"), "machine-2/r1", now)
        .await
        .unwrap();
    let engines = store.engines().await.unwrap();
    assert_eq!(engines.len(), 2);
    assert_eq!(engines[0].engine.engine.as_str(), "an-engine");
    assert_eq!(engines[0].engine_version, "1.2.4");
    assert_eq!(engines[0].kinds, d.kinds);
    assert_eq!(engines[1].engine.line.as_str(), "3");

    let live = store.live_runners(now).await.unwrap();
    assert_eq!(live.iter().map(|l| l.live).collect::<Vec<_>>(), [1, 1]);
    let later = now + Duration::minutes(5);
    store.seen("machine-1/r1", None, later).await.unwrap();
    let live = store.live_runners(later).await.unwrap();
    assert_eq!(live.iter().map(|l| l.live).collect::<Vec<_>>(), [1, 0]);
}

#[tokio::test]
async fn a_hold_is_extended_and_released_by_its_id() {
    let db = common::database().await;
    let store = db.store();
    let now = at("2026-11-25T10:00:00Z");
    let engine = EngineName::new("an-engine").unwrap();
    assert!(!store.held(&engine, now).await.unwrap());
    let until = now + Duration::minutes(15);
    assert_eq!(store.hold(&engine, "h-1", until, now).await.unwrap(), 0);
    assert!(store.held(&engine, now).await.unwrap());
    assert!(
        !store.held(&engine, until).await.unwrap(),
        "it ends by itself"
    );
    let longer = until + Duration::minutes(15);
    store.hold(&engine, "h-1", longer, now).await.unwrap();
    assert!(store.held(&engine, until).await.unwrap());
    assert!(
        !store.release(&engine, "h-2").await.unwrap(),
        "not that hold"
    );
    assert!(store.release(&engine, "h-1").await.unwrap());
    assert!(!store.held(&engine, now).await.unwrap());

    store
        .pause_engine(&engine, "upgrade", Some("person-1"), now)
        .await
        .unwrap();
    store
        .pause_engine(&engine, "upgrade again", None, now)
        .await
        .unwrap();
    assert_eq!(
        store.paused_engines().await.unwrap(),
        std::slice::from_ref(&engine)
    );
    store.resume_engine(&engine).await.unwrap();
    assert!(store.paused_engines().await.unwrap().is_empty());
}
