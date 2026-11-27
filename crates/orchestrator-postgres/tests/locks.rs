//! A run's locks: one running holder per name, free again when it gives
//! the lock back or ends, excluding run-level locks of the same name both
//! ways, and only under the run's lease.

mod common;

use chrono::{DateTime, Duration, Utc};
use common::{at, descriptor, new_flow};
use engine_port::Finish;
use orchestration_model::{EngineLine, EngineName, FinalState, Line, Trigger};
use orchestrator_postgres::{ClaimFor, NewRun, PgStore, StoreError};

async fn queue(store: &PgStore, folder: &str, name: &str, lock: Option<&str>) -> String {
    let flow = store.create_flow(&new_flow(folder, name)).await.unwrap();
    let run = NewRun {
        flow_id: flow.id.clone(),
        engine: flow.engine.clone(),
        kind: flow.kind.clone(),
        cluster: None,
        trigger: Trigger::Manual("person-1".into()),
        fire_at: None,
        parameters: flow.parameters.clone(),
        timeout_secs: flow.timeout_secs,
        lock_name: lock.map(str::to_string),
        slot_pool: None,
        queued_at: at("2026-11-25T10:00:00Z"),
    };
    store.enqueue(&run).await.unwrap().unwrap()
}

async fn claim(store: &PgStore, lease: &str, now: DateTime<Utc>) -> Option<String> {
    let e = EngineLine {
        engine: EngineName::new("an-engine").unwrap(),
        line: Line::new("-").unwrap(),
    };
    let c = ClaimFor {
        engine: &e,
        runner: "r1",
        cluster: "machine-1",
        lease,
        lease_until: now + Duration::minutes(2),
        now,
    };
    store.claim(&c).await.unwrap().map(|c| c.run)
}

#[tokio::test]
async fn one_running_run_holds_a_lock() {
    let db = common::database().await;
    let store = db.store();
    let now = at("2026-11-25T10:00:00Z");
    store
        .register(&descriptor("an-engine", "-"), "r1", now)
        .await
        .unwrap();
    let folder = store.create_folder("locks").await.unwrap().id;
    queue(&store, &folder, "a", None).await;
    queue(&store, &folder, "b", None).await;
    let a = claim(&store, "la", now).await.unwrap();
    let b = claim(&store, "lb", now).await.unwrap();

    let gold = "lake.quality_gold.rule_results";
    assert!(store.take_lock(&a, "la", gold, now).await.unwrap());
    assert!(
        store.take_lock(&a, "la", gold, now).await.unwrap(),
        "again: still its"
    );
    assert!(
        !store.take_lock(&b, "lb", gold, now).await.unwrap(),
        "another's"
    );
    assert!(store
        .take_lock(&b, "lb", "lake.quality_gold.cells", now)
        .await
        .unwrap());
    assert!(matches!(
        store.take_lock(&b, "not-its-lease", gold, now).await,
        Err(StoreError::Lease)
    ));

    // A run-level lock of the same name waits while a run holds it.
    let c = queue(&store, &folder, "c", Some(gold)).await;
    assert_eq!(claim(&store, "lc", now).await, None);
    store.release_lock(&b, "lb", gold).await.unwrap();
    assert_eq!(claim(&store, "lc", now).await, None, "not b's to give back");
    store.release_lock(&a, "la", gold).await.unwrap();
    assert_eq!(claim(&store, "lc", now).await.as_deref(), Some(c.as_str()));
    assert!(
        !store.take_lock(&b, "lb", gold, now).await.unwrap(),
        "c's run holds it"
    );

    // A run that ends frees its locks, given back or not.
    store.finish(&c, &done("lc"), now).await.unwrap();
    assert!(store.take_lock(&b, "lb", gold, now).await.unwrap());
    store.finish(&b, &done("lb"), now).await.unwrap();
    assert!(store.take_lock(&a, "la", gold, now).await.unwrap());
}

fn done(lease: &str) -> Finish {
    Finish {
        lease: lease.into(),
        state: FinalState::Succeeded,
        counts: Default::default(),
        error_class: None,
        interruption: None,
    }
}
