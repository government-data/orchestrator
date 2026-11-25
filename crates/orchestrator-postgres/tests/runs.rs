//! Runs: queued once per fire, claimed under a lease with SKIP LOCKED and
//! only when admitted, renewed, logged and finished by their lease alone.

mod common;

use std::collections::BTreeMap;

use chrono::{DateTime, Duration, Utc};
use common::{at, descriptor, new_flow};
use engine_port::{Finish, LogLine, LogStream, Progress};
use orchestration_model::{EngineLine, EngineName, FinalState, Flow, Line, Trigger};
use orchestrator_postgres::{ClaimFor, NewRun, PgStore, StoreError};
use serde_json::json;

fn engine() -> EngineLine {
    EngineLine {
        engine: EngineName::new("an-engine").unwrap(),
        line: Line::new("-").unwrap(),
    }
}

fn run_of(flow: &Flow, now: DateTime<Utc>) -> NewRun {
    NewRun {
        flow_id: flow.id.clone(),
        engine: flow.engine.clone(),
        kind: flow.kind.clone(),
        cluster: flow.cluster.clone(),
        trigger: Trigger::Manual("person-1".into()),
        fire_at: None,
        parameters: flow.parameters.clone(),
        timeout_secs: flow.timeout_secs,
        lock_name: None,
        slot_pool: None,
        queued_at: now,
    }
}

async fn claim(store: &PgStore, runner: &str, lease: &str, now: DateTime<Utc>) -> Option<String> {
    let e = engine();
    let c = ClaimFor {
        engine: &e,
        runner,
        cluster: "machine-1",
        lease,
        lease_until: now + Duration::minutes(2),
        now,
    };
    store.claim(&c).await.unwrap().map(|c| c.run)
}

async fn setup(store: &PgStore) -> Flow {
    let now = at("2026-11-25T10:00:00Z");
    let d = descriptor("an-engine", "-");
    store.register(&d, "r1", now).await.unwrap();
    store.register(&d, "r2", now).await.unwrap();
    let folder = store.create_folder("f").await.unwrap();
    store.create_flow(&new_flow(&folder.id, "x")).await.unwrap()
}

#[tokio::test]
async fn a_fire_queues_one_run() {
    let db = common::database().await;
    let store = db.store();
    let flow = setup(&store).await;
    let now = at("2026-11-25T10:00:00Z");
    let fire = NewRun {
        trigger: Trigger::Schedule("00000000-0000-0000-0000-000000000001".into()),
        fire_at: Some(now),
        ..run_of(&flow, now)
    };
    // The schedule must exist for its reference.
    sqlx::query("INSERT INTO schedules (id, flow_id, timezone) VALUES ($1::uuid, $2::uuid, 'UTC')")
        .bind("00000000-0000-0000-0000-000000000001")
        .bind(&flow.id)
        .execute(&db.pool)
        .await
        .unwrap();
    assert!(store.enqueue(&fire).await.unwrap().is_some());
    assert!(
        store.enqueue(&fire).await.unwrap().is_none(),
        "seen twice, queued once"
    );
    assert!(store.enqueue(&run_of(&flow, now)).await.unwrap().is_some());
}

#[tokio::test]
async fn a_claim_takes_only_what_is_admitted() {
    let db = common::database().await;
    let store = db.store();
    let flow = setup(&store).await;
    let now = at("2026-11-25T10:00:00Z");
    let first = store.enqueue(&run_of(&flow, now)).await.unwrap().unwrap();
    store
        .enqueue(&run_of(&flow, now + Duration::seconds(1)))
        .await
        .unwrap();
    let later = NewRun {
        queued_at: now + Duration::hours(1),
        ..run_of(&flow, now)
    };
    store.enqueue(&later).await.unwrap();

    let held = EngineName::new("an-engine").unwrap();
    store
        .hold(&held, "h", now + Duration::minutes(5), now)
        .await
        .unwrap();
    assert_eq!(claim(&store, "r1", "l0", now).await, None, "held");
    store.release(&held, "h").await.unwrap();
    store.pause_engine(&held, "x", None, now).await.unwrap();
    assert_eq!(claim(&store, "r1", "l0", now).await, None, "paused");
    store.resume_engine(&held).await.unwrap();

    assert_eq!(claim(&store, "r1", "l1", now).await, Some(first));
    assert_eq!(
        claim(&store, "r2", "l2", now).await,
        None,
        "no overlap of one flow"
    );
}

#[tokio::test]
async fn locks_and_pools_admit_runs() {
    let db = common::database().await;
    let store = db.store();
    let folder = store.create_folder("g").await.unwrap();
    let now = at("2026-11-25T10:00:00Z");
    store
        .register(&descriptor("an-engine", "-"), "r1", now)
        .await
        .unwrap();
    let mut flows = Vec::new();
    for name in ["a", "b", "c", "d"] {
        flows.push(
            store
                .create_flow(&new_flow(&folder.id, name))
                .await
                .unwrap(),
        );
    }
    sqlx::query("INSERT INTO slot_pools (name, size) VALUES ('rules', 1)")
        .execute(&db.pool)
        .await
        .unwrap();
    let with = |f: &Flow, lock: Option<&str>, pool: Option<&str>| NewRun {
        lock_name: lock.map(str::to_string),
        slot_pool: pool.map(str::to_string),
        ..run_of(f, now)
    };
    store
        .enqueue(&with(&flows[0], Some("gold.t"), None))
        .await
        .unwrap();
    store
        .enqueue(&with(&flows[1], Some("gold.t"), None))
        .await
        .unwrap();
    store
        .enqueue(&with(&flows[2], None, Some("rules")))
        .await
        .unwrap();
    store
        .enqueue(&with(&flows[3], None, Some("rules")))
        .await
        .unwrap();

    let mut taken = 0;
    while claim(&store, "r1", &format!("l{taken}"), now)
        .await
        .is_some()
    {
        taken += 1;
    }
    assert_eq!(
        taken, 2,
        "one of the lock's and one of the pool's of size 1"
    );
}

#[tokio::test]
async fn concurrent_claims_never_take_one_run_twice() {
    let db = common::database().await;
    let store = db.store();
    let folder = store.create_folder("h").await.unwrap();
    let now = at("2026-11-25T10:00:00Z");
    store
        .register(&descriptor("an-engine", "-"), "r1", now)
        .await
        .unwrap();
    for i in 0..20 {
        let flow = store
            .create_flow(&new_flow(&folder.id, &format!("f{i}")))
            .await
            .unwrap();
        store.enqueue(&run_of(&flow, now)).await.unwrap();
    }
    let tasks: Vec<_> = (0..8)
        .map(|t| {
            let store = store.clone();
            tokio::spawn(async move {
                let mut got = Vec::new();
                let mut i = 0;
                while let Some(run) = claim(&store, "r1", &format!("l{t}-{i}"), now).await {
                    got.push(run);
                    i += 1;
                }
                got
            })
        })
        .collect();
    let mut all = Vec::new();
    for t in tasks {
        all.extend(t.await.unwrap());
    }
    let unique: std::collections::BTreeSet<_> = all.iter().collect();
    assert_eq!((all.len(), unique.len()), (20, 20));
}

#[tokio::test]
async fn only_the_lease_holder_renews_logs_and_finishes() {
    let db = common::database().await;
    let store = db.store();
    let flow = setup(&store).await;
    let now = at("2026-11-25T10:00:00Z");
    store.enqueue(&run_of(&flow, now)).await.unwrap();
    let run = claim(&store, "r1", "lease-1", now).await.unwrap();
    let until = now + Duration::minutes(3);
    let progress = Progress {
        fraction: Some(0.5),
        detail: Some("step 2".into()),
    };
    let renewal = store
        .renew(&run, "lease-1", Some(&progress), Some("q-1"), until)
        .await
        .unwrap();
    assert!(!renewal.cancel_requested);
    assert!(matches!(
        store.renew(&run, "lease-0", None, None, until).await,
        Err(StoreError::Lease)
    ));
    let line = |seq| LogLine {
        seq,
        stream: LogStream::Stdout,
        line: format!("line {seq}"),
    };
    store
        .append_logs(&run, "lease-1", &[line(1), line(2)], now)
        .await
        .unwrap();
    store
        .append_logs(&run, "lease-1", &[line(2), line(3)], now)
        .await
        .unwrap();
    assert!(matches!(
        store.append_logs(&run, "lease-0", &[line(4)], now).await,
        Err(StoreError::Lease)
    ));
    let logged: i64 = sqlx::query_scalar("SELECT count(*) FROM run_logs")
        .fetch_one(&db.pool)
        .await
        .unwrap();
    assert_eq!(logged, 3, "a line sent twice is kept once");

    let finish = |lease: &str| Finish {
        lease: lease.into(),
        state: FinalState::Succeeded,
        counts: BTreeMap::from([("rows".into(), 10)]),
        error_class: None,
        interruption: None,
    };
    assert!(matches!(
        store.finish(&run, &finish("lease-0"), now).await,
        Err(StoreError::Lease)
    ));
    store.finish(&run, &finish("lease-1"), now).await.unwrap();
    assert!(matches!(
        store.finish(&run, &finish("lease-1"), now).await,
        Err(StoreError::Lease)
    ));
    let (state, counts, engine_ref): (String, serde_json::Value, Option<String>) =
        sqlx::query_as("SELECT state, counts, engine_ref FROM runs WHERE id = $1::uuid")
            .bind(&run)
            .fetch_one(&db.pool)
            .await
            .unwrap();
    assert_eq!((state.as_str(), counts), ("succeeded", json!({"rows": 10})));
    assert_eq!(engine_ref.as_deref(), Some("q-1"));
}
