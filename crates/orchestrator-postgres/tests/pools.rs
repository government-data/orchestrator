//! Slot pools by kind: a claim admits at most the pool's size, takes turns
//! between the values of the pool's parameter, and stamps the pool on the
//! run; the size is replaced when the engine sends its pools again.

mod common;

use chrono::{DateTime, Duration, Utc};
use common::{at, descriptor, new_flow};
use engine_port::{Finish, SlotPool};
use orchestration_model::{EngineLine, EngineName, FinalState, Line, Trigger};
use orchestrator_postgres::{ClaimFor, NewRun, PgStore};
use serde_json::json;

fn engine() -> EngineLine {
    EngineLine {
        engine: EngineName::new("an-engine").unwrap(),
        line: Line::new("-").unwrap(),
    }
}

async fn queue(store: &PgStore, folder: &str, name: &str, dataset: &str, minute: i64) -> String {
    let flow = store.create_flow(&new_flow(folder, name)).await.unwrap();
    let run = NewRun {
        flow_id: flow.id.clone(),
        engine: flow.engine.clone(),
        kind: flow.kind.clone(),
        cluster: None,
        trigger: Trigger::Manual("person-1".into()),
        fire_at: None,
        parameters: json!({"dataset": dataset}),
        timeout_secs: flow.timeout_secs,
        lock_name: None,
        slot_pool: None,
        queued_at: at("2026-11-25T10:00:00Z") + Duration::minutes(minute),
    };
    store.enqueue(&run).await.unwrap().unwrap()
}

async fn claim(store: &PgStore, now: DateTime<Utc>) -> Option<String> {
    let e = engine();
    let lease = format!("l-{}", now.timestamp());
    let c = ClaimFor {
        engine: &e,
        runner: "r1",
        cluster: "machine-1",
        lease: &lease,
        lease_until: now + Duration::minutes(2),
        now,
    };
    store.claim(&c).await.unwrap().map(|c| c.run)
}

async fn done(store: &PgStore, run: &str, now: DateTime<Utc>) {
    let lease: String = sqlx::query_scalar("SELECT lease FROM runs WHERE id = $1::uuid")
        .bind(run)
        .fetch_one(store.pool())
        .await
        .unwrap();
    let f = Finish {
        lease,
        state: FinalState::Succeeded,
        counts: Default::default(),
        error_class: None,
        interruption: None,
    };
    store.finish(run, &f, now).await.unwrap();
}

fn rules(size: u32) -> SlotPool {
    SlotPool {
        name: "rules".into(),
        size,
        kinds: vec!["load".into()],
        fair_by: Some("dataset".into()),
    }
}

#[tokio::test]
async fn a_pool_admits_its_size_and_takes_turns_by_dataset() {
    let db = common::database().await;
    let store = db.store();
    let now = at("2026-11-25T11:00:00Z");
    store
        .register(&descriptor("an-engine", "-"), "r1", now)
        .await
        .unwrap();
    store.set_pools(&engine(), &[rules(1)]).await.unwrap();
    let folder = store.create_folder("pools").await.unwrap().id;
    let a1 = queue(&store, &folder, "a1", "sales", 0).await;
    let a2 = queue(&store, &folder, "a2", "sales", 1).await;
    let b1 = queue(&store, &folder, "b1", "stock", 2).await;

    assert_eq!(claim(&store, now).await.as_deref(), Some(a1.as_str()));
    let pool: Option<String> = sqlx::query_scalar("SELECT slot_pool FROM runs WHERE id = $1::uuid")
        .bind(&a1)
        .fetch_one(&db.pool)
        .await
        .unwrap();
    assert_eq!(pool.as_deref(), Some("rules"), "stamped at the claim");
    assert_eq!(claim(&store, now).await, None, "the pool is full");
    done(&store, &a1, now).await;

    // `stock` was never served: it goes before the older `sales` run.
    let t = now + Duration::minutes(1);
    assert_eq!(claim(&store, t).await.as_deref(), Some(b1.as_str()));
    done(&store, &b1, t).await;
    let t = t + Duration::minutes(1);
    assert_eq!(claim(&store, t).await.as_deref(), Some(a2.as_str()));

    // A larger pool, sent again, admits one more at once.
    store.set_pools(&engine(), &[rules(2)]).await.unwrap();
    let a3 = queue(&store, &folder, "a3", "sales", 3).await;
    assert_eq!(claim(&store, t).await.as_deref(), Some(a3.as_str()));
    store.set_pools(&engine(), &[]).await.unwrap();
    queue(&store, &folder, "a4", "sales", 4).await;
    assert!(claim(&store, t).await.is_some(), "in no pool now");
}
