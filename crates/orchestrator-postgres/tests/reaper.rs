//! The reaper, retries and cancels.

mod common;

use chrono::{Duration, Utc};
use common::{at, descriptor, new_flow};
use engine_port::Finish;
use orchestration_model::{
    retry, EngineName, FinalState, Interruption, OnInterrupt, Trigger, MAX_ATTEMPTS,
};
use orchestrator_postgres::{Cancelled, ClaimFor, NewRun, PgStore};

async fn queue_one(store: &PgStore, name: &str) -> (String, NewRun) {
    let now = at("2026-11-25T10:00:00Z");
    store
        .register(&descriptor("an-engine", "-"), "r1", now)
        .await
        .unwrap();
    let folder = store.create_folder(name).await.unwrap();
    let flow = store.create_flow(&new_flow(&folder.id, "x")).await.unwrap();
    let run = NewRun {
        flow_id: flow.id.clone(),
        engine: flow.engine.clone(),
        kind: flow.kind.clone(),
        cluster: None,
        trigger: Trigger::Manual("person-1".into()),
        fire_at: None,
        parameters: flow.parameters.clone(),
        timeout_secs: flow.timeout_secs,
        lock_name: None,
        slot_pool: None,
        queued_at: now,
    };
    let id = store.enqueue(&run).await.unwrap().unwrap();
    (id, run)
}

async fn claim(store: &PgStore, lease: &str, now: chrono::DateTime<Utc>) -> Option<String> {
    let e = orchestration_model::EngineLine {
        engine: EngineName::new("an-engine").unwrap(),
        line: orchestration_model::Line::new("-").unwrap(),
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
async fn an_expired_lease_is_lost_and_repeated_once() {
    let db = common::database().await;
    let store = db.store();
    let (id, _) = queue_one(&store, "f").await;
    let now = at("2026-11-25T10:00:00Z");
    assert_eq!(claim(&store, "l1", now).await.as_deref(), Some(id.as_str()));
    assert!(store
        .reap(now + Duration::minutes(1))
        .await
        .unwrap()
        .is_empty());
    let later = now + Duration::minutes(3);
    assert_eq!(store.reap(later).await.unwrap(), std::slice::from_ref(&id));

    let cut = store.interrupted(later, true).await.unwrap();
    assert_eq!(cut.len(), 1);
    let (run, interrupted) = &cut[0];
    assert_eq!(run, &id);
    assert_eq!(interrupted.interruption, Interruption::RunnerLost);
    assert_eq!(
        interrupted.on_interrupt,
        OnInterrupt::Repeat,
        "the flow's own rule"
    );
    let r = retry(interrupted, later).expect("the flow repeats it");
    let again = store.queue_retry(run, &r, later).await.unwrap().unwrap();
    assert!(
        store.queue_retry(run, &r, later).await.unwrap().is_none(),
        "once"
    );
    assert!(store.interrupted(later, true).await.unwrap().is_empty());

    // Not before its delay; then claimed as attempt 2.
    assert_eq!(claim(&store, "l2", later).await, None);
    let claimed_at = r.not_before + Duration::seconds(1);
    let e = orchestration_model::EngineLine {
        engine: EngineName::new("an-engine").unwrap(),
        line: orchestration_model::Line::new("-").unwrap(),
    };
    let claimed = store
        .claim(&ClaimFor {
            engine: &e,
            runner: "r1",
            cluster: "machine-1",
            lease: "l3",
            lease_until: claimed_at + Duration::minutes(2),
            now: claimed_at,
        })
        .await
        .unwrap()
        .unwrap();
    assert_eq!((claimed.run.as_str(), claimed.attempt), (again.as_str(), 2));
    assert!(claimed.resume.is_none(), "repeat, not continue");
    assert!(r.attempt < MAX_ATTEMPTS);
}

#[tokio::test]
async fn a_run_that_failed_by_itself_is_not_repeated() {
    let db = common::database().await;
    let store = db.store();
    let (id, _) = queue_one(&store, "g").await;
    let now = at("2026-11-25T10:00:00Z");
    claim(&store, "l1", now).await.unwrap();
    let failed = Finish {
        lease: "l1".into(),
        state: FinalState::Failed,
        counts: Default::default(),
        error_class: Some("syntax".into()),
        interruption: None,
    };
    store.finish(&id, &failed, now).await.unwrap();
    assert!(store.interrupted(now, true).await.unwrap().is_empty());
}

#[tokio::test]
async fn a_cancel_ends_a_queued_run_and_asks_a_running_one() {
    let db = common::database().await;
    let store = db.store();
    let (first, run) = queue_one(&store, "h").await;
    let now = at("2026-11-25T10:00:00Z");
    assert_eq!(store.cancel(&first, now).await.unwrap(), Cancelled::Now);
    assert_eq!(store.cancel(&first, now).await.unwrap(), Cancelled::Nothing);
    let second = store.enqueue(&run).await.unwrap().unwrap();
    claim(&store, "l1", now).await.unwrap();
    assert_eq!(
        store.cancel(&second, now).await.unwrap(),
        Cancelled::Requested
    );
    let renewal = store
        .renew(&second, "l1", None, None, now + Duration::minutes(2))
        .await
        .unwrap();
    assert!(renewal.cancel_requested);
}

#[tokio::test]
async fn a_queued_run_nobody_claims_is_cancelled_after_the_wait() {
    let db = common::database().await;
    let store = db.store();
    let (id, _) = queue_one(&store, "k").await;
    let now = at("2026-11-25T10:00:00Z");
    assert!(store.cancel_stale(now, now).await.unwrap().is_empty());
    let later = now + Duration::hours(2);
    assert_eq!(
        store
            .cancel_stale(now + Duration::hours(1), later)
            .await
            .unwrap(),
        [id]
    );
    let engine = EngineName::new("an-engine").unwrap();
    assert_eq!(store.running(&engine).await.unwrap(), 0);
}
