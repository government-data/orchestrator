//! Chains: one chain per origin, each step's run queued once and only
//! after the step before it succeeded, a setup skipped while its
//! fingerprint is done, a failed step failing the chain until a repetition
//! of it succeeds.

mod common;

use chrono::{DateTime, Duration, Utc};
use common::{at, descriptor, new_flow};
use engine_port::Finish;
use orchestration_model::{
    retry, ChainStep, EngineLine, EngineName, FinalState, Line, Setup, Trigger,
};
use orchestrator_postgres::{ChainsMoved, ClaimFor, NewChain, PgStore};
use serde_json::json;

fn step(kind: &str, setup: Option<&str>) -> ChainStep {
    ChainStep {
        kind: kind.into(),
        parameters: json!({"dataset": "sales", "cut": "2026-11-25"}),
        setup: setup.map(|fingerprint| Setup {
            key: "dataset:sales".into(),
            fingerprint: fingerprint.into(),
        }),
    }
}

async fn flow(store: &PgStore) -> String {
    let now = at("2026-11-25T10:00:00Z");
    store
        .register(&descriptor("an-engine", "-"), "r1", now)
        .await
        .unwrap();
    let folder = store.create_folder("chains").await.unwrap();
    store
        .create_flow(&new_flow(&folder.id, "cut"))
        .await
        .unwrap()
        .id
}

fn chain(flow: &str, origin: &str, fingerprint: &str, now: DateTime<Utc>) -> NewChain {
    NewChain {
        flow_id: flow.into(),
        origin: origin.into(),
        trigger: Trigger::Manual("person-1".into()),
        steps: vec![
            step("prepare", Some(fingerprint)),
            step("extract", None),
            step("evaluate", None),
        ],
        now,
    }
}

/// Claims the next run and ends it as `state`: its id, kind and parameters.
async fn run_one(
    store: &PgStore,
    now: DateTime<Utc>,
    state: FinalState,
) -> (String, String, serde_json::Value) {
    let e = EngineLine {
        engine: EngineName::new("an-engine").unwrap(),
        line: Line::new("-").unwrap(),
    };
    let lease = format!("l-{}", now.timestamp());
    let claimed = store
        .claim(&ClaimFor {
            engine: &e,
            runner: "r1",
            cluster: "machine-1",
            lease: &lease,
            lease_until: now + Duration::minutes(2),
            now,
        })
        .await
        .unwrap()
        .expect("a run is queued");
    let finish = Finish {
        lease,
        state,
        counts: Default::default(),
        error_class: (state != FinalState::Succeeded).then(|| "an_error".into()),
        interruption: None,
    };
    store.finish(&claimed.run, &finish, now).await.unwrap();
    (claimed.run, claimed.kind, claimed.parameters)
}

#[tokio::test]
async fn a_chain_runs_its_steps_in_order_once() {
    let db = common::database().await;
    let store = db.store();
    let flow = flow(&store).await;
    let now = at("2026-11-25T10:00:00Z");
    let id = store
        .start_chain(&chain(&flow, "fire-1", "f1", now))
        .await
        .unwrap()
        .unwrap();
    assert!(
        store
            .start_chain(&chain(&flow, "fire-1", "f1", now))
            .await
            .unwrap()
            .is_none(),
        "one chain per origin"
    );
    let mut kinds = Vec::new();
    for i in 0..3 {
        let t = now + Duration::minutes(i);
        let moved = store.move_chains(t).await.unwrap();
        assert_eq!(moved.queued, 1);
        assert_eq!(
            store.move_chains(t).await.unwrap(),
            ChainsMoved::default(),
            "waits"
        );
        let (_, kind, parameters) = run_one(&store, t, FinalState::Succeeded).await;
        assert_eq!(parameters["dataset"], "sales", "the step's over the flow's");
        assert_eq!(parameters["day"], "today");
        kinds.push(kind);
    }
    assert_eq!(kinds, ["prepare", "extract", "evaluate"]);
    let moved = store.move_chains(now + Duration::minutes(5)).await.unwrap();
    assert_eq!(moved.succeeded, 1);
    assert_eq!(
        store.chain_state(&id).await.unwrap(),
        Some(("succeeded".into(), 3))
    );

    // The next fire with the same fingerprint skips its setup; a new
    // fingerprint (the columns changed) runs it again.
    let later = now + Duration::hours(1);
    store
        .start_chain(&chain(&flow, "fire-2", "f1", later))
        .await
        .unwrap();
    store.move_chains(later).await.unwrap();
    assert_eq!(
        run_one(&store, later, FinalState::Succeeded).await.1,
        "extract"
    );
    let next = later + Duration::hours(1);
    store
        .start_chain(&chain(&flow, "fire-3", "f2", next))
        .await
        .unwrap();
    store.move_chains(next).await.unwrap();
    let queued: Vec<String> = sqlx::query_scalar(
        "SELECT r.kind FROM runs r JOIN chains c ON c.id = r.chain_id
          WHERE c.origin = 'fire-3' AND r.state = 'queued'",
    )
    .fetch_all(&db.pool)
    .await
    .unwrap();
    assert_eq!(queued, ["prepare"]);
}

#[tokio::test]
async fn a_failed_step_stops_the_chain_until_its_repetition_succeeds() {
    let db = common::database().await;
    let store = db.store();
    let flow = flow(&store).await;
    let now = at("2026-11-25T10:00:00Z");
    let mut c = chain(&flow, "fire-1", "f1", now);
    c.steps.remove(0);
    let id = store.start_chain(&c).await.unwrap().unwrap();
    store.move_chains(now).await.unwrap();
    let (run, kind, _) = run_one(&store, now, FinalState::Failed).await;
    assert_eq!(kind, "extract");
    let t = now + Duration::minutes(1);
    assert_eq!(store.move_chains(t).await.unwrap().failed, 1);
    assert_eq!(
        store.chain_state(&id).await.unwrap(),
        Some(("failed".into(), 0))
    );
    assert_eq!(
        store.move_chains(t).await.unwrap(),
        ChainsMoved::default(),
        "failed once"
    );

    // A repetition of the step (the reaper's, here by hand) keeps the
    // chain's step and, once it succeeds, the chain goes on.
    sqlx::query("UPDATE runs SET interruption = 'runner_lost' WHERE id = $1::uuid")
        .bind(&run)
        .execute(&db.pool)
        .await
        .unwrap();
    let (_, interrupted) = store.interrupted(t, true).await.unwrap().remove(0);
    let r = retry(&interrupted, t).expect("the flow repeats it");
    store.queue_retry(&run, &r, t).await.unwrap().unwrap();
    store.move_chains(t).await.unwrap();
    assert_eq!(
        store.chain_state(&id).await.unwrap(),
        Some(("running".into(), 0))
    );
    let after = r.not_before + Duration::seconds(1);
    run_one(&store, after, FinalState::Succeeded).await;
    assert_eq!(store.move_chains(after).await.unwrap().queued, 1);
    assert_eq!(
        run_one(&store, after, FinalState::Succeeded).await.1,
        "evaluate"
    );
}

#[tokio::test]
async fn a_chain_with_steps_that_do_not_hold_is_refused() {
    let db = common::database().await;
    let store = db.store();
    let flow = flow(&store).await;
    let mut c = chain(&flow, "fire-1", "f1", at("2026-11-25T10:00:00Z"));
    c.steps[1].kind = "Not A Kind".into();
    assert!(store.start_chain(&c).await.is_err());
    c.steps.clear();
    assert!(store.start_chain(&c).await.is_err());
}
