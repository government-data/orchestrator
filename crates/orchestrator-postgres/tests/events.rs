//! The outbox's other events (ruling 99): a claim writes one
//! `orchestrator.run.started`, a chain's end one
//! `orchestrator.chain.finished`, each fitting the module's schema, in the
//! transaction of the state they tell; no parameter and no person.

mod common;

use chrono::{DateTime, Duration, Utc};
use common::{at, descriptor, fits_the_schema, new_flow};
use engine_port::Finish;
use orchestration_model::{ChainStep, EngineLine, EngineName, FinalState, Line, Trigger};
use orchestrator_postgres::{ClaimFor, NewChain, NewRun, OutboxEvent, PgStore};
use serde_json::json;

const STARTED: &str = "orchestrator.run.started";
const CHAIN_FINISHED: &str = "orchestrator.chain.finished";

async fn events_of(store: &PgStore, topic: &str) -> Vec<OutboxEvent> {
    let mut events = store.pending_events(100).await.unwrap();
    events.retain(|e| e.topic == topic);
    events
}

async fn flow(store: &PgStore) -> String {
    let now = at("2026-11-27T10:00:00Z");
    store
        .register(&descriptor("an-engine", "-"), "r1", now)
        .await
        .unwrap();
    let folder = store.create_folder("events").await.unwrap();
    store
        .create_flow(&new_flow(&folder.id, "x"))
        .await
        .unwrap()
        .id
}

/// Claims the next run, at `now`: its id and lease.
async fn claim(store: &PgStore, now: DateTime<Utc>) -> (String, String) {
    let e = EngineLine {
        engine: EngineName::new("an-engine").unwrap(),
        line: Line::new("-").unwrap(),
    };
    let lease = format!("l-{}", now.timestamp());
    let c = ClaimFor {
        engine: &e,
        runner: "r1",
        cluster: "machine-1",
        lease: &lease,
        lease_until: now + Duration::minutes(2),
        now,
    };
    let run = store.claim(&c).await.unwrap().expect("a run is queued").run;
    (run, lease)
}

async fn finish(store: &PgStore, run: &str, lease: &str, state: FinalState, now: DateTime<Utc>) {
    let f = Finish {
        lease: lease.into(),
        state,
        counts: Default::default(),
        error_class: (state != FinalState::Succeeded).then(|| "an_error".into()),
        interruption: None,
    };
    store.finish(run, &f, now).await.unwrap();
}

#[tokio::test]
async fn a_claim_tells_the_run_started_once() {
    let db = common::database().await;
    let store = db.store();
    let flow_id = flow(&store).await;
    let now = at("2026-11-27T10:00:00Z");
    let queued = NewRun {
        flow_id,
        engine: EngineLine {
            engine: EngineName::new("an-engine").unwrap(),
            line: Line::new("-").unwrap(),
        },
        kind: "job".into(),
        cluster: None,
        trigger: Trigger::Manual("person-1".into()),
        fire_at: None,
        parameters: json!({"dataset": "sales"}),
        timeout_secs: 60,
        lock_name: None,
        slot_pool: None,
        queued_at: now,
    };
    store.enqueue(&queued).await.unwrap().unwrap();
    assert!(events_of(&store, STARTED).await.is_empty(), "queued only");

    let t = now + Duration::seconds(5);
    let (run, lease) = claim(&store, t).await;
    store
        .renew(&run, &lease, None, None, t + Duration::minutes(2))
        .await
        .unwrap();
    finish(&store, &run, &lease, FinalState::Succeeded, t).await;

    let started = events_of(&store, STARTED).await;
    assert_eq!(started.len(), 1, "one per claim");
    let e = &started[0];
    assert_eq!((e.version, e.occurred_at), (1, t));
    fits_the_schema("run-started.v1.json", &e.data);
    assert_eq!(e.data["run_id"], run);
    assert_eq!(e.data["attempt"], 1);
    assert_eq!(
        (&e.data["engine"], &e.data["line"], &e.data["kind"]),
        (&json!("an-engine"), &json!("-"), &json!("job"))
    );
    let started_at: DateTime<Utc> = e.data["started_at"].as_str().unwrap().parse().unwrap();
    assert_eq!(started_at, t);
    assert!(e.data.get("chain_id").is_none(), "not a chain's");
    assert!(e.data.get("parameters").is_none(), "no parameter");
    assert!(e.data.get("trigger").is_none(), "no person");
}

fn chain(flow: &str, origin: &str, now: DateTime<Utc>) -> NewChain {
    NewChain {
        flow_id: flow.into(),
        origin: origin.into(),
        trigger: Trigger::Manual("person-1".into()),
        steps: vec![ChainStep {
            kind: "extract".into(),
            parameters: json!({"dataset": "sales"}),
            setup: None,
        }],
        now,
    }
}

#[tokio::test]
async fn a_chains_end_is_told_with_its_state_and_origin() {
    let db = common::database().await;
    let store = db.store();
    let flow = flow(&store).await;
    let now = at("2026-11-27T10:00:00Z");
    let ok = store
        .start_chain(&chain(&flow, "core-job:j-1", now))
        .await
        .unwrap()
        .unwrap();
    store.move_chains(now).await.unwrap();
    let (run, lease) = claim(&store, now).await;
    let started = events_of(&store, STARTED).await;
    assert_eq!(started[0].data["chain_id"], ok, "a chain's run");
    store.move_chains(now).await.unwrap();
    assert!(
        events_of(&store, CHAIN_FINISHED).await.is_empty(),
        "running"
    );
    finish(&store, &run, &lease, FinalState::Succeeded, now).await;

    let t = now + Duration::minutes(1);
    let failed = store
        .start_chain(&chain(&flow, "core:sched-1:1764237600", t))
        .await
        .unwrap()
        .unwrap();
    store.move_chains(t).await.unwrap();
    let (run, lease) = claim(&store, t).await;
    finish(&store, &run, &lease, FinalState::Failed, t).await;
    let end = t + Duration::minutes(1);
    store.move_chains(end).await.unwrap();
    store.move_chains(end).await.unwrap();

    let ends = events_of(&store, CHAIN_FINISHED).await;
    let told: Vec<_> = ends
        .iter()
        .map(|e| {
            assert_eq!(e.version, 1);
            fits_the_schema("chain-finished.v1.json", &e.data);
            assert!(e.data.get("triggered_by").is_none(), "no person");
            (
                e.data["chain_id"].as_str().unwrap(),
                e.data["state"].as_str().unwrap(),
                e.data["origin"].as_str().unwrap(),
            )
        })
        .collect();
    assert_eq!(
        told,
        [
            (ok.as_str(), "succeeded", "core-job:j-1"),
            (failed.as_str(), "failed", "core:sched-1:1764237600"),
        ],
        "one per end"
    );
    let when: Vec<_> = ends.iter().map(|e| e.occurred_at).collect();
    assert_eq!(when, [t, end], "at the chain's end");
}
