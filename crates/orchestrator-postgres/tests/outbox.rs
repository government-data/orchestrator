//! The outbox: every way a run ends writes one `orchestrator.run.finished`
//! event whose data fits the module's schema; a renewal writes none; the
//! events are read in order until marked sent or refused. A claim's
//! `orchestrator.run.started` and a chain's `orchestrator.chain.finished`
//! are in `events.rs`.

mod common;

use chrono::{Duration, Utc};
use common::{at, descriptor, fits_the_schema, new_flow};
use engine_port::Finish;
use orchestration_model::{EngineLine, EngineName, FinalState, Line, Trigger};
use orchestrator_postgres::{ClaimFor, NewRun, OutboxEvent, PgStore};

const FINISHED: &str = "orchestrator.run.finished";

/// The pending `orchestrator.run.finished` events, in order.
async fn finished_events(store: &PgStore) -> Vec<OutboxEvent> {
    let mut events = store.pending_events(100).await.unwrap();
    events.retain(|e| e.topic == FINISHED);
    events
}

async fn queue(store: &PgStore, folder: &str) -> String {
    let now = at("2026-11-27T10:00:00Z");
    store
        .register(&descriptor("an-engine", "-"), "r1", now)
        .await
        .unwrap();
    let folder = store.create_folder(folder).await.unwrap();
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
    store.enqueue(&run).await.unwrap().unwrap()
}

async fn claim(store: &PgStore, lease: &str, now: chrono::DateTime<Utc>) -> String {
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
    store.claim(&c).await.unwrap().unwrap().run
}

#[tokio::test]
async fn every_end_of_a_run_writes_one_event() {
    let db = common::database().await;
    let store = db.store();
    let now = at("2026-11-27T10:00:00Z");

    let finished = queue(&store, "a").await;
    claim(&store, "l1", now).await;
    store
        .renew(&finished, "l1", None, None, now + Duration::minutes(2))
        .await
        .unwrap();
    assert!(finished_events(&store).await.is_empty(), "a renewal");
    let failed = Finish {
        lease: "l1".into(),
        state: FinalState::Failed,
        counts: Default::default(),
        error_class: Some("syntax".into()),
        interruption: None,
    };
    store.finish(&finished, &failed, now).await.unwrap();

    let cancelled = queue(&store, "b").await;
    store.cancel(&cancelled, now).await.unwrap();
    let lost = queue(&store, "c").await;
    claim(&store, "l2", now).await;
    store.reap(now + Duration::minutes(3)).await.unwrap();

    let events = finished_events(&store).await;
    let told: Vec<(&str, &str)> = events
        .iter()
        .map(|e| {
            assert_eq!((e.topic.as_str(), e.version), (FINISHED, 1));
            fits_the_schema("run-finished.v1.json", &e.data);
            (
                e.data["run_id"].as_str().unwrap(),
                e.data["state"].as_str().unwrap(),
            )
        })
        .collect();
    assert_eq!(
        told,
        [
            (finished.as_str(), "failed"),
            (cancelled.as_str(), "cancelled"),
            (lost.as_str(), "lost"),
        ]
    );
    let first = &events[0].data;
    assert_eq!(first["error_class"], "syntax");
    assert_eq!(first["trigger"], "manual");
    assert!(first.get("triggered_by").is_none(), "no person");
    assert!(first.get("parameters").is_none(), "no parameter");

    store.event_sent(events[0].seq, now).await.unwrap();
    store.event_refused(events[1].seq, 400).await.unwrap();
    let left = finished_events(&store).await;
    assert_eq!(left.len(), 1);
    assert_eq!(left[0].id, events[2].id, "the same id at every read");
    assert_eq!(store.forget_sent_events(now).await.unwrap(), 0);
    let later = now + Duration::days(8);
    assert_eq!(store.forget_sent_events(later).await.unwrap(), 1);
}
