//! The outbox sent to the core: a run's end reaches the core's events with
//! its id; a refused event is kept and the next goes on; while the core
//! does not answer, nothing is lost and the next pass sends it.

mod common;

use std::sync::Mutex;

use async_trait::async_trait;
use orchestrator_server::{send_outbox, Clock, CoreEvents, Event, OutboxSent, Published, Received};

/// The core's events: what it answers to each publish, and what it got.
#[derive(Default)]
struct FakeEvents {
    answers: Mutex<Vec<Published>>,
    got: Mutex<Vec<Event>>,
}

#[async_trait]
impl CoreEvents for FakeEvents {
    async fn publish(&self, event: &Event) -> Published {
        self.got.lock().unwrap().push(event.clone());
        let mut answers = self.answers.lock().unwrap();
        if answers.is_empty() {
            Published::Stored
        } else {
            answers.remove(0)
        }
    }

    async fn read(&self, _: (&str, u32), _: Option<i64>, _: u64) -> Option<(Vec<Received>, i64)> {
        None
    }

    async fn confirm(&self, _: (&str, u32), _: i64) -> bool {
        false
    }
}

#[tokio::test]
async fn run_outcomes_reach_the_cores_events_once_stored() {
    let h = common::harness().await;
    let flow = h.flow("outbox").await;
    let first = h.queue(&flow).await;
    let second = h.queue(&flow).await;
    let third = h.queue(&flow).await;
    let now = h.clock.now();
    for run in [&first, &second, &third] {
        h.store.cancel(run, now).await.unwrap();
    }
    let core = FakeEvents::default();
    *core.answers.lock().unwrap() = vec![Published::Refused(400), Published::Unavailable];

    let done = send_outbox(&h.store, &core, now).await.unwrap();
    let waiting = OutboxSent {
        sent: 0,
        refused: 1,
        waiting: true,
    };
    assert_eq!(done, waiting);
    let done = send_outbox(&h.store, &core, now).await.unwrap();
    let all_sent = OutboxSent {
        sent: 2,
        refused: 0,
        waiting: false,
    };
    assert_eq!(done, all_sent);
    assert_eq!(
        send_outbox(&h.store, &core, now).await.unwrap(),
        OutboxSent::default()
    );

    let got = core.got.lock().unwrap().clone();
    let runs: Vec<&str> = got
        .iter()
        .map(|e| e.data["run_id"].as_str().unwrap())
        .collect();
    assert_eq!(
        runs,
        [
            first.as_str(),
            second.as_str(),
            second.as_str(),
            third.as_str()
        ]
    );
    assert_eq!(got[1].id, got[2].id, "tried again with the same id");
    assert!(got
        .iter()
        .all(|e| e.topic == "orchestrator.run.finished" && e.version == 1));
    assert!(got.iter().all(|e| e.data["state"] == "cancelled"));
}
