//! Ruling 92(b)'s per-table locks through the engine and the contract
//! fake: a run takes the locks its statements name before the first that
//! carries one, in name order, and gives each back after its last; another
//! run waits for a lock held; a failed or cancelled run frees what it
//! holds; a repeated attempt resumes under the lock; a malformed lock name
//! refuses the bundle.

mod common;

use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use common::{bundle, key, keys, sign, write, FakeCore, FakeTrino, Script};
use engine_port::Resume;
use engine_runner::contract::{FakeOrchestrator, Seed};
use engine_runner::{Ended, NoGuard, Runner};
use orchestration_model::{FinalState, RunState};
use serde_json::{json, Value};
use trino_runner::{parameters, Answer, BundleRefused, Trino, TrinoEngine, TrinoFailure};

const RESULTS: &str = "lake.quality_gold.rule_results";
const SUMMARY: &str = "lake.quality_gold.run_summary";

fn locked(id: &str, sql: &str, lock: &str) -> Value {
    let mut st = write(id, sql);
    st["lock"] = json!(lock);
    st
}

/// A statement's SQL and the holders of `RESULTS` and `SUMMARY` then.
type Seen = (String, Option<String>, Option<String>);

/// A Trino that notes, at each statement, who holds each gold lock.
#[derive(Clone)]
struct Watching {
    trino: FakeTrino,
    fake: FakeOrchestrator,
    seen: Arc<Mutex<Vec<Seen>>>,
}

#[async_trait]
impl Trino for Watching {
    async fn run(
        &self,
        token: &str,
        sql: &str,
        extra: &[(String, String)],
        started: &(dyn for<'q> Fn(&'q str) + Send + Sync),
    ) -> Result<Answer, TrinoFailure> {
        self.seen.lock().unwrap().push((
            sql.into(),
            self.fake.lock_holder(RESULTS),
            self.fake.lock_holder(SUMMARY),
        ));
        self.trino.run(token, sql, extra, started).await
    }
    async fn kill(&self, token: &str, id: &str) -> Result<(), TrinoFailure> {
        self.trino.kill(token, id).await
    }
    async fn ended(&self, token: &str, id: &str) -> Result<bool, TrinoFailure> {
        self.trino.ended(token, id).await
    }
}

type Tested = Runner<TrinoEngine<FakeCore, Watching>, FakeOrchestrator, NoGuard>;

fn setup(payload: Value) -> (Tested, FakeOrchestrator, Watching) {
    let fake = FakeOrchestrator::default();
    let watching = Watching {
        trino: FakeTrino::default(),
        fake: fake.clone(),
        seen: Arc::default(),
    };
    let engine = TrinoEngine::new(FakeCore::new(payload, vec![]), watching.clone(), "484", 2)
        .polling_every(Duration::from_millis(10));
    let runner =
        Runner::new(engine, fake.clone(), NoGuard, "r").renewing_every(Duration::from_millis(50));
    (runner, fake, watching)
}

/// Scratch, two gold inserts, the flip, a delete, then a scratch delete.
fn gold_write() -> Value {
    bundle(json!([
        write("scratch", "INSERT scratch"),
        locked("results", "INSERT results", RESULTS),
        locked("flip", "INSERT summary", SUMMARY),
        locked("drop_old", "DELETE results", RESULTS),
        write("tidy", "DELETE scratch"),
    ]))
}

fn script(w: &Watching) {
    for sql in [
        "INSERT scratch",
        "INSERT results",
        "INSERT summary",
        "DELETE results",
        "DELETE scratch",
    ] {
        w.trino.on(sql, vec![Script::Count(1)]);
    }
}

async fn drive(runner: &Tested) -> Ended {
    runner.register().await.unwrap();
    let task = runner.claim_once().await.unwrap().expect("claimed");
    tokio::time::timeout(Duration::from_secs(10), task)
        .await
        .unwrap()
        .unwrap()
}

#[tokio::test]
async fn locks_are_held_from_the_first_locked_statement_to_each_ones_last() {
    let (runner, fake, w) = setup(gold_write());
    script(&w);
    let run = fake.queue_run("evaluate", parameters("loans", "2026-10-31"));
    drive(&runner).await;
    assert_eq!(fake.run(&run).unwrap().state, RunState::Succeeded);
    let me = Some(run.clone());
    let seen = w.seen.lock().unwrap().clone();
    let expected = vec![
        ("INSERT scratch".to_string(), None, None),
        ("INSERT results".into(), me.clone(), me.clone()),
        ("INSERT summary".into(), me.clone(), me.clone()),
        // The flip was the summary lock's last statement.
        ("DELETE results".into(), me.clone(), None),
        ("DELETE scratch".into(), None, None),
    ];
    assert_eq!(seen, expected);
    assert_eq!(fake.lock_holder(RESULTS), None);
}

#[tokio::test]
async fn a_held_lock_is_waited_for_and_a_failed_run_frees_it() {
    let p = bundle(json!([locked("results", "INSERT results", RESULTS)]));
    let (runner, fake, w) = setup(p);
    // The first run's insert waits until it is killed; the second's counts.
    w.trino
        .on("INSERT results", vec![Script::Wait, Script::Count(1)]);
    let first = fake.queue_run("evaluate", parameters("loans", "2026-10-31"));
    let second = fake.queue_run("evaluate", parameters("cars", "2026-10-31"));
    runner.register().await.unwrap();
    let a = runner.claim_once().await.unwrap().expect("first");
    tokio::time::sleep(Duration::from_millis(150)).await;
    let b = runner.claim_once().await.unwrap().expect("second");
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(fake.lock_holder(RESULTS), Some(first.clone()));
    assert_eq!(w.seen.lock().unwrap().len(), 1, "the second waits");
    let waits = |l: &engine_port::LogLine| l.line == format!("waiting for lock {RESULTS}");
    assert!(fake.run(&second).unwrap().logs.iter().any(waits));
    w.trino.killed.notify_waiters();
    for t in [a, b] {
        tokio::time::timeout(Duration::from_secs(5), t)
            .await
            .unwrap()
            .unwrap();
    }
    assert_eq!(fake.run(&first).unwrap().state, RunState::Failed);
    assert_eq!(fake.run(&second).unwrap().state, RunState::Succeeded);
    let seen = w.seen.lock().unwrap().clone();
    assert_eq!(seen[1].1, Some(second), "the second ran under the lock");
    assert_eq!(fake.lock_holder(RESULTS), None);
}

#[tokio::test]
async fn a_cancelled_run_waiting_for_a_lock_ends_and_holds_nothing() {
    let p = bundle(json!([locked("results", "INSERT results", RESULTS)]));
    let (runner, fake, w) = setup(p);
    w.trino.on("INSERT results", vec![Script::Wait]);
    let first = fake.queue_run("evaluate", parameters("loans", "2026-10-31"));
    let second = fake.queue_run("evaluate", parameters("cars", "2026-10-31"));
    runner.register().await.unwrap();
    let a = runner.claim_once().await.unwrap().expect("first");
    tokio::time::sleep(Duration::from_millis(150)).await;
    let b = runner.claim_once().await.unwrap().expect("second");
    tokio::time::sleep(Duration::from_millis(150)).await;
    fake.cancel(&second).await;
    let ended = tokio::time::timeout(Duration::from_secs(5), b)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(ended, Ended::Reported(FinalState::Cancelled));
    assert_eq!(fake.lock_holder(RESULTS), Some(first.clone()));
    fake.cancel(&first).await;
    tokio::time::timeout(Duration::from_secs(5), a)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(fake.lock_holder(RESULTS), None);
    assert_eq!(w.seen.lock().unwrap().len(), 1, "the second ran nothing");
}

#[tokio::test]
async fn a_repeated_attempt_within_a_locks_statements_takes_it_first() {
    let (runner, fake, w) = setup(gold_write());
    script(&w);
    let resume = Resume {
        previous_run: "run-0".into(),
        engine_ref: None,
        last_detail: Some("done:3".into()),
    };
    let run = fake.queue_attempt(
        "evaluate",
        parameters("loans", "2026-10-31"),
        2,
        Some(resume),
    );
    drive(&runner).await;
    assert_eq!(fake.run(&run).unwrap().state, RunState::Succeeded);
    let seen = w.seen.lock().unwrap().clone();
    let expected = vec![
        ("DELETE results".to_string(), Some(run.clone()), None),
        ("DELETE scratch".into(), None, None),
    ];
    assert_eq!(seen, expected, "the summary's statements were done");
}

#[test]
fn a_malformed_lock_refuses_the_bundle() {
    let k = key();
    let ks = keys(&k, "k1");
    for bad in [
        "",
        "quality_gold.rule_results",
        "lake.quality_gold.rule_results.x",
        "lake..rule_results",
        "Lake.quality_gold.rule_results",
        "lake.quality_gold.rule-results",
        "lake:quality_gold:rule_results",
    ] {
        let p = bundle(json!([locked("w", "INSERT w", bad)]));
        assert_eq!(
            sign(&p, &k).verify(&ks, common::JOB),
            Err(BundleRefused::Content),
            "{bad:?}"
        );
    }
    let mut not_text = bundle(json!([write("w", "INSERT w")]));
    not_text["statements"][0]["lock"] = json!(1);
    assert_eq!(
        sign(&not_text, &k).verify(&ks, common::JOB),
        Err(BundleRefused::Content)
    );
    let ok = bundle(json!([locked("w", "INSERT w", RESULTS)]));
    let b = sign(&ok, &k).verify(&ks, common::JOB).unwrap();
    assert_eq!(b.statements[0].lock.as_deref(), Some(RESULTS));
}
