//! A run takes and gives back a lock through its context, under its lease;
//! another running run waits for it; a run that ends frees it. A runner
//! sends its engine's slot pools.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use engine_port::{Claimed, Descriptor, Pools, RunKind, SlotPool, PORT};
use engine_runner::contract::FakeOrchestrator;
use engine_runner::{Engine, EngineError, NoGuard, Outcome, Refused, RunContext, RunRef, Runner};
use orchestration_model::{EngineName, Line};
use serde_json::json;
use tokio::sync::Notify;

const GOLD: &str = "lake.quality_gold.rule_results";

#[derive(Default)]
struct Locking {
    took: Mutex<Vec<(String, bool)>>,
    go: Notify,
}

struct LockingEngine(Arc<Locking>);

#[async_trait]
impl Engine for LockingEngine {
    fn descriptor(&self) -> Descriptor {
        Descriptor {
            engine: EngineName::new("engine-a").unwrap(),
            line: Line::new("-").unwrap(),
            engine_version: "1.0.0".into(),
            port: PORT,
            kinds: vec![RunKind {
                name: "job".into(),
                parameters: json!({"type": "object"}),
            }],
            concurrency: 2,
        }
    }

    async fn validate(&self, _: &Claimed) -> Result<(), Refused> {
        Ok(())
    }

    async fn run(&self, run: Claimed, ctx: RunContext) -> Outcome {
        let got = ctx.lock(GOLD).await.unwrap();
        self.0.took.lock().unwrap().push((run.run.clone(), got));
        if run.parameters["behave"] == "hold" {
            self.0.go.notified().await;
        }
        if run.parameters["behave"] == "give_back" {
            ctx.unlock(GOLD).await.unwrap();
        }
        Outcome::succeeded()
    }

    async fn cancel(&self, _: &RunRef) -> Result<(), EngineError> {
        Ok(())
    }
}

async fn took(engine: &Locking, n: usize) -> Vec<(String, bool)> {
    for _ in 0..200 {
        let took = engine.took.lock().unwrap().clone();
        if took.len() >= n {
            return took;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("the runs did not ask for the lock");
}

#[tokio::test]
async fn one_running_run_holds_a_lock_until_it_ends() {
    let fake = FakeOrchestrator::default();
    let engine = Arc::new(Locking::default());
    let runner = Runner::new(LockingEngine(engine.clone()), fake.clone(), NoGuard, "r-1");
    runner.register().await.unwrap();

    let a = fake.queue_run("job", json!({"behave": "hold"}));
    let first = runner.claim_once().await.unwrap().expect("a");
    assert_eq!(took(&engine, 1).await[0], (a.clone(), true));
    assert_eq!(fake.lock_holder(GOLD), Some(a.clone()));

    let b = fake.queue_run("job", json!({"behave": "done"}));
    runner
        .claim_once()
        .await
        .unwrap()
        .expect("b")
        .await
        .unwrap();
    assert_eq!(took(&engine, 2).await[1], (b, false), "a's: b waits");

    engine.go.notify_one();
    first.await.unwrap();
    assert_eq!(fake.lock_holder(GOLD), None, "a ended: free");
    let c = fake.queue_run("job", json!({"behave": "give_back"}));
    runner
        .claim_once()
        .await
        .unwrap()
        .expect("c")
        .await
        .unwrap();
    assert_eq!(took(&engine, 3).await[2], (c, true));
    assert_eq!(fake.lock_holder(GOLD), None, "given back");
}

#[tokio::test]
async fn a_runner_sends_its_engines_pools() {
    let fake = FakeOrchestrator::default();
    let runner = Runner::new(LockingEngine(Arc::default()), fake.clone(), NoGuard, "r-1");
    runner.register().await.unwrap();
    let rules = SlotPool {
        name: "rules".into(),
        size: 1,
        kinds: vec!["job".into()],
        fair_by: Some("dataset".into()),
    };
    let pools = Pools {
        pools: vec![rules.clone()],
    };
    runner.send_pools(&pools).await.unwrap();
    assert_eq!(fake.pools(), Some(pools));
    let other = Pools {
        pools: vec![SlotPool {
            kinds: vec!["not_its".into()],
            ..rules
        }],
    };
    assert!(runner.send_pools(&other).await.is_err());
}
