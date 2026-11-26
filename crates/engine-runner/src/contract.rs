//! The engine port's contract suite (decision 0011): the same cases, with a
//! fake engine, against any orchestrator that serves the port: this
//! repository's server in its CI, a fake orchestrator in an engine module's.
//! A [`Seed`] queues and cancels runs and reads their state on that side;
//! [`FakeOrchestrator`] is both, in memory.

mod fake;

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use engine_port::{
    Claim, Claimed, DeployFinish, DeployState, Descriptor, LogStream, RunKind, PORT,
};
use orchestration_model::{EngineName, Line};
use serde_json::{json, Value};
use tokio::sync::Notify;

pub use fake::{FakeDeploy, FakeOrchestrator, FakeRun};

use crate::{
    CallError, Ended, Engine, EngineError, NoGuard, Orchestrator, Outcome, Refused, RunContext,
    RunRef, Runner,
};

/// The orchestrator's side of a case.
#[async_trait]
pub trait Seed: Send + Sync {
    /// Queues a run of the fake engine with these parameters; its id.
    async fn queue(&self, parameters: Value) -> String;
    /// Cancels a run as a person would.
    async fn cancel(&self, run: &str);
    /// A run's state as stored (`succeeded`, `failed`, …).
    async fn state(&self, run: &str) -> String;
    /// Queues an import deploy for the fake engine on the runner's cluster;
    /// its id.
    async fn queue_deploy(&self) -> String;
    /// A deploy's state as stored (`queued`, `claimed`, `succeeded`, …).
    async fn deploy_state(&self, deploy: &str) -> String;
}

/// An engine that does what a run's `behave` parameter says: `succeed`,
/// `fail`, or `wait` until it is cancelled.
#[derive(Debug, Default, Clone)]
pub struct FakeEngine {
    cancelled: Arc<Notify>,
}

/// The fake engine's name, line and kind.
pub const FAKE_ENGINE: &str = "contract-engine";

#[async_trait]
impl Engine for FakeEngine {
    fn descriptor(&self) -> Descriptor {
        Descriptor {
            engine: EngineName::new(FAKE_ENGINE).unwrap_or_else(|_| unreachable!()),
            line: Line::new(Line::SINGLE).unwrap_or_else(|_| unreachable!()),
            engine_version: "1.0.0".into(),
            port: PORT,
            kinds: vec![RunKind {
                name: "job".into(),
                parameters: json!({"type": "object", "required": ["behave"]}),
            }],
            concurrency: 2,
        }
    }

    async fn validate(&self, run: &Claimed) -> Result<(), Refused> {
        match run.parameters.get("behave").and_then(Value::as_str) {
            Some("succeed" | "fail" | "wait") => Ok(()),
            _ => Err(Refused {
                class: "invalid_parameters".into(),
            }),
        }
    }

    async fn run(&self, run: Claimed, ctx: RunContext) -> Outcome {
        ctx.engine_ref(&format!("ref-{}", run.run));
        ctx.log(LogStream::Stdout, "started");
        ctx.progress(Some(0.5), Some("half"));
        match run.parameters["behave"].as_str() {
            Some("fail") => Outcome::failed("fake_failure"),
            Some("wait") => {
                self.cancelled.notified().await;
                Outcome::cancelled()
            }
            _ => Outcome {
                counts: BTreeMap::from([("items".into(), 3)]),
                ..Outcome::succeeded()
            },
        }
    }

    async fn cancel(&self, _: &RunRef) -> Result<(), EngineError> {
        self.cancelled.notify_waiters();
        Ok(())
    }
}

async fn claim_until<O: Orchestrator>(
    runner: &Runner<FakeEngine, O, NoGuard>,
) -> tokio::task::JoinHandle<Ended> {
    for _ in 0..100 {
        if let Ok(Some(task)) = runner.claim_once().await {
            return task;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("no run was handed out");
}

/// Every case, in order, against `orchestrator` with `seed`.
pub async fn run_suite<O: Orchestrator>(orchestrator: O, seed: &dyn Seed) {
    let runner = Runner::new(FakeEngine::default(), orchestrator, NoGuard, "contract")
        .renewing_every(Duration::from_millis(100));
    let registered = runner
        .register()
        .await
        .expect("the descriptor is registered");
    assert_eq!(registered.port, PORT);
    deploy_case(runner.orchestrator(), seed).await;

    let run = seed.queue(json!({"behave": "succeed"})).await;
    let ended = claim_until(&runner).await.await.expect("the run task ends");
    assert_eq!(
        ended,
        Ended::Reported(orchestration_model::FinalState::Succeeded)
    );
    assert_eq!(seed.state(&run).await, "succeeded", "a run that succeeds");

    let run = seed.queue(json!({"behave": "fail"})).await;
    claim_until(&runner).await.await.expect("the run task ends");
    assert_eq!(seed.state(&run).await, "failed", "a run that fails");

    let run = seed.queue(json!({"behave": "nonsense"})).await;
    claim_until(&runner).await.await.expect("the run task ends");
    assert_eq!(seed.state(&run).await, "failed", "a run the engine refuses");

    let run = seed.queue(json!({"behave": "wait"})).await;
    let task = claim_until(&runner).await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    seed.cancel(&run).await;
    let ended = tokio::time::timeout(Duration::from_secs(10), task)
        .await
        .expect("a cancel reaches the engine at the next renewal")
        .expect("the run task ends");
    assert_eq!(
        ended,
        Ended::Reported(orchestration_model::FinalState::Cancelled)
    );
    assert_eq!(
        seed.state(&run).await,
        "cancelled",
        "a run cancelled by a person"
    );
}

/// A deploy claimed, taken up again by the same runner, and finished once.
async fn deploy_case<O: Orchestrator>(orchestrator: &O, seed: &dyn Seed) {
    let d = FakeEngine::default().descriptor();
    let claim = Claim {
        runner: "contract".into(),
    };
    let id = seed.queue_deploy().await;
    let claimed = orchestrator
        .claim_deploy(&d, &claim)
        .await
        .expect("a deploy claim is answered")
        .expect("the deploy is handed out");
    assert_eq!(claimed.deploy, id);
    let again = orchestrator.claim_deploy(&d, &claim).await.unwrap();
    assert_eq!(
        again.map(|a| a.deploy).as_deref(),
        Some(id.as_str()),
        "the same runner takes its deploy up again"
    );
    assert_eq!(seed.deploy_state(&id).await, "claimed");
    let end = DeployFinish {
        state: DeployState::Succeeded,
        path: Some("/srv/managed/contract".into()),
        version: Some("v-1".into()),
        sha256: Some(claimed.source_sha256.clone()),
        external_disabled: false,
    };
    orchestrator
        .finish_deploy(&id, &end)
        .await
        .expect("a deploy finishes");
    orchestrator
        .finish_deploy(&id, &end)
        .await
        .expect("the same finish again changes nothing");
    let other = DeployFinish {
        external_disabled: true,
        ..end
    };
    assert_eq!(
        orchestrator.finish_deploy(&id, &other).await,
        Err(CallError::Lease),
        "another finish after it"
    );
    assert_eq!(seed.deploy_state(&id).await, "succeeded");
    let next = orchestrator.claim_deploy(&d, &claim).await.unwrap();
    assert!(next.is_none(), "a finished deploy is not handed out again");
}
