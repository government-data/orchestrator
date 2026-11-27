//! A job's stages in the runner's metrics (core decision 0090, ruling 70):
//! the bundle, the token, each statement by what it answers and the
//! report, by the run's kind and fixed stage names only.

mod common;

use std::time::Duration;

use common::{bundle, write, FakeCore, FakeTrino, Script};
use engine_runner::contract::FakeOrchestrator;
use engine_runner::{Engine, NoGuard, Runner, RunnerMetrics};
use orchestration_model::RunState;
use prometheus_client::registry::Registry;
use serde_json::json;
use trino_runner::{parameters, TrinoEngine};

#[tokio::test]
async fn a_jobs_stages_are_timed_by_kind_and_stage() {
    let payload = bundle(json!([
        write("a", "INSERT a"),
        {"id": "index_run_summary", "sql": "SELECT s", "run_as": "report",
            "expects": {"kind": "rows", "columns": [["n", "bigint"]], "max_rows": 1}}
    ]));
    let trino = FakeTrino::default();
    trino.on("INSERT a", vec![Script::Count(1)]);
    trino.on(
        "SELECT s",
        vec![Script::Read(vec![("n", "bigint")], json!([1]))],
    );
    let engine = TrinoEngine::new(FakeCore::new(payload, vec![]), trino, "484", 1);
    let mut registry = Registry::default();
    let metrics = RunnerMetrics::register(&engine.descriptor(), &mut registry);
    let fake = FakeOrchestrator::default();
    let runner = Runner::new(engine, fake.clone(), NoGuard, "r")
        .renewing_every(Duration::from_millis(50))
        .with_metrics(metrics);
    let run = fake.queue_run("evaluate", parameters("loans", "2026-10-31"));
    runner.register().await.unwrap();
    let task = runner.claim_once().await.unwrap().expect("claimed");
    tokio::time::timeout(Duration::from_secs(10), task)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(fake.run(&run).unwrap().state, RunState::Succeeded);
    let mut text = String::new();
    prometheus_client::encoding::text::encode(&mut text, &registry).unwrap();
    let counts: Vec<&str> = text
        .lines()
        .filter(|l| l.starts_with("orchestrator_runner_stage_seconds_count"))
        .collect();
    for stage in ["bundle", "token", "write", "read", "report"] {
        assert!(
            counts.iter().any(|l| l.contains("kind=\"evaluate\"")
                && l.contains(&format!("stage=\"{stage}\""))
                && l.ends_with(" 1")),
            "{stage}: {counts:?}"
        );
    }
    assert_eq!(counts.len(), 5, "no other stage: {counts:?}");
    assert!(!text.contains("loans") && !text.contains("index_run_summary"));
}
