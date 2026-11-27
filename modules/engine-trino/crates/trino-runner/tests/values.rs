//! Values reads (core decision 0090, rulings 79 and 80): their rows go to
//! the core's rows route before the job goes on, never to the report or a
//! log; a bundle that declares one beyond the core's bounds runs nothing;
//! a refusal fails the job, a core that does not answer cuts it so that a
//! repeated attempt reads them again. Synthetic values only.

mod common;

use std::time::Duration;

use common::{bundle, FakeCore, FakeTrino, Script};
use engine_runner::contract::FakeOrchestrator;
use engine_runner::{NoGuard, Runner};
use orchestration_model::RunState;
use serde_json::{json, Value};
use trino_runner::{CoreError, Delivery, StatementState, TrinoEngine, MAX_VALUES_ROWS};

type Tested = Runner<TrinoEngine<FakeCore, FakeTrino>, FakeOrchestrator, NoGuard>;

const NAME: &str = "Synthetic Person";

fn setup(core: FakeCore) -> (Tested, FakeOrchestrator, FakeTrino) {
    let trino = FakeTrino::default();
    trino.on(
        "SELECT page",
        vec![Script::Read(
            vec![("name", "varchar"), ("n", "bigint")],
            json!([NAME, 3]),
        )],
    );
    let engine =
        TrinoEngine::new(core, trino.clone(), "484", 1).polling_every(Duration::from_millis(5));
    let fake = FakeOrchestrator::default();
    let runner =
        Runner::new(engine, fake.clone(), NoGuard, "r").renewing_every(Duration::from_millis(50));
    (runner, fake, trino)
}

async fn run_one(runner: &Tested) {
    runner.register().await.unwrap();
    let task = runner.claim_once().await.unwrap().expect("claimed");
    tokio::time::timeout(Duration::from_secs(10), task)
        .await
        .unwrap()
        .unwrap();
}

fn page(kind: &str, max_rows: u64) -> Value {
    let mut b = bundle(
        json!([{"id": "page", "sql": "SELECT page", "expects": {"kind": "values",
        "columns": [["name", "varchar"], ["n", "bigint"]], "max_rows": max_rows}}]),
    );
    b["kind"] = json!(kind);
    b
}

fn records() -> Value {
    json!({"source": "crm"})
}

#[tokio::test]
async fn a_values_read_posts_its_rows_and_reports_none_of_them() {
    let core = FakeCore::new(page("records", 100), vec![]);
    let (runner, fake, trino) = setup(core.clone());
    let run = fake.queue_run("records", records());
    run_one(&runner).await;
    let r = fake.run(&run).unwrap();
    assert_eq!(r.state, RunState::Succeeded, "{r:?}");
    assert_eq!(
        core.rows.lock().unwrap().clone(),
        [("page".to_string(), vec![vec![json!(NAME), json!(3)]])]
    );
    assert_eq!(trino.seen.lock().unwrap().runs[0].1, "source-token");
    let reports = core.reports.lock().unwrap();
    let st = &reports[0].1.statements[0];
    assert_eq!((st.state, st.rows.is_none()), (StatementState::Done, true));
    let report = serde_json::to_string(&reports[0].1).unwrap();
    assert!(!report.contains(NAME), "no value in the report");
    assert!(
        r.logs.iter().all(|l| !l.line.contains(NAME)),
        "no value logged"
    );
}

#[tokio::test]
async fn a_test_rule_reads_its_samples_as_its_sample_user() {
    let core = FakeCore::new(page("test_rule", 10), vec![]);
    let (runner, fake, trino) = setup(core.clone());
    let run = fake.queue_run(
        "test_rule",
        json!({"dataset": "loans", "cut": "2026-10-31"}),
    );
    run_one(&runner).await;
    assert_eq!(fake.run(&run).unwrap().state, RunState::Succeeded);
    assert_eq!(trino.seen.lock().unwrap().runs[0].1, "sample-token");
    assert_eq!(core.rows.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn a_values_read_beyond_the_cores_bounds_runs_nothing() {
    let mut as_report = page("records", 10);
    as_report["statements"][0]["run_as"] = json!("report");
    for payload in [
        page("records", MAX_VALUES_ROWS + 1),
        page("records", 0),
        as_report,
    ] {
        let (runner, fake, trino) = setup(FakeCore::new(payload, vec![]));
        let run = fake.queue_run("records", records());
        run_one(&runner).await;
        let r = fake.run(&run).unwrap();
        assert_eq!(r.error_class.as_deref(), Some("bundle_invalid"));
        assert!(trino.seen.lock().unwrap().runs.is_empty());
    }
}

#[tokio::test]
async fn refused_rows_fail_the_job() {
    let core = FakeCore::new(page("records", 100), vec![]);
    *core.row_answers.lock().unwrap() = vec![Ok(Delivery::Refused("rows_not_allowed".into()))];
    let (runner, fake, _) = setup(core.clone());
    let run = fake.queue_run("records", records());
    run_one(&runner).await;
    let r = fake.run(&run).unwrap();
    assert_eq!(r.error_class.as_deref(), Some("rows_refused"));
    let reports = core.reports.lock().unwrap();
    assert_eq!(reports[0].1.statements[0].state, StatementState::Failed);
}

#[tokio::test]
async fn rows_the_core_never_takes_cut_the_job_before_its_progress() {
    let core = FakeCore::new(page("records", 100), vec![]);
    *core.row_answers.lock().unwrap() = (0..5)
        .map(|_| Err(CoreError("rows: HTTP 503".into())))
        .collect();
    let (runner, fake, _) = setup(core.clone());
    let run = fake.queue_run("records", records());
    run_one(&runner).await;
    let r = fake.run(&run).unwrap();
    assert_eq!(r.error_class.as_deref(), Some("core_unavailable"));
    assert_eq!(core.rows.lock().unwrap().len(), 5);
    assert!(core.reports.lock().unwrap().is_empty(), "a repeat reports");
    assert!(
        r.progress_detail.is_none_or(|d| d.starts_with("done:0")),
        "a repeated attempt reads the page again"
    );
}
