//! A run's report to a fake core (core decision 0090, ruling 75): sent
//! once a job ends, done or failed for good, with rows only for `Rows`
//! reads and the placeholders it filled; not sent for a run cut by the
//! system; sent again while the core does not answer, and the run fails
//! if the core refuses it or never answers.

mod common;

use std::time::Duration;

use common::{bundle, write, FakeCore, FakeTrino, Script};
use engine_port::Resume;
use engine_runner::contract::FakeOrchestrator;
use engine_runner::{NoGuard, Runner};
use orchestration_model::RunState;
use serde_json::{json, Value};
use trino_runner::{
    parameters, CoreError, Delivery, ReportState, RunReport, StatementState, TrinoEngine,
};

type Tested = Runner<TrinoEngine<FakeCore, FakeTrino>, FakeOrchestrator, NoGuard>;

fn setup(core: FakeCore) -> (Tested, FakeOrchestrator, FakeTrino) {
    let trino = FakeTrino::default();
    let engine =
        TrinoEngine::new(core, trino.clone(), "484", 2).polling_every(Duration::from_millis(5));
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

/// An evaluate: a pin, a write at the pinned snapshot, a guarded delete
/// and the index's read-back.
fn evaluate() -> Value {
    let mut b = bundle(json!([
        {"id": "pin", "sql": "SELECT s", "expects": {"kind": "rows",
            "columns": [["snapshot_id", "bigint"]], "max_rows": 1}},
        write("insert", "INSERT {{silver}}"),
        {"id": "old", "sql": "SELECT n", "expects": {"kind": "rows",
            "columns": [["n", "bigint"]], "max_rows": 1}},
        {"id": "drop_old", "sql": "DELETE o", "only_if_rows": {"after": "old"},
            "expects": {"kind": "count", "update_types": ["DELETE"]}},
        {"id": "index_run_summary", "sql": "SELECT summary", "run_as": "report",
            "expects": {"kind": "rows", "columns": [["records", "bigint"], ["ms", "bigint"]],
            "max_rows": 1}}
    ]));
    b["placeholders"] = json!([{"name": "silver", "type": "snapshot_id",
        "from": {"statement": "pin", "column": "snapshot_id"}}]);
    b
}

fn script(trino: &FakeTrino) {
    trino.on(
        "SELECT s",
        vec![Script::Read(vec![("snapshot_id", "bigint")], json!([42]))],
    );
    trino.on("INSERT 42", vec![Script::Count(7)]);
    trino.on(
        "SELECT n",
        vec![Script::Read(vec![("n", "bigint")], json!([0]))],
    );
    trino.on(
        "SELECT summary",
        vec![Script::Read(
            vec![("records", "bigint"), ("ms", "bigint")],
            json!([7, 12]),
        )],
    );
}

fn reports(core: &FakeCore) -> Vec<(String, RunReport)> {
    core.reports.lock().unwrap().clone()
}

#[tokio::test]
async fn a_done_job_reports_its_statements_reads_and_snapshots() {
    let core = FakeCore::new(evaluate(), vec![]);
    let (runner, fake, trino) = setup(core.clone());
    script(&trino);
    let run = fake.queue_run("evaluate", parameters("loans", "2026-10-31"));
    run_one(&runner).await;
    assert_eq!(fake.run(&run).unwrap().state, RunState::Succeeded);
    let sent = reports(&core);
    assert_eq!(sent.len(), 1);
    let (to, report) = &sent[0];
    assert_eq!(to, &run);
    let wire = serde_json::to_value(report).unwrap();
    assert_eq!(
        wire,
        json!({
            "digest": common::sign(&{
                let mut p = evaluate();
                p["job_id"] = json!(run);
                p
            }, &core.key).digest,
            "state": "done",
            "statements": [
                {"id": "pin", "state": "done", "rows": [[42]]},
                {"id": "insert", "state": "done"},
                {"id": "old", "state": "done", "rows": [[0]]},
                {"id": "drop_old", "state": "skipped"},
                {"id": "index_run_summary", "state": "done", "rows": [[7, 12]]}
            ],
            "snapshots": [{"placeholder": "silver", "value": "42"}]
        })
    );
}

#[tokio::test]
async fn a_failed_job_reports_where_it_failed_and_its_class() {
    let core = FakeCore::new(evaluate(), vec![]);
    let (runner, fake, trino) = setup(core.clone());
    script(&trino);
    trino.on("INSERT 42", vec![Script::Fail("PERMISSION_DENIED")]);
    let run = fake.queue_run("evaluate", parameters("loans", "2026-10-31"));
    run_one(&runner).await;
    let r = fake.run(&run).unwrap();
    assert_eq!(r.error_class.as_deref(), Some("permission_denied"));
    let (_, report) = &reports(&core)[0];
    assert_eq!(report.state, ReportState::Failed);
    assert_eq!(report.error_class.as_deref(), Some("permission_denied"));
    let states: Vec<_> = report
        .statements
        .iter()
        .map(|s| (s.id.as_str(), s.state))
        .collect();
    assert_eq!(
        states,
        [
            ("pin", StatementState::Done),
            ("insert", StatementState::Failed)
        ]
    );
    assert!(report.snapshots.is_empty());
}

#[tokio::test]
async fn a_job_cut_by_the_system_is_not_reported() {
    let core = FakeCore::new(evaluate(), vec![]);
    let (runner, fake, trino) = setup(core.clone());
    script(&trino);
    trino.on("INSERT 42", vec![Script::Fail("unavailable")]);
    let run = fake.queue_run("evaluate", parameters("loans", "2026-10-31"));
    run_one(&runner).await;
    assert_eq!(
        fake.run(&run).unwrap().error_class.as_deref(),
        Some("trino_unavailable")
    );
    assert!(reports(&core).is_empty(), "the repeated attempt reports");
}

#[tokio::test]
async fn a_resumed_attempt_reports_the_earlier_statements_done_without_rows() {
    let core = FakeCore::new(evaluate(), vec![]);
    let (runner, fake, trino) = setup(core.clone());
    script(&trino);
    let resume = Resume {
        previous_run: "run-0".into(),
        engine_ref: None,
        last_detail: Some("done:2|silver=42".into()),
    };
    let params = parameters("loans", "2026-10-31");
    let run = fake.queue_attempt("evaluate", params, 2, Some(resume));
    run_one(&runner).await;
    assert_eq!(fake.run(&run).unwrap().state, RunState::Succeeded);
    let (_, report) = &reports(&core)[0];
    assert_eq!(report.statements[0].rows, None, "the pin is not read again");
    assert_eq!(report.statements[0].state, StatementState::Done);
    assert_eq!(
        report.statements[4].rows,
        Some(vec![vec![json!(7), json!(12)]])
    );
    assert_eq!(report.snapshots[0].value, "42");
}

#[tokio::test]
async fn a_report_is_sent_again_until_the_core_keeps_it() {
    let core = FakeCore::new(bundle(json!([write("a", "INSERT a")])), vec![]);
    *core.answers.lock().unwrap() = vec![
        Err(CoreError("report: HTTP 503".into())),
        Err(CoreError("report: HTTP 503".into())),
    ];
    let (runner, fake, trino) = setup(core.clone());
    trino.on("INSERT a", vec![Script::Count(1)]);
    let run = fake.queue_run("evaluate", parameters("loans", "2026-10-31"));
    run_one(&runner).await;
    assert_eq!(fake.run(&run).unwrap().state, RunState::Succeeded);
    let sent = reports(&core);
    assert_eq!(sent.len(), 3);
    assert!(sent.iter().all(|s| s.1 == sent[0].1), "the same report");
}

#[tokio::test]
async fn a_refused_report_fails_a_done_run_and_keeps_a_failed_runs_class() {
    let core = FakeCore::new(bundle(json!([write("a", "INSERT a")])), vec![]);
    *core.answers.lock().unwrap() = vec![
        Ok(Delivery::Refused("digest_mismatch".into())),
        Ok(Delivery::Refused("rows_not_allowed".into())),
    ];
    let (runner, fake, trino) = setup(core.clone());
    trino.on("INSERT a", vec![Script::Count(1)]);
    let done = fake.queue_run("evaluate", parameters("loans", "2026-10-31"));
    run_one(&runner).await;
    let r = fake.run(&done).unwrap();
    assert_eq!(r.error_class.as_deref(), Some("report_refused"));
    assert_eq!(r.counts["rows_written"], 1);
    trino.on("INSERT a", vec![Script::Fail("SYNTAX_ERROR")]);
    let failed = fake.queue_run("evaluate", parameters("loans", "2026-11-30"));
    run_one(&runner).await;
    let r = fake.run(&failed).unwrap();
    assert_eq!(r.error_class.as_deref(), Some("syntax_error"));
    assert_eq!(reports(&core).len(), 2, "a refusal is not sent again");
}

#[tokio::test]
async fn a_core_that_never_answers_fails_the_run_after_five_sends() {
    let core = FakeCore::new(bundle(json!([write("a", "INSERT a")])), vec![]);
    *core.answers.lock().unwrap() = (0..5)
        .map(|_| Err(CoreError("report: HTTP 503".into())))
        .collect();
    let (runner, fake, trino) = setup(core.clone());
    trino.on("INSERT a", vec![Script::Count(1)]);
    let run = fake.queue_run("evaluate", parameters("loans", "2026-10-31"));
    run_one(&runner).await;
    assert_eq!(
        fake.run(&run).unwrap().error_class.as_deref(),
        Some("report_not_delivered")
    );
    assert_eq!(reports(&core).len(), 5);
}
