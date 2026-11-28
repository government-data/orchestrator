//! Text reads (core decision 0090, rulings 80 and 84): a console
//! statement's answer, whose columns the bundle does not know, goes to the
//! core's rows route with its column names and every value as text, never
//! to the report or a log; a text read beyond the core's bounds, or an
//! answer beyond its own, sends nothing. Synthetic values only.

mod common;

use std::time::Duration;

use common::{bundle, FakeCore, FakeTrino, Script};
use engine_runner::contract::FakeOrchestrator;
use engine_runner::{NoGuard, Runner};
use orchestration_model::RunState;
use serde_json::{json, Value};
use trino_runner::{as_text, StatementState, TrinoEngine, MAX_VALUES_ROWS};

type Tested = Runner<TrinoEngine<FakeCore, FakeTrino>, FakeOrchestrator, NoGuard>;

const REGION: &str = "Synthetic Region";

fn setup(core: FakeCore) -> (Tested, FakeOrchestrator, FakeTrino) {
    let trino = FakeTrino::default();
    trino.on(
        "SELECT q",
        vec![Script::Table(
            vec![("region", "varchar"), ("n", "bigint"), ("share", "double")],
            json!([[REGION, 3, 0.5], [null, 12, null]]),
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

fn console(max_rows: u64) -> Value {
    let mut b = bundle(json!([{"id": "result", "sql": "SELECT q",
        "expects": {"kind": "text", "max_rows": max_rows}}]));
    b["kind"] = json!("query");
    b["dataset"] = Value::Null;
    b["cut"] = Value::Null;
    b
}

#[tokio::test]
async fn a_console_statement_posts_its_columns_and_text_and_reports_none() {
    let core = FakeCore::new(console(201), vec![]);
    let (runner, fake, trino) = setup(core.clone());
    let run = fake.queue_run("query", json!({}));
    run_one(&runner).await;
    let r = fake.run(&run).unwrap();
    assert_eq!(r.state, RunState::Succeeded, "{r:?}");
    assert_eq!(trino.seen.lock().unwrap().runs[0].1, "console-token");
    assert_eq!(
        core.rows.lock().unwrap().clone(),
        [(
            "result".to_string(),
            vec![
                vec![json!(REGION), json!("3"), json!("0.5")],
                vec![Value::Null, json!("12"), Value::Null],
            ]
        )]
    );
    let names = ["region", "n", "share"].map(String::from).to_vec();
    assert_eq!(core.columns.lock().unwrap().clone(), [Some(names)]);
    let reports = core.reports.lock().unwrap();
    let st = &reports[0].1.statements[0];
    assert_eq!((st.state, st.rows.is_none()), (StatementState::Done, true));
    let report = serde_json::to_string(&reports[0].1).unwrap();
    assert!(!report.contains(REGION), "no value in the report");
    assert!(r.logs.iter().all(|l| !l.line.contains(REGION)), "no value logged");
}

#[tokio::test]
async fn a_text_read_beyond_the_cores_bounds_runs_nothing() {
    let mut as_report = console(10);
    as_report["statements"][0]["run_as"] = json!("report");
    let mut with_columns = console(10);
    with_columns["statements"][0]["expects"]["columns"] = json!([["region", "varchar"]]);
    for payload in [console(MAX_VALUES_ROWS + 1), console(0), as_report, with_columns] {
        let (runner, fake, trino) = setup(FakeCore::new(payload, vec![]));
        let run = fake.queue_run("query", json!({}));
        run_one(&runner).await;
        let r = fake.run(&run).unwrap();
        assert_eq!(r.error_class.as_deref(), Some("bundle_invalid"));
        assert!(trino.seen.lock().unwrap().runs.is_empty());
    }
}

#[tokio::test]
async fn more_rows_than_the_text_read_takes_send_nothing() {
    let core = FakeCore::new(console(1), vec![]);
    let (runner, fake, _) = setup(core.clone());
    let run = fake.queue_run("query", json!({}));
    run_one(&runner).await;
    let r = fake.run(&run).unwrap();
    assert_eq!(r.error_class.as_deref(), Some("unexpected_answer"));
    assert!(core.rows.lock().unwrap().is_empty());
    assert!(r.logs.iter().all(|l| !l.line.contains(REGION)), "no value logged");
}

#[test]
fn every_value_is_text_or_null() {
    let rows = vec![vec![
        json!("a"),
        json!(-7),
        json!(1.25),
        json!(true),
        Value::Null,
        json!([1, null]),
    ]];
    assert_eq!(
        as_text(&rows),
        [vec![
            json!("a"),
            json!("-7"),
            json!("1.25"),
            json!("true"),
            Value::Null,
            json!("[1,null]"),
        ]]
    );
}
