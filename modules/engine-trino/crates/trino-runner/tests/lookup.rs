//! An entity lookup (core decision 0090, rulings 81, 85 and 102): its one
//! `text` placeholder is filled with the run's document, asked once from
//! the core and written as one quoted literal into the lookup's values
//! read; the document never reaches a log, the progress detail, the
//! report, the counts or a `Debug`. A bundle that puts a `text`
//! placeholder anywhere else runs nothing. Synthetic values only.

mod common;

use std::time::Duration;

use common::{bundle, FakeCore, FakeTrino, Script};
use engine_runner::contract::FakeOrchestrator;
use engine_runner::{NoGuard, Runner};
use orchestration_model::RunState;
use serde_json::{json, Value};
use trino_runner::{Document, TrinoEngine};

type Tested = Runner<TrinoEngine<FakeCore, FakeTrino>, FakeOrchestrator, NoGuard>;

/// A synthetic document with a quote, which the literal doubles.
const DOCUMENT: &str = "90'7319";
const LOOKUP: &str = "SELECT e FROM k WHERE h = hmac({{document}})";
const FILLED: &str = "SELECT e FROM k WHERE h = hmac('90''7319')";

fn setup(core: FakeCore) -> (Tested, FakeOrchestrator, FakeTrino) {
    let trino = FakeTrino::default();
    trino.on(
        FILLED,
        vec![Script::Read(
            vec![
                ("dataset", "varchar"),
                ("cut", "varchar"),
                ("icd", "double"),
                ("band", "varchar"),
            ],
            json!(["loans", "2026-10-31", 0.75, "b"]),
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

fn entity_read(sql: &str, expects: Value) -> Value {
    json!({"id": "entity", "sql": sql, "expects": expects})
}

fn values() -> Value {
    json!({"kind": "values", "columns": [["dataset", "varchar"], ["cut", "varchar"],
        ["icd", "double"], ["band", "varchar"]], "max_rows": 1})
}

fn lookup(kind: &str, statements: Value, placeholders: Value) -> Value {
    let mut b = bundle(statements);
    b["kind"] = json!(kind);
    b["dataset"] = Value::Null;
    b["cut"] = Value::Null;
    b["placeholders"] = placeholders;
    b
}

fn document() -> Value {
    json!([{"name": "document", "type": "text"}])
}

fn core(payload: Value, document: Option<&str>) -> FakeCore {
    let core = FakeCore::new(payload, vec![]);
    *core.document.lock().unwrap() = document.map(str::to_string);
    core
}

#[tokio::test]
async fn the_document_is_asked_once_and_written_only_as_a_quoted_literal() {
    let payload = lookup(
        "entity_key",
        json!([entity_read(LOOKUP, values())]),
        document(),
    );
    let core = core(payload, Some(DOCUMENT));
    let (runner, fake, trino) = setup(core.clone());
    let run = fake.queue_run("entity_key", json!({}));
    run_one(&runner).await;
    let r = fake.run(&run).unwrap();
    assert_eq!(r.state, RunState::Succeeded, "{r:?}");
    let seen = trino.seen.lock().unwrap();
    assert_eq!(
        (seen.runs[0].0.as_str(), seen.runs[0].1.as_str()),
        (FILLED, "lookup-token")
    );
    assert_eq!(*core.document_asks.lock().unwrap(), 1);
    assert_eq!(core.rows.lock().unwrap()[0].0, "entity");
    let report = serde_json::to_string(&core.reports.lock().unwrap()[0].1).unwrap();
    for shown in [report, format!("{r:?}")] {
        assert!(!shown.contains("7319"), "no document in {shown}");
    }
    assert!(
        r.logs.iter().all(|l| !l.line.contains("7319")),
        "none logged"
    );
    assert!(r.progress_detail.is_none_or(|d| !d.contains("7319")));
}

#[test]
fn a_document_shows_nothing_of_itself_in_debug() {
    assert_eq!(
        format!("{:?}", Document::new(DOCUMENT.into())),
        "Document(..)"
    );
}

#[tokio::test]
async fn a_document_given_already_or_not_one_the_core_takes_runs_nothing() {
    let long = "1".repeat(65);
    for (given, class) in [
        (None, "document_gone"),
        (Some("12\u{7}34"), "document_invalid"),
        (Some(long.as_str()), "document_invalid"),
        (Some(""), "document_invalid"),
    ] {
        let payload = lookup(
            "entity_key",
            json!([entity_read(LOOKUP, values())]),
            document(),
        );
        let (runner, fake, trino) = setup(core(payload, given));
        let run = fake.queue_run("entity_key", json!({}));
        run_one(&runner).await;
        let r = fake.run(&run).unwrap();
        assert_eq!(r.error_class.as_deref(), Some(class), "{given:?}");
        assert!(trino.seen.lock().unwrap().runs.is_empty());
    }
}

#[tokio::test]
async fn a_text_placeholder_outside_a_lookups_values_read_runs_nothing() {
    let rows = json!({"kind": "rows", "columns": [["n", "bigint"]], "max_rows": 1});
    let unsourced = json!([{"name": "document", "type": "bigint"}]);
    for (what, kind, statements, placeholders) in [
        (
            "another kind",
            "records",
            json!([entity_read(LOOKUP, values())]),
            document(),
        ),
        (
            "a rows read",
            "entity_key",
            json!([entity_read(LOOKUP, rows)]),
            document(),
        ),
        (
            "no source",
            "entity_key",
            json!([entity_read(LOOKUP, values())]),
            unsourced,
        ),
    ] {
        let payload = lookup(kind, statements, placeholders);
        let core = core(payload, Some(DOCUMENT));
        let (runner, fake, trino) = setup(core.clone());
        let run = fake.queue_run(kind, json!({}));
        run_one(&runner).await;
        let r = fake.run(&run).unwrap();
        assert_eq!(r.error_class.as_deref(), Some("bundle_invalid"), "{what}");
        assert!(trino.seen.lock().unwrap().runs.is_empty(), "{what}");
        assert_eq!(*core.document_asks.lock().unwrap(), 0, "{what}");
    }
}
