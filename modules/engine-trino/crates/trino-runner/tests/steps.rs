//! Amendment A1's step forms through the engine: `only_if_rows` skips a
//! DELETE after a count of 0, placeholders are filled from an earlier
//! answer and reported, a bad value or a broken declaration runs nothing
//! after it, and a repeated attempt resumes with what was known.

mod common;

use std::time::Duration;

use common::{bundle, key, keys, sign, FakeCore, FakeTrino, Script};
use engine_port::Resume;
use engine_runner::contract::FakeOrchestrator;
use engine_runner::{NoGuard, Runner};
use orchestration_model::RunState;
use serde_json::{json, Value};
use trino_runner::{parameters, BundleRefused, TrinoEngine};

fn count(id: &str, sql: &str) -> Value {
    json!({"id": id, "sql": sql, "expects": {"kind": "rows", "columns": [["n", "bigint"]], "max_rows": 1}})
}

fn delete(id: &str, sql: &str, after: &str) -> Value {
    json!({"id": id, "sql": sql, "expects": {"kind": "count", "update_types": ["DELETE"]},
        "only_if_rows": {"after": after}})
}

fn pin() -> Value {
    json!({"id": "pin", "sql": "SELECT snapshot_id FROM refs",
        "expects": {"kind": "rows", "columns": [["snapshot_id", "bigint"]], "max_rows": 1}})
}

fn snapshot() -> Value {
    json!({"name": "silver_snapshot", "type": "snapshot_id",
        "from": {"statement": "pin", "column": "snapshot_id"}})
}

fn with_placeholders(statements: Value, placeholders: Value) -> Value {
    let mut b = bundle(statements);
    b["placeholders"] = placeholders;
    b
}

type Tested = Runner<TrinoEngine<FakeCore, FakeTrino>, FakeOrchestrator, NoGuard>;

fn setup(payload: Value) -> (Tested, FakeOrchestrator, FakeTrino) {
    let trino = FakeTrino::default();
    let engine = TrinoEngine::new(FakeCore::new(payload, vec![]), trino.clone(), "484", 2)
        .polling_every(Duration::from_millis(10));
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

fn sqls(trino: &FakeTrino) -> Vec<String> {
    trino
        .seen
        .lock()
        .unwrap()
        .runs
        .iter()
        .map(|r| r.0.clone())
        .collect()
}

#[tokio::test]
async fn a_delete_after_a_zero_count_is_skipped_and_one_after_rows_runs() {
    let p = bundle(json!([
        count("old", "SELECT count old"),
        delete("drop_old", "DELETE old", "old"),
        count("older", "SELECT count older"),
        delete("drop_older", "DELETE older", "older")
    ]));
    let (runner, fake, trino) = setup(p);
    trino.on(
        "SELECT count old",
        vec![Script::Read(vec![("n", "bigint")], json!([0]))],
    );
    trino.on(
        "SELECT count older",
        vec![Script::Read(vec![("n", "bigint")], json!([5]))],
    );
    trino.on("DELETE older", vec![Script::Deleted(5)]);
    let run = fake.queue_run("evaluate", parameters("loans", "2026-10-31"));
    run_one(&runner).await;
    let r = fake.run(&run).unwrap();
    assert_eq!(r.state, RunState::Succeeded);
    assert_eq!(
        sqls(&trino),
        ["SELECT count old", "SELECT count older", "DELETE older"]
    );
    assert_eq!(
        (r.counts["statements_skipped"], r.counts["statements"]),
        (1, 3)
    );
    assert!(r
        .logs
        .iter()
        .any(|l| l.line == "statement drop_old skipped"));
}

#[tokio::test]
async fn a_placeholder_is_filled_from_an_earlier_answer_and_reported() {
    let read = json!({"id": "read", "sql": "INSERT INTO g SELECT * FROM s FOR VERSION AS OF {{silver_snapshot}}",
        "expects": {"kind": "count", "update_types": ["INSERT"]}});
    let p = with_placeholders(json!([pin(), read]), json!([snapshot()]));
    let (runner, fake, trino) = setup(p);
    trino.on(
        "SELECT snapshot_id FROM refs",
        vec![Script::Read(
            vec![("snapshot_id", "bigint")],
            json!([8_744_417_563_216_474_212_u64]),
        )],
    );
    trino.on(
        "INSERT INTO g SELECT * FROM s FOR VERSION AS OF 8744417563216474212",
        vec![Script::Count(2)],
    );
    let run = fake.queue_run("evaluate", parameters("loans", "2026-10-31"));
    run_one(&runner).await;
    let r = fake.run(&run).unwrap();
    assert_eq!(r.state, RunState::Succeeded, "{r:?}");
    assert_eq!(
        r.counts["snapshot_silver_snapshot"],
        8_744_417_563_216_474_212
    );
    assert!(r
        .progress_detail
        .unwrap()
        .contains("silver_snapshot=8744417563216474212"));
}

#[tokio::test]
async fn a_value_that_is_not_its_type_submits_nothing_after_it() {
    let read = json!({"id": "read", "sql": "INSERT x {{silver_snapshot}}",
        "expects": {"kind": "count", "update_types": ["INSERT"]}});
    let p = with_placeholders(json!([pin(), read]), json!([snapshot()]));
    let (runner, fake, trino) = setup(p);
    trino.on(
        "SELECT snapshot_id FROM refs",
        vec![Script::Read(
            vec![("snapshot_id", "bigint")],
            json!(["1; DROP"]),
        )],
    );
    let run = fake.queue_run("evaluate", parameters("loans", "2026-10-31"));
    run_one(&runner).await;
    assert_eq!(
        fake.run(&run).unwrap().error_class.as_deref(),
        Some("step_value_invalid")
    );
    assert_eq!(sqls(&trino).len(), 1, "nothing after the pin");
}

#[tokio::test]
async fn a_repeated_attempt_resumes_with_the_known_values() {
    let read = json!({"id": "read", "sql": "INSERT y {{silver_snapshot}}",
        "expects": {"kind": "count", "update_types": ["INSERT"]}});
    let p = with_placeholders(json!([pin(), read]), json!([snapshot()]));
    let (runner, fake, trino) = setup(p);
    trino.on("INSERT y 42", vec![Script::Count(1)]);
    let resume = Resume {
        previous_run: "run-0".into(),
        engine_ref: None,
        last_detail: Some("done:1|silver_snapshot=42".into()),
    };
    let run = fake.queue_attempt(
        "evaluate",
        parameters("loans", "2026-10-31"),
        2,
        Some(resume),
    );
    run_one(&runner).await;
    assert_eq!(fake.run(&run).unwrap().state, RunState::Succeeded);
    assert_eq!(sqls(&trino), ["INSERT y 42"], "the pin is not read again");
}

#[test]
fn broken_declarations_refuse_the_bundle_at_load() {
    let k = key();
    let ks = keys(&k, "k1");
    let job = common::JOB;
    let refused = |p: Value| sign(&p, &k).verify(&ks, job);
    // `after` names a later statement, then a statement that is not a count.
    let later = bundle(json!([
        delete("d", "DELETE a", "c"),
        count("c", "SELECT c")
    ]));
    let not_count = bundle(json!([
        common::write("w", "INSERT w"),
        delete("d", "DELETE a", "w")
    ]));
    let undeclared = bundle(json!([common::write("w", "INSERT {{x}}")]));
    let from_later = with_placeholders(
        json!([common::write("w", "INSERT {{silver_snapshot}}"), pin()]),
        json!([snapshot()]),
    );
    let text = with_placeholders(
        json!([pin()]),
        json!([{"name": "t", "type": "text",
        "from": {"statement": "pin", "column": "snapshot_id"}}]),
    );
    for (what, p) in [
        ("later", later),
        ("not a count", not_count),
        ("undeclared", undeclared),
        ("from a later statement", from_later),
        ("text", text),
    ] {
        assert_eq!(refused(p), Err(BundleRefused::Content), "{what}");
    }
    let unknown = {
        let mut p = bundle(json!([pin()]));
        p["statements"][0]["only_if_rows"] = json!({"after": "pin", "when": 1});
        p
    };
    assert_eq!(
        refused(unknown),
        Err(BundleRefused::Content),
        "unknown field"
    );
}
