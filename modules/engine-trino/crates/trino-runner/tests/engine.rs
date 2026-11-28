//! engine-trino's engine driven by engine-runner against the fake
//! orchestrator (decision 0011 step 11's check), with a signing core and a
//! scripted Trino: the contract suite, a job's statements in order, a
//! refused bundle, a shape that does not fit, a transient error, the
//! resume of a repeated attempt after its old query is ended, a cancel,
//! and a source's previous secret.

mod common;

use std::time::Duration;

use common::{bundle, write, FakeCore, FakeTrino, Script};
use engine_port::Resume;
use engine_runner::contract::{run_suite, FakeOrchestrator, Seed};
use engine_runner::{Ended, Engine, NoGuard, Runner};
use orchestration_model::RunState;
use serde_json::{json, Value};
use trino_runner::{parameters, Credential, TrinoEngine, TrinoUser};

type Tested = Runner<TrinoEngine<FakeCore, FakeTrino>, FakeOrchestrator, NoGuard>;

fn setup(payload: &Value, credentials: Vec<Credential>) -> (Tested, FakeOrchestrator, FakeTrino) {
    setup_with(FakeCore::new(payload.clone(), credentials))
}

fn setup_with(core: FakeCore) -> (Tested, FakeOrchestrator, FakeTrino) {
    let trino = FakeTrino::default();
    let engine =
        TrinoEngine::new(core, trino.clone(), "484", 2).polling_every(Duration::from_millis(10));
    let fake = FakeOrchestrator::default();
    let runner =
        Runner::new(engine, fake.clone(), NoGuard, "r").renewing_every(Duration::from_millis(50));
    (runner, fake, trino)
}

fn params() -> Value {
    parameters("loans", "2026-10-31")
}

async fn drive(runner: &Tested) -> Ended {
    runner.register().await.unwrap();
    let task = runner
        .claim_once()
        .await
        .unwrap()
        .expect("a run is claimed");
    tokio::time::timeout(Duration::from_secs(10), task)
        .await
        .unwrap()
        .unwrap()
}

#[tokio::test]
async fn the_contract_suite_passes_with_the_fake_orchestrator() {
    let fake = FakeOrchestrator::default();
    run_suite(fake.clone(), &fake).await;
}

#[tokio::test]
async fn a_jobs_statements_run_in_order_with_its_tokens() {
    let mut report = write("count_back", "SELECT 1");
    report["run_as"] = json!("report");
    let p = bundle(json!([
        write("a", "INSERT a"),
        write("b", "INSERT b"),
        report
    ]));
    let (runner, fake, trino) = setup(&p, vec![]);
    for (sql, n) in [("INSERT a", 2), ("INSERT b", 3), ("SELECT 1", 0)] {
        trino.on(sql, vec![Script::Count(n)]);
    }
    let run = fake.queue_run("evaluate", params());
    drive(&runner).await;
    let r = fake.run(&run).unwrap();
    assert_eq!(r.state, RunState::Succeeded);
    assert_eq!(r.counts["statements"], 3);
    assert_eq!(r.counts["rows_written"], 5);
    let seen = trino.seen.lock().unwrap();
    let order: Vec<(&str, &str)> = seen
        .runs
        .iter()
        .map(|r| (r.0.as_str(), r.1.as_str()))
        .collect();
    assert_eq!(
        order,
        [
            ("INSERT a", "job-token"),
            ("INSERT b", "job-token"),
            ("SELECT 1", "report-token")
        ]
    );
}

#[tokio::test]
async fn a_bundle_of_another_digest_runs_nothing() {
    let mut core = FakeCore::new(bundle(json!([write("a", "INSERT a")])), vec![]);
    core.digest = Some("0".repeat(64));
    let (runner, fake, trino) = setup_with(core);
    let run = fake.queue_run("evaluate", params());
    drive(&runner).await;
    let r = fake.run(&run).unwrap();
    assert_eq!(r.error_class.as_deref(), Some("bundle_digest_mismatch"));
    assert!(trino.seen.lock().unwrap().runs.is_empty());
}

#[tokio::test]
async fn the_core_is_asked_for_the_runs_job() {
    let core = FakeCore::new(bundle(json!([write("a", "INSERT a")])), vec![]);
    let asked = core.asked.clone();
    let (runner, fake, trino) = setup_with(core);
    trino.on("INSERT a", vec![Script::Count(1)]);
    let run = fake.queue_run("evaluate", params());
    drive(&runner).await;
    assert_eq!(fake.run(&run).unwrap().state, RunState::Succeeded);
    let asked = asked.lock().unwrap();
    assert_eq!(asked[0].0, run, "the core's job is the operator's run");
    assert_eq!(
        (
            asked[0].1.kind.as_str(),
            asked[0].1.dataset.as_deref(),
            asked[0].1.cut.as_deref()
        ),
        ("evaluate", Some("loans"), Some("2026-10-31"))
    );
}

#[tokio::test]
async fn an_answer_of_another_shape_fails_the_job() {
    let mut st = write("a", "INSERT a");
    st["expects"] = json!({"kind": "count", "update_types": ["DELETE"]});
    let (runner, fake, trino) = setup(&bundle(json!([st, write("b", "INSERT b")])), vec![]);
    trino.on("INSERT a", vec![Script::Count(1)]);
    let run = fake.queue_run("evaluate", params());
    drive(&runner).await;
    assert_eq!(
        fake.run(&run).unwrap().error_class.as_deref(),
        Some("unexpected_answer")
    );
    assert_eq!(trino.seen.lock().unwrap().runs.len(), 1, "nothing after it");
}

#[tokio::test]
async fn a_transient_error_is_an_interruption_and_a_retry_resumes_after_killing() {
    let p = bundle(json!([write("a", "INSERT a"), write("b", "INSERT b")]));
    let (runner, fake, trino) = setup(&p, vec![]);
    trino.on("INSERT a", vec![Script::Count(1)]);
    trino.on(
        "INSERT b",
        vec![Script::Fail("SERVER_STARTING_UP"), Script::Count(4)],
    );
    let first = fake.queue_run("evaluate", params());
    drive(&runner).await;
    let r = fake.run(&first).unwrap();
    assert_eq!(r.error_class.as_deref(), Some("trino_starting"));
    assert_eq!(r.progress_detail.as_deref(), Some("done:1"));
    let resume = Resume {
        previous_run: first,
        engine_ref: r.engine_ref.clone(),
        last_detail: r.progress_detail.clone(),
    };
    let second = fake.queue_attempt("evaluate", params(), 2, Some(resume));
    let task = runner.claim_once().await.unwrap().unwrap();
    task.await.unwrap();
    assert_eq!(fake.run(&second).unwrap().state, RunState::Succeeded);
    let seen = trino.seen.lock().unwrap();
    assert_eq!(
        seen.kills,
        [r.engine_ref.unwrap()],
        "the old query ended first"
    );
    let sqls: Vec<&str> = seen.runs.iter().map(|r| r.0.as_str()).collect();
    assert_eq!(
        sqls,
        ["INSERT a", "INSERT b", "INSERT b"],
        "statement a not again"
    );
}

#[tokio::test]
async fn a_cancel_kills_the_running_query() {
    let (runner, fake, trino) = setup(&bundle(json!([write("a", "INSERT a")])), vec![]);
    trino.on("INSERT a", vec![Script::Wait]);
    let run = fake.queue_run("evaluate", params());
    runner.register().await.unwrap();
    let task = runner.claim_once().await.unwrap().unwrap();
    tokio::time::sleep(Duration::from_millis(150)).await;
    fake.cancel(&run).await;
    let ended = tokio::time::timeout(Duration::from_secs(5), task)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        ended,
        Ended::Reported(orchestration_model::FinalState::Cancelled)
    );
    assert_eq!(trino.seen.lock().unwrap().kills, ["q_1"]);
}

#[tokio::test]
async fn a_source_statement_tries_the_previous_secret_once() {
    let mut st = write("extract", "INSERT x");
    st["reads_source"] = json!(true);
    let creds = vec![Credential {
        name: "src_crm_password".into(),
        current: "new".into(),
        previous: Some("old".into()),
    }];
    let (runner, fake, trino) = setup(&bundle(json!([st])), creds);
    trino.on(
        "INSERT x",
        vec![Script::Fail("JDBC_ERROR"), Script::Count(7)],
    );
    let run = fake.queue_run("evaluate", params());
    drive(&runner).await;
    assert_eq!(fake.run(&run).unwrap().state, RunState::Succeeded);
    let seen = trino.seen.lock().unwrap();
    let extras: Vec<&Vec<String>> = seen.runs.iter().map(|r| &r.2).collect();
    assert_eq!(extras, [&vec!["new".to_string()], &vec!["old".to_string()]]);
    assert!(!format!("{:?}", creds_debug()).contains("new"));
}

fn creds_debug() -> Credential {
    Credential {
        name: "n".into(),
        current: "new".into(),
        previous: None,
    }
}

#[test]
fn the_descriptor_is_trino_with_one_line_and_the_jobs_kinds() {
    let core = FakeCore::new(json!({}), vec![]);
    let d = TrinoEngine::new(core, FakeTrino::default(), "484", 2).descriptor();
    d.validate().unwrap();
    assert_eq!(
        (
            d.engine.as_str(),
            d.line.as_str(),
            d.engine_version.as_str()
        ),
        ("trino", "-", "484")
    );
    assert_eq!(d.kinds.len(), 15);
    for kind in ["prepare", "rule_test", "backfill", "tables"] {
        assert!(d.kinds.iter().any(|k| k.name == kind), "{kind}");
    }
}

#[test]
fn each_kind_compiled_for_trino_has_its_job_user() {
    let users: Vec<_> = [
        "evaluate",
        "backfill",
        "rule_test",
        "retract",
        "slas",
        "prepare",
        "extract",
        "tables",
        "map",
        "records",
        "test_rule",
        "query",
        "entity_key",
        "derive",
    ]
    .iter()
    .map(|k| TrinoUser::of_kind(k).map(TrinoUser::as_str))
    .collect();
    let rules = Some("rules");
    assert_eq!(
        users,
        [
            rules,
            rules,
            rules,
            rules,
            rules,
            Some("maint"),
            Some("extract"),
            Some("source"),
            Some("source"),
            Some("source"),
            Some("sample"),
            Some("console"),
            Some("lookup"),
            None
        ]
    );
}

#[tokio::test]
async fn a_prepare_runs_as_its_maintenance_user() {
    let mut payload = bundle(json!([{"id": "bronze", "sql": "CREATE TABLE b",
        "expects": {"kind": "done"}}]));
    payload["kind"] = json!("prepare");
    let (runner, fake, trino) = setup(&payload, vec![]);
    trino.on("CREATE TABLE b", vec![Script::Read(vec![], json!([]))]);
    let run = fake.queue_run("prepare", json!({"dataset": "loans"}));
    drive(&runner).await;
    assert_eq!(fake.run(&run).unwrap().state, RunState::Succeeded);
    assert_eq!(trino.seen.lock().unwrap().runs[0].1, "maint-token");
}

#[tokio::test]
async fn a_tables_listing_asks_the_core_for_its_source() {
    let mut payload = bundle(
        json!([{"id": "listing", "sql": "SELECT l", "reads_source": true,
        "expects": {"kind": "rows", "columns": [["table_name", "varchar"]], "max_rows": 10}}]),
    );
    payload["kind"] = json!("tables");
    let core = FakeCore::new(payload, vec![]);
    let asked = core.asked.clone();
    let (runner, fake, trino) = setup_with(core);
    trino.on(
        "SELECT l",
        vec![Script::Read(vec![("table_name", "varchar")], json!(["t"]))],
    );
    let run = fake.queue_run("tables", json!({"source": "crm"}));
    drive(&runner).await;
    assert_eq!(fake.run(&run).unwrap().state, RunState::Succeeded);
    assert_eq!(asked.lock().unwrap()[0].1.source.as_deref(), Some("crm"));
    assert_eq!(trino.seen.lock().unwrap().runs[0].1, "source-token");
}

#[tokio::test]
async fn a_kind_without_a_trino_user_runs_nothing() {
    let mut payload = bundle(json!([write("a", "INSERT a")]));
    payload["kind"] = json!("derive");
    let (runner, fake, trino) = setup(&payload, vec![]);
    let run = fake.queue_run("derive", params());
    drive(&runner).await;
    let r = fake.run(&run).unwrap();
    assert_eq!(r.error_class.as_deref(), Some("kind_not_on_trino"));
    assert!(trino.seen.lock().unwrap().runs.is_empty());
}
