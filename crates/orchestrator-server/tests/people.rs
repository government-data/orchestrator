//! The people's API with a fake core: a call without a person's token,
//! of a person who does not hold `orchestrator:ImportFlows`, or while the
//! core cannot say, is refused before anything is read or changed; a
//! person who holds it lists what a runner found, imports and asks a scan.

mod common;

use axum::http::{Method, StatusCode};
use common::harness;
use common::people::{call, people_app, refused_each_way, FakeCore};
use orchestrator_server::IMPORT_FLOWS;
use serde_json::{json, Value};

const FOUND: &str = "/api/v1/found?machine=machine-1&engine=fake-engine&line=-";

fn import_body() -> Value {
    json!({"machine": "machine-1", "engine": "fake-engine", "line": "-", "items": [
        {"path": "/srv/flows/close", "folder": "Sales", "name": "Close", "kind": "job",
         "timeout_secs": 600, "confirmed_flags": [{"kind": "code", "subject": "Script (#3)"}]}
    ]})
}

fn scan_body() -> Value {
    json!({"machine": "machine-1", "engine": "fake-engine", "line": "-"})
}

async fn setup() -> (common::Harness, axum::Router, FakeCore) {
    let h = harness().await;
    h.register().await;
    let report = json!({
        "artefacts": [{"path": "/srv/flows/close", "segments": ["close"], "sha256": "ab".repeat(32),
                       "size_bytes": 1, "flags": [{"kind": "code", "subject": "Script (#3)"}]}],
        "schedules": [{
            "source": {"kind": "cron", "file": "/etc/cron.d/flows", "line": 2},
            "user": "svc", "calendar": {"calendar": "cron", "value": "0 6 * * 1-5"},
            "timezone": "UTC", "artefact": "/srv/flows/close", "parameters": {"region": "north"}
        }]
    });
    let path = "/internal/engines/fake-engine/-/found";
    let (status, _) = h.call(Method::PUT, path, Some("machine-1"), report).await;
    assert_eq!(status, StatusCode::OK);
    let (app, core) = people_app(&h);
    (h, app, core)
}

#[tokio::test]
async fn an_unauthorised_call_is_refused_and_changes_nothing() {
    let (h, app, core) = setup().await;
    let calls = [
        (Method::GET, FOUND.to_string(), Value::Null),
        (Method::POST, "/api/v1/imports".into(), import_body()),
        (Method::POST, "/api/v1/scans".into(), scan_body()),
    ];
    refused_each_way(&app, &core, IMPORT_FLOWS, &calls).await;
    assert!(
        h.store.flows(None).await.unwrap().is_empty(),
        "nothing imported"
    );
    let runner = "machine-1/fake-engine/-";
    assert!(
        h.store.scan_requested(runner).await.unwrap().is_none(),
        "no scan"
    );
}

#[tokio::test]
async fn a_person_with_import_flows_chooses_imports_and_asks_a_scan() {
    let (h, app, core) = setup().await;
    core.holds(&[IMPORT_FLOWS]);

    let (status, found) = call(&app, Method::GET, FOUND, true, Value::Null).await;
    assert_eq!(status, StatusCode::OK);
    let artefact = &found["artefacts"][0];
    assert_eq!(artefact["path"], "/srv/flows/close");
    assert_eq!(artefact["flags"][0]["kind"], "code");
    let schedule = &artefact["schedules"][0];
    assert_eq!(schedule["calendar"]["value"], "0 6 * * 1-5");
    assert!(schedule.get("user").is_none() && schedule.get("parameters").is_none());

    let (status, answer) = call(&app, Method::POST, "/api/v1/imports", true, import_body()).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(answer["imported"][0]["path"], "/srv/flows/close");
    assert_eq!(answer["skipped"], json!([]));
    let (_, again) = call(&app, Method::POST, "/api/v1/imports", true, import_body()).await;
    assert_eq!(again["skipped"][0]["reason"], "already_imported");
    assert_eq!(h.store.flows(None).await.unwrap().len(), 1);

    let mut none = import_body();
    none["items"] = json!([]);
    let (status, _) = call(&app, Method::POST, "/api/v1/imports", true, none).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    let mut other = import_body();
    other["line"] = json!("2");
    let (status, _) = call(&app, Method::POST, "/api/v1/imports", true, other).await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    let (status, _) = call(&app, Method::POST, "/api/v1/scans", true, scan_body()).await;
    assert_eq!(status, StatusCode::ACCEPTED);
    let runner = "machine-1/fake-engine/-";
    assert!(h.store.scan_requested(runner).await.unwrap().is_some());
    let mut unknown = scan_body();
    unknown["machine"] = json!("machine-9");
    let (status, _) = call(&app, Method::POST, "/api/v1/scans", true, unknown).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}
