//! A runner's report of what it found: kept item by item, a schedule only
//! with its artefact, replaced by the next report, refused before the
//! runner registers.

mod common;

use axum::http::{Method, StatusCode};
use common::harness;
use serde_json::{json, Value};

const PATH: &str = "/internal/engines/fake-engine/-/found";
const RUNNER: &str = "machine-1/fake-engine/-";

fn artefact(path: &str, segments: &[&str]) -> Value {
    json!({
        "path": path, "segments": segments, "sha256": "ab".repeat(32), "size_bytes": 10,
        "flags": [{"kind": "code", "subject": "Script (#3)"}]
    })
}

fn schedule(artefact: &str, calendar: &str) -> Value {
    json!({
        "source": {"kind": "cron", "file": "/etc/cron.d/flows", "line": 2},
        "user": "svc", "calendar": {"calendar": "cron", "value": calendar},
        "timezone": "UTC", "artefact": artefact, "parameters": {"region": "north"}
    })
}

#[tokio::test]
async fn what_a_runner_found_is_kept_item_by_item_until_its_next_report() {
    let h = harness().await;
    let report = json!({
        "artefacts": [
            artefact("/srv/flows/sales/close", &["sales", "close"]),
            artefact("/srv/flows/loose", &["loose"]),
            artefact("relative/path", &["x"]),
        ],
        "schedules": [
            schedule("/srv/flows/sales/close", "0 6 * * 1-5"),
            schedule("/srv/flows/nowhere", "0 6 * * *"),
            schedule("/srv/flows/loose", "0 0 6 * * *"),
        ]
    });
    let (status, _) = h
        .call(Method::PUT, PATH, Some("machine-1"), report.clone())
        .await;
    assert_eq!(status, StatusCode::NOT_FOUND, "a runner registers first");
    let (status, _) = h.call(Method::PUT, PATH, None, report.clone()).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);

    h.register().await;
    let (status, answer) = h.call(Method::PUT, PATH, Some("machine-1"), report).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(answer, json!({"kept": 2, "relinked": 0}));
    let found = h.store.found_of(RUNNER).await.unwrap();
    let paths: Vec<&str> = found.iter().map(|f| f.path.as_str()).collect();
    assert_eq!(paths, ["/srv/flows/loose", "/srv/flows/sales/close"]);
    assert_eq!(found[1].flags.len(), 1);
    let close = h
        .store
        .found_schedules_of(RUNNER, "/srv/flows/sales/close")
        .await
        .unwrap();
    assert_eq!(close.len(), 1);
    assert_eq!(close[0].parameters["region"], "north");
    let loose = h
        .store
        .found_schedules_of(RUNNER, "/srv/flows/loose")
        .await
        .unwrap();
    assert!(loose.is_empty(), "a 6-field calendar is dropped");

    let next = json!({"artefacts": [artefact("/srv/flows/loose", &["loose"])]});
    let (_, answer) = h.call(Method::PUT, PATH, Some("machine-1"), next).await;
    assert_eq!(answer["kept"], 1);
    assert_eq!(h.store.found_of(RUNNER).await.unwrap().len(), 1);
    assert!(h
        .store
        .found_schedules_of(RUNNER, "/srv/flows/sales/close")
        .await
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn a_report_over_the_limit_is_refused_whole() {
    let h = harness().await;
    h.register().await;
    let many: Vec<Value> = (0..=engine_port::MAX_FOUND)
        .map(|i| artefact(&format!("/srv/{i}"), &["x"]))
        .collect();
    let (status, _) = h
        .call(
            Method::PUT,
            PATH,
            Some("machine-1"),
            json!({"artefacts": many}),
        )
        .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
}
