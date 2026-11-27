//! The report route as the adapter calls it (core decision 0090, ruling
//! 75), against a stand-in core that answers what the core answers: 204
//! when kept, 409 `already_reported` for a report it kept before (taken as
//! kept), 409 or 422 for a report it refuses, and 503 when it is down.

use std::sync::{Arc, Mutex};

use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::routing::post;
use axum::{Json, Router};
use engine_trino_service::{HttpCore, MachineClient};
use serde_json::{json, Value};
use trino_runner::{
    Core, Delivery, ReportState, RunReport, SnapshotFilled, StatementReport, StatementState,
};

const RUN: &str = "00000000-0000-4000-8000-00000000000a";

type Seen = Arc<Mutex<Vec<(String, String, Value)>>>;
type Answers = Arc<Mutex<Vec<(u16, Value)>>>;

async fn core(answers: Vec<(u16, Value)>) -> (HttpCore, Seen) {
    let seen: Seen = Arc::default();
    let answers = Arc::new(Mutex::new(answers));
    let state = (Arc::clone(&seen), answers);
    let app = Router::new()
        .route(
            "/api/internal/quality/v1/runs/{run}/report",
            post(
                |State((seen, answers)): State<(Seen, Answers)>,
                 Path(run): Path<String>,
                 headers: HeaderMap,
                 Json(body): Json<Value>| async move {
                    let auth = headers["authorization"].to_str().unwrap().to_string();
                    seen.lock().unwrap().push((run, auth, body));
                    let (code, answer) = answers.lock().unwrap().remove(0);
                    (StatusCode::from_u16(code).unwrap(), Json(answer))
                },
            ),
        )
        .route(
            "/api/internal/quality/v1/runs/{run}/rows",
            post(
                |State((seen, answers)): State<(Seen, Answers)>,
                 Path(run): Path<String>,
                 Json(body): Json<Value>| async move {
                    seen.lock().unwrap().push((run, "rows".into(), body));
                    let (code, answer) = answers.lock().unwrap().remove(0);
                    (StatusCode::from_u16(code).unwrap(), Json(answer))
                },
            ),
        )
        .route(
            "/oauth/token",
            post(|| async { Json(json!({"access_token": "machine", "expires_in": 300})) }),
        )
        .with_state(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await });
    let client = MachineClient {
        id: "engine-trino-machine".into(),
        secret: "s".into(),
    };
    let core = HttpCore::new(
        reqwest::Client::new(),
        &format!("http://{addr}/api/"),
        &format!("http://{addr}"),
        client,
    );
    (core, seen)
}

fn report() -> RunReport {
    RunReport {
        digest: "ab12".into(),
        state: ReportState::Done,
        statements: vec![
            StatementReport {
                id: "insert".into(),
                state: StatementState::Done,
                rows: None,
            },
            StatementReport {
                id: "index_run_summary".into(),
                state: StatementState::Done,
                rows: Some(vec![vec![json!(7), json!(12)]]),
            },
        ],
        snapshots: vec![SnapshotFilled {
            placeholder: "silver".into(),
            value: "42".into(),
        }],
        error_class: None,
    }
}

#[tokio::test]
async fn the_report_is_posted_with_the_machine_token_in_the_cores_shape() {
    let (c, seen) = core(vec![(204, Value::Null)]).await;
    assert_eq!(c.report(RUN, &report()).await.unwrap(), Delivery::Kept);
    let seen = seen.lock().unwrap();
    assert_eq!(seen[0].0, RUN);
    assert_eq!(seen[0].1, "Bearer machine");
    assert_eq!(
        seen[0].2,
        json!({"digest": "ab12", "state": "done",
            "statements": [{"id": "insert", "state": "done"},
                {"id": "index_run_summary", "state": "done", "rows": [[7, 12]]}],
            "snapshots": [{"placeholder": "silver", "value": "42"}]})
    );
}

#[tokio::test]
async fn already_reported_is_kept_a_refusal_is_final_and_a_down_core_an_error() {
    let (c, _) = core(vec![
        (409, json!({"error": "already_reported"})),
        (409, json!({"error": "digest_mismatch"})),
        (422, json!({"error": "rows_not_allowed"})),
        (422, json!({"error": "Some text: not a name"})),
        (503, json!({"error": "internal"})),
        (429, Value::Null),
    ])
    .await;
    let r = report();
    assert_eq!(c.report(RUN, &r).await.unwrap(), Delivery::Kept);
    let refused = |e: &str| Delivery::Refused(e.into());
    assert_eq!(c.report(RUN, &r).await.unwrap(), refused("digest_mismatch"));
    assert_eq!(
        c.report(RUN, &r).await.unwrap(),
        refused("rows_not_allowed")
    );
    assert_eq!(c.report(RUN, &r).await.unwrap(), refused("http_422"));
    assert!(c.report(RUN, &r).await.is_err(), "503: sent again");
    assert!(c.report(RUN, &r).await.is_err(), "429: sent again");
    assert!(c.report("../x", &r).await.is_err(), "not a run id");
}

#[tokio::test]
async fn rows_go_to_their_route_and_after_the_report_are_refused() {
    let (c, seen) = core(vec![
        (204, Value::Null),
        (409, json!({"error": "already_reported"})),
    ])
    .await;
    let rows = vec![vec![json!("Synthetic"), json!(1)]];
    assert_eq!(c.rows(RUN, "page", &rows).await.unwrap(), Delivery::Kept);
    assert_eq!(
        c.rows(RUN, "page", &rows).await.unwrap(),
        Delivery::Refused("already_reported".into())
    );
    let seen = seen.lock().unwrap();
    assert_eq!(
        seen[0].2,
        json!({"statement": "page", "rows": [["Synthetic", 1]]})
    );
}
