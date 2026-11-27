//! `HttpCoreSchedules` against a stand-in core over HTTP: the listed
//! schedules and a fire's steps (a setup step's key and fingerprint
//! included) are the answer; the fire is asked with its time in UTC; a
//! fire of a schedule the core no longer has runs nothing; a 401, a 503, a
//! malformed body or no token is no answer.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use chrono::{DateTime, Utc};
use orchestration_model::{ChainStep, Setup};
use orchestrator_server::{CoreSchedules, CoreToken, HttpCoreSchedules, MachineClient};
use serde_json::{json, Value};

#[derive(Clone, Default)]
struct Core {
    /// What both routes answer.
    answer: Arc<Mutex<(u16, String)>>,
    tokens: Arc<AtomicUsize>,
    asked: Arc<Mutex<Vec<(String, Value)>>>,
}

async fn token(State(core): State<Core>, body: String) -> Response {
    assert!(body.contains("audience=core"));
    let n = core.tokens.fetch_add(1, Ordering::SeqCst) + 1;
    Json(json!({"access_token": format!("t{n}"), "expires_in": 300})).into_response()
}

fn answered(core: &Core, headers: &HeaderMap) -> Response {
    let n = core.tokens.load(Ordering::SeqCst);
    if headers.get("authorization").and_then(|h| h.to_str().ok()) != Some(&format!("Bearer t{n}")) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    let (status, body) = core.answer.lock().unwrap().clone();
    let status = StatusCode::from_u16(status).unwrap();
    (status, [("content-type", "application/json")], body).into_response()
}

async fn listing(State(core): State<Core>, headers: HeaderMap) -> Response {
    answered(&core, &headers)
}

async fn fires(
    State(core): State<Core>,
    Path(id): Path<String>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> Response {
    core.asked.lock().unwrap().push((id, body));
    answered(&core, &headers)
}

async fn start() -> (Core, HttpCoreSchedules) {
    let core = Core::default();
    let app = Router::new()
        .route("/oauth/token", post(token))
        .route("/api/internal/quality/v1/schedules", get(listing))
        .route("/api/internal/quality/v1/schedules/{id}/fires", post(fires))
        .with_state(core.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await });
    let client = MachineClient {
        id: "orchestrator-machine".into(),
        secret: "s".into(),
    };
    let http = reqwest::Client::new();
    let token = Arc::new(CoreToken::new(
        http.clone(),
        &format!("http://{addr}"),
        client,
    ));
    let schedules = HttpCoreSchedules::new(http, &format!("http://{addr}/api/"), token);
    (core, schedules)
}

fn answer(core: &Core, status: u16, body: &str) {
    *core.answer.lock().unwrap() = (status, body.to_string());
}

fn at(s: &str) -> DateTime<Utc> {
    s.parse().unwrap()
}

#[tokio::test]
async fn lists_the_cores_schedules_cron_or_trigger() {
    let (core, schedules) = start().await;
    answer(
        &core,
        200,
        r#"{"schedules": [
        {"id": "0b9d6a52-2f5e-4c1e-9d8a-1f2e3d4c5b6a", "cron": "0 6 * * *",
         "timezone": "Europe/Madrid", "enabled": true, "fires": "cut", "dataset": "sales"},
        {"id": "s-2", "timezone": "UTC", "enabled": false,
         "trigger": {"start": "2026-11-25T06:00:00", "recurrence": {"kind": "daily", "every": 2}}}
    ]}"#,
    );
    let listed = schedules.schedules().await.unwrap();
    assert_eq!(listed.len(), 2);
    assert_eq!(listed[0].id, "0b9d6a52-2f5e-4c1e-9d8a-1f2e3d4c5b6a");
    assert_eq!(listed[0].spec.cron, "0 6 * * *");
    assert_eq!(listed[0].spec.timezone, "Europe/Madrid");
    assert!(listed[0].enabled && listed[0].spec.trigger.is_none());
    assert!(!listed[1].enabled);
    let next = listed[1].spec.next_after("s-2", at("2026-11-25T00:00:00Z"));
    assert_eq!(next.unwrap(), Some(at("2026-11-25T06:00:00Z")));
}

#[tokio::test]
async fn a_fire_answers_its_steps_with_the_setup_and_is_asked_in_utc() {
    let (core, schedules) = start().await;
    answer(
        &core,
        200,
        r#"{"steps": [
        {"kind": "prepare", "parameters": {"dataset": "sales"},
         "setup": {"key": "dataset:sales", "fingerprint": "9f86d081884c7d65"}},
        {"kind": "extract", "parameters": {"dataset": "sales", "cut": "2026-11-25"}},
        {"kind": "evaluate", "parameters": {"dataset": "sales", "cut": "2026-11-25"}}
    ]}"#,
    );
    let fired = schedules
        .fire("sched-1", at("2026-11-25T11:00:00.250Z"))
        .await
        .unwrap();
    assert_eq!(fired.steps.len(), 3);
    assert_eq!(
        fired.steps[0],
        ChainStep {
            kind: "prepare".into(),
            parameters: json!({"dataset": "sales"}),
            setup: Some(Setup {
                key: "dataset:sales".into(),
                fingerprint: "9f86d081884c7d65".into(),
            }),
        }
    );
    assert_eq!(fired.steps[2].setup, None);
    let asked = core.asked.lock().unwrap().clone();
    assert_eq!(
        asked,
        vec![("sched-1".into(), json!({"fire_at": "2026-11-25T11:00:00Z"}))]
    );
}

#[tokio::test]
async fn a_blocked_cut_or_a_gone_schedule_runs_nothing() {
    let (core, schedules) = start().await;
    answer(&core, 200, r#"{"steps": []}"#);
    let fired = schedules.fire("s", at("2026-11-25T11:00:00Z")).await;
    assert_eq!(fired.unwrap().steps, vec![]);
    answer(&core, 404, r#"{"error": "not_found"}"#);
    let fired = schedules.fire("s", at("2026-11-25T11:00:00Z")).await;
    assert_eq!(fired.unwrap().steps, vec![], "404: nothing runs");
}

#[tokio::test]
async fn what_cannot_be_read_is_no_answer() {
    let (core, schedules) = start().await;
    let fire_at = at("2026-11-25T11:00:00Z");
    for (status, body) in [
        (503, "{}"),
        (202, r#"{"steps": []}"#),
        (200, "not json"),
        (200, r#"{"steps": [{"kind": "extract", "extra": 1}]}"#),
    ] {
        answer(&core, status, body);
        assert_eq!(schedules.fire("s", fire_at).await, None, "{status} {body}");
    }
    for body in [
        r#"{"schedules": [{"id": "s", "enabled": true}]}"#,
        r#"{"items": []}"#,
    ] {
        answer(&core, 200, body);
        assert_eq!(schedules.schedules().await, None, "{body}");
    }
    assert_eq!(schedules.fire("a/b", fire_at).await, None, "not a core id");
}

#[tokio::test]
async fn a_refused_token_is_fetched_again() {
    let (core, schedules) = start().await;
    answer(&core, 200, r#"{"schedules": []}"#);
    assert_eq!(schedules.schedules().await, Some(vec![]));
    core.tokens.fetch_add(1, Ordering::SeqCst);
    let fire_at = at("2026-11-25T11:00:00Z");
    answer(&core, 200, r#"{"steps": []}"#);
    assert_eq!(schedules.fire("s", fire_at).await, None, "401: no answer");
    assert!(schedules.fire("s", fire_at).await.is_some(), "a new token");
}
